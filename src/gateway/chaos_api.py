"""Chaos controller: API-driven fault injection with automatic Grafana annotation.

FastAPI service (localhost only) that sets fault flags in Redis with TTLs.
The ingress gateway and FIX engine poll those flags and apply the faults.
Every injection posts an annotation to Grafana so dashboards self-document
the experiment timeline (fault start -> latency spike -> recovery).

Endpoints:
    POST /chaos/latency      {"ms": 200, "duration_s": 60}
    POST /chaos/drop         {"percent": 10, "duration_s": 60}
    POST /chaos/disconnect-ws            (one-shot)
    POST /chaos/kill-fix-session         (one-shot)
    POST /chaos/reset
    GET  /chaos/status
"""

import asyncio
import logging
import time

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest
from pydantic import BaseModel, Field

from gateway import config

log = config.setup_logging("chaos-api")

app = FastAPI(title="NOC-Gateway Chaos Controller", version="1.0")

ACTIVE_FAULTS = Gauge("chaos_active_faults", "Number of currently active chaos faults")

# Background closer tasks for each active fault, so regions shade the
# correct duration even if the caller never calls /chaos/reset.
_ANNOTATION_CLOSERS: dict[str, asyncio.Task] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


@app.get("/metrics")
async def metrics() -> Response:
    """Serve Prometheus metrics as a plain route (a mounted sub-app would
    307-redirect /metrics -> /metrics/, doubling every scrape request)."""
    await count_active()  # refresh the gauge on each scrape
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


r = aioredis.from_url(config.REDIS_URL, decode_responses=True)


class LatencyFault(BaseModel):
    ms: float = Field(gt=0, le=10_000, description="Added latency per message (ms)")
    duration_s: int = Field(default=60, gt=0, le=3600)


class DropFault(BaseModel):
    percent: float = Field(gt=0, le=100, description="Percent of messages to drop")
    duration_s: int = Field(default=60, gt=0, le=3600)


async def create_annotation(text: str, tags: list[str], start_ms: int) -> int | None:
    """Post a single-point Grafana annotation; returns the annotation id."""
    url, token = config.grafana_config()
    if not token:
        log.warning("GRAFANA_API_TOKEN not set; skipping annotation: %s", text)
        return None
    payload = {"time": start_ms, "tags": ["chaos"] + tags, "text": text}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{url}/api/annotations",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            aid = resp.json().get("id")
        log.info("grafana annotation posted: %s (id=%s)", text, aid)
        return aid
    except Exception as exc:
        log.error("grafana annotation failed: %s", exc)
        return None


async def close_annotation(annotation_id: int, end_ms: int) -> None:
    """PATCH the annotation's timeEnd so Grafana shades a fault region."""
    url, token = config.grafana_config()
    if not token or not annotation_id:
        return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.patch(
                f"{url}/api/annotations/{annotation_id}",
                json={"timeEnd": end_ms},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
        log.info("grafana annotation %s closed at %s", annotation_id, end_ms)
    except Exception as exc:
        log.error("failed to close grafana annotation %s: %s", annotation_id, exc)


async def _store_annotation_id(fault_key: str, aid: int) -> None:
    try:
        await r.hset(config.CHAOS_ANNOTATION_HASH, fault_key, str(aid))
        await r.expire(config.CHAOS_ANNOTATION_HASH, 86_400)
    except Exception as exc:
        log.warning("failed to store annotation id for %s: %s", fault_key, exc)


async def _take_annotation_id(fault_key: str) -> int | None:
    try:
        raw = await r.hget(config.CHAOS_ANNOTATION_HASH, fault_key)
        if raw:
            await r.hdel(config.CHAOS_ANNOTATION_HASH, fault_key)
            return int(raw)
    except Exception as exc:
        log.warning("failed to retrieve annotation id for %s: %s", fault_key, exc)
    return None


async def _close_after(fault_key: str, aid: int, end_ms: int) -> None:
    """Sleep until the planned end, then close the annotation unless reset
    already removed it from the hash."""
    delay = max(0, (end_ms - _now_ms()) / 1000.0)
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return
    stored = await _take_annotation_id(fault_key)
    if stored == aid:
        await close_annotation(aid, end_ms)


def _start_ms_and_end(duration_s: int) -> tuple[int, int]:
    start = _now_ms()
    return start, start + (duration_s * 1000)


async def _inject_fault(
    fault_key: str,
    value: str,
    text: str,
    tags: list[str],
    duration_s: int,
) -> None:
    """Set the Redis key and create/schedule the Grafana region annotation."""
    await r.set(fault_key, value, ex=duration_s)
    start_ms, end_ms = _start_ms_and_end(duration_s)
    aid = await create_annotation(text, tags, start_ms)
    if aid:
        await _store_annotation_id(fault_key, aid)
        _ANNOTATION_CLOSERS[fault_key] = asyncio.create_task(
            _close_after(fault_key, aid, end_ms)
        )


async def count_active() -> int:
    keys = [config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
            config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY]
    values = await r.mget(keys)
    n = sum(1 for v in values if v is not None)
    ACTIVE_FAULTS.set(n)
    return n


@app.post("/chaos/latency")
async def inject_latency(fault: LatencyFault):
    await _inject_fault(
        config.CHAOS_LATENCY_KEY,
        str(fault.ms),
        f"CHAOS: latency +{fault.ms}ms for {fault.duration_s}s",
        ["latency", f"{fault.ms}ms"],
        fault.duration_s,
    )
    return {"status": "injected", "fault": "latency", "ms": fault.ms,
            "expires_in_s": fault.duration_s, "active_faults": await count_active()}


@app.post("/chaos/drop")
async def inject_drop(fault: DropFault):
    await _inject_fault(
        config.CHAOS_DROP_KEY,
        str(fault.percent),
        f"CHAOS: drop {fault.percent}% for {fault.duration_s}s",
        ["drop", f"{fault.percent}%"],
        fault.duration_s,
    )
    return {"status": "injected", "fault": "drop", "percent": fault.percent,
            "expires_in_s": fault.duration_s, "active_faults": await count_active()}


@app.post("/chaos/disconnect-ws")
async def disconnect_ws():
    duration_s = 1  # one-shot, but the region is 1s to show the action
    await _inject_fault(
        config.CHAOS_WS_DISCONNECT_KEY,
        "1",
        "CHAOS: upstream WebSocket disconnect",
        ["ws_disconnect"],
        duration_s,
    )
    return {"status": "injected", "fault": "ws_disconnect",
            "active_faults": await count_active()}


@app.post("/chaos/kill-fix-session")
async def kill_fix_session():
    duration_s = 1  # one-shot region
    await _inject_fault(
        config.CHAOS_FIX_KILL_KEY,
        "1",
        "CHAOS: FIX session kill",
        ["fix_session_kill"],
        duration_s,
    )
    return {"status": "injected", "fault": "fix_session_kill",
            "active_faults": await count_active()}


@app.post("/chaos/reset")
async def reset():
    deleted = await r.delete(
        config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
        config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY,
    )
    now_ms = _now_ms()
    for fault_key in [
        config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
        config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY,
    ]:
        task = _ANNOTATION_CLOSERS.pop(fault_key, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        aid = await _take_annotation_id(fault_key)
        if aid:
            await close_annotation(aid, now_ms)

    await create_annotation("CHAOS: all faults cleared", ["reset"], now_ms)
    return {"status": "reset", "faults_cleared": deleted,
            "active_faults": await count_active()}


@app.get("/chaos/status")
async def status():
    latency, drop, ws_disc, fix_kill = await r.mget(
        config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
        config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY,
    )
    ttls = {}
    for name, key in [("latency", config.CHAOS_LATENCY_KEY), ("drop", config.CHAOS_DROP_KEY)]:
        ttl = await r.ttl(key)
        if ttl > 0:
            ttls[name] = ttl
    await count_active()
    return {
        "latency_ms": float(latency) if latency else 0,
        "drop_percent": float(drop) if drop else 0,
        "ws_disconnect_pending": ws_disc is not None,
        "fix_kill_pending": fix_kill is not None,
        "ttl_seconds": ttls,
    }


async def startup_cleanup() -> None:
    """On startup, close any open chaos-tagged annotation older than the
    maximum fault duration plus a safety margin, so a controller restart
    does not leave infinitely open fault regions on the dashboard.
    """
    url, token = config.grafana_config()
    if not token:
        return
    max_age_ms = config.CHAOS_MAX_DURATION_S * 1000
    # Wait an extra 5 min before considering an annotation stale.
    stale_threshold_ms = _now_ms() - max_age_ms - (5 * 60 * 1000)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{url}/api/annotations",
                params={"tags": "chaos", "limit": 100, "from": 0, "to": _now_ms()},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            for ann in resp.json():
                tags = ann.get("tags") or []
                if "reset" in tags:
                    continue
                ann_time = ann.get("time", 0)
                time_end = ann.get("timeEnd", ann_time)
                # Open (point) annotations only; skip already closed regions.
                if time_end <= ann_time and ann_time <= stale_threshold_ms:
                    await close_annotation(ann["id"], _now_ms())
    except Exception as exc:
        log.warning("startup annotation cleanup failed: %s", exc)


@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(startup_cleanup())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.BIND_HOST, port=config.CHAOS_PORT, log_level="info")
