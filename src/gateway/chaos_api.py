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


async def annotate(text: str, tags: list[str]) -> None:
    """Post an annotation to Grafana; disabled (warn once) if no token configured."""
    url, token = config.grafana_config()
    if not token:
        log.warning("GRAFANA_API_TOKEN not set; skipping annotation: %s", text)
        return
    payload = {"time": int(time.time() * 1000), "tags": ["chaos"] + tags, "text": text}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{url}/api/annotations",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
        log.info("grafana annotation posted: %s", text)
    except Exception as exc:
        log.error("grafana annotation failed: %s", exc)


async def count_active() -> int:
    keys = [config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
            config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY]
    values = await r.mget(keys)
    n = sum(1 for v in values if v is not None)
    ACTIVE_FAULTS.set(n)
    return n


@app.post("/chaos/latency")
async def inject_latency(fault: LatencyFault):
    await r.set(config.CHAOS_LATENCY_KEY, fault.ms, ex=fault.duration_s)
    await annotate(f"CHAOS: +{fault.ms}ms latency injected for {fault.duration_s}s", ["latency"])
    return {"status": "injected", "fault": "latency", "ms": fault.ms,
            "expires_in_s": fault.duration_s, "active_faults": await count_active()}


@app.post("/chaos/drop")
async def inject_drop(fault: DropFault):
    await r.set(config.CHAOS_DROP_KEY, fault.percent, ex=fault.duration_s)
    await annotate(f"CHAOS: {fault.percent}% message drop for {fault.duration_s}s", ["drop"])
    return {"status": "injected", "fault": "drop", "percent": fault.percent,
            "expires_in_s": fault.duration_s, "active_faults": await count_active()}


@app.post("/chaos/disconnect-ws")
async def disconnect_ws():
    await r.set(config.CHAOS_WS_DISCONNECT_KEY, "1", ex=30)
    await annotate("CHAOS: forced upstream WebSocket disconnect", ["disconnect"])
    return {"status": "injected", "fault": "ws_disconnect",
            "active_faults": await count_active()}


@app.post("/chaos/kill-fix-session")
async def kill_fix_session():
    await r.set(config.CHAOS_FIX_KILL_KEY, "1", ex=30)
    await annotate("CHAOS: FIX session terminated", ["fix-session"])
    return {"status": "injected", "fault": "fix_session_kill",
            "active_faults": await count_active()}


@app.post("/chaos/reset")
async def reset():
    deleted = await r.delete(
        config.CHAOS_LATENCY_KEY, config.CHAOS_DROP_KEY,
        config.CHAOS_WS_DISCONNECT_KEY, config.CHAOS_FIX_KILL_KEY,
    )
    await annotate("CHAOS: all faults cleared", ["reset"])
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.BIND_HOST, port=config.CHAOS_PORT, log_level="info")
