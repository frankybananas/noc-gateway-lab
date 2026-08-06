"""Ingress gateway: Alpaca IEX WebSocket -> Redis Streams, with Prometheus telemetry.

Responsibilities:
  * Connect/authenticate/subscribe to the Alpaca IEX market data WebSocket.
  * Normalize trades & quotes and publish them to the Redis Stream `md.ticks`
    (capped with MAXLEN so memory is bounded).
  * Expose NOC-grade telemetry: message rates, processing latency, reconnects,
    connection state, feed staleness, and stream depth.
  * Honor chaos faults (added latency, message drops, forced disconnects).

Design notes:
  * asyncio: network-bound workload; blocking I/O would drop packets.
  * time.perf_counter(): monotonic, immune to NTP clock steps; time.time()
    would produce negative latency spikes after clock sync.
  * time.time_ns() is used for the *ingest timestamp* stamped onto each tick,
    because downstream consumers live in other processes where a wall-clock
    epoch is the only shared timebase.
  * Exponential backoff with jitter on reconnect avoids hammering the feed
    during an outage (thundering herd).
"""

import asyncio
import json
import random
import time

import redis.asyncio as aioredis
import websockets
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from gateway import config
from gateway.common.chaos import ChaosState, consume_one_shot, poll_chaos_flags

log = config.setup_logging("ingress")

# --- Prometheus telemetry ---
MSGS_RECEIVED = Counter(
    "gateway_msgs_received_total",
    "Total market data messages received from the upstream feed",
    ["symbol", "msg_type"],
)
PROCESSING_LATENCY = Histogram(
    "gateway_processing_latency_seconds",
    "Time to parse, normalize and publish one WS frame",
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)
WS_RECONNECTS = Counter(
    "gateway_ws_reconnects_total", "WebSocket reconnect attempts", ["reason"]
)
CONNECTION_STATE = Gauge(
    "gateway_connection_state",
    "Feed connection state (0=down, 1=connected, 2=authenticated, 3=subscribed)",
)
LAST_MSG_TIMESTAMP = Gauge(
    "gateway_feed_last_message_timestamp_seconds",
    "Unix time of the last data message; alert when time() - this > 60s in market hours",
)
STREAM_PUBLISHED = Counter(
    "gateway_stream_publish_total", "Ticks published to the Redis stream"
)
STREAM_ERRORS = Counter(
    "gateway_stream_publish_errors_total", "Failed Redis stream publishes"
)
STREAM_DEPTH = Gauge(
    "gateway_stream_depth", "Current XLEN of the md.ticks stream (backpressure signal)"
)
CHAOS_DROPPED = Counter(
    "gateway_chaos_dropped_total", "Messages intentionally dropped by chaos injection"
)


async def stream_depth_sampler(r: aioredis.Redis) -> None:
    """Sample XLEN periodically; stream depth growth == downstream backpressure."""
    while True:
        try:
            STREAM_DEPTH.set(await r.xlen(config.STREAM_KEY))
        except Exception as exc:
            log.warning("stream depth sample failed: %s", exc)
        await asyncio.sleep(5)


def normalize(msg: dict) -> dict | None:
    """Flatten an Alpaca trade/quote into stream fields. Returns None for non-data msgs."""
    msg_type = msg.get("T")
    if msg_type == "t":  # trade
        return {
            "type": "trade",
            "symbol": msg.get("S", ""),
            "price": str(msg.get("p", "")),
            "size": str(msg.get("s", "")),
            "src_ts": msg.get("t", ""),
            "ingest_ts_ns": str(time.time_ns()),
        }
    if msg_type == "q":  # quote
        return {
            "type": "quote",
            "symbol": msg.get("S", ""),
            "bid": str(msg.get("bp", "")),
            "bid_size": str(msg.get("bs", "")),
            "ask": str(msg.get("ap", "")),
            "ask_size": str(msg.get("as", "")),
            "src_ts": msg.get("t", ""),
            "ingest_ts_ns": str(time.time_ns()),
        }
    return None


async def run_session(r: aioredis.Redis, chaos: ChaosState) -> None:
    """One full WebSocket session: connect, auth, subscribe, ingest until closed."""
    api_key, secret_key = config.alpaca_credentials()
    log.info("connecting to %s", config.ALPACA_WS_URL)

    async with websockets.connect(config.ALPACA_WS_URL, ping_interval=20, ping_timeout=20) as ws:
        conn_msg = await ws.recv()
        CONNECTION_STATE.set(1)
        log.info("connection status: %s", conn_msg)

        await ws.send(json.dumps({"action": "auth", "key": api_key, "secret": secret_key}))
        auth_msg = json.loads(await ws.recv())
        if any(m.get("T") == "error" for m in auth_msg):
            raise RuntimeError(f"authentication failed: {auth_msg}")
        CONNECTION_STATE.set(2)
        log.info("authenticated")

        await ws.send(
            json.dumps(
                {"action": "subscribe", "trades": config.SYMBOLS, "quotes": config.SYMBOLS}
            )
        )
        sub_msg = await ws.recv()
        CONNECTION_STATE.set(3)
        log.info("subscribed: %s", sub_msg)
        log.info("gateway active; metrics on %s:%d", config.BIND_HOST, config.INGRESS_METRICS_PORT)

        while True:
            if chaos.ws_disconnect_requested:
                await consume_one_shot(r, config.CHAOS_WS_DISCONNECT_KEY)
                chaos.ws_disconnect_requested = False
                log.warning("CHAOS: forcing WebSocket disconnect")
                await ws.close(code=1000, reason="chaos-injected disconnect")
                return

            raw = await ws.recv()
            start = time.perf_counter()
            await chaos.apply_latency()

            for msg in json.loads(raw):
                msg_type = msg.get("T", "unknown")
                symbol = msg.get("S", "unknown")
                MSGS_RECEIVED.labels(symbol=symbol, msg_type=msg_type).inc()

                fields = normalize(msg)
                if fields is None:
                    continue
                LAST_MSG_TIMESTAMP.set(time.time())

                if chaos.should_drop():
                    CHAOS_DROPPED.inc()
                    continue

                try:
                    await r.xadd(
                        config.STREAM_KEY,
                        fields,
                        maxlen=config.STREAM_MAXLEN,
                        approximate=True,
                    )
                    STREAM_PUBLISHED.inc()
                except Exception as exc:
                    STREAM_ERRORS.inc()
                    log.error("stream publish failed: %s", exc)

            PROCESSING_LATENCY.observe(time.perf_counter() - start)


async def main() -> None:
    start_http_server(config.INGRESS_METRICS_PORT, addr=config.BIND_HOST)
    # Initialize to process start so the staleness panel reads "seconds since
    # last data OR startup" instead of "seconds since the Unix epoch" (a
    # never-set gauge defaults to 0 and produces a 56-year staleness reading).
    LAST_MSG_TIMESTAMP.set(time.time())
    r = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    chaos = ChaosState()
    asyncio.create_task(poll_chaos_flags(r, chaos))
    asyncio.create_task(stream_depth_sampler(r))

    backoff = 1.0
    while True:
        try:
            await run_session(r, chaos)
            reason = "server_close"
        except websockets.exceptions.ConnectionClosed:
            reason = "connection_closed"
        except Exception as exc:
            reason = type(exc).__name__
            log.error("session error: %s", exc)
        CONNECTION_STATE.set(0)
        WS_RECONNECTS.labels(reason=reason).inc()
        sleep_for = backoff + random.uniform(0, backoff / 2)
        log.info("reconnecting in %.1fs (reason=%s)", sleep_for, reason)
        await asyncio.sleep(sleep_for)
        backoff = min(backoff * 2, 60.0)
        if reason == "server_close":
            backoff = 1.0  # clean chaos-induced close: recover fast


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down")
