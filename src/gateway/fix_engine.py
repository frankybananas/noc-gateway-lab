"""FIX 4.4 engine: consumes ticks from Redis Streams and serves them to FIX clients.

Acts as a simplified FIX *acceptor* (the exchange side):
  * Listens on a local TCP port; handles Logon (35=A), Heartbeat (35=0),
    TestRequest (35=1) and Logout (35=5) with proper sequence numbers.
  * Consumes `md.ticks` via a Redis *consumer group* (XREADGROUP/XACK), which
    gives at-least-once delivery and a measurable pending/lag count — the same
    semantics a NOC monitors on production message buses.
  * Translates ticks to Market Data Incremental Refresh (35=X) messages.
  * Tracks the tick-level end-to-end SLA: ingest timestamp -> FIX send.

This is a session/application-layer *simulation* for observability practice,
not a certified FIX implementation.
"""

import asyncio
import time

import redis.asyncio as aioredis
import simplefix
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from gateway import config
from gateway.common.chaos import ChaosState, consume_one_shot, poll_chaos_flags
from gateway.common.telemetry import parse_xinfo_group

log = config.setup_logging("fix-engine")

# --- Prometheus telemetry ---
SESSION_STATE = Gauge(
    "fix_session_state", "FIX session state (0=disconnected, 1=connected, 2=logged_on)"
)
MSGS_SENT = Counter("fix_msgs_sent_total", "FIX messages sent", ["msg_type"])
MSGS_RECV = Counter("fix_msgs_received_total", "FIX messages received", ["msg_type"])
CONSUMER_LAG = Gauge(
    "fix_consumer_lag",
    "Entries in md.ticks not yet read by the fix-engine group. NaN when Redis "
    "cannot compute it (MAXLEN trimming makes the exact count undefined).",
)
CONSUMER_TIME_LAG = Gauge(
    "fix_consumer_time_lag_seconds",
    "Age gap between the newest stream entry and the newest entry delivered to "
    "the fix-engine group, derived from stream ID timestamps. Survives MAXLEN "
    "trimming (added after observing that the entry-count lag could read null "
    "under load when MAXLEN trimming made the exact count uncomputable).",
)
E2E_LATENCY = Histogram(
    "fix_end_to_end_latency_seconds",
    "Tick SLA: WS ingest timestamp to FIX message send",
    # Upper buckets extended after a chaos drill showed p99 clamped at the
    # then-largest bucket (2.5s) during a latency fault, hiding how bad the
    # tail really was. Buckets must exceed the worst latency you need to SEE.
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)
SESSION_KILLS = Counter("fix_session_kills_total", "Chaos-injected FIX session terminations")

MDUPDATE_ACTION = {"trade": "0", "quote": "0"}  # 279=0 (New)


class FixSession:
    """A single logged-on FIX counterparty connection."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.parser = simplefix.FixParser()
        self.out_seq = 1
        self.logged_on = False
        self.peer = writer.get_extra_info("peername")

    def build(self, msg_type: str) -> simplefix.FixMessage:
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")
        msg.append_pair(35, msg_type)
        msg.append_pair(49, config.FIX_SENDER_COMP_ID)
        msg.append_pair(56, config.FIX_TARGET_COMP_ID)
        msg.append_pair(34, self.out_seq)
        msg.append_utc_timestamp(52)
        return msg

    async def send(self, msg: simplefix.FixMessage) -> None:
        self.writer.write(msg.encode())
        await self.writer.drain()
        self.out_seq += 1
        MSGS_SENT.labels(msg_type=msg.get(35).decode()).inc()

    async def close(self) -> None:
        self.logged_on = False
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


class FixEngine:
    def __init__(self) -> None:
        self.session: FixSession | None = None
        self.chaos = ChaosState()
        self.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

    # --- session layer ---
    async def _set_session_state(self, value: int) -> None:
        SESSION_STATE.set(value)
        try:
            await self.redis.setex(config.TRAFFIC_SESSION_STATE_KEY, 60, value)
        except Exception as exc:
            log.warning("failed to publish session state: %s", exc)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = FixSession(reader, writer)
        if self.session is not None:
            log.warning("rejecting second concurrent session from %s", session.peer)
            await session.close()
            return
        self.session = session
        await self._set_session_state(1)
        log.info("TCP connection from %s", session.peer)
        try:
            await self._session_loop(session)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            log.warning("counterparty %s dropped", session.peer)
        finally:
            await session.close()
            self.session = None
            await self._set_session_state(0)
            log.info("session with %s closed", session.peer)

    async def _session_loop(self, session: FixSession) -> None:
        while True:
            data = await session.reader.read(4096)
            if not data:
                return
            session.parser.append_buffer(data)
            while (msg := session.parser.get_message()) is not None:
                await self._dispatch(session, msg)

    async def _dispatch(self, session: FixSession, msg: simplefix.FixMessage) -> None:
        msg_type = msg.get(35).decode()
        MSGS_RECV.labels(msg_type=msg_type).inc()
        if msg_type == "A":  # Logon -> respond with Logon
            reply = session.build("A")
            reply.append_pair(98, 0)  # EncryptMethod: none
            reply.append_pair(108, config.FIX_HEARTBEAT_INTERVAL)
            await session.send(reply)
            session.logged_on = True
            await self._set_session_state(2)
            log.info("logon complete with %s", session.peer)
        elif msg_type == "1":  # TestRequest -> Heartbeat echoing 112
            reply = session.build("0")
            test_req_id = msg.get(112)
            if test_req_id:
                reply.append_pair(112, test_req_id.decode())
            await session.send(reply)
        elif msg_type == "0":  # Heartbeat from client
            pass
        elif msg_type == "5":  # Logout -> confirm and close
            await session.send(session.build("5"))
            await session.close()

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(config.FIX_HEARTBEAT_INTERVAL)
            s = self.session
            if s and s.logged_on:
                try:
                    await s.send(s.build("0"))
                except Exception as exc:
                    log.warning("heartbeat send failed: %s", exc)

    # --- application layer ---
    def tick_to_fix(self, session: FixSession, fields: dict) -> simplefix.FixMessage:
        """Build a Market Data Incremental Refresh (35=X) from a normalized tick."""
        msg = session.build("X")
        msg.append_pair(268, 1)  # NoMDEntries
        if fields.get("type") == "trade":
            msg.append_pair(279, "0")  # MDUpdateAction: New
            msg.append_pair(269, "2")  # MDEntryType: Trade
            msg.append_pair(55, fields.get("symbol", ""))
            msg.append_pair(270, fields.get("price", ""))
            msg.append_pair(271, fields.get("size", ""))
        else:  # quote -> publish bid side (one entry keeps the demo simple)
            msg.append_pair(279, "0")
            msg.append_pair(269, "0")  # MDEntryType: Bid
            msg.append_pair(55, fields.get("symbol", ""))
            msg.append_pair(270, fields.get("bid", ""))
            msg.append_pair(271, fields.get("bid_size", ""))
        return msg

    async def consume_loop(self) -> None:
        """XREADGROUP consumer: at-least-once tick delivery to the FIX session."""
        r = self.redis
        try:
            await r.xgroup_create(config.STREAM_KEY, config.CONSUMER_GROUP, id="$", mkstream=True)
            log.info("created consumer group %s", config.CONSUMER_GROUP)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        while True:
            entries = await r.xreadgroup(
                config.CONSUMER_GROUP,
                "fix-1",
                {config.STREAM_KEY: ">"},
                count=100,
                block=1000,
            )
            if not entries:
                continue
            for _stream, records in entries:
                for record_id, fields in records:
                    await self._process_tick(record_id, fields)

    async def _process_tick(self, record_id: str, fields: dict) -> None:
        await self.chaos.apply_latency()
        session = self.session
        if session and session.logged_on:
            if self.chaos.should_drop():
                pass  # drop silently: the SLA panels expose the gap
            else:
                try:
                    await session.send(self.tick_to_fix(session, fields))
                    ingest_ns = int(fields.get("ingest_ts_ns", "0"))
                    if ingest_ns:
                        E2E_LATENCY.observe(time.time_ns() / 1e9 - ingest_ns / 1e9)
                except Exception as exc:
                    log.warning("FIX send failed: %s", exc)
        await self.redis.xack(config.STREAM_KEY, config.CONSUMER_GROUP, record_id)

    # --- ops loops ---
    @staticmethod
    def _id_ms(stream_id: str) -> int:
        """Millisecond timestamp embedded in a Redis stream ID ('<ms>-<seq>')."""
        return int(stream_id.split("-")[0])

    async def lag_sampler(self) -> None:
        while True:
            try:
                # If the data path is not expected to be active, the lag/staleness
                # gauges become NaN so the dashboard shows grey "MARKET CLOSED"
                # instead of a misleading green 0 on weekends or when no FIX
                # client is logged on.
                expected = int(await self.redis.get(config.TRAFFIC_EXPECTED_KEY) or 0)
                if expected != 1:
                    CONSUMER_LAG.set(float("nan"))
                    CONSUMER_TIME_LAG.set(float("nan"))
                    continue

                stream_info = await self.redis.xinfo_stream(config.STREAM_KEY)
                last_generated = stream_info.get("last-generated-id")
                for group in await self.redis.xinfo_groups(config.STREAM_KEY):
                    if group.get("name") != config.CONSUMER_GROUP:
                        continue
                    # Entry-count lag: preserve null. Redis returns null when
                    # trimming makes the count uncomputable; coercing that to 0
                    # made the sensor "fail toward healthy" during a chaos drill.
                    lag = group.get("lag")
                    CONSUMER_LAG.set(float("nan") if lag is None else lag)
                    # Time lag from ID timestamps: trim-proof, and reads 0 when
                    # idle (both IDs stop advancing together outside market hours).
                    last_delivered = group.get("last-delivered-id")
                    if last_generated and last_delivered:
                        delta_ms = self._id_ms(last_generated) - self._id_ms(last_delivered)
                        CONSUMER_TIME_LAG.set(max(0, delta_ms) / 1000.0)
            except Exception as exc:
                log.debug("lag sample failed: %s", exc)
            await asyncio.sleep(5)

    async def chaos_watch(self) -> None:
        while True:
            if self.chaos.fix_kill_requested and self.session:
                await consume_one_shot(self.redis, config.CHAOS_FIX_KILL_KEY)
                self.chaos.fix_kill_requested = False
                SESSION_KILLS.inc()
                log.warning("CHAOS: killing FIX session with %s", self.session.peer)
                await self.session.close()
            await asyncio.sleep(1)

    async def run(self) -> None:
        start_http_server(config.FIX_METRICS_PORT, addr=config.BIND_HOST)
        server = await asyncio.start_server(
            self.handle_client, config.BIND_HOST, config.FIX_TCP_PORT
        )
        log.info(
            "FIX acceptor on %s:%d, metrics on :%d",
            config.BIND_HOST, config.FIX_TCP_PORT, config.FIX_METRICS_PORT,
        )
        asyncio.create_task(poll_chaos_flags(self.redis, self.chaos))
        async with server:
            await asyncio.gather(
                server.serve_forever(),
                self.consume_loop(),
                self.heartbeat_loop(),
                self.lag_sampler(),
                self.chaos_watch(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(FixEngine().run())
    except KeyboardInterrupt:
        log.info("shutting down")
