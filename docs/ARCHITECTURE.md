# Architecture

## Data Flow

```
                        ┌─────────────────────────── VPS (Ubuntu) ────────────────────────────┐
                        │                                                                      │
 Alpaca IEX feed        │  ┌──────────────┐    XADD     ┌───────────────┐   XREADGROUP        │
 (WebSocket, JSON) ─────┼─▶│   ingress    │────────────▶│ Redis Streams │─────────────┐       │
 wss://stream.data...   │  │ (asyncio)    │             │   md.ticks    │             ▼       │
                        │  │ :9101 metrics│             │ (Docker,      │      ┌────────────┐ │
                        │  └──────────────┘             │  localhost)   │      │ fix-engine │ │
                        │         ▲                     └───────────────┘      │ FIX 4.4    │ │
                        │         │ chaos flags (Redis keys w/ TTL)   ▲        │ :5001 TCP  │ │
                        │  ┌──────┴───────┐                           │        │ :9102 mtrcs│ │
                        │  │  chaos-api   │───────────────────────────┘        └─────┬──────┘ │
                        │  │ FastAPI :9103│──── annotations ──▶ Grafana              │ 35=X   │
                        │  └──────────────┘                       ▲                  ▼        │
                        │                                         │           ┌────────────┐  │
                        │  Prometheus (existing Docker stack) ────┘           │ FIX client │  │
                        │  scrapes :9101 :9102 :9103                          │  (demo)    │  │
                        │                                                     └────────────┘  │
                        └──────────────────────────────────────────────────────────────────────┘
```

## Components

| Component   | Runtime                | Port (localhost only)   | Role |
|-------------|------------------------|-------------------------|------|
| ingress     | venv + systemd         | 9101 (metrics)          | WS ingest, normalize, publish to stream |
| Redis 7     | Docker Compose         | 6379                    | Message bus (Streams, AOF persistence) |
| fix-engine  | venv + systemd         | 5001 (FIX), 9102 (metrics) | Consumer group -> FIX 4.4 acceptor |
| chaos-api   | venv + systemd         | 9103 (API + metrics)    | Fault injection + Grafana annotations |
| Prometheus / Grafana | existing Docker stack | 9090 / 3000     | Scrape, visualize, alert, annotate |

## Design Decisions (the "why")

### Why asyncio, not threads?
The workload is network-bound: one WS connection in, Redis out, TCP session out.
Blocking I/O anywhere means messages queue up in kernel buffers and eventually
drop. `asyncio` multiplexes all of it on one event loop without thread overhead
or locking bugs.

### Why `time.perf_counter()` for latency, `time.time_ns()` for timestamps?
`time.time()` follows the wall clock; when NTP steps the clock, latency graphs
show impossible negative spikes. `perf_counter()` is monotonic — correct for
*durations within one process*. But the end-to-end SLA spans two processes
(ingress -> fix-engine), and monotonic clocks aren't comparable across
processes, so ticks carry a wall-clock `time_ns()` ingest timestamp instead.
Knowing when each is appropriate is a key telemetry design decision.

### Why Redis Streams instead of a plain pub/sub or a direct call?
1. **Backpressure isolation:** if the FIX consumer stalls, the ingress keeps
   ingesting; the stream absorbs the burst. Depth (`XLEN`) becomes a measurable
   backpressure signal — exactly what a NOC watches on production buses.
2. **Consumer groups** give at-least-once delivery, `XACK`-based progress
   tracking, and a `lag` counter (pending entries) that maps 1:1 to the
   "consumer lag" alerts run against Kafka in real exchanges.
3. **MAXLEN ~ 100000** caps the stream so a stuck consumer can't OOM the box
   (`~` = approximate trimming, cheaper than exact).

### Why a FIX layer at all?
FIX is the lingua franca of exchange connectivity (CME iLink is FIX-based).
Implementing Logon/Heartbeat/sequence numbers demonstrates fluency with the
session-layer concepts NOC analysts troubleshoot daily: sequence gaps,
heartbeat timeouts, session flapping. This is a simulation, not a certified
engine.

### Why chaos flags in Redis with TTLs?
Faults must reach two separate processes; Redis is already the shared fabric.
TTLs make every fault self-healing: even if the chaos controller crashes
mid-experiment, the fault expires. Chaos tooling that can wedge the system in
a broken state is worse than no chaos tooling.

### Why localhost binding + SSH tunnel instead of open ports?
Attack surface. Nothing here needs to be public. Metrics, FIX, and the chaos
API bind to loopback; Prometheus reaches them through the Docker host gateway;
humans reach Grafana through an SSH tunnel. The chaos API especially must
never be internet-reachable — it is a self-DoS button.

### Why systemd for the apps but Docker for Redis?
The Python services are the *product* — running them as hardened systemd units
(`ProtectSystem=strict`, non-root user, `Restart=on-failure`) demonstrates
Linux service operations, which is the actual NOC skill. Redis is commodity
infrastructure; a pinned container with a named volume is the pragmatic choice.
