# Dashboard Field Guide — Market Data Gateway

A panel-by-panel explanation of the Grafana dashboard: what each panel shows,
where the number comes from, why it behaves the way it does, and why a NOC
monitors it. Written as a learning document — read it with the live dashboard
open next to it.

---

## Part 1: The Foundations (read this first)

### How a number gets onto the dashboard

Every panel follows the same pipeline:

```
Python code           Prometheus                    Grafana
─────────────         ──────────────────            ─────────────────
metric object    ──▶  scrapes /metrics    ──▶       runs a PromQL query
(counter/gauge/       every 5-15s, stores           against Prometheus
 histogram)           timestamped samples           every 10s, draws result
```

The service never "sends" anything — Prometheus *pulls* (scrapes) a text page
like `gateway_msgs_received_total{symbol="SPY"} 48123` on an interval. This
pull model is deliberate: if a service dies, the scrape fails, and Prometheus
itself records the target as down. Push-based systems can't distinguish "no
news" from "sender is dead" — pull-based systems can. That distinction is the
foundation of half the alerts a NOC runs.

### The three metric types (everything on this dashboard is one of these)

**Counter** — a number that only ever goes up (e.g. total messages received).
Raw counters are almost useless to look at (48123... so what?). Their value
comes from the *rate of change*: `rate(counter[1m])` = "per-second speed over
the last minute". A counter that stops climbing is a thing that stopped
happening — which is why counters power "did the feed die?" alerts. Counters
reset to zero when a process restarts; `rate()` detects and handles resets
automatically, which is why you never subtract counter values by hand.

**Gauge** — a number that goes up and down (e.g. connection state, stream
depth, seconds-since-last-message). Gauges are snapshots: "what is true right
now". They power the stat panels at the top of the dashboard.

**Histogram** — a set of buckets counting observations by size (e.g. how many
messages processed in <1ms, <5ms, <10ms...). Histograms exist because
*averages lie about latency*: if 99 messages take 1ms and one takes 2 seconds,
the average is ~21ms — a number that describes nothing that ever happened.
Percentiles (p50/p95/p99) describe the actual experience: "99% of messages
were faster than X". Exchanges write SLAs in percentiles for exactly this
reason.

### Reading a PromQL query (you'll see this pattern everywhere)

```
histogram_quantile(0.99, sum by (le) (rate(gateway_processing_latency_seconds_bucket[5m])))
```

Read it inside-out:
1. `..._bucket` — the histogram's raw bucket counters
2. `rate(...[5m])` — how fast each bucket grew over the last 5 minutes
3. `sum by (le)` — merge all label variants, keeping bucket boundaries (`le` = "less than or equal")
4. `histogram_quantile(0.99, ...)` — interpolate: "below which value do 99% of observations fall?"

---

## Part 2: The Stat Panels (top row — the "is anything wrong?" strip)

The top row is designed for a 2-second glance: all green = system nominal.
Each panel maps a gauge to a color-coded state. This mirrors how real NOC
video walls work — humans can't parse graphs at a glance, but they can parse
colors instantly.

### Feed Connection State

- **Metric:** `gateway_connection_state` (gauge: 0=down, 1=connected, 2=authenticated, 3=subscribed)
- **What it means:** where the ingress currently is in the WebSocket lifecycle.
  Connecting to Alpaca is a 3-step handshake (TCP/TLS connect → authenticate
  with API keys → subscribe to symbols), and each step can fail independently
  with a different root cause: network problem, credential problem,
  subscription/permission problem.
- **Why numbered states, not just up/down:** when this panel reads
  CONNECTED (1) but never reaches SUBSCRIBED (3), you *instantly* know auth
  succeeded but the subscription failed — you've localized the fault before
  opening a single log. Encoding the state machine into the metric turns the
  first 10 minutes of an investigation into a glance.
- **Failure signatures:** flapping 0↔3 = upstream instability (see reconnect
  panel); stuck at 1 = bad credentials; stuck at 0 = network/DNS/Alpaca outage.

### FIX Session State

- **Metric:** `fix_session_state` (gauge: 0=disconnected, 1=connected, 2=logged on)
- **What it means:** same state-machine idea, for the *downstream* consumer
  side. A FIX session isn't just a TCP connection — a counterparty must
  connect AND complete a Logon (35=A) exchange before it receives data.
  State 1 (connected but not logged on) is its own diagnostic: TCP works,
  the application-layer handshake didn't.
- **Why the NOC watches it:** at a real exchange, "customer session dropped"
  is among the most common and most revenue-critical tickets. Session state,
  heartbeat health, and sequence numbers are the trader-connectivity holy
  trinity. DISCONNECTED here isn't an error unless a client *should* be
  connected — context the runbook (INC-2) encodes.

### Feed Staleness (seconds)

- **Query:** `time() - gateway_feed_last_message_timestamp_seconds`
- **What it means:** how long since the last actual data message arrived.
  The ingest loop stamps a gauge with the wall-clock time on every message;
  the panel subtracts that from "now". Healthy market hours: ~1-2s.
- **Why this exists when we already have connection state:** because of the
  worst failure mode in market data — the **silent feed death**. The TCP
  connection stays "up" (state=3, all green) but no data flows: upstream
  stopped publishing, a subscription silently expired, traffic is black-holed.
  Connection state cannot see this; only "when did data last arrive?" can.
  This is THE canonical exchange NOC alert.
- **The 56-year lesson:** on first deploy this panel read 56.6 years — the
  gauge defaulted to 0 (the Unix epoch, 1970) before the first message. Fixed
  by initializing to process-start time. General lesson: any alert built on
  "time since X" must define what X is *before X has ever happened*.
- **Reading it correctly:** climbing staleness outside market hours (nights,
  weekends) is normal — IEX only streams 09:30–16:00 ET Mon–Fri. The runbook's
  first diagnostic step for a staleness alert is literally "check the clock".

### Active Chaos Faults

- **Metric:** `chaos_active_faults` (gauge, recomputed from Redis on every scrape)
- **What it means:** how many fault injections are live *right now*. Faults
  live in Redis keys with TTLs, so they expire on their own; this gauge is
  recomputed from Redis at scrape time so it self-corrects when a fault expires.
- **Why it's on the board:** the first question in any incident triage is
  "is this real, or is someone testing?" At real exchanges the equivalent is
  a maintenance/change-freeze indicator. If latency spikes while this reads 1,
  you check the annotations before paging anyone. It closes the loop between
  *causing* failures and *seeing* them.

### Stream Depth (md.ticks)

- **Metric:** `gateway_stream_depth` (gauge, sampled from Redis `XLEN` every 5s)
- **What it means:** the number of entries currently held in the Redis Stream.
- **Why it reads ~100,000 all day (and why that's healthy):** the ingress
  writes with `MAXLEN ~ 100000` — Redis trims the oldest entries once the cap
  is hit. Crucially, a consumer *acknowledging* a message (`XACK`) does NOT
  delete it from the stream; entries only leave via trimming. So during market
  hours depth fills to the cap in minutes and stays there forever. It is a
  high-water mark, not a backlog.
- **So why monitor it at all?** Three reasons: (1) depth *below* cap while
  climbing tells you how fast the buffer is filling on a fresh start; (2) a
  depth of 0 during market hours means the ingress stopped publishing;
  (3) the cap itself is your memory-safety guarantee — this panel is proof the
  bound is enforced. The *real* backpressure signal is the next panel.
- **The `~` in MAXLEN:** approximate trimming — Redis trims in whole
  macro-nodes rather than exactly, trading a few extra entries (100033 vs
  100000) for much cheaper trims. Knowing why the number isn't exactly 100000
  is a useful implementation detail to monitor.

### FIX Consumer Lag

- **Metric:** `fix_consumer_lag` (gauge, from `XINFO GROUPS` — entries added to
  the stream that the fix-engine consumer group has not yet read)
- **What it means:** the true backpressure number. Producer rate vs consumer
  rate, as a queue length. 0 = the FIX engine processes ticks as fast as they
  arrive. Sustained growth = the consumer can't keep up (slow consumer, stuck
  process, or injected latency).
- **Why lag and not depth:** see above — depth saturates by design; lag
  measures actual unprocessed work. This mirrors *Kafka consumer lag*, one of
  the most-watched metrics in any real trading/data platform. If you learn one
  concept deeply from this project, make it this one: **the health of a
  pipeline is measured at the consumer, not the buffer.**
- **Failure signature:** during a latency fault injection, watch lag climb
  while the fault is active and drain (steep downslope) after it expires —
  that drain rate is your consumer's catch-up capacity, i.e. your headroom.

---

## Part 3: The Time-Series Panels (the "what happened and when?" layer)

### Message Rate by Symbol / Type (msg/s)

- **Query:** `sum by (symbol, msg_type) (rate(gateway_msgs_received_total[1m]))`
- **What it means:** per-second message throughput, split per symbol and per
  type (`t`=trade, `q`=quote). The counter is incremented once per message with
  labels; the query turns it into per-second speed and keeps the label split.
- **Why labels matter (the whole point of modern monitoring):** one metric,
  filterable by dimension. Alert says "AAPL data stopped"? Query
  `{symbol="AAPL"}` and know in seconds whether it's one symbol (subscription
  issue), all symbols (feed issue), or trades-only (market microstructure —
  quotes always vastly outnumber trades). Without labels you'd maintain a
  separate metric per symbol per type — unmanageable at exchange scale where
  it's thousands of instruments.
- **What "normal" looks like:** quotes 10-100x trades (SPY quotes dominate —
  it's the most actively quoted instrument in the world); rates spike at
  09:30 ET open and 16:00 close, dip over lunch. Learning a system's *diurnal
  rhythm* is core NOC skill — you can't spot abnormal until you know normal.
- **Failure signatures:** all lines → 0 with connection green = silent feed
  death; one symbol → 0 = subscription problem; rate halves during a drop
  fault = the chaos drop applied upstream of this counter? No — drops apply
  *after* counting, which is deliberate: received-rate stays true while
  published-rate falls, and the *gap between the two* is your data-loss
  measurement.

### Processing Latency Percentiles (ingress)

- **Query:** `histogram_quantile(0.50|0.95|0.99, sum by (le) (rate(gateway_processing_latency_seconds_bucket[5m])))`
- **What it measures — precisely:** time from "WebSocket frame received" to
  "all messages in that frame parsed, counted, and published to Redis".
  Measured with `time.perf_counter()`, a monotonic clock that can't jump
  backwards when NTP adjusts the system clock (a wall clock would produce
  negative latencies after a sync).
- **Why three lines:** p50 = typical experience; p95 = the bad minute each
  hour; p99 = the worst 1%, which in trading is where the money is (the
  slowest ticks are often during the most volatile — most valuable — moments).
  Watch the *gap* between p50 and p99: a widening gap with a flat p50 means
  jitter, not load — different root causes (GC pauses, CPU steal, bursty
  frames) than uniform slowdown.
- **Why it is 10-30ms:** each message in a frame does an awaited
  Redis XADD round-trip; frames arrive in batches; Python on a shared VPS has
  CPU steal. A C++ gateway on dedicated hardware measures this path in
  microseconds. Knowing *where your latency comes from* matters more than the
  absolute number — the source of the latency matters more than the absolute value.

### End-to-End Tick SLA (WS ingest → FIX send)

- **Query:** same `histogram_quantile` pattern over `fix_end_to_end_latency_seconds_bucket`
- **What it measures:** the full pipeline: ingress stamps each tick with a
  wall-clock nanosecond timestamp (`time.time_ns()`) when it arrives; the FIX
  engine computes `now - ingest_ts` at the moment it sends the corresponding
  35=X message. Ingest → Redis → consumer group → FIX encode → TCP send.
- **Why wall clock here but monotonic clock above:** monotonic clocks
  (`perf_counter`) are only meaningful *within one process* — each process's
  monotonic zero is arbitrary. This measurement spans two processes, so the
  only shared timebase is the wall clock. The trade-off: an NTP step mid-tick
  can distort individual measurements. Same machine ⇒ same clock, so it
  cancels here — but measuring across *machines* makes clock sync (NTP/PTP)
  itself the accuracy limit. That's why exchanges run PTP with hardware
  timestamping; CME timestamps are legally load-bearing.
- **Why the panel is empty when no FIX client is connected:** no session ⇒
  no 35=X sends ⇒ no observations ⇒ no data. "No data" and "zero latency"
  are different things, and the panel clearly shows which one is true.
- **This is the headline SLA.** If you had to pick one number that defines
  this system's service quality, it's this p99. Chaos latency injections show
  up here first and biggest.

### Stream Depth & Consumer Lag (backpressure)

- **Queries:** `gateway_stream_depth` and `fix_consumer_lag` on one panel
- **Why plot them together:** the *relationship* is the diagnosis. Depth flat
  at cap + lag at 0 = nominal. Depth at cap + lag climbing = slow consumer.
  Depth falling toward 0 = producer stopped. One panel, three distinct
  failure modes distinguishable at a glance.
- **What to watch during a chaos drill:** the latency fault makes lag climb
  linearly, then drain after expiry. The drain slope is your catch-up
  capacity — if injected load ever made lag grow *without bound*, you'd have
  discovered your throughput ceiling. (That experiment — find the drop/latency
  values where the system stops recovering — is a genuinely useful stress test
  this rig can run.)

### WS Reconnects / FIX Session Kills

- **Queries:** `sum by (reason) (increase(gateway_ws_reconnects_total[5m]))` and `increase(fix_session_kills_total[5m])`
- **What it means:** `increase()` = "how many in the window" (readable event
  counts) vs `rate()` = per-second speed (better for high-frequency things).
  Reconnects are labeled by *reason* (connection_closed, server_close, error
  class), so the panel shows not just "we reconnected" but *why*.
- **Why frequency matters more than occurrence:** a reconnect per day is
  weather; ten per hour is a pattern worth a ticket; one per minute is an
  incident. Counting events over windows is how you distinguish those — and
  why "it reconnects automatically" is not the same claim as "the connection
  is healthy". Self-healing systems still need their healing *counted*.

### FIX Messages Sent by Type / Chaos Drops

- **Queries:** `sum by (msg_type) (rate(fix_msgs_sent_total[1m]))`, plus
  `rate(gateway_chaos_dropped_total[1m])` and `rate(gateway_stream_publish_errors_total[1m])`
- **What it means:** outbound FIX traffic split by message type — 35=X (market
  data) dominates when a client is connected; 35=0 (heartbeats) tick along at
  one per 30s; 35=A (logons) should be rare. Plus the two "data went missing"
  counters: intentional chaos drops and real publish errors.
- **Why heartbeats deserve a line on a graph:** heartbeats are the FIX
  session's liveness proof. Steady 35=0 at the configured interval = session
  layer healthy even when the market is silent. Missing heartbeats while data
  flows = timer logic broken. Heartbeats *without* data during market hours =
  application layer broken while session layer lives. The ratio patterns are
  diagnostic.
- **Why drops are counted, not hidden:** when chaos injects 25% loss, this
  panel is your *measured* loss rate vs the *configured* one — validating the
  fault injection itself. In production the equivalent counter (messages
  dropped due to full buffers etc.) is a data-integrity metric: exchanges must
  account for every message. "We drop silently" is never acceptable; "we drop
  and count" is an engineering decision.

### Chaos Annotations (the red vertical lines)

- **Source:** Grafana's annotation API, tag `chaos` — posted by the chaos
  controller at injection time, not derived from any metric.
- **Why annotations matter:** they overlay *cause* onto graphs of *effect*.
  Correlating "what changed" with "what broke" is most of incident response;
  in production the same mechanism auto-annotates deployments, config changes,
  and maintenance windows. A latency spike with a deploy annotation on it is a
  5-minute incident; the same spike without one is a 2-hour investigation.

---

## Part 4: The Habits This Dashboard Teaches

1. **Glance the stat row first** — color beats curves for "is anything wrong".
2. **Know normal before you hunt abnormal** — market rhythm, baseline p99,
   heartbeat cadence. Spend time watching the healthy system.
3. **Distinguish signal semantics:** counters that stop, gauges that stick,
   percentile gaps that widen — each is a different class of problem.
4. **Measure consumers, not buffers** — lag over depth, always.
5. **Correlate cause and effect** — annotations first, logs second.
6. **Distrust unmeasured health:** empty panel ≠ healthy; it means *not
   measured*. The FIX SLA panel is blank until a client connects — the
   dashboard tells you what it doesn't know.
