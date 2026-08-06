# Edge Cases & Production-Grade Considerations

This lab is intentionally scoped to demonstrate NOC/SRE workflow, but a senior
exchange operator will ask hard questions. This file records the known edge
cases, what the project does about them, and what a full production system
would do next.

---

## 1. Prometheus classic histograms and bucket clamping

**The edge case:** A latency histogram with fixed buckets cannot report
percentiles larger than its largest finite bucket. During the first chaos
drill, a 200 ms injected delay produced an e2e p99 that pinned at 2.5 s — the
code was recording much larger latencies, but the `+Inf` bucket only says
"more than 2.5 s"; `histogram_quantile()` clamps at the boundary.

**What the lab does now:** buckets were extended to 30 s (0.001, 0.005,
0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0). This is
sufficient to see the tail during a VPS-scale drill and is recorded in the
postmortem.

**The production-grade answer:** use **Prometheus Native Histograms**
(exponential bucket schema) for any metric whose dynamic range is unknown.
Native histograms allocate buckets automatically across many orders of
magnitude and also support `exemplars` (linking a metric observation to a
specific trace/log line). They require Prometheus >= v2.40 with the native
histogram feature enabled and client support in `prometheus-client` (still
marked experimental in Python as of 2026), so this lab deliberately uses
traditional histograms for portability.

**Production note:** For a lab, explicit buckets keep the stack simple and
avoid feature flags, but in a production exchange, Native Histograms are
preferable for any latency metric. The drill exposed that even 30 s may be
too small during a serious fault; exponential bucketing removes the risk of
a bucket-sizing mistake clamping an SLA percentile.

---

## 2. The "weekend page" problem — market-hours gating

**The edge case:** A `staleness > 60s` alert is correct during market hours
but will fire every Saturday, Sunday, and every holiday. The IEX feed only
streams when US equities trade (Mon–Fri 09:30–16:00 ET), so the natural state
of the system at 2 AM Sunday is "perfectly healthy, just no data." Alerting
on that is the classic "weekend page" that destroys on-call trust.

**What the lab does now:** the ingress gateway computes a `market_open` gauge
(1 during US regular hours, 0 otherwise) using `America/New_York` via
`zoneinfo`. Alerts in `deploy/prometheus/gateway-alerts.yml` are gated:
`...staleness > 60 and market_open == 1`. The dashboard also shows a `US
Market Open` stat panel so an operator can see the gate at a glance.

**Why the consumer time-lag metric does not have the same weekend problem:**
the lag is `newest stream entry - newest entry delivered to the consumer`,
both derived from Redis stream IDs. When the market closes, no new entries
are generated *and* no new entries are delivered; both IDs freeze, so the lag
stays constant. It does not climb to 16 hours. This is why lag is a better
backpressure signal than staleness: it measures *unprocessed work*, not
*time since anything happened*.

**The production-grade answer:** use the exchange's *official trading
calendar* (CME, CBOE, and ICE publish holiday/early-close calendars). Wall
clock is the 80% solution but misses: half-days, exchange holidays, emergency
closings, and DST quirks. A real system ingests a calendar file or API and
exposes `market_open` as a metric based on that. The runbook's first
staleness diagnostic is still "check the clock and the calendar" — the alert
just automates that step.

**Production note:** The lab uses a wall-clock `market_open` gauge, but the
alert rule's `and market_open == 1` pattern is the important part. In a real
exchange, a trading-calendar feed should be used so the on-call is not paged
on holidays.

---

## 3. FIX 4.4 heartbeats and session-kill behavior

**The edge case:** FIX session layers define a `HeartBtInt` (heartbeat
interval). If a counterparty does not receive a heartbeat within
`1.5 * HeartBtInt`, it must send a `TestRequest` (35=1); if no response
arrives within another `HeartBtInt`, the session is considered lost and both
sides must log out. In a real exchange, a 2.5 s per-tick processing latency
would likely starve the heartbeat thread and trigger this timeout, killing
the session. The lab must either model that or be honest that it does not.

**What the lab does now:** the FIX engine sends heartbeats from a dedicated
`asyncio` task that is not blocked by the consumer-latency sleep. Because
`asyncio.sleep()` yields, the heartbeat task continues to run on the event
loop and the demo client receives heartbeats every 30 s. Therefore the
session did **not** drop during the 200 ms/2.5 s latency drill — it kept
receiving heartbeats. The demo client does not yet enforce receiving
heartbeats, so it also did not disconnect.

**The honest simplification:** this is a session/application-layer simulation
focused on observability and message flow, not a full QuickFIX-style
compliance test. Heartbeat *sending* is implemented; heartbeat *reception and
timeout enforcement* is not. The runbook's "FIX Session Flapping" incident
procedure is written assuming the NOC can see the session state and heartbeat
rate in the dashboard, which this project does provide.

**The production-grade answer:** a real FIX engine runs a per-session timer
that starts on the last received message of any type (data or heartbeat). If
`HeartBtInt * 1.5` passes with no traffic, send `TestRequest`; if the echo is
missing after `HeartBtInt` more, close the socket and reset the session. The
`fix_heartbeat_rtt_seconds` metric would be the observable for this. Adding
the timeout behavior to this simulation would make the chaos drills more
realistic and is a strong next feature.

**Production note:** The FIX engine implements outbound heartbeats and the
Logon/TestRequest/Logout exchange, but not full inbound heartbeat timeout
enforcement. In a real acceptor, that enforcement is load-bearing because a
slow consumer can make the session miss its heartbeats; it should be added
before any production use, and the existing heartbeat metrics in the dashboard
are the natural place to alert on it.

---

## 4. Other known simplifications

- **Single-node only:** no clustering, no leader election, no consensus. The
  point is observability and failure handling, not horizontal scale.
- **No replay / recovery service:** if a consumer is down for hours and the
  stream trims past its last-read ID, messages are lost. MAXLEN is a memory
  safety bound, not a data-retention guarantee.
- **No TLS on the FIX session:** the demo uses TCP. Real exchange FIX
  sessions run over TLS or stunnel.
- **One active consumer per group:** the consumer group name is fixed to
  `fix-engine` with one consumer (`fix-1`). Scaling out would require
  partitioning the stream or handling duplicate-aware idempotency.

---

## Using this file

Use the structure above to walk through:

1. What the edge case is
2. What the simulation does now
3. What the production-grade solution is
4. Why the lab is scoped the way it is

This demonstrates *design under constraints* and *system awareness*.
