# Postmortems & Chaos Drill Findings

Blameless postmortem records for issues found operating this system. Kept
deliberately: what broke, why the monitoring did or didn't catch it, and what
changed as a result. Each finding was validated (or is scheduled for
re-validation) in a subsequent chaos chaos drill.

---

## PM-1: End-to-end latency histogram clamped at its top bucket

**Date found:** 2026-08-05 (chaos chaos drill #1, live market hours)

**What happened:** During a 200 ms latency injection, the e2e SLA panel showed
p99 pinned at *exactly* 2.5 s for the duration of the fault.

**Why the number was wrong:** 2.5 s was the largest finite bucket of
`fix_end_to_end_latency_seconds`. `histogram_quantile()` linearly interpolates
*within* buckets — observations beyond the last finite bucket all land in
`+Inf`, so the quantile clamps at the largest finite boundary. Real tail
latency during the fault was worse than 2.5 s, but the instrumentation was
structurally incapable of showing it.

**Impact:** During a genuine incident we would have under-reported severity —
"p99 is 2.5 s" when the truth might have been 10 s. SLA breach magnitude drives
escalation decisions; an instrument that saturates hides exactly the signal
that matters most.

**Fix:** Extended buckets to 5 s / 10 s / 30 s. Rule of thumb adopted: bucket
range must exceed the worst latency you need to *see*, not the worst latency
you consider acceptable.

**General lesson:** A percentile pinned suspiciously at a round number is a
saturated instrument, not a stable system. Validate the measurement range of
every histogram against fault conditions, not just steady state.

---

## PM-2: Consumer lag gauge read 0 ("healthy") during heavy load

**Date found:** 2026-08-05 (chaos chaos drill #1)

**What happened:** While the latency fault pushed e2e latency past 2.5 s, the
FIX consumer lag panel read 0 throughout — physically implausible: a consumer
sleeping 200 ms per tick at ~150 msg/s must fall behind.

**Why the number was wrong:** Redis's `XINFO GROUPS ... lag` field is reported
as **null** when `MAXLEN` trimming removes entries the group never read — the
exact count becomes uncomputable, so Redis declines to report it.
Our sampler coerced that null to 0 (`group.get("lag") or 0`). The sensor
didn't fail loudly; it **failed toward "healthy"** — the worst possible
failure mode for a monitoring instrument.

**Impact:** The single metric designated as "the true backpressure signal"
was blind during precisely the conditions it exists to detect. Every backlog
alert built on it was silently disarmed during market hours (trimming is
continuous once the stream reaches its cap).

**Fix (two parts):**
1. Preserve null: null lag now surfaces as NaN (panel shows *no data* —
   "I don't know" instead of "all good").
2. Added a trim-proof metric: `fix_consumer_time_lag_seconds`, derived from
   stream ID timestamps (`<ms>-<seq>`): age of newest stream entry minus age
   of newest entry delivered to the group. Survives trimming, is more
   intuitive for triage (seconds behind, not entries behind), and correctly
   reads 0 when the market is closed because both IDs stop advancing together.

**General lessons:**
- Know your datasource's null semantics; `or 0` on a health metric is a bug
  pattern, not a default.
- Sensors must fail toward *alarm* or toward *unknown* — never toward healthy.
- Cross-check instruments against each other: e2e latency at 2.5 s+ with lag
  at 0 was the contradiction that exposed this. Contradictory panels means at
  least one of them is lying.

---

## Finding F-1: Queueing amplification (system behavior, working as designed)

**Observed:** A 200 ms injected per-frame delay produced an observed e2e p99 of
2.5 s+ — more than 12x the injected value.

**Explanation:** The delay applies per WebSocket frame in the ingest loop.
While the loop sleeps, new frames queue in the socket buffer; each message's
end-to-end time includes waiting behind every slowed predecessor. Injected
service-time delay compounds into queueing delay (Little's Law observed live).

**Operational takeaway:** In pipeline systems, "slightly slow" is more
dangerous than "down" — slowness *accumulates* into backlog while everything
still reports "up". This is why latency SLAs and consumer lag deserve alerting
parity with liveness checks.

## Finding F-2: Cascading failure — latency fault induced upstream disconnects

**Observed:** During the latency fault, the WS reconnect counter climbed (~2
reconnects) with no disconnect fault injected.

**Explanation:** The slowed ingest loop stopped draining the WebSocket socket
buffer; the upstream feed (Alpaca) disconnected the slow consumer — a
protection mechanism real exchanges also employ. One injected fault class
(latency) induced a second, uninjected fault class (session loss).

**Operational takeaway:** Fault effects propagate across component boundaries
and change failure class as they go. Incident diagnosis must consider that the
visible symptom (disconnects) may be a *consequence* of a quieter upstream
condition (slowness) — treat correlated events on the timeline as one story,
not separate tickets. This is the concrete argument for chaos testing: none of
this was predicted; all of it was discovered.

---

## PM-3: Three ways my dashboard lied

**Date found:** 2026-08-07 (spec review after game-day drills)

This record is the consolidated form of the three measurement bugs above and
serves as the rule the team now enforces: **a sensor that cannot see must say
"I cannot see", never "all good".**

### Lie 1 — "E2E p99 is 2.5 s" (histogram clamping)

What it showed: the end-to-end latency p99 pinned at exactly 2.5 s during a
200 ms latency injection. The real tail was larger.

Why it was a lie: 2.5 s was the largest *finite* bucket in the histogram.
Observations above it land in `+Inf`; `histogram_quantile()` interpolates
only within the top finite bucket and clamps.

Fix: extend buckets to 30 s and add `e2e_tick_latency_max_seconds`, a
scrape-resetting gauge that shows the true worst tick in each window.

### Lie 2 — "Consumer lag is 0" (null → 0 coercion)

What it showed: `fix_consumer_lag` read 0 while the consumer was visibly
falling behind.

Why it was a lie: Redis `XINFO GROUPS` returns `lag: null` when `MAXLEN`
trimming removes entries the group never read. The original code used
`group.get("lag") or 0`, so a null ("unknown") became a green 0.

Fix: preserve null as NaN; add `redis_group_pending_entries` and
`redis_group_undelivered_entries`; and add a trim-proof
`fix_consumer_time_lag_seconds` metric.

### Lie 3 — "Green 0 at market close" (ungated metrics)

What it showed: the feed staleness and consumer time-lag stat panels glowed
0/1 s on a Saturday morning.

Why it was a lie: the metrics had no concept of *expected* traffic. A
perfectly healthy, idle system was being colored as a live but slightly stale
feed.

Fix: add `gateway_traffic_expected` (1 only when feed is SUBSCRIBED, FIX is
LOGGED ON, and the US market is open); set `gateway_feed_staleness_seconds`
and `fix_consumer_time_lag_seconds` to NaN when not expected; render no-data
as grey "MARKET CLOSED" in Grafana; and gate the relevant alerts on
`gateway_traffic_expected == 1`.

---

## Re-validation log

| Date | Drill | Result |
|------|-------|--------|
| 2026-08-05 | Chaos drill #1 (latency / drop / WS disconnect / FIX kill) | All faults recovered; PM-1, PM-2, F-1, F-2 discovered |
| 2026-08-06 | Chaos drill #2 (same script, post-fix) | *scheduled — expect: e2e p99 resolves above 2.5 s; time lag visibly climbs and drains; ingress p99 resolves to ~200 ms instead of clamping at ~400 ms* |
| 2026-08-07 | Spec-revision drill | *pending — expect: grey MARKET CLOSED when no traffic; backlog panel shows pending/undelivered + time lag; e2e max series visible above p99; fault regions shaded on timeline* |
