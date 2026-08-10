# LinkedIn post — NOC Gateway Lab chaos drill

A dashboard that reports “healthy” while a system is falling behind is worse than no dashboard at all.

I built a NOC gateway lab for market-data operations: an async ingest path, Redis Streams, a FIX 4.4 delivery layer, Prometheus metrics, Grafana, and an API-driven chaos test harness. The goal was not just to make the services run, but to verify that the telemetry remains trustworthy when the system is under stress.

In the latest drill, I exercised the full recovery path:

- established a healthy market-hours baseline before injecting any fault;
- injected 200 ms of per-frame latency and watched queueing amplify the end-to-end tail well beyond the injected delay;
- removed the latency fault and verified that the consumer drained its backlog while traffic was still expected;
- injected 25% message loss and confirmed the observable gap between ingress received and FIX messages sent, then recovery when their rates aligned again;
- forced an upstream WebSocket disconnect and verified reconnect through backoff;
- killed the FIX session and verified that the acceptor handled a clean new Logon; and
- reset the chaos state and confirmed the dashboard returned to baseline.

The most valuable outcome was finding issues in the observability itself.

First, end-to-end p99 appeared pinned at exactly 2.5 seconds during latency injection. That was not a stable system; it was a saturated histogram. The original highest finite bucket was 2.5 seconds, so the measurement could not show a worse tail. I extended the buckets through 30 seconds and added `gateway_e2e_tick_latency_max_seconds` to expose the worst tick seen in each scrape window.

Second, the consumer-lag panel showed 0 while the consumer was visibly falling behind. Redis reports group lag as null when stream trimming makes the exact count unknowable; the sampler had converted that unknown value into 0. In other words, the metric failed toward healthy.

I corrected that by preserving unknown lag as NaN, adding pending and undelivered entry signals, and introducing the trim-proof `fix_consumer_time_lag_seconds` metric derived from Redis Stream ID timestamps. I also added `gateway_traffic_expected` so lag and staleness alerts only evaluate when the feed is subscribed, the FIX session is logged on, and the market is open.

The key lesson: observability needs its own failure testing. A clean dashboard during an outage is not evidence of health unless the sensors have been proven under the same conditions.

The attached timeline shows the sequence: baseline → fault → effect → recovery.

If you would like to explore the implementation, runbook, and full evidence set, the GitHub repository is linked in the first comment.

#SRE #Observability #ChaosEngineering #FinTech #Prometheus #Redis #FIXProtocol #NOC

---

## Suggested attachment order

Use this five-image carousel, in order:

1. `Screenshots/11-overview-1h.png` — cover image; the complete baseline → fault → recovery timeline.
2. `Screenshots/02-latency-deep-90s.png` — queueing amplification, end-to-end maximum, and consumer time-lag growth.
3. `Screenshots/04-drop-during.png` — packet-loss accounting through the gap between ingress and FIX-send rates.
4. `Screenshots/06-ws-disconnect-during.png` — upstream feed state down during the injected disconnect.
5. `Screenshots/09-fix-relogon-after.png` — finish on clean recovery: feed subscribed and FIX logged on.

Do not include the baseline or individual recovery screenshots in the LinkedIn carousel; the overview already establishes the baseline and the final re-Logon image provides a stronger closing outcome.

## Optional first comment

Full repository, runbook, postmortems, Grafana dashboard, and chaos-drill evidence: https://github.com/frankybananas/noc-gateway-lab

I documented the drill as a testable runbook rather than a one-off demo: baseline, injected fault, expected signal, recovery condition, and reset. The important part was not that every component recovered; it was learning where the dashboard could have told an operator the wrong story.
