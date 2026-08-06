# NOC Runbook — Market Data Gateway

Incident response procedures, written the way an exchange NOC would use them:
**Detect → Diagnose → Resolve → Verify**. All commands run on the VPS.

Conventions:
- `jc` = `journalctl -u <unit> -f --no-pager`
- Metrics: `curl -s localhost:9101/metrics | grep <metric>`
- Redis: `docker exec noc-gateway-redis redis-cli <cmd>`

---

## INC-1: Feed Silent (no market data arriving)

**Detect:** Grafana "Feed Staleness" panel red; alert `time() - gateway_feed_last_message_timestamp_seconds > 60` during market hours.

**Diagnose (in order):**
1. Is it just a quiet market? IEX only streams Mon–Fri 09:30–16:00 ET.
   `date -u` — outside market hours, staleness is expected, close as no-action.
2. Is the process up? `systemctl status gateway-ingress`
3. Is the WS session alive? `curl -s localhost:9101/metrics | grep gateway_connection_state`
   — `3` = subscribed (healthy), `0` = down.
4. Check reconnect churn: `grep gateway_ws_reconnects_total` — a climbing counter
   with state flapping 0↔3 means the upstream is dropping us (or chaos is active).
5. Check chaos: `curl -s localhost:9103/chaos/status` — someone may be running an experiment.
6. Check upstream reachability: `curl -sI https://stream.data.alpaca.markets` and Alpaca status page.

**Resolve:**
- Chaos active → `curl -X POST localhost:9103/chaos/reset`
- Process wedged → `sudo systemctl restart gateway-ingress`
- Auth errors in journal (`authentication failed`) → verify keys in `/etc/noc-gateway/gateway.env`, then restart.
- Upstream outage → nothing to fix locally; note incident start/end, monitor for recovery (backoff handles reconnection automatically).

**Verify:** connection state back to 3; staleness < 10 s; msg rate panel recovering.

---

## INC-2: FIX Session Flapping

**Detect:** "FIX Session State" panel oscillating; `fix_session_kills_total` or client-side disconnects climbing.

**Diagnose:**
1. `journalctl -u fix-engine -n 50` — look for `counterparty dropped` vs `CHAOS: killing FIX session`.
2. Chaos check: `curl -s localhost:9103/chaos/status`.
3. Heartbeat health: `curl -s localhost:9102/metrics | grep fix_msgs_sent_total` —
   are 35=0 heartbeats being sent at the configured interval?
4. Is the client actually connecting? `ss -tnp | grep 5001`

**Resolve:**
- Chaos → reset.
- Engine crash-looping → `journalctl -u fix-engine --since "-10 min"`, fix root cause, `sudo systemctl restart fix-engine`.
- Client-side issue → reproduce with the known-good demo client:
  `sudo -u gateway PYTHONPATH=/opt/noc-gateway/src /opt/noc-gateway/venv/bin/python -m gateway.fix_client_demo --seconds 15`

**Verify:** session state = 2 (LOGGED ON) stable for 5+ minutes; heartbeats flowing both ways.

---

## INC-3: Redis Stream Depth Growing (backpressure)

**Detect:** "Stream Depth & Consumer Lag" trending up; alert `fix_consumer_lag > 10000`.

**Diagnose:**
1. Is the consumer alive? `systemctl status fix-engine`
2. Is it consuming but slow? `docker exec noc-gateway-redis redis-cli XINFO GROUPS md.ticks`
   — `pending` high = delivered-but-unacked (consumer stuck mid-processing);
   `lag` high = not even read yet (consumer down or starved).
3. Chaos latency injection makes the consumer artificially slow — check `/chaos/status`.
4. Host resource exhaustion: `top`, existing node-exporter dashboards.

**Resolve:**
- Consumer down → restart `fix-engine`; consumer group resumes from last-acked ID (no data loss — that's the point of groups).
- Sustained overload → this system trims at MAXLEN 100k (oldest ticks dropped);
  document the data-loss window in the incident notes.

**Verify:** lag returns to ~0; depth plateaus at/below MAXLEN.

---

## INC-4: Latency SLA Breach (p99 end-to-end)

**Detect:** "End-to-End Tick SLA" p99 above threshold (e.g. > 250 ms).

**Diagnose:**
1. Which hop? Compare ingress `gateway_processing_latency_seconds` (parse+publish)
   vs `fix_end_to_end_latency_seconds` (full path). Ingress flat + e2e elevated
   = bottleneck is the bus or the FIX hop.
2. Chaos annotations on the dashboard line up with the spike? Then it's an experiment.
3. Stream depth rising at the same time → consumer can't keep up (see INC-3).
4. Host: CPU steal on a shared VPS (`top`, `%st`) can cause this with no code change.

**Resolve:** clear chaos, restart the slow component, or accept + document if host-level (CPU steal) and transient.

**Verify:** p99 back under threshold for 15 minutes.

---

## INC-5: Redis Down

**Detect:** `gateway_stream_publish_errors_total` climbing; fix-engine logs connection errors; both services degraded.

**Diagnose:**
1. `docker ps | grep noc-gateway-redis` and `docker inspect --format '{{.State.Health.Status}}' noc-gateway-redis`
2. `docker logs --tail 50 noc-gateway-redis` — OOM? disk full (`df -h`) breaking AOF?

**Resolve:**
- `docker compose -f /opt/noc-gateway/deploy/docker-compose.redis.yml up -d`
- Data survives restarts via the AOF on the named volume.
- Services reconnect automatically; restart them only if errors persist after Redis is healthy.

**Verify:** `redis-cli ping` = PONG; publish errors stop; stream depth sampling resumes.

---

## INC-6: Service Won't Start After Reboot

**Detect:** `systemctl --failed` lists a gateway unit after maintenance.

**Diagnose:**
1. `journalctl -u <unit> -b` — read the *first* error, not the last.
2. Common causes here:
   - `FATAL: required environment variable ... not set` → `/etc/noc-gateway/gateway.env` missing/unreadable (perms must allow the `gateway` group to read).
   - Redis not up yet → units retry every 5 s (`Restart=on-failure`), self-heals once Docker brings Redis up.
   - venv path broken after a Python upgrade → recreate venv, reinstall requirements.

**Resolve & Verify:** fix root cause, `sudo systemctl restart <unit>`, confirm all three units active and Prometheus targets UP.

---

## Planned Chaos Drill

Run during market hours, watch the dashboard live:

```bash
# 1. Baseline: all green, note p99.
# 2. Latency fault: p99 rises, annotation appears.
curl -X POST localhost:9103/chaos/latency -H 'Content-Type: application/json' -d '{"ms": 200, "duration_s": 120}'
# 3. Packet loss: msg gap visible in FIX send rate vs ingress rate.
curl -X POST localhost:9103/chaos/drop -H 'Content-Type: application/json' -d '{"percent": 25, "duration_s": 60}'
# 4. Feed failure: connection state drops, backoff reconnect kicks in.
curl -X POST localhost:9103/chaos/disconnect-ws
# 5. FIX session kill: session state 2 -> 0, client must re-logon.
curl -X POST localhost:9103/chaos/kill-fix-session
# 6. Recovery: reset and verify all panels return to baseline.
curl -X POST localhost:9103/chaos/reset
```
