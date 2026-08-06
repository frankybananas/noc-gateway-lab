# NOC-Gateway-Lab: Exchange-Grade Market Data Relay

A simulated exchange market data gateway running on a Linux VPS, built to
demonstrate the observability, protocol fluency, and incident-response
workflows of an exchange NOC (CME-style operations).

**This is not a trading bot.** It is the *infrastructure that supports
trading*: a real-time ingest pipeline with tick-level latency SLAs, a FIX 4.4
session layer, a message bus with backpressure telemetry, and an API-driven
chaos-engineering framework that auto-annotates Grafana dashboards.

## Architecture

```
Alpaca IEX WS ──▶ ingress (asyncio) ──▶ Redis Streams ──▶ FIX 4.4 engine ──▶ FIX clients
                     │                      │                  │
                     └──────── Prometheus metrics ────────────┘
                                    │
                 Grafana dashboards + chaos annotations
                                    ▲
                     chaos-api (FastAPI fault injection)
```

Full details and design rationale: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## What it demonstrates

| Capability | Implementation |
|---|---|
| Real-time ingest | asyncio WebSocket client, exponential-backoff reconnects, monotonic-clock latency measurement |
| Message bus operations | Redis Streams with MAXLEN caps, consumer groups, XACK, lag/depth telemetry |
| Exchange protocol fluency | FIX 4.4 session layer: Logon, Heartbeat/TestRequest, sequence numbers, Market Data Incremental Refresh (35=X) |
| Observability | Prometheus counters/gauges/histograms, Grafana SLA dashboards, feed-staleness alerting |
| Chaos engineering | API-injected latency / message loss / session kills, TTL-bounded faults, automatic Grafana annotations |
| Linux operations | Hardened systemd units, non-root service user, localhost-only binding, secrets via EnvironmentFile |
| Incident response | [NOC runbook](docs/RUNBOOK.md) with 6 detect→diagnose→resolve procedures, validated by chaos game days |

## Services

| Service | Port (localhost) | Description |
|---|---|---|
| `gateway-ingress` | 9101 (metrics) | Alpaca IEX WS → normalize → `md.ticks` stream |
| `fix-engine` | 5001 (FIX TCP), 9102 (metrics) | Stream consumer → FIX 4.4 acceptor |
| `chaos-api` | 9103 (API+metrics) | Fault injection + Grafana annotations |
| Redis 7 | 6379 | Message bus (Docker, AOF persistence) |

## Deployment (Ubuntu VPS)

Prereqs: Python 3.10+, Docker + Compose, an existing Prometheus/Grafana stack,
Alpaca paper account keys.

```bash
# 1. Service user + directories
sudo useradd -r -m -s /usr/sbin/nologin gateway
sudo mkdir -p /opt/noc-gateway /etc/noc-gateway
sudo cp .env.example /etc/noc-gateway/gateway.env   # then edit in real keys
sudo chown root:gateway /etc/noc-gateway/gateway.env && sudo chmod 640 /etc/noc-gateway/gateway.env

# 2. Code + venv
sudo rsync -a --exclude venv ./ /opt/noc-gateway/
sudo python3 -m venv /opt/noc-gateway/venv
sudo /opt/noc-gateway/venv/bin/pip install -r /opt/noc-gateway/requirements.txt
sudo chown -R gateway:gateway /opt/noc-gateway

# 3. Redis
docker compose -f /opt/noc-gateway/deploy/docker-compose.redis.yml up -d

# 4. Services
sudo cp /opt/noc-gateway/deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now gateway-ingress fix-engine chaos-api

# 5. Observability: merge deploy/prometheus/gateway-scrape.yml into the
#    monitoring stack's prometheus.yml, import deploy/grafana/gateway-dashboard.json
```

Everything binds to `127.0.0.1`; access dashboards via SSH tunnel:
`ssh -L 3000:localhost:3000 user@vps`.

## Demo

```bash
# Watch live FIX messages (market hours: Mon–Fri 09:30–16:00 ET):
sudo -u gateway PYTHONPATH=/opt/noc-gateway/src \
  /opt/noc-gateway/venv/bin/python -m gateway.fix_client_demo --seconds 30

# Inject a fault and watch the dashboard react:
curl -X POST localhost:9103/chaos/latency -H 'Content-Type: application/json' \
  -d '{"ms": 200, "duration_s": 60}'
```

## Honest scope notes

- The FIX engine is a session/application-layer *simulation* built for
  observability practice — not a certified implementation.
- Market data is Alpaca's free IEX feed (streams during US market hours only).
- Single-node by design: the interesting part is the telemetry and failure
  handling, not horizontal scale.
