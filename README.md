# NOC-Gateway-Lab: Exchange-Grade Market Data Relay

A simulated exchange market data gateway built to demonstrate the observability, protocol fluency, and incident-response workflows of an exchange NOC (CME-style operations).

**This is not a trading bot.** It is the *infrastructure that supports trading*: a real-time ingest pipeline with tick-level latency SLAs, a FIX 4.4 session layer, a Redis Streams message bus, and an API-driven chaos-engineering framework.

---

## Table of contents

1. [Quick start on your laptop](#quick-start-on-your-laptop)
2. [What each service does](#what-each-service-does)
3. [Architecture](#architecture)
4. [What it demonstrates](#what-it-demonstrates)
5. [Production deployment (Ubuntu VPS)](#production-deployment-ubuntu-vps)
6. [Observability hookup](#observability-hookup)
7. [Troubleshooting](#troubleshooting)
8. [Honest scope notes](#honest-scope-notes)

---

## Quick start on your laptop

You can be up and running in about 10 minutes. You only need Python, Docker, and a free Alpaca paper account for live market data.

### Prerequisites

- Python 3.10+
- Docker with `docker compose`
- A free [Alpaca paper trading](https://app.alpaca.markets) API key
- A terminal that supports `set -a; source <file>; set +a` (bash/zsh)

### 1. Clone the repo and create a venv

```bash
cd noc-gateway-lab
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Copy and fill out the environment file

```bash
cp .env.example .env
```

Edit `.env` and replace the placeholder values with your real keys:

```bash
ALPACA_API_KEY=your_paper_key
ALPACA_SECRET_KEY=your_paper_secret
```

You can leave `GRAFANA_API_TOKEN` and `GRAFANA_URL` empty if you are not running Grafana yet. The chaos API will warn once and skip annotations.

### 3. Start Redis

```bash
docker compose -f deploy/docker-compose.redis.yml up -d
```

To stop Redis later:

```bash
docker compose -f deploy/docker-compose.redis.yml down
```

### 4. Run the three services

Open three terminal windows/tabs and run one service in each. From the repo root with the venv active:

```bash
# Tab 1: ingest live ticks into Redis Streams
set -a; source .env; set +a
PYTHONPATH=src python -m gateway.ingress
```

```bash
# Tab 2: serve ticks out over FIX 4.4
set -a; source .env; set +a
PYTHONPATH=src python -m gateway.fix_engine
```

```bash
# Tab 3: chaos / fault-injection API
set -a; source .env; set +a
PYTHONPATH=src python -m gateway.chaos_api
```

Each service logs to the console. You should see `ingress` connect to Alpaca, `fix_engine` wait for a FIX client, and `chaos_api` start on port `9103`.

### 5. Watch the FIX stream

In a fourth terminal:

```bash
set -a; source .env; set +a
PYTHONPATH=src python -m gateway.fix_client_demo --seconds 60
```

You will see a `Logon (35=A)` exchange, followed by `Heartbeat` messages. During US market hours (Mon–Fri 09:30–16:00 ET) you will also see live `MarketDataIncrementalRefresh` messages for the default symbols (`AAPL`, `SPY`, `TSLA`).

### 6. Inject a fault and watch it recover

```bash
# Add 200 ms of latency to every tick for 60 seconds
curl -X POST http://localhost:9103/chaos/latency \
  -H 'Content-Type: application/json' \
  -d '{"ms": 200, "duration_s": 60}'

# Check what faults are active
curl http://localhost:9103/chaos/status

# Clear all active faults
curl -X POST http://localhost:9103/chaos/reset
```

### 7. Peek at the Prometheus metrics

Each service exposes metrics on its own port:

```bash
curl -s http://localhost:9101/metrics | grep -E '^gateway_msgs_received_total'
curl -s http://localhost:9102/metrics | grep -E '^fix_session_state'
curl -s http://localhost:9103/metrics | grep -E '^chaos_active_faults'
```

### 8. Stop everything

- Press `Ctrl+C` in each service terminal, then run `docker compose -f deploy/docker-compose.redis.yml down`.
- Or, if you ran services in the background, `pkill -f 'python -m gateway'` and stop the Redis container.

---

## What each service does

| Service | Port (localhost) | Description |
|---|---|---|
| `gateway-ingress` | 9101 (metrics) | Alpaca IEX WS → normalize → `md.ticks` stream |
| `fix-engine` | 5001 (FIX TCP), 9102 (metrics) | Stream consumer → FIX 4.4 acceptor |
| `chaos-api` | 9103 (API+metrics) | Fault injection + Grafana annotations |
| Redis 7 | 6379 | Message bus (Docker, AOF persistence) |

---

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

---

## What it demonstrates

| Capability | Implementation |
|---|---|
| Real-time ingest | asyncio WebSocket client, exponential-backoff reconnects, monotonic-clock latency measurement |
| Message bus operations | Redis Streams with MAXLEN caps, consumer groups, XACK, lag/depth telemetry |
| Exchange protocol fluency | FIX 4.4 session layer: Logon, Heartbeat/TestRequest, sequence numbers, Market Data Incremental Refresh (35=X) |
| Observability | Prometheus counters/gauges/histograms, Grafana SLA dashboards, feed-staleness alerting |
| Chaos engineering | API-injected latency / message loss / session kills, TTL-bounded faults, automatic Grafana annotations |
| Linux operations | Hardened systemd units, non-root service user, localhost-only binding, secrets via EnvironmentFile |
| Incident response | [NOC runbook](docs/RUNBOOK.md) with 6 detect→diagnose→resolve procedures, validated by chaos drills |

---

## Production deployment (Ubuntu VPS)

Prereqs: Python 3.10+, Docker + Compose, an existing Prometheus/Grafana stack, Alpaca paper account keys.

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
```

Everything binds to `127.0.0.1`; access dashboards via SSH tunnel:
`ssh -L 3000:localhost:3000 user@vps`.

---

## Observability hookup

### Prometheus

Merge the contents of [deploy/prometheus/gateway-scrape.yml](deploy/prometheus/gateway-scrape.yml) into your Prometheus `prometheus.yml` under the existing `scrape_configs:` section.

The gateway services bind to `127.0.0.1` on the host. If Prometheus runs in Docker, use `host.docker.internal` and make sure the Prometheus container has the `extra_hosts` entry shown in the file.

### Grafana

1. Open Grafana → **Dashboards → Import**.
2. Upload the JSON file `deploy/grafana/gateway-dashboard.json`.
3. Set `GRAFANA_URL` and `GRAFANA_API_TOKEN` in `gateway.env` so the chaos API can post annotations.

### Demo on the VPS

```bash
# Watch live FIX messages (market hours: Mon–Fri 09:30–16:00 ET):
sudo -u gateway PYTHONPATH=/opt/noc-gateway/src \
  /opt/noc-gateway/venv/bin/python -m gateway.fix_client_demo --seconds 30

# Inject a fault and watch the dashboard react:
curl -X POST localhost:9103/chaos/latency -H 'Content-Type: application/json' \
  -d '{"ms": 200, "duration_s": 60}'
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `No such file or directory` for `redis` or `docker compose` not found | Start Docker Desktop / Docker daemon, or install `docker compose` |
| `FATAL: required environment variable ALPACA_API_KEY is not set` | `set -a; source .env; set +a` before running Python |
| `fix_client_demo` prints only `Heartbeat` | Market is closed; IEX data flows only during US market hours. The FIX session is still healthy. |
| `chaos_api` logs a Grafana 404 or warning | Ignore it if you are not running Grafana, or set `GRAFANA_API_TOKEN` correctly. |
| Redis connection refused | Make sure `docker compose -f deploy/docker-compose.redis.yml up -d` succeeded and `redis-cli ping` returns `PONG`. |

---

## Honest scope notes

- The FIX engine is a session/application-layer *simulation* built for observability practice — not a certified implementation.
- Market data is Alpaca's free IEX feed (streams during US market hours only).
- Single-node by design: the interesting part is the telemetry and failure handling, not horizontal scale.
