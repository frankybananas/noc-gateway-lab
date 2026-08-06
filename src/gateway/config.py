"""Central, environment-driven configuration.

All services read settings from environment variables (loaded by systemd via
EnvironmentFile=/etc/noc-gateway/gateway.env). Fails fast with a clear error
when required secrets are missing, so a misconfigured unit dies loudly instead
of silently retrying with bad credentials.
"""

import logging
import os
import sys


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"FATAL: required environment variable {name} is not set", file=sys.stderr)
        sys.exit(2)
    return value


# --- Secrets (required only by services that use them) ---
def alpaca_credentials() -> tuple[str, str]:
    return _require("ALPACA_API_KEY"), _require("ALPACA_SECRET_KEY")


def grafana_config() -> tuple[str, str | None]:
    """Grafana URL and token; token is optional (annotations disabled if absent)."""
    return (
        os.getenv("GRAFANA_URL", "http://127.0.0.1:3000"),
        os.getenv("GRAFANA_API_TOKEN"),
    )


# --- General settings ---
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL,SPY,TSLA").split(",") if s.strip()]

ALPACA_WS_URL = os.getenv("ALPACA_WS_URL", "wss://stream.data.alpaca.markets/v2/iex")

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
STREAM_KEY = os.getenv("STREAM_KEY", "md.ticks")
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "100000"))
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fix-engine")

BIND_HOST = os.getenv("BIND_HOST", "127.0.0.1")
# 9100-range = Prometheus exporter convention; 9101+ avoids node-exporter (9100)
# and Coolify's UI on host port 8000.
INGRESS_METRICS_PORT = int(os.getenv("INGRESS_METRICS_PORT", "9101"))
FIX_METRICS_PORT = int(os.getenv("FIX_METRICS_PORT", "9102"))
CHAOS_PORT = int(os.getenv("CHAOS_PORT", "9103"))
FIX_TCP_PORT = int(os.getenv("FIX_TCP_PORT", "5001"))

# FIX session identity
FIX_SENDER_COMP_ID = os.getenv("FIX_SENDER_COMP_ID", "NOCGW")
FIX_TARGET_COMP_ID = os.getenv("FIX_TARGET_COMP_ID", "CLIENT1")
FIX_HEARTBEAT_INTERVAL = int(os.getenv("FIX_HEARTBEAT_INTERVAL", "30"))

# Chaos flag keys in Redis
CHAOS_LATENCY_KEY = "chaos:latency_ms"
CHAOS_DROP_KEY = "chaos:drop_percent"
CHAOS_WS_DISCONNECT_KEY = "chaos:ws_disconnect"
CHAOS_FIX_KILL_KEY = "chaos:fix_kill"


def setup_logging(service: str) -> logging.Logger:
    """Structured, journald-friendly logging shared by all services."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format=f"%(asctime)s %(levelname)s [{service}] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger(service)
