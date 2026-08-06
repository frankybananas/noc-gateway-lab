"""Chaos-fault polling shared by the ingress gateway and FIX engine.

Fault flags are plain Redis keys with TTLs, set by the chaos controller
(chaos_api.py). Each data-plane service polls them once a second so fault
application costs nothing on the hot path. Keys expire automatically, which
guarantees faults are self-healing even if the controller dies mid-experiment.
"""

import asyncio
import logging
import random

import redis.asyncio as aioredis

from gateway import config

log = logging.getLogger("chaos-poller")


class ChaosState:
    """In-memory snapshot of the currently active faults."""

    def __init__(self) -> None:
        self.latency_ms: float = 0.0
        self.drop_percent: float = 0.0
        self.ws_disconnect_requested: bool = False
        self.fix_kill_requested: bool = False

    async def apply_latency(self) -> None:
        """Sleep for the injected latency, if any (call on the hot path)."""
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)

    def should_drop(self) -> bool:
        """Return True if this message should be dropped."""
        return self.drop_percent > 0 and random.uniform(0, 100) < self.drop_percent


async def poll_chaos_flags(r: aioredis.Redis, state: ChaosState, interval: float = 1.0) -> None:
    """Background task: refresh the ChaosState from Redis every `interval` seconds."""
    while True:
        try:
            latency, drop, ws_disc, fix_kill = await asyncio.gather(
                r.get(config.CHAOS_LATENCY_KEY),
                r.get(config.CHAOS_DROP_KEY),
                r.get(config.CHAOS_WS_DISCONNECT_KEY),
                r.get(config.CHAOS_FIX_KILL_KEY),
            )
            state.latency_ms = float(latency) if latency else 0.0
            state.drop_percent = float(drop) if drop else 0.0
            state.ws_disconnect_requested = ws_disc is not None
            state.fix_kill_requested = fix_kill is not None
        except Exception as exc:  # Redis briefly down: fail open (no faults)
            log.warning("chaos flag poll failed: %s", exc)
            state.latency_ms = 0.0
            state.drop_percent = 0.0
        await asyncio.sleep(interval)


async def consume_one_shot(r: aioredis.Redis, key: str) -> None:
    """Delete a one-shot fault flag after acting on it."""
    try:
        await r.delete(key)
    except Exception as exc:
        log.warning("failed to clear one-shot chaos key %s: %s", key, exc)
