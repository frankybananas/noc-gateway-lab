"""Pure telemetry helpers for traffic gating and XINFO parsing.

These functions contain no I/O and are easy to unit test.
"""

import math


def compute_traffic_expected(
    connection_state: int,
    session_state: int,
    market_open: float,
    gate_on_market: bool = True,
) -> float:
    """Return 1.0 when the data path should be actively moving ticks.

    Conditions:
      * feed_state == SUBSCRIBED (3)
      * fix_session_state == LOGGED_ON (2)
      * (optional) market_open == 1.0

    A NaN/None market_open is treated as NOT open, so the metric fails toward
    "not expected" rather than "all good" if the host lacks tzdata.
    """
    if connection_state != 3:
        return 0.0
    if session_state != 2:
        return 0.0
    if gate_on_market and (market_open is None or not (market_open == 1.0)):
        return 0.0
    return 1.0


def feed_staleness(now: float, last_msg_ts: float, expected: float) -> float:
    """Seconds since the last data message, or NaN when no traffic is expected."""
    if expected != 1.0:
        return float("nan")
    if math.isnan(last_msg_ts):
        return float("nan")
    return now - last_msg_ts


def parse_xinfo_group(group: dict) -> dict:
    """Extract pending/lag from redis `XINFO GROUPS` and preserve null as NaN.

    Redis returns `lag` as `null` when `MAXLEN` trimming makes the exact
    undelivered count uncomputable. Coercing that to 0 would be a sensor that
    fails toward healthy, so it is surfaced as NaN instead.
    """
    # pending is a real counter from the consumer group (0 if no entries have
    # been delivered), while lag can be None when MAXLEN trimming makes the
    # exact undelivered count uncomputable.
    pending = group.get("pending", 0)
    lag = group.get("lag")
    return {
        "pending": int(pending) if pending is not None else 0,
        "lag": int(lag) if lag is not None else float("nan"),
    }
