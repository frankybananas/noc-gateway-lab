"""Unit tests for the no-coercion telemetry helpers."""

import math
import sys
import unittest
from pathlib import Path

# Make src/ importable without an installed package.
SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gateway.common.telemetry import compute_traffic_expected, feed_staleness, parse_xinfo_group


class TestComputeTrafficExpected(unittest.TestCase):
    def test_all_clear(self):
        self.assertEqual(
            compute_traffic_expected(3, 2, 1.0),
            1.0,
        )

    def test_not_subscribed(self):
        self.assertEqual(compute_traffic_expected(2, 2, 1.0), 0.0)
        self.assertEqual(compute_traffic_expected(0, 2, 1.0), 0.0)

    def test_fix_not_logged_on(self):
        self.assertEqual(compute_traffic_expected(3, 1, 1.0), 0.0)
        self.assertEqual(compute_traffic_expected(3, 0, 1.0), 0.0)

    def test_market_closed(self):
        self.assertEqual(compute_traffic_expected(3, 2, 0.0), 0.0)

    def test_market_nan(self):
        # NaN market_open should fail toward "not expected", not "healthy".
        self.assertEqual(compute_traffic_expected(3, 2, float("nan")), 0.0)

    def test_market_gating_disabled(self):
        self.assertEqual(compute_traffic_expected(3, 2, 0.0, gate_on_market=False), 1.0)


class TestFeedStaleness(unittest.TestCase):
    def test_expected(self):
        self.assertAlmostEqual(feed_staleness(1000.0, 950.0, 1.0), 50.0)

    def test_not_expected_is_nan(self):
        self.assertTrue(math.isnan(feed_staleness(1000.0, 950.0, 0.0)))

    def test_no_last_message(self):
        # No last-message timestamp yet = NaN, even when expected.
        self.assertTrue(math.isnan(feed_staleness(1000.0, float("nan"), 1.0)))


class TestParseXinfoGroup(unittest.TestCase):
    def test_normal(self):
        parsed = parse_xinfo_group({"pending": 5, "lag": 12})
        self.assertEqual(parsed["pending"], 5)
        self.assertEqual(parsed["lag"], 12)

    def test_lag_null(self):
        # Redis returns null for lag when MAXLEN trimming makes it unknown.
        parsed = parse_xinfo_group({"pending": 5, "lag": None})
        self.assertEqual(parsed["pending"], 5)
        self.assertTrue(math.isnan(parsed["lag"]))

    def test_both_missing(self):
        parsed = parse_xinfo_group({})
        self.assertTrue(math.isnan(parsed["pending"]))
        self.assertTrue(math.isnan(parsed["lag"]))


if __name__ == "__main__":
    unittest.main()
