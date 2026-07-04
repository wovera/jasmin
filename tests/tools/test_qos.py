from datetime import datetime, timedelta

from twisted.trial import unittest

from jasmin.tools.qos import throttle_delay


class ThrottleDelayTestCase(unittest.TestCase):
    def test_sub_one_per_second_keeps_the_whole_seconds(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        last = now - timedelta(seconds=0.5)
        # 0.5/s -> 2s interval, 0.5s elapsed -> wait 1.5s. Reading only the microseconds
        # component of the remainder would drop the whole second and return ~0.5s.
        self.assertApproximates(throttle_delay(0.5, last, now), 1.5, 0.01)

    def test_within_rate_returns_fractional_delay(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        last = now - timedelta(seconds=0.05)
        self.assertApproximates(throttle_delay(10, last, now), 0.05, 0.005)

    def test_slower_than_rate_returns_zero(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        last = now - timedelta(seconds=5)
        self.assertEqual(throttle_delay(1, last, now), 0)

    def test_zero_throughput_returns_zero(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        self.assertEqual(throttle_delay(0, now, now), 0)
