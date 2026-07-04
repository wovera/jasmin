from twisted.trial import unittest

from jasmin.tools.jitter import jittered


class JitteredTestCase(unittest.TestCase):
    def test_stays_within_factor_bounds(self):
        for _ in range(2000):
            value = jittered(120)
            self.assertTrue(90.0 <= value <= 150.0, value)

    def test_mean_is_preserved(self):
        values = [jittered(120) for _ in range(5000)]
        self.assertApproximates(sum(values) / len(values), 120, 3)

    def test_decorrelates_concurrent_callers(self):
        values = [jittered(10) for _ in range(1000)]
        self.assertGreater(len(set(values)), 900)

    def test_custom_factor_widens_the_spread(self):
        for _ in range(2000):
            value = jittered(100, 0.5)
            self.assertTrue(50.0 <= value <= 150.0, value)

    def test_zero_delay_is_unchanged(self):
        self.assertEqual(jittered(0), 0)

    def test_zero_factor_is_unchanged(self):
        self.assertEqual(jittered(10, 0), 10)
