from twisted.trial import unittest

from jasmin.tools.jitter import backoff, jittered


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


class BackoffTestCase(unittest.TestCase):
    def test_doubles_from_the_base_delay(self):
        for attempt, expected in enumerate((10, 20, 40, 80, 160)):
            value = backoff(10, attempt)
            self.assertTrue(expected * 0.75 <= value <= expected * 1.25, (attempt, value))

    def test_never_waits_longer_than_the_ceiling(self):
        for attempt in (6, 20, 10 ** 6):
            self.assertLessEqual(backoff(10, attempt, ceiling=300), 300 * 1.25)

    def test_a_base_above_the_ceiling_is_kept(self):
        value = backoff(600, 3, ceiling=300)
        self.assertTrue(450 <= value <= 750, value)

    def test_a_zero_base_still_backs_off(self):
        for attempt, expected in enumerate((1, 2, 4)):
            value = backoff(0, attempt)
            self.assertTrue(expected * 0.75 <= value <= expected * 1.25, (attempt, value))
