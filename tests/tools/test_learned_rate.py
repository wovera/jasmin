from datetime import datetime, timedelta
from twisted.trial.unittest import TestCase
from jasmin.tools.qos import LearnedRate, throttle_delay


class LearnedRateTestCase(TestCase):
    def test_halves_on_throttle_and_never_below_one(self):
        rate = LearnedRate(ceiling=10)
        self.assertEqual(rate.current, 10)
        rate.throttled()
        self.assertEqual(rate.current, 5)
        for _ in range(10):
            rate.throttled()
        self.assertEqual(rate.current, 1)

    def test_recovers_one_per_clean_second_up_to_the_ceiling(self):
        rate = LearnedRate(ceiling=10)
        rate.throttled()
        t0 = datetime(2026, 1, 1)
        rate.clean(t0)
        rate.clean(t0 + timedelta(seconds=1))
        self.assertEqual(rate.current, 6)
        for i in range(2, 30):
            rate.clean(t0 + timedelta(seconds=i))
        self.assertEqual(rate.current, 10)

    def test_credits_the_whole_elapsed_gap_not_one_per_call(self):
        rate = LearnedRate(ceiling=10)
        rate.throttled()
        t0 = datetime(2026, 1, 1)
        rate.clean(t0)
        rate.clean(t0 + timedelta(seconds=10))
        self.assertEqual(rate.current, 10)

    def test_a_burst_inside_the_same_second_adds_nothing(self):
        rate = LearnedRate(ceiling=10)
        rate.throttled()
        t0 = datetime(2026, 1, 1)
        rate.clean(t0)
        rate.clean(t0 + timedelta(milliseconds=300))
        rate.clean(t0 + timedelta(milliseconds=600))
        rate.clean(t0 + timedelta(milliseconds=900))
        self.assertEqual(rate.current, 5)


class ThrottleDelayCountTestCase(TestCase):
    def test_multi_pdu_send_scales_the_delay_by_count(self):
        t0 = datetime(2026, 1, 1)
        self.assertApproximates(throttle_delay(10, t0, t0, count=3), 0.3, 0.005)
