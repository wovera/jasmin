import pickle
from twisted.trial.unittest import TestCase
from smpp.pdu import smpp_time
from datetime import datetime


class SMPPTimeTest(TestCase):
    def test_pickle_unpickle_datetime_with_tz(self):
        """Related to #267

        Pickling tzinfo results in an error:
        Error in submit_sm_errback: __init__() takes exactly 3 arguments (1 given)

        This test will pickle then unpickle tzinfo object to ensure no exception is raised
        """

        tz = smpp_time.FixedOffset(72, 'Paris')

        # Pickle then unpickle
        pickled_tz = pickle.dumps(tz, pickle.HIGHEST_PROTOCOL)
        unpickled_tz = pickle.loads(pickled_tz)

        # Asserts
        self.assertEqual(unpickled_tz.dst(datetime.now()), tz.dst(datetime.now()))
        self.assertEqual(unpickled_tz.utcoffset(datetime.now()), tz.utcoffset(datetime.now()))
        self.assertEqual(unpickled_tz.tzname(datetime.now()), tz.tzname(datetime.now()))

    def test_unparse_absolute_time_rejects_a_sub_tenth_instant(self):
        """The absolute-time format carries one tenths-of-a-second digit, and the encoder derives it from
        microseconds by true division, so any wall-clock instant past .9 raises instead of encoding. A caller
        minting an absolute time from the clock must therefore drop the sub-second part.
        """
        self.assertRaises(ValueError, smpp_time.unparse_absolute_time,
                          datetime(2026, 1, 1, 12, 30, 15, 950000))

    def test_unparse_absolute_time_accepts_a_second_truncated_instant(self):
        """Truncating to whole seconds encodes from every originating microsecond, including the ones that
        otherwise raise.
        """
        for microsecond in (0, 1, 499999, 900000, 900001, 999999):
            minted = datetime(2026, 1, 1, 12, 30, 15, microsecond).replace(microsecond=0)
            self.assertEqual(smpp_time.unparse_absolute_time(minted), b'260101123015000+')
