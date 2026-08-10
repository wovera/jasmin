from twisted.trial.unittest import TestCase

from jasmin.protocols.smpp.operations import LongMessageExceedsMaxPartsError, SMPPOperationFactory


class LongContentCapTestCase(TestCase):
    """A message that needs more segments than the connector may send is refused, not shortened. Sending the
    first N segments puts a message on the handset missing its tail, with nothing reporting the loss: the
    caller is told the send succeeded, and is billed for it."""

    MAX_PARTS = 2
    # GSM 03.38 septets per segment once the concatenation UDH is carried.
    SEGMENT_LENGTH = 153

    def _factory(self):
        return SMPPOperationFactory(long_content_max_parts=self.MAX_PARTS)

    def _segmentCount(self, pdu):
        count = 1
        while hasattr(pdu, 'nextPdu'):
            pdu = pdu.nextPdu
            count += 1
        return count

    def _submit(self, characterCount):
        return self._factory().SubmitSM(
            source_addr='1423',
            destination_addr='06155423',
            short_message=b'a' * characterCount,
        )

    def test_a_message_filling_the_cap_exactly_is_built_whole(self):
        pdu = self._submit(self.SEGMENT_LENGTH * self.MAX_PARTS)

        self.assertEqual(self._segmentCount(pdu), self.MAX_PARTS)

    def test_one_character_past_the_cap_is_refused_rather_than_truncated(self):
        # One septet more than the cap can carry: the old behaviour built MAX_PARTS segments and dropped the
        # rest, so the assertion that matters is that nothing is returned at all.
        self.assertRaises(
            LongMessageExceedsMaxPartsError,
            self._submit,
            self.SEGMENT_LENGTH * self.MAX_PARTS + 1,
        )

    def test_the_refusal_names_what_was_needed_and_what_is_allowed(self):
        error = self.assertRaises(
            LongMessageExceedsMaxPartsError,
            self._submit,
            self.SEGMENT_LENGTH * (self.MAX_PARTS + 1),
        )

        self.assertEqual(error.segmentCount, self.MAX_PARTS + 1)
        self.assertEqual(error.maxParts, self.MAX_PARTS)

    def test_a_short_message_is_untouched_by_the_cap(self):
        pdu = self._submit(10)

        self.assertEqual(self._segmentCount(pdu), 1)
