from unittest.mock import Mock

from twisted.trial.unittest import TestCase

from jasmin.protocols.smpp.protocol import MAX_SEQ_NUM, SMPPClientProtocol, SMPPServerProtocol


class SeqNumWrapTestCase(TestCase):
    """SMPP v3.4 §5.1.4 allows sequence_number 0x00000001 to 0x7FFFFFFF. The header encoder accepts the full
    0xFFFFFFFF, so a counter run past the ceiling emits off-spec numbers raising nothing, and only fails at
    2^32 — where the encoder's ValueError reaches a generic handler that discards the message."""

    def _client(self):
        protocol = SMPPClientProtocol()
        protocol.factory = Mock()
        return protocol

    def test_client_restarts_at_one_instead_of_leaving_the_allowed_range(self):
        protocol = self._client()
        protocol.lastSeqNum = MAX_SEQ_NUM - 1

        self.assertEqual(protocol.claimSeqNum(), MAX_SEQ_NUM)
        # Unwrapped this is MAX_SEQ_NUM + 1, which the encoder emits without complaint.
        self.assertEqual(protocol.claimSeqNum(), 1)

    def test_client_never_claims_above_the_ceiling(self):
        protocol = self._client()
        protocol.lastSeqNum = MAX_SEQ_NUM - 3

        claimed = [protocol.claimSeqNum() for _ in range(6)]

        self.assertEqual(max(claimed), MAX_SEQ_NUM)
        self.assertEqual(claimed, [MAX_SEQ_NUM - 2, MAX_SEQ_NUM - 1, MAX_SEQ_NUM, 1, 2, 3])

    def test_server_wraps_too(self):
        # The server claims its own numbers when it sends to a bound ESME, off the same unbounded base counter.
        protocol = SMPPServerProtocol()
        protocol.lastSeqNum = MAX_SEQ_NUM

        self.assertEqual(protocol.claimSeqNum(), 1)
