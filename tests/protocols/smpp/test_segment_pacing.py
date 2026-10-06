from unittest.mock import Mock

from smpp.pdu.error import SMPPGenericNackTransactionError, SMPPProtocolError
from smpp.pdu.operations import DeliverSMResp, EnquireLink, GenericNack
from smpp.pdu.pdu_types import CommandStatus
from smpp.twisted.protocol import SMPPSessionStates
from twisted.internet import task
from twisted.trial.unittest import TestCase

from jasmin.protocols.smpp import protocol as protocol_module
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.error import LongSubmitSmTransactionError
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.protocols.smpp.protocol import SMPPClientProtocol


class ClientProtocolFixture:
    def setUp(self):
        self.clock = task.Clock()
        self.patch(protocol_module, 'reactor', self.clock)
        self.protocol = SMPPClientProtocol()
        self.protocol.factory = Mock()
        self.protocol.callLater = self.clock.callLater
        self.protocol.sessionState = SMPPSessionStates.BOUND_TRX
        self.sent = []
        self.protocol.sendPDU = self.sent.append

    def _submit(self, short_message):
        return SMPPOperationFactory(SMPPClientConfig(id='pacing')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message=short_message)


class SegmentPacingTestCase(ClientProtocolFixture, TestCase):
    def test_segments_leave_one_interval_apart(self):
        pdu = self._submit('x' * 400)
        pdu.segment_interval = 0.5

        self.protocol.doSendRequest(pdu, 30)
        self.assertEqual(self.sent, [pdu])

        self.clock.advance(0.5)
        self.assertEqual(self.sent, [pdu, pdu.nextPdu])
        self.clock.advance(0.5)
        self.assertEqual(self.sent, [pdu, pdu.nextPdu, pdu.nextPdu.nextPdu])

    def test_unpaced_segments_leave_together(self):
        pdu = self._submit('x' * 400)

        self.protocol.doSendRequest(pdu, 30)

        self.assertEqual(self.sent, [pdu, pdu.nextPdu, pdu.nextPdu.nextPdu])

    def test_the_transaction_waits_for_the_paced_segments(self):
        pdu = self._submit('x' * 400)
        pdu.segment_interval = 0.5
        self.protocol.doSendRequest(pdu, 30)

        (pending,) = self.protocol.longSubmitSmTxns.values()
        self.assertEqual(pending['txn'].timer.getTime(), 31)

    def test_a_segment_due_after_the_bind_dropped_fails_the_message_unsent(self):
        pdu = self._submit('x' * 400)
        pdu.segment_interval = 0.5
        transaction = self.protocol.doSendRequest(pdu, 30)

        self.protocol.sessionState = SMPPSessionStates.UNBOUND
        self.clock.advance(1)

        self.assertEqual(self.sent, [pdu])
        self.assertEqual(self.protocol.longSubmitSmTxns, {})
        return self.assertFailure(transaction, LongSubmitSmTransactionError)


class SegmentFailureTestCase(ClientProtocolFixture, TestCase):
    def test_a_nacked_segment_fails_the_message_at_once_and_stops_the_scheduled_ones(self):
        pdu = self._submit('x' * 400)
        pdu.segment_interval = 0.5
        transaction = self.protocol.doSendRequest(pdu, 30)

        self.protocol.endOutboundTransaction(GenericNack(seqNum=pdu.seqNum, status=CommandStatus.ESME_RINVCMDLEN))
        self.clock.advance(1)

        self.assertEqual(self.sent, [pdu])
        self.assertEqual(self.protocol.longSubmitSmTxns, {})
        return self.assertFailure(transaction, SMPPGenericNackTransactionError)

    def test_a_late_failure_of_an_earlier_message_leaves_a_reused_reference_alone(self):
        earlier, later = self._submit('x' * 400), self._submit('y' * 400)
        self.protocol.doSendRequest(earlier, 30).addErrback(lambda _: None)
        self.protocol.closeLongSubmitSmTransaction(earlier.LongSubmitSm['msg_ref_num'])
        for segment in (later, later.nextPdu, later.nextPdu.nextPdu):
            segment.params['sar_msg_ref_num'] = earlier.params['sar_msg_ref_num']
        transaction = self.protocol.doSendRequest(later, 30)

        self.protocol.endOutboundTransaction(GenericNack(seqNum=earlier.seqNum, status=CommandStatus.ESME_RINVCMDLEN))

        self.assertFalse(transaction.called)
        self.assertEqual(len(self.protocol.longSubmitSmTxns), 1)


class SegmentDeadlineTestCase(ClientProtocolFixture, TestCase):
    def test_a_segment_due_after_the_deadline_fails_the_message_unsent(self):
        pdu = self._submit('x' * 400)
        pdu.segment_interval = 0.5
        pdu.send_by = self.clock.seconds() + 0.7
        transaction = self.protocol.doSendRequest(pdu, 30)

        self.clock.advance(0.5)
        self.assertEqual(self.sent, [pdu, pdu.nextPdu])
        self.clock.advance(0.5)

        self.assertEqual(self.sent, [pdu, pdu.nextPdu])
        self.assertEqual(self.protocol.longSubmitSmTxns, {})
        return self.assertFailure(transaction, LongSubmitSmTransactionError)


class SubmitResponseTypeTestCase(ClientProtocolFixture, TestCase):
    def _outbound(self):
        pdu = self._submit('Hello')
        pdu.seqNum = self.protocol.claimSeqNum()
        return pdu, self.protocol.startOutboundTransaction(pdu, 30)

    def test_generic_nack_to_a_submit_fails_its_transaction(self):
        pdu, transaction = self._outbound()

        self.protocol.endOutboundTransaction(GenericNack(seqNum=pdu.seqNum, status=CommandStatus.ESME_RINVCMDLEN))

        return self.assertFailure(transaction, SMPPGenericNackTransactionError)

    def test_mismatched_response_to_a_submit_fails_its_transaction(self):
        pdu, transaction = self._outbound()

        self.protocol.endOutboundTransaction(DeliverSMResp(seqNum=pdu.seqNum))

        return self.assertFailure(transaction, SMPPProtocolError)


class TransmissionTestCase(TestCase):
    def test_a_pdu_written_to_the_session_is_marked_transmitted(self):
        protocol = SMPPClientProtocol()
        protocol.factory = Mock()
        protocol.transport = Mock()
        protocol.onSMPPOperation = Mock()
        pdu = EnquireLink(seqNum=1)
        self.assertFalse(getattr(pdu, 'transmitted', False))

        protocol.sendPDU(pdu)

        protocol.transport.write.assert_called_once()
        self.assertTrue(pdu.transmitted)

    def test_a_pdu_that_cannot_be_encoded_is_not_marked_transmitted(self):
        protocol = SMPPClientProtocol()
        protocol.factory = Mock()
        protocol.transport = Mock()
        protocol.encoder = Mock()
        protocol.encoder.encode.side_effect = ValueError('invalid TLV')
        pdu = EnquireLink(seqNum=1)

        self.assertRaises(ValueError, protocol.sendPDU, pdu)

        protocol.transport.write.assert_not_called()
        self.assertFalse(getattr(pdu, 'transmitted', False))
