import pickle
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from uuid import uuid4

from smpp.pdu.error import (SMPPClientSessionStateError, SMPPGenericNackTransactionError, SMPPProtocolError,
                            SMPPRequestTimoutError)
from smpp.pdu.operations import GenericNack, SubmitSMResp
from smpp.pdu.pdu_types import CommandStatus
from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from jasmin.managers import listeners as listeners_module
from jasmin.managers.listeners import reactor as listener_reactor
from jasmin.managers.listeners import OUTCOME_KEY, OUTCOME_PUBLISH_MAX_ATTEMPTS, WIRE_CLAIM_KEY
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.error import LongSubmitSmTransactionError
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.redis.client import SimpleConnection
from jasmin.routing.Bills import SubmitSmBill
from jasmin.routing.jasminApi import Group, User
from tests.managers.listener_fixture import ListenerFixture


def transmitted_then(failure):
    """What the SMPP client does when the request left on the wire and the transaction then failed."""
    def send(pdu):
        pdu.transmitted = True
        return defer.fail(failure)
    return send


class WireClaimFixture(ListenerFixture):
    timeout = 10

    @defer.inlineCallbacks
    def setUp(self):
        self.redis = yield SimpleConnection(reconnect=False)
        self.addCleanup(self.redis.disconnect)
        self.msgid = str(uuid4())
        self.addCleanup(self.redis.delete, [WIRE_CLAIM_KEY % self.msgid, OUTCOME_KEY % self.msgid])

    def _bound_listener(self, submit_sm_throughput=0):
        listener, broker = self._listener('wire-claim', submit_sm_throughput)
        listener.redisClient = self.redis
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        return listener, broker

    def _submit(self, expiration=None, body=None, created_at=None):
        if body is None:
            body = pickle.dumps(SMPPOperationFactory(SMPPClientConfig(id='wire-claim')).SubmitSM(
                source_addr='1423', destination_addr='06155423', short_message='Hello'))
        message = self._message(self.msgid, expiration=expiration, body=body, created_at=created_at)
        message.content.properties['priority'] = 1
        return message

    def _submit_without_validity(self, created_at):
        message = self._submit(created_at=created_at)
        del message.content.properties['headers']['expiration']
        return message

    def _frozen_clock(self):
        class Clock(datetime):
            current = datetime.now()

            @classmethod
            def now(cls, tz=None):
                return cls.current
        self.patch(listeners_module, 'datetime', Clock)
        return Clock

    def _sending(self, listener):
        sent = defer.Deferred()

        def send(pdu):
            sent.callback(pdu)
            return defer.Deferred()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=send)
        return sent

    def _discard_reasons(self, broker):
        return [content.properties['headers']['discard_reason'] for content in self._published(broker, 'dlr.discard')]


class WireClaimTestCase(WireClaimFixture, TestCase):
    @defer.inlineCallbacks
    def test_claim_outlives_the_message_validity(self):
        listener, _ = self._bound_listener()
        sent = self._sending(listener)

        listener.submit_sm_callback(self._submit(expiration=datetime.now() + timedelta(days=3)))
        yield sent

        ttl = yield self.redis.ttl(WIRE_CLAIM_KEY % self.msgid)
        self.assertGreater(ttl, 3 * 86400 - 10)
        self.assertGreater(ttl, listener.SMPPClientFactory.config.dlr_expiry)

    @defer.inlineCallbacks
    def test_a_submit_without_validity_gets_a_deadline_its_claim_outlives(self):
        listener, _ = self._bound_listener()
        sent = self._sending(listener)
        dlr_expiry = listener.SMPPClientFactory.config.dlr_expiry

        listener.submit_sm_callback(self._submit_without_validity(created_at=datetime.now() - timedelta(hours=1)))
        yield sent

        ttl = yield self.redis.ttl(WIRE_CLAIM_KEY % self.msgid)
        self.assertApproximates(ttl, (dlr_expiry - 3600) + dlr_expiry, 10)

    @defer.inlineCallbacks
    def test_a_sent_submit_redelivered_past_its_deadline_is_in_doubt_and_never_resent(self):
        listener, broker = self._bound_listener()
        yield self.redis.set(WIRE_CLAIM_KEY % self.msgid, 'wire-claim')
        dlr_expiry = listener.SMPPClientFactory.config.dlr_expiry

        yield listener.submit_sm_callback(
            self._submit_without_validity(created_at=datetime.now() - timedelta(seconds=dlr_expiry + 10)))

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])

    @defer.inlineCallbacks
    def test_a_submit_past_its_deadline_that_never_went_out_is_expired(self):
        listener, broker = self._bound_listener()
        dlr_expiry = listener.SMPPClientFactory.config.dlr_expiry

        yield listener.submit_sm_callback(
            self._submit_without_validity(created_at=datetime.now() - timedelta(seconds=dlr_expiry + 10)))

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['expired'])

    @defer.inlineCallbacks
    def test_timed_out_submit_is_discarded_in_doubt_and_never_requeued(self):
        listener, broker = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=transmitted_then(SMPPRequestTimoutError('no submit_sm_resp')))
        message = self._submit()

        yield listener.submit_sm_callback(message)

        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])
        message.channel.basic_reject.assert_called_once_with(delivery_tag=message.delivery_tag, requeue=0)
        listener.rejectAndRequeueMessage.assert_not_called()

    @defer.inlineCallbacks
    def test_long_submit_transaction_failure_is_discarded_in_doubt(self):
        listener, broker = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=transmitted_then(LongSubmitSmTransactionError('segment 2 not sent')))

        yield listener.submit_sm_callback(self._submit())

        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])
        listener.rejectAndRequeueMessage.assert_not_called()

    @defer.inlineCallbacks
    def test_an_unbind_cancelling_a_transmitted_submit_is_in_doubt(self):
        listener, broker = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=transmitted_then(SMPPClientSessionStateError('Unbind received')))

        yield listener.submit_sm_callback(self._submit())

        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])

    @defer.inlineCallbacks
    def test_redelivery_of_an_in_flight_submit_never_reaches_the_wire(self):
        listener, broker = self._bound_listener()
        sent = self._sending(listener)
        listener.submit_sm_callback(self._submit())
        yield sent

        # The broker hands the unacked delivery back after its channel died.
        yield listener.submit_sm_callback(self._submit())

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()
        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])

    @defer.inlineCallbacks
    def test_unavailable_claim_store_holds_the_submit_back(self):
        listener, broker = self._bound_listener()
        listener.redisClient = Mock()
        listener.redisClient.get = Mock(return_value=defer.succeed(None))
        listener.redisClient.set = Mock(return_value=defer.fail(ConnectionError('redis down')))
        message = self._submit()

        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once_with(message)
        self.assertEqual(self._discard_reasons(broker), [])

    @defer.inlineCallbacks
    def test_retryable_refusal_releases_the_claim_so_the_retry_is_sent(self):
        listener, _ = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=lambda pdu: defer.succeed(self._resp('wire-claim', CommandStatus.ESME_RSYSERR)))

        yield listener.submit_sm_callback(self._submit())
        self.assertIsNone((yield self.redis.get(WIRE_CLAIM_KEY % self.msgid)))
        listener.rejectAndRequeueMessage.assert_called_once()

        yield listener.submit_sm_callback(self._submit())
        self.assertEqual(listener.SMPPClientFactory.smpp.sendDataRequest.call_count, 2)

    @defer.inlineCallbacks
    def test_a_refused_submit_is_requeued_only_once_its_claim_is_released(self):
        listener, _ = self._bound_listener()
        clock = task.Clock()
        self.patch(listeners_module, 'reactor', clock)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=lambda pdu: defer.succeed(self._resp('wire-claim', CommandStatus.ESME_RSYSERR)))
        listener.redisClient = Mock(get=self.redis.get, set=self.redis.set, delete=Mock(
            side_effect=[defer.fail(ConnectionError('redis down')), defer.succeed(1)]))

        yield listener.submit_sm_callback(self._submit())
        listener.rejectAndRequeueMessage.assert_not_called()

        clock.advance(listener.SMPPClientFactory.config.requeue_delay)

        self.assertEqual(listener.redisClient.delete.call_count, 2)
        listener.rejectAndRequeueMessage.assert_called_once()

    @defer.inlineCallbacks
    def test_accepted_submit_keeps_its_claim(self):
        listener, _ = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            return_value=defer.succeed(self._resp('wire-claim', CommandStatus.ESME_ROK)))

        yield listener.submit_sm_callback(self._submit())

        self.assertEqual((yield self.redis.get(WIRE_CLAIM_KEY % self.msgid)), 'wire-claim')

    @defer.inlineCallbacks
    def test_unexpected_failure_after_the_wire_is_in_doubt(self):
        listener, broker = self._bound_listener()
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=transmitted_then(
            SMPPProtocolError('Invalid PDU response type', CommandStatus.ESME_RINVCMDID)))

        yield listener.submit_sm_callback(self._submit())

        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])

    @defer.inlineCallbacks
    def test_definite_refusals_are_not_in_doubt(self):
        request = self._resp('wire-claim', CommandStatus.ESME_ROK).request
        for send in (lambda pdu: defer.fail(SMPPClientSessionStateError('Not bound')),
                     transmitted_then(SMPPGenericNackTransactionError(
                         GenericNack(status=CommandStatus.ESME_RINVCMDLEN), request))):
            listener, broker = self._bound_listener()
            listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=send)
            yield self.redis.delete(WIRE_CLAIM_KEY % self.msgid)

            yield listener.submit_sm_callback(self._submit())

            self.assertEqual(self._discard_reasons(broker), ['exception'])


    @defer.inlineCallbacks
    def test_without_redis_a_submit_is_held_back_and_never_sent(self):
        listener, broker = self._bound_listener()
        listener.redisClient = None
        message = self._submit()

        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once_with(message)
        self.assertEqual(self._discard_reasons(broker), [])

    @defer.inlineCallbacks
    def test_a_paced_long_submit_whose_segments_cannot_all_leave_before_its_deadline_is_expired_unsent(self):
        listener, broker = self._bound_listener(submit_sm_throughput=1)
        message = self._submit(expiration=datetime.now() + timedelta(seconds=1),
                               body=pickle.dumps(SMPPOperationFactory(SMPPClientConfig(id='wire-claim')).SubmitSM(
                                   source_addr='1423', destination_addr='06155423', short_message='x' * 400)))

        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['expired'])

    @defer.inlineCallbacks
    def test_a_submit_whose_deadline_passes_while_it_is_paced_is_expired_unsent(self):
        listener, broker = self._bound_listener(submit_sm_throughput=1)
        clock = self._frozen_clock()
        listener.qos_last_submit_sm_at = clock.current
        message = self._submit(expiration=clock.current + timedelta(seconds=0.2))

        def slow_down(seconds):
            clock.current += timedelta(seconds=seconds)
            return defer.succeed(None)
        with patch('jasmin.managers.listeners.qos.slow_down', slow_down):
            yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['expired'])


    def _slow_claim(self, listener, clock, seconds):
        claim = listener.claimWire

        def slow(msgid, headers):
            clock.current += timedelta(seconds=seconds)
            return claim(msgid, headers)
        listener.claimWire = slow

    @defer.inlineCallbacks
    def test_a_submit_whose_deadline_passes_while_its_claim_is_taken_is_expired_unsent_and_unclaimed(self):
        listener, broker = self._bound_listener()
        clock = self._frozen_clock()
        self._slow_claim(listener, clock, 5)

        yield listener.submit_sm_callback(self._submit(expiration=clock.current + timedelta(seconds=2)))

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['expired'])
        self.assertIsNone((yield self.redis.get(WIRE_CLAIM_KEY % self.msgid)))

    @defer.inlineCallbacks
    def test_the_send_deadline_counts_the_time_the_claim_took(self):
        listener, _ = self._bound_listener()
        clock = self._frozen_clock()
        self._slow_claim(listener, clock, 5)
        sent = self._sending(listener)

        listener.submit_sm_callback(self._submit(expiration=clock.current + timedelta(seconds=60)))
        pdu = yield sent

        self.assertApproximates(pdu.send_by - listener_reactor.seconds(), 55, 1)

class OutcomeHandoffTestCase(WireClaimFixture, TestCase):
    def _accepting_listener(self, bill=None):
        listener, broker = self._bound_listener()
        listener.ackMessage = Mock(return_value=defer.succeed(None))
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            side_effect=lambda pdu: defer.succeed(self._resp('wire-claim', CommandStatus.ESME_ROK)))
        message = self._submit()
        if bill is not None:
            message.content.properties['headers']['submit_sm_bill'] = pickle.dumps(bill)
        return listener, broker, message

    def _bill(self, amount):
        bill = SubmitSmBill(User('u1', Group('g1'), 'username', 'password'))
        bill.setAmount('submit_sm_resp', amount)
        return bill

    @defer.inlineCallbacks
    def test_an_accepted_submit_is_acked_only_once_its_receipt_is_confirmed(self):
        listener, broker, message = self._accepting_listener()
        confirmation, published = defer.Deferred(), defer.Deferred()

        def publish(**kwargs):
            published.callback(None)
            return confirmation
        broker.publish_confirmed = Mock(side_effect=publish)

        sending = listener.submit_sm_callback(message)
        yield published
        listener.ackMessage.assert_not_called()

        confirmation.callback(None)
        yield sending

        listener.ackMessage.assert_called_once_with(message)
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['routing_key'], 'dlr.submit_sm_resp')

    @defer.inlineCallbacks
    def test_an_unconfirmed_outcome_is_replayed_by_the_redelivery_without_sending_again(self):
        listener, broker, message = self._accepting_listener()
        broker.publish_confirmed = Mock(return_value=defer.fail(IOError('dlr.submit_sm_resp unroutable')))

        yield listener.submit_sm_callback(message)

        listener.ackMessage.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once()
        unconfirmed = broker.publish_confirmed.call_args.kwargs['content']

        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()
        replayed = broker.publish_confirmed.call_args.kwargs['content']
        self.assertEqual((replayed.body, replayed.properties), (unconfirmed.body, unconfirmed.properties))
        listener.ackMessage.assert_called_once_with(message)

    @defer.inlineCallbacks
    def test_a_settled_redelivery_leaves_no_retry_tracking(self):
        listener, _ = self._bound_listener()
        yield self.redis.set(WIRE_CLAIM_KEY % self.msgid, 'wire-claim')
        for outcome in (pickle.dumps([]), None):
            if outcome is None:
                yield self.redis.delete(OUTCOME_KEY % self.msgid)
            else:
                yield self.redis.set(OUTCOME_KEY % self.msgid, outcome)
            message = self._submit()

            yield listener.submit_sm_callback(message)

            self.assertEqual(len(message.channel.method_calls), 1)
            self.assertNotIn(self.msgid, listener.submit_retrials)

    @defer.inlineCallbacks
    def test_a_bill_whose_confirmation_is_lost_is_never_requested_again(self):
        listener, broker, message = self._accepting_listener(bill=self._bill(2.0))
        bills = []

        def publish(exchange, routing_key, content, **kwargs):
            if exchange == 'billing':
                bills.append(content)
                return defer.fail(IOError('confirmation lost'))
            return defer.succeed(None)
        broker.publish = Mock(side_effect=publish)
        broker.publish_confirmed = Mock(side_effect=publish)

        yield listener.submit_sm_callback(message)
        yield listener.submit_sm_callback(message)

        self.assertEqual([bill.properties['headers']['amount'] for bill in bills], ['2.0'])
        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()

    @defer.inlineCallbacks
    def test_an_outcome_redis_did_not_take_is_still_replayed_not_doubted(self):
        listener, broker, message = self._accepting_listener()
        store = self.redis

        def set_unless_outcome(key, *args, **kwargs):
            if key.startswith('outcome:'):
                return defer.fail(ConnectionError('redis write failed'))
            return store.set(key, *args, **kwargs)
        listener.redisClient = Mock(get=store.get, delete=store.delete, set=Mock(side_effect=set_unless_outcome))
        broker.publish_confirmed = Mock(return_value=defer.fail(IOError('dlr.submit_sm_resp unconfirmed')))
        yield listener.submit_sm_callback(message)

        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()
        self.assertEqual(self._discard_reasons(broker), [])
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['routing_key'], 'dlr.submit_sm_resp')
        listener.ackMessage.assert_called_once_with(message)

    @defer.inlineCallbacks
    def test_an_answered_submit_is_replayed_even_once_its_connector_is_down_and_it_is_over_aged(self):
        listener, broker, message = self._accepting_listener()
        broker.publish_confirmed = Mock(return_value=defer.fail(IOError('dlr.submit_sm_resp unconfirmed')))
        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp = None
        message.content.properties['headers']['created_at'] = str(
            datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1))
        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        yield listener.submit_sm_callback(message)

        self.assertEqual(self._discard_reasons(broker), [])
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['routing_key'], 'dlr.submit_sm_resp')
        listener.ackMessage.assert_called_once_with(message)

    @defer.inlineCallbacks
    def test_an_answered_submit_whose_claim_expired_is_still_replayed_not_expired(self):
        listener, broker, message = self._accepting_listener()
        broker.publish_confirmed = Mock(return_value=defer.fail(IOError('dlr.submit_sm_resp unconfirmed')))
        yield listener.submit_sm_callback(message)

        yield self.redis.delete([WIRE_CLAIM_KEY % self.msgid, OUTCOME_KEY % self.msgid])
        message.content.properties['headers']['expiration'] = str(datetime.now() - timedelta(seconds=1))
        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        yield listener.submit_sm_callback(message)

        self.assertEqual(self._discard_reasons(broker), [])
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['routing_key'], 'dlr.submit_sm_resp')
        listener.ackMessage.assert_called_once_with(message)

    @defer.inlineCallbacks
    def test_an_outcome_failing_for_good_is_parked_durably_before_the_submit_is_acked(self):
        listener, broker, message = self._accepting_listener()

        def publish(exchange, routing_key, content, mandatory=True):
            if routing_key == 'dlr.submit_sm_resp.parked':
                return defer.succeed(None)
            return defer.fail(IOError('dlr.submit_sm_resp unroutable'))
        broker.publish_confirmed = Mock(side_effect=publish)

        for _ in range(OUTCOME_PUBLISH_MAX_ATTEMPTS):
            yield listener.submit_sm_callback(message)
        listener.ackMessage.assert_not_called()
        yield listener.submit_sm_callback(message)

        parked = broker.publish_confirmed.call_args.kwargs
        self.assertEqual((parked['exchange'], parked['routing_key']), ('', 'dlr.submit_sm_resp.parked'))
        broker.named_queue_declare.assert_called_with(queue='dlr.submit_sm_resp.parked', durable=True)
        listener.ackMessage.assert_called_once_with(message)
        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()

    @defer.inlineCallbacks
    def test_outcomes_are_mandatory_only_when_they_are_forwarded(self):
        for forwarded in (True, False):
            yield self.redis.delete(WIRE_CLAIM_KEY % self.msgid)
            listener, broker, message = self._accepting_listener()
            listener.config.mandatory_outcomes = forwarded

            yield listener.submit_sm_callback(message)

            self.assertIs(broker.publish_confirmed.call_args.kwargs['mandatory'], forwarded)

    @defer.inlineCallbacks
    def test_a_sent_submit_without_a_recorded_outcome_is_in_doubt(self):
        listener, broker, message = self._accepting_listener()
        yield self.redis.set(WIRE_CLAIM_KEY % self.msgid, 'wire-claim')

        yield listener.submit_sm_callback(message)

        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(self._discard_reasons(broker), ['in_doubt'])


class LongSubmitOutcomeTestCase(WireClaimFixture, TestCase):
    def _long_submit(self):
        return SMPPOperationFactory(SMPPClientConfig(id='wire-claim')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='x' * 400)

    def _answer(self, pdu, statuses):
        segments = []
        while True:
            segments.append(pdu)
            if not hasattr(pdu, 'nextPdu'):
                break
            pdu = pdu.nextPdu
        self.assertEqual(len(segments), len(statuses))
        for sequence, (segment, status) in enumerate(zip(segments, statuses), start=1):
            if status == CommandStatus.ESME_ROK:
                segment.response = SubmitSMResp(seqNum=sequence, status=status, message_id=('0%s' % sequence).encode())
            else:
                segment.response = SubmitSMResp(seqNum=sequence, status=status)
        result = Mock()
        result.request = segments[0]
        result.response = segments[-1].response
        return result

    def _amqp_message(self, bill_amount=None):
        message = Mock()
        message.content.properties = {'message-id': self.msgid, 'priority': 1,
                                      'headers': {'created_at': str(datetime.now())}}
        if bill_amount is not None:
            bill = SubmitSmBill(User('u1', Group('g1'), 'username', 'password'))
            bill.setAmount('submit_sm_resp', bill_amount)
            message.content.properties['headers']['submit_sm_bill'] = pickle.dumps(bill)
        return message

    def _settle(self, statuses, bill_amount=None):
        listener, broker = self._bound_listener()
        listener.ackMessage = Mock(return_value=defer.succeed(None))
        listener.submit_retrials[self.msgid] = 1
        message = self._amqp_message(bill_amount)
        settled = listener.submit_sm_resp_event(self._answer(self._long_submit(), statuses), message)
        return settled, listener, broker, message

    def _billed(self, broker):
        return [float(call.kwargs['content'].properties['headers']['amount'])
                for call in broker.publish.call_args_list if call.kwargs['exchange'] == 'billing']

    @defer.inlineCallbacks
    def test_a_partly_accepted_long_message_reports_the_refusal_and_bills_accepted_segments_only(self):
        settled, listener, broker, message = self._settle(
            [CommandStatus.ESME_ROK, CommandStatus.ESME_ROK, CommandStatus.ESME_RTHROTTLED], bill_amount=1.5)
        yield settled

        listener.rejectAndRequeueMessage.assert_not_called()
        listener.ackMessage.assert_called_once_with(message)
        (receipt,) = self._published(broker, 'dlr.submit_sm_resp')
        self.assertEqual(receipt.body, 'ESME_RTHROTTLED')
        self.assertEqual(receipt.properties['headers']['accepted_segments'], 2)
        self.assertEqual(receipt.properties['headers']['smpp_msgids'], '1,2')
        self.assertEqual(self._billed(broker), [3.0])

    @defer.inlineCallbacks
    def test_a_refused_first_segment_does_not_hide_the_accepted_ones(self):
        settled, listener, broker, _ = self._settle(
            [CommandStatus.ESME_RSYSERR, CommandStatus.ESME_ROK, CommandStatus.ESME_ROK], bill_amount=1.5)
        yield settled

        listener.rejectAndRequeueMessage.assert_not_called()
        (receipt,) = self._published(broker, 'dlr.submit_sm_resp')
        self.assertEqual(receipt.body, 'ESME_RSYSERR')
        self.assertEqual(receipt.properties['headers']['accepted_segments'], 2)
        self.assertEqual(self._billed(broker), [3.0])

    @defer.inlineCallbacks
    def test_a_fully_accepted_long_message_bills_every_segment(self):
        settled, _, broker, _ = self._settle([CommandStatus.ESME_ROK] * 3, bill_amount=1.5)
        yield settled

        (receipt,) = self._published(broker, 'dlr.submit_sm_resp')
        self.assertEqual(receipt.body, 'ESME_ROK')
        self.assertNotIn('accepted_segments', receipt.properties['headers'])
        self.assertEqual(receipt.properties['headers']['smpp_msgids'], '1,2,3')
        self.assertEqual(self._billed(broker), [4.5])

    @defer.inlineCallbacks
    def test_long_message_refused_in_every_segment_is_retried_whole_and_billed_nothing(self):
        yield self.redis.set(WIRE_CLAIM_KEY % self.msgid, 'wire-claim')
        settled, listener, broker, _ = self._settle([CommandStatus.ESME_RSYSERR] * 3, bill_amount=1.5)
        yield settled

        listener.rejectAndRequeueMessage.assert_called_once()
        self.assertIsNone((yield self.redis.get(WIRE_CLAIM_KEY % self.msgid)))
        self.assertEqual(broker.publish_confirmed.call_args_list, [])
        self.assertEqual(self._billed(broker), [])

    def _accepted_then(self, failure):
        def send(pdu):
            for segment in (pdu, pdu.nextPdu):
                segment.transmitted = True
            pdu.response = SubmitSMResp(seqNum=1, status=CommandStatus.ESME_ROK, message_id=b'0A')
            return defer.fail(failure)
        return send

    @defer.inlineCallbacks
    def _fail_after_an_accepted_segment(self, failure):
        listener, broker = self._bound_listener()
        listener.ackMessage = Mock(return_value=defer.succeed(None))
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=self._accepted_then(failure))
        message = self._submit(body=pickle.dumps(self._long_submit()))

        yield listener.submit_sm_callback(message)

        listener.rejectAndRequeueMessage.assert_not_called()
        listener.ackMessage.assert_called_once_with(message)
        self.assertEqual(self._discard_reasons(broker), [])
        (receipt,) = self._published(broker, 'dlr.submit_sm_resp')
        self.assertEqual(receipt.properties['headers']['smpp_msgids'], 'A')
        self.assertEqual(receipt.properties['headers']['accepted_segments'], 1)
        self.assertEqual(receipt.properties['headers']['discard_reason'], 'in_doubt')
        recorded = pickle.loads((yield self.redis.get(OUTCOME_KEY % self.msgid)))
        self.assertEqual(recorded, [])
        self.assertIsNotNone((yield self.redis.get(WIRE_CLAIM_KEY % self.msgid)))

    def test_a_timeout_after_an_accepted_segment_keeps_its_id_and_count_and_stays_in_doubt(self):
        return self._fail_after_an_accepted_segment(SMPPRequestTimoutError('segment 2 unanswered'))

    def test_a_nack_after_an_accepted_segment_keeps_its_id_and_count_and_stays_in_doubt(self):
        nack = GenericNack(seqNum=2, status=CommandStatus.ESME_RINVCMDLEN)
        return self._fail_after_an_accepted_segment(SMPPGenericNackTransactionError(nack, None))

    @defer.inlineCallbacks
    def test_segments_are_paced_at_the_learned_rate(self):
        listener, _ = self._bound_listener(submit_sm_throughput=2)
        clock = self._frozen_clock()
        listener.qos_last_submit_sm_at = clock.current
        sent = self._sending(listener)
        message = self._submit(body=pickle.dumps(self._long_submit()))
        waited = []

        def slow_down(seconds):
            waited.append(seconds)
            clock.current += timedelta(seconds=seconds)
            return defer.succeed(None)
        with patch('jasmin.managers.listeners.qos.slow_down', slow_down):
            listener.submit_sm_callback(message)
            sent = yield sent

        self.assertEqual(sent.segment_interval, 0.5)
        self.assertEqual(waited, [0.5])
        # The next submit waits for the last segment's slot, one interval after each earlier segment.
        self.assertEqual(listener.qos_last_submit_sm_at - clock.current, timedelta(seconds=1))
