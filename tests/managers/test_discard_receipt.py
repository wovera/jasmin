import json
import pickle
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from smpp.pdu.pdu_types import CommandStatus
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.content import Discard
from jasmin.managers.dlr import DLRLookup
from jasmin.managers.listeners import OUTCOME_PUBLISH_MAX_ATTEMPTS
from jasmin.tools.jitter import BACKOFF_CEILING_SECONDS
from jasmin.tools.qos import LearnedRate
from tests.managers.listener_fixture import ListenerFixture


class DiscardReceiptTestCase(ListenerFixture, TestCase):
    def _listener(self, submit_sm_throughput=0):
        listener, broker = super()._listener('discard-probe', submit_sm_throughput)
        listener.rejectMessage = Mock(return_value=defer.succeed(None))
        return listener, broker

    def _message(self, expiration=None, created_at=None, body=None):
        return super()._message('discard-probe', expiration, created_at, body)

    @defer.inlineCallbacks
    def test_expired_message_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        yield listener.submit_sm_callback(self._message(datetime.now() - timedelta(seconds=1)))
        listener.rejectMessage.assert_called_once()
        broker.publish_confirmed.assert_called_once()
        kwargs = broker.publish_confirmed.call_args.kwargs
        self.assertEqual(kwargs['routing_key'], 'dlr.discard')
        self.assertEqual(kwargs['content'].properties['message-id'], 'discard-probe')
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'expired')
        self.assertEqual(kwargs['content'].properties['headers']['connector'], 'discard-probe')

    @defer.inlineCallbacks
    def test_non_submit_sm_object_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        message = self._message(body=pickle.dumps({'not': 'a pdu'}))
        yield listener.submit_sm_callback(message)
        broker.publish_confirmed.assert_called_once()
        kwargs = broker.publish_confirmed.call_args.kwargs
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'tlv')

    @defer.inlineCallbacks
    def test_over_aged_unbound_connector_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=False)
        over_aged = datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1)
        message = self._message(created_at=over_aged)
        yield listener.submit_sm_callback(message)
        broker.publish_confirmed.assert_called_once()
        kwargs = broker.publish_confirmed.call_args.kwargs
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'smppc_not_ready')

    @defer.inlineCallbacks
    def test_send_failure_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=Exception('boom'))
        message = self._message()
        yield listener.submit_sm_callback(message)
        broker.publish_confirmed.assert_called_once()
        kwargs = broker.publish_confirmed.call_args.kwargs
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'exception')

    @defer.inlineCallbacks
    def test_revoked_batch_is_discarded_before_the_wire(self):
        listener, broker = self._listener(submit_sm_throughput=1)
        listener.qos_last_submit_sm_at = datetime.now()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.redisClient.exists = Mock(return_value=defer.succeed(1))
        message = self._message(datetime.now() + timedelta(hours=1))
        message.content.properties['headers']['tag_batch'] = 'b-1'
        with patch('jasmin.managers.listeners.qos.slow_down', Mock(return_value=defer.succeed(None))) as slow_down:
            yield listener.submit_sm_callback(message)
        slow_down.assert_not_called()
        listener.SMPPClientFactory.smpp.sendDataRequest.assert_not_called()
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['content'].properties['headers']['discard_reason'], 'revoked')

    def test_a_real_send_pays_the_pacing_slot(self):
        listener, broker = self._listener(submit_sm_throughput=1)
        listener.qos_last_submit_sm_at = datetime.now()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(return_value=defer.Deferred())
        with patch('jasmin.managers.listeners.qos.slow_down', Mock(return_value=defer.succeed(None))) as slow_down:
            listener.submit_sm_callback(self._message())
        slow_down.assert_called_once()
        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()

    @defer.inlineCallbacks
    def test_a_failed_receipt_publish_requeues_instead_of_dropping(self):
        listener, broker = self._listener()
        broker.publish_confirmed = Mock(return_value=defer.fail(Exception('broker down')))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        message = self._message(datetime.now() - timedelta(seconds=1))
        yield listener.submit_sm_callback(message)
        listener.rejectMessage.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once()
        self.assertEqual(listener.rejectAndRequeueMessage.call_args.args, (message,))

    def _failing_discards(self, broker, parking):
        self.attempted = []

        def publish(exchange, routing_key, content, mandatory=True):
            self.attempted.append(routing_key)
            if routing_key == 'dlr.discard.parked':
                return parking()
            return defer.fail(Exception('dlr.discard unroutable'))
        broker.publish_confirmed = Mock(side_effect=publish)

    @defer.inlineCallbacks
    def test_a_discard_failing_for_good_is_parked_durably_before_the_submit_is_removed(self):
        listener, broker = self._listener()
        self._failing_discards(broker, parking=lambda: defer.succeed(None))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        message = self._message(datetime.now() - timedelta(seconds=1))

        for _ in range(OUTCOME_PUBLISH_MAX_ATTEMPTS):
            yield listener.submit_sm_callback(message)
        listener.rejectMessage.assert_not_called()
        yield listener.submit_sm_callback(message)

        self.assertEqual(listener.rejectAndRequeueMessage.call_count, OUTCOME_PUBLISH_MAX_ATTEMPTS)
        self.assertEqual(self.attempted, ['dlr.discard'] * OUTCOME_PUBLISH_MAX_ATTEMPTS + ['dlr.discard.parked'])
        broker.named_queue_declare.assert_called_with(queue='dlr.discard.parked', durable=True)
        parked = broker.publish_confirmed.call_args.kwargs
        self.assertEqual((parked['exchange'], parked['routing_key']), ('', 'dlr.discard.parked'))
        self.assertEqual(parked['content'].properties['headers']['discard_reason'], 'expired')
        self.assertEqual(parked['content'].properties['delivery-mode'], 2)
        listener.rejectMessage.assert_called_once_with(message)
        self.assertEqual(listener.discard_retrials, {})

    @defer.inlineCallbacks
    def test_a_discard_that_cannot_be_parked_either_is_never_removed(self):
        listener, broker = self._listener()
        self._failing_discards(broker, parking=lambda: defer.fail(Exception('parking unroutable')))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        message = self._message(datetime.now() - timedelta(seconds=1))

        for _ in range(OUTCOME_PUBLISH_MAX_ATTEMPTS + 10):
            yield listener.submit_sm_callback(message)

        listener.rejectMessage.assert_not_called()
        delays = [call.kwargs['delay'] for call in listener.rejectAndRequeueMessage.call_args_list]
        self.assertEqual(len(delays), OUTCOME_PUBLISH_MAX_ATTEMPTS + 10)
        self.assertLessEqual(delays[0], 1.25 * listener.SMPPClientFactory.config.requeue_delay)
        self.assertGreaterEqual(delays[-1], 0.75 * BACKOFF_CEILING_SECONDS)
        self.assertLessEqual(max(delays), 1.25 * BACKOFF_CEILING_SECONDS)

    @defer.inlineCallbacks
    def test_not_ready_discard_releases_the_idempotency_claim(self):
        listener, broker = self._listener()
        listener.redisClient.delete = Mock(return_value=defer.succeed(1))
        over_aged = datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1)

        yield listener.submit_sm_callback(self._message(created_at=over_aged))

        listener.redisClient.delete.assert_called_once_with('idem:discard-probe')
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['content'].properties['headers']['discard_reason'],
                         'smppc_not_ready')

    @defer.inlineCallbacks
    def test_not_ready_discard_waits_while_the_idempotency_claim_cannot_be_released(self):
        listener, broker = self._listener()
        listener.redisClient.delete = Mock(return_value=defer.fail(Exception('redis down')))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        over_aged = datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1)
        message = self._message(created_at=over_aged)

        yield listener.submit_sm_callback(message)

        listener.rejectMessage.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once()
        self.assertEqual(listener.rejectAndRequeueMessage.call_args.args, (message,))

    @defer.inlineCallbacks
    def test_not_ready_discard_keeps_the_idempotency_claim_until_its_receipt_is_confirmed(self):
        listener, broker = self._listener()
        listener.redisClient.delete = Mock(return_value=defer.succeed(1))
        broker.publish_confirmed = Mock(return_value=defer.fail(Exception('dlr.discard unconfirmed')))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        over_aged = datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1)

        yield listener.submit_sm_callback(self._message(created_at=over_aged))

        listener.redisClient.delete.assert_not_called()

    @defer.inlineCallbacks
    def test_other_discards_keep_the_idempotency_claim(self):
        listener, _ = self._listener()
        listener.redisClient.delete = Mock(return_value=defer.succeed(1))

        yield listener.submit_sm_callback(self._message(datetime.now() - timedelta(seconds=1)))

        listener.redisClient.delete.assert_not_called()

    @defer.inlineCallbacks
    def test_an_unreadable_body_still_rearms_the_consumer(self):
        listener, broker = self._listener()

        yield listener.submit_sm_callback(self._message(body=b'not a pickle'))

        listener.submit_sm_q.get.assert_called_once()
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['content'].properties['headers']['discard_reason'],
                         'exception')

    @defer.inlineCallbacks
    def test_revoke_check_fails_open_when_redis_errors(self):
        listener, broker = self._listener()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.redisClient.exists = Mock(side_effect=Exception('down'))
        message = self._message(datetime.now() + timedelta(hours=1))
        message.content.properties['headers']['tag_batch'] = 'b-1'
        yield listener.submit_sm_callback(message)
        listener.SMPPClientFactory.smpp.sendDataRequest.assert_called_once()
        discard_calls = [call for call in broker.publish_confirmed.call_args_list
                         if call.kwargs.get('routing_key') == 'dlr.discard']
        self.assertEqual(discard_calls, [])


class ThrottleRequeueTestCase(ListenerFixture, TestCase):
    def _listener(self, submit_sm_throughput=0):
        listener, _ = super()._listener('throttle-probe', submit_sm_throughput)
        if submit_sm_throughput > 0:
            listener.learned_rate = LearnedRate(submit_sm_throughput)
        listener.ackMessage = Mock(return_value=defer.succeed(None))
        return listener

    def _resp(self, status):
        return super()._resp('throttle-probe', status)

    def _amqp_message(self, msgid='throttle-probe'):
        message = Mock()
        message.content.properties = {'message-id': msgid, 'priority': 1,
                                      'headers': {'created_at': str(datetime.now())}}
        return message

    @defer.inlineCallbacks
    def test_throttled_response_requeues_without_delay_and_learns(self):
        listener = self._listener(submit_sm_throughput=10)
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        listener.submit_retrials['throttle-probe'] = 1
        amqp_message = self._amqp_message()

        yield listener.submit_sm_resp_event(self._resp(CommandStatus.ESME_RTHROTTLED), amqp_message)

        listener.rejectAndRequeueMessage.assert_called_once_with(amqp_message, delay=False)
        self.assertEqual(listener.learned_rate.current, 5)

    @defer.inlineCallbacks
    def test_msgqful_response_takes_the_delayed_path_and_keeps_the_pacer(self):
        listener = self._listener(submit_sm_throughput=10)
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        listener.submit_retrials['throttle-probe'] = 0
        amqp_message = self._amqp_message()

        yield listener.submit_sm_resp_event(self._resp(CommandStatus.ESME_RMSGQFUL), amqp_message)

        listener.rejectAndRequeueMessage.assert_called_once_with(
            amqp_message, delay=listener.config.submit_error_retrial['ESME_RMSGQFUL']['delay'])
        self.assertEqual(listener.learned_rate.current, 10)

    @defer.inlineCallbacks
    def test_throttled_response_without_a_pacer_takes_the_delayed_path(self):
        listener = self._listener(submit_sm_throughput=0)
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        listener.submit_retrials['throttle-probe'] = 1
        amqp_message = self._amqp_message()

        yield listener.submit_sm_resp_event(self._resp(CommandStatus.ESME_RTHROTTLED), amqp_message)

        self.assertIsNone(listener.learned_rate)
        listener.rejectAndRequeueMessage.assert_called_once_with(
            amqp_message, delay=listener.config.submit_error_retrial['ESME_RTHROTTLED']['delay'])

    @defer.inlineCallbacks
    def test_throughput_switched_off_forgets_the_learned_rate(self):
        listener = self._listener(submit_sm_throughput=10)
        listener.learned_rate.throttled()
        listener.SMPPClientFactory.config.submit_sm_throughput = 0
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(
            return_value=defer.succeed(self._resp(CommandStatus.ESME_RTHROTTLED)))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        message = self._message('throttle-probe')
        message.content.properties['priority'] = 1

        yield listener.submit_sm_callback(message)

        self.assertIsNone(listener.learned_rate)
        # An unpaced connector must not take the immediate requeue a learned rate would have paced.
        listener.rejectAndRequeueMessage.assert_called_once_with(
            message, delay=listener.config.submit_error_retrial['ESME_RTHROTTLED']['delay'])

    @defer.inlineCallbacks
    def test_ceiling_rebuilds_when_throughput_flips_live(self):
        listener = self._listener(submit_sm_throughput=0)
        self.assertIsNone(listener.learned_rate)

        listener.SMPPClientFactory.config.submit_sm_throughput = 10
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)

        response = self._resp(CommandStatus.ESME_ROK)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(return_value=defer.succeed(response))
        message = self._message('throttle-probe')
        message.content.properties['priority'] = 1

        yield listener.submit_sm_callback(message)

        discard_calls = [call for call in listener.amqpBroker.publish_confirmed.call_args_list
                         if call.kwargs.get('routing_key') == 'dlr.discard']
        self.assertEqual(discard_calls, [])
        self.assertEqual(listener.learned_rate.ceiling, 10)


class DiscardForwardTestCase(TestCase):
    @defer.inlineCallbacks
    def test_discard_is_forwarded_with_its_reason_and_keeps_the_receipt_map(self):
        config = DLRLookupConfig()
        config.dlr_forward_queue = 'outcomes'
        broker = Mock()
        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        redis = Mock()
        redis.delete = Mock(return_value=defer.succeed(1))
        lookup = DLRLookup(config, broker, redis)
        lookup.ackMessage = Mock(return_value=defer.succeed(None))
        message = Mock()
        content = Discard('m1', 'smppc_not_ready', 'smppc-1')
        message.content = content
        message.routing_key = 'dlr.discard'

        yield lookup.discard_dlr_callback(message)

        body = json.loads(broker.publish_confirmed.call_args.kwargs['content'].body)
        self.assertEqual(body['msgid'], 'm1')
        self.assertEqual(body['command_status'], 'DISCARDED')
        self.assertEqual(body['discard_reason'], 'smppc_not_ready')
        self.assertEqual(body['connector'], 'smppc-1')
        self.assertEqual(broker.publish_confirmed.call_args.kwargs['routing_key'], 'outcomes')
        redis.delete.assert_not_called()
        lookup.ackMessage.assert_called_once()
