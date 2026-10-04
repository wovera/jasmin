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
        broker.publish.assert_called_once()
        kwargs = broker.publish.call_args.kwargs
        self.assertEqual(kwargs['routing_key'], 'dlr.discard')
        self.assertEqual(kwargs['content'].properties['message-id'], 'discard-probe')
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'expired')
        self.assertEqual(kwargs['content'].properties['headers']['connector'], 'discard-probe')

    @defer.inlineCallbacks
    def test_non_submit_sm_object_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        message = self._message(body=pickle.dumps({'not': 'a pdu'}))
        yield listener.submit_sm_callback(message)
        broker.publish.assert_called_once()
        kwargs = broker.publish.call_args.kwargs
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'tlv')

    @defer.inlineCallbacks
    def test_over_aged_unbound_connector_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=False)
        over_aged = datetime.now() - timedelta(seconds=listener.config.submit_max_age_smppc_not_ready + 1)
        message = self._message(created_at=over_aged)
        yield listener.submit_sm_callback(message)
        broker.publish.assert_called_once()
        kwargs = broker.publish.call_args.kwargs
        self.assertEqual(kwargs['content'].properties['headers']['discard_reason'], 'smppc_not_ready')

    @defer.inlineCallbacks
    def test_send_failure_emits_a_discard_receipt(self):
        listener, broker = self._listener()
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=Exception('boom'))
        message = self._message()
        yield listener.submit_sm_callback(message)
        broker.publish.assert_called_once()
        kwargs = broker.publish.call_args.kwargs
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
        self.assertEqual(broker.publish.call_args.kwargs['content'].properties['headers']['discard_reason'], 'revoked')

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
        broker.publish = Mock(return_value=defer.fail(Exception('broker down')))
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        message = self._message(datetime.now() - timedelta(seconds=1))
        yield listener.submit_sm_callback(message)
        listener.rejectMessage.assert_not_called()
        listener.rejectAndRequeueMessage.assert_called_once_with(message, delay=True)

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
        discard_calls = [call for call in broker.publish.call_args_list
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
        message.content.properties = {'message-id': msgid, 'priority': 1, 'headers': {}}
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

        discard_calls = [call for call in listener.amqpBroker.publish.call_args_list
                         if call.kwargs.get('routing_key') == 'dlr.discard']
        self.assertEqual(discard_calls, [])
        self.assertEqual(listener.learned_rate.ceiling, 10)


class DiscardForwardTestCase(TestCase):
    @defer.inlineCallbacks
    def test_discard_is_forwarded_with_its_reason(self):
        config = DLRLookupConfig()
        config.dlr_forward_queue = 'outcomes'
        broker = Mock()
        redis = Mock()
        redis.delete = Mock(return_value=defer.succeed(1))
        lookup = DLRLookup(config, broker, redis)
        lookup.forward_chan = Mock()
        lookup.forward_chan.basic_publish = Mock(return_value=defer.succeed(None))
        lookup.ackMessage = Mock(return_value=defer.succeed(None))
        message = Mock()
        content = Discard('m1', 'revoked', 'smppc-1')
        message.content = content
        message.routing_key = 'dlr.discard'

        yield lookup.discard_dlr_callback(message)

        body = json.loads(lookup.forward_chan.basic_publish.call_args.kwargs['body'])
        self.assertEqual(body['msgid'], 'm1')
        self.assertEqual(body['command_status'], 'DISCARDED')
        self.assertEqual(body['discard_reason'], 'revoked')
        self.assertEqual(body['connector'], 'smppc-1')
        self.assertTrue(lookup.forward_chan.basic_publish.call_args.kwargs['mandatory'])
        redis.delete.assert_called_once_with('dlr:m1')
        lookup.ackMessage.assert_called_once()
