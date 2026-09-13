import pickle
from datetime import datetime, timedelta
from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig, SMPPClientSMListenerConfig
from jasmin.managers.listeners import SMPPClientSMListener
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory


class QueueDepthListenerTestCase(TestCase):
    def _listener(self):
        client_config = SMPPClientConfig(id='depth-probe')
        client_config.submit_sm_throughput = 0
        client_factory = Mock()
        client_factory.config = client_config
        client_factory.smpp = Mock()
        client_factory.smpp.isBound = Mock(return_value=True)
        client_factory.smpp.sendDataRequest = Mock(side_effect=Exception('boom'))
        broker = Mock()
        broker.connected = True
        broker.publish = Mock(return_value=defer.succeed(None))
        listener = SMPPClientSMListener(SMPPClientSMListenerConfig(), client_factory, broker, Mock())
        listener.submit_sm_q = Mock()
        listener.submit_sm_q.get.return_value = defer.Deferred()
        listener.rejectMessage = Mock(return_value=defer.succeed(None))
        return listener

    def _message(self, created_at=None, with_created_at=True):
        pdu = SMPPOperationFactory(SMPPClientConfig(id='depth-probe')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='Hello')
        headers = {'expiration': str(datetime.now() + timedelta(hours=1))}
        if with_created_at:
            headers['created_at'] = str(created_at or datetime.now())
        message = Mock()
        message.content.properties = {'message-id': 'depth-probe', 'headers': headers}
        message.content.body = pickle.dumps(pdu)
        return message

    def test_oldest_ready_age_seconds_is_none_before_any_dequeue(self):
        listener = self._listener()
        self.assertIsNone(listener.oldest_ready_age_seconds())

    @defer.inlineCallbacks
    def test_oldest_ready_age_seconds_is_nonnegative_after_a_dequeue(self):
        listener = self._listener()
        yield listener.submit_sm_callback(self._message())
        age = listener.oldest_ready_age_seconds()
        self.assertIsNotNone(age)
        self.assertGreaterEqual(age, 0.0)

    @defer.inlineCallbacks
    def test_a_message_with_no_created_at_leaves_last_dequeued_created_at_untouched(self):
        listener = self._listener()
        yield listener.submit_sm_callback(self._message(with_created_at=False))
        self.assertIsNone(listener.last_dequeued_created_at)

    @defer.inlineCallbacks
    def test_last_ready_count_decrements_per_dequeue_and_floors_at_zero(self):
        listener = self._listener()
        listener.last_ready_count = 1

        yield listener.submit_sm_callback(self._message())
        self.assertEqual(listener.last_ready_count, 0)

        yield listener.submit_sm_callback(self._message())
        self.assertEqual(listener.last_ready_count, 0)


class QueueDepthManagerTestCase(TestCase):
    def _manager(self):
        config = SMPPClientPBConfig()
        config.authentication = False
        return SMPPClientManagerPB(config)

    @defer.inlineCallbacks
    def test_connector_with_no_chan_returns_both_none(self):
        manager = self._manager()
        manager.connectors.append({
            'id': 'no-chan-probe',
            'config': SMPPClientConfig(id='no-chan-probe'),
            'service': Mock(),
            'chan': None,
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': Mock(),
        })

        depth = yield manager.perspective_connector_depth('no-chan-probe')

        self.assertEqual(depth, {'messages_ready': None, 'oldest_ready_age_seconds': None})

    @defer.inlineCallbacks
    def test_a_failing_status_channel_returns_both_none_and_is_dropped(self):
        manager = self._manager()
        sm_listener = Mock()
        manager.connectors.append({
            'id': 'failing-chan-probe',
            'config': SMPPClientConfig(id='failing-chan-probe'),
            'service': Mock(),
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })
        status_chan = Mock()
        status_chan.queue_declare = Mock(return_value=defer.fail(Exception('queue gone')))
        manager.amqpBroker = Mock()
        manager.amqpBroker.newChannel = Mock(return_value=defer.succeed(status_chan))

        depth = yield manager.perspective_connector_depth('failing-chan-probe')

        self.assertEqual(depth, {'messages_ready': None, 'oldest_ready_age_seconds': None})
        self.assertIsNone(manager.status_chan)
        sm_listener.oldest_ready_age_seconds.assert_not_called()

    @defer.inlineCallbacks
    def test_an_idle_queue_reports_a_zero_oldest_age(self):
        manager = self._manager()
        sm_listener = Mock()
        sm_listener.oldest_ready_age_seconds = Mock(return_value=3600.0)
        manager.connectors.append({
            'id': 'idle-probe',
            'config': SMPPClientConfig(id='idle-probe'),
            'service': Mock(),
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })
        declared = Mock()
        declared.method.message_count = 0
        status_chan = Mock()
        status_chan.queue_declare = Mock(return_value=defer.succeed(declared))
        manager.amqpBroker = Mock()
        manager.amqpBroker.newChannel = Mock(return_value=defer.succeed(status_chan))

        depth = yield manager.perspective_connector_depth('idle-probe')

        self.assertEqual(depth, {'messages_ready': 0, 'oldest_ready_age_seconds': 0.0})

    @defer.inlineCallbacks
    def test_a_broker_that_cannot_open_a_channel_returns_both_none(self):
        manager = self._manager()
        manager.connectors.append({
            'id': 'down-broker-probe',
            'config': SMPPClientConfig(id='down-broker-probe'),
            'service': Mock(),
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': Mock(),
        })
        manager.amqpBroker = Mock()
        manager.amqpBroker.newChannel = Mock(return_value=defer.fail(Exception('broker down')))

        depth = yield manager.perspective_connector_depth('down-broker-probe')

        self.assertEqual(depth, {'messages_ready': None, 'oldest_ready_age_seconds': None})
        self.assertIsNone(manager.status_chan)

    @defer.inlineCallbacks
    def test_a_successful_publish_increments_the_ready_count(self):
        manager = self._manager()
        sm_listener = Mock()
        sm_listener.last_ready_count = 0
        manager.connectors.append({
            'id': 'increment-probe',
            'config': SMPPClientConfig(id='increment-probe'),
            'service': Mock(),
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })
        manager.amqpBroker = Mock()
        manager.amqpBroker.connected = True
        manager.submit_chan = Mock()
        manager.submit_chan.basic_publish = Mock(return_value=defer.succeed(None))

        pdu = SMPPOperationFactory(SMPPClientConfig(id='increment-probe')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='Hello')

        yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='increment-probe',
            SubmitSmPDU=pdu,
            submit_sm_bill=None,
            pickled=False)

        self.assertEqual(sm_listener.last_ready_count, 1)

    @defer.inlineCallbacks
    def test_a_stop_with_delqueues_prunes_the_broker_queue_cache(self):
        manager = self._manager()
        sm_listener = Mock()
        sm_listener.rejectTimers = {}
        service = Mock()
        service.running = 1
        chan = Mock()
        chan.queue_delete = Mock(return_value=defer.succeed(None))
        manager.connectors.append({
            'id': 'stop-probe',
            'config': SMPPClientConfig(id='stop-probe'),
            'service': service,
            'chan': chan,
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })
        manager.amqpBroker = Mock()
        manager.amqpBroker.queues = ['submit.sm.stop-probe', 'other-queue']

        yield manager.perspective_connector_stop('stop-probe', delQueues=True)

        self.assertNotIn('submit.sm.stop-probe', manager.amqpBroker.queues)
        self.assertIn('other-queue', manager.amqpBroker.queues)
