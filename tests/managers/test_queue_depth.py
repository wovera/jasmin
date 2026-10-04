from datetime import datetime
from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.protocols.smpp.configs import SMPPClientConfig
from tests.managers.listener_fixture import ListenerFixture


class QueueDepthListenerTestCase(ListenerFixture, TestCase):
    def _listener(self):
        listener, _ = super()._listener('depth-probe')
        listener.SMPPClientFactory.smpp = Mock()
        listener.SMPPClientFactory.smpp.isBound = Mock(return_value=True)
        listener.SMPPClientFactory.smpp.sendDataRequest = Mock(side_effect=Exception('boom'))
        listener.rejectMessage = Mock(return_value=defer.succeed(None))
        return listener

    def _message(self, created_at=None, with_created_at=True):
        message = super()._message('depth-probe', created_at=created_at)
        if not with_created_at:
            del message.content.properties['headers']['created_at']
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
    def _manager(self, cid=None, sm_listener=None, chan=None):
        config = SMPPClientPBConfig()
        config.authentication = False
        manager = SMPPClientManagerPB(config)
        if cid is not None:
            manager.connectors.append({
                'id': cid,
                'config': SMPPClientConfig(id=cid),
                'service': Mock(),
                'chan': chan,
                'consumer_tag': None,
                'submit_sm_q': None,
                'sm_listener': sm_listener or Mock(),
            })
        return manager

    @defer.inlineCallbacks
    def test_connector_with_no_chan_returns_both_none(self):
        manager = self._manager('no-chan-probe')

        depth = yield manager.perspective_connector_depth('no-chan-probe')

        self.assertEqual(depth, {'messages_ready': None, 'oldest_ready_age_seconds': None})

    @defer.inlineCallbacks
    def test_a_failing_status_channel_returns_both_none_and_is_dropped(self):
        sm_listener = Mock()
        manager = self._manager('failing-chan-probe', sm_listener, Mock())
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
        sm_listener = Mock()
        sm_listener.oldest_ready_age_seconds = Mock(return_value=3600.0)
        manager = self._manager('idle-probe', sm_listener, Mock())
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
        manager = self._manager('down-broker-probe', chan=Mock())
        manager.amqpBroker = Mock()
        manager.amqpBroker.newChannel = Mock(return_value=defer.fail(Exception('broker down')))

        depth = yield manager.perspective_connector_depth('down-broker-probe')

        self.assertEqual(depth, {'messages_ready': None, 'oldest_ready_age_seconds': None})
        self.assertIsNone(manager.status_chan)

    @defer.inlineCallbacks
    def test_a_stop_with_delqueues_prunes_the_broker_queue_cache(self):
        sm_listener = Mock()
        sm_listener.rejectTimers = {}
        service = Mock()
        service.running = 1
        chan = Mock()
        chan.queue_delete = Mock(return_value=defer.succeed(None))
        manager = self._manager('stop-probe', sm_listener, chan)
        manager.connectors[0]['service'] = service
        manager.amqpBroker = Mock()
        manager.amqpBroker.queues = ['submit.sm.stop-probe', 'other-queue']

        yield manager.perspective_connector_stop('stop-probe', delQueues=True)

        self.assertNotIn('submit.sm.stop-probe', manager.amqpBroker.queues)
        self.assertIn('other-queue', manager.amqpBroker.queues)

    @defer.inlineCallbacks
    def test_a_stopped_connector_still_releases_its_consumer_and_queue(self):
        sm_listener = Mock()
        sm_listener.rejectTimers = {}
        service = Mock()
        service.running = 0
        chan = Mock()
        chan.basic_cancel = Mock(return_value=defer.succeed(None))
        chan.queue_delete = Mock(return_value=defer.succeed(None))
        manager = self._manager('stopped-probe', sm_listener, chan)
        manager.connectors[0]['service'] = service
        manager.connectors[0]['consumer_tag'] = 'tag'
        manager.amqpBroker = Mock()
        manager.amqpBroker.queues = ['submit.sm.stopped-probe']

        yield manager.perspective_connector_stop('stopped-probe', delQueues=True)

        chan.basic_cancel.assert_called_once_with(consumer_tag='tag')
        chan.queue_delete.assert_called_once_with(queue='submit.sm.stopped-probe')
        self.assertEqual(manager.amqpBroker.queues, [])
