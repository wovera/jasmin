import json
from unittest.mock import Mock

from smpp.pdu.operations import DeliverSM
from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from jasmin.bin.jasmind import JasminDaemon
from jasmin.managers.content import DeliverSmContent
from jasmin.managers.dlr import DLR_FORWARD_MAX_ATTEMPTS
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.configs import RouterPBConfig
from jasmin.routing.jasminApi import Connector
import jasmin.routing.router as router_module
from jasmin.routing.router import MO_FORWARD_PREFETCH_COUNT, RouterPB
from jasmin.tools.jitter import BACKOFF_CEILING_SECONDS


class MoForwardRecoveryTestCase(TestCase):
    def setUp(self):
        config = RouterPBConfig()
        config.mo_forward_queue = 'test.mo.recovery'
        self.router = RouterPB(config, persistenceTimer=False)
        self.router.deliver_sm_q = Mock()
        self.router.deliver_sm_q.get.return_value = defer.Deferred()
        self.router.amqpBroker = Mock()
        self.router.amqpBroker.publish_confirmed.return_value = defer.succeed(None)
        self.clock = task.Clock()
        self.patch(router_module, 'reactor', self.clock)
        self.message = Mock()
        self.message.content = DeliverSmContent(RoutableDeliverSm(DeliverSM(
            source_addr='1234', destination_addr='4567', short_message=b'recover'), Connector('src')), 'src')

    @defer.inlineCallbacks
    def test_an_mo_is_acked_only_once_its_forward_is_confirmed(self):
        confirmation = defer.Deferred()
        self.router.amqpBroker.publish_confirmed.return_value = confirmation
        delivery = self.router.deliver_sm_callback(self.message)

        self.message.channel.basic_ack.assert_not_called()
        confirmation.callback(None)
        yield delivery

        self.assertEqual(self.router.amqpBroker.publish_confirmed.call_args.kwargs['routing_key'],
                         'test.mo.recovery')
        self.message.channel.basic_ack.assert_called_once_with(delivery_tag=self.message.delivery_tag)

    @defer.inlineCallbacks
    def test_an_mo_an_interceptor_rewrote_as_text_is_forwarded_as_its_bytes(self):
        pdu = DeliverSM(source_addr='1234', destination_addr='4567', short_message=b'original')
        pdu.params['short_message'] = 'Intercepted message'
        self.message.content = DeliverSmContent(RoutableDeliverSm(pdu, Connector('src')), 'src')

        yield self.router.deliver_sm_callback(self.message)

        forward = self.router.amqpBroker.publish_confirmed.call_args.kwargs['content']
        self.assertEqual(json.loads(forward.body)['binary'], b'Intercepted message'.hex())
        self.message.channel.basic_ack.assert_called_once_with(delivery_tag=self.message.delivery_tag)

    @defer.inlineCallbacks
    def test_an_mo_failing_for_good_is_parked_durably_before_it_is_acked(self):
        def publish(exchange, routing_key, content):
            if routing_key == 'test.mo.recovery.parked':
                return defer.succeed(None)
            return defer.fail(RuntimeError('unroutable'))
        self.router.amqpBroker.publish_confirmed.side_effect = publish

        for _ in range(DLR_FORWARD_MAX_ATTEMPTS):
            yield self.router.deliver_sm_callback(self.message)
            self.clock.advance(1.25 * BACKOFF_CEILING_SECONDS)
        self.message.channel.basic_ack.assert_not_called()
        yield self.router.deliver_sm_callback(self.message)

        self.assertEqual(self.router.amqpBroker.publish_confirmed.call_args.kwargs['routing_key'],
                         'test.mo.recovery.parked')
        self.message.channel.basic_ack.assert_called_once_with(delivery_tag=self.message.delivery_tag)

    @defer.inlineCallbacks
    def test_concurrent_failures_with_same_message_id_requeue_each_delivery(self):
        confirmations = [defer.Deferred(), defer.Deferred()]
        self.router.amqpBroker.publish_confirmed.side_effect = confirmations
        second_message = Mock(content=self.message.content)
        deliveries = [self.router.deliver_sm_callback(message)
                      for message in (self.message, second_message)]
        self.assertEqual(self.router.amqpBroker.publish_confirmed.call_count, 2)

        for confirmation in confirmations:
            confirmation.errback(RuntimeError('publish failed'))
        yield defer.gatherResults(deliveries)
        self.assertEqual(len(self.router.requeueTimers), 2)

        self.clock.advance(1.25 * self.router.config.mo_forward_retry_delay)

        for message in (self.message, second_message):
            message.channel.basic_reject.assert_called_once_with(
                delivery_tag=message.delivery_tag, requeue=1)
            message.channel.basic_ack.assert_not_called()
        self.assertEqual(self.router.requeueTimers, {})
        self.assertEqual(self.clock.getDelayedCalls(), [])

    @defer.inlineCallbacks
    def test_fired_requeue_timer_is_removed(self):
        self.router.amqpBroker.publish_confirmed.side_effect = lambda **kwargs: defer.fail(RuntimeError('unroutable'))
        yield self.router.deliver_sm_callback(self.message)
        self.assertEqual(len(self.router.requeueTimers), 1)

        self.clock.advance(1.25 * self.router.config.mo_forward_retry_delay)

        self.message.channel.basic_reject.assert_called_once_with(
            delivery_tag=self.message.delivery_tag, requeue=1)
        self.assertEqual(self.router.requeueTimers, {})
        self.assertEqual(self.clock.getDelayedCalls(), [])

    @defer.inlineCallbacks
    def test_stop_cancels_all_pending_requeues(self):
        self.router.amqpBroker.publish_confirmed.side_effect = lambda **kwargs: defer.fail(RuntimeError('unroutable'))
        second_message = Mock(content=self.message.content)
        yield self.router.deliver_sm_callback(self.message)
        yield self.router.deliver_sm_callback(second_message)
        self.assertEqual(len(self.router.requeueTimers), 2)
        timers = list(self.router.requeueTimers.values())

        yield self.router.stopService()
        self.clock.advance(1.25 * self.router.config.mo_forward_retry_delay)

        self.assertTrue(all(not timer.active() for timer in timers))
        self.assertEqual(self.router.requeueTimers, {})
        self.assertEqual(self.clock.getDelayedCalls(), [])
        self.message.channel.basic_reject.assert_not_called()
        second_message.channel.basic_reject.assert_not_called()

    @defer.inlineCallbacks
    def test_daemon_stop_cancels_router_timers_before_stopping_port(self):
        self.router.amqpBroker.publish_confirmed.side_effect = lambda **kwargs: defer.fail(RuntimeError('unroutable'))
        yield self.router.deliver_sm_callback(self.message)
        self.router.activatePersistenceTimer()
        daemon = JasminDaemon.__new__(JasminDaemon)
        port = Mock()
        port.stopListening.side_effect = lambda: self.assertEqual(self.clock.getDelayedCalls(), [])
        daemon.components = {'router-pb-factory': self.router, 'router-pb-server': port}

        yield daemon.stopRouterPBService()
        self.clock.advance(1.25 * self.router.config.mo_forward_retry_delay)

        port.stopListening.assert_called_once_with()
        self.message.channel.basic_reject.assert_not_called()
        self.assertEqual(self.router.requeueTimers, {})


class MoForwardPrefetchTestCase(TestCase):
    def setUp(self):
        config = RouterPBConfig()
        config.mo_forward_queue = 'test.mo.prefetch'
        self.router = RouterPB(config, persistenceTimer=False)
        self.channels = [Mock(is_closed=False) for _ in range(2)]
        for channel in self.channels:
            channel.confirm_delivery.return_value = defer.succeed(None)
            queue = Mock()
            queue.get.return_value = defer.Deferred()
            channel.basic_consume.return_value = defer.succeed((queue, 'consumer'))
        self.router.amqpBroker = Mock()
        self.router.amqpBroker.newChannel.side_effect = [
            defer.succeed(channel) for channel in self.channels]

    @defer.inlineCallbacks
    def test_enabled_prefetch_is_set_before_consumer_starts(self):
        qos = defer.Deferred()
        consumer, bill_consumer = self.channels
        consumer.basic_qos.return_value = qos
        subscription = self.router.subscribe()

        consumer.basic_qos.assert_called_once_with(prefetch_count=MO_FORWARD_PREFETCH_COUNT)
        consumer.basic_consume.assert_not_called()
        qos.callback(None)
        yield subscription

        consumer.basic_consume.assert_called_once()
        self.router.amqpBroker.named_queue_declare.assert_any_call(queue='test.mo.prefetch', durable=True)
        self.router.amqpBroker.named_queue_declare.assert_any_call(queue='RouterPB_deliver_sm_all.durable', durable=True)
        bill_consumer.basic_qos.assert_not_called()

    @defer.inlineCallbacks
    def test_disabled_forwarding_does_not_change_prefetch(self):
        self.router.config.mo_forward_queue = ''
        yield self.router.subscribe()

        self.assertEqual(self.router.amqpBroker.newChannel.call_count, 2)
        for channel in self.channels:
            channel.basic_qos.assert_not_called()
