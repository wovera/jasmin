"""
Test cases for AmqpFactory (pika-backed).
These are test cases for only Jasmin's code, smpp.twisted tests are not included here.
"""

import pickle
import logging
import time
import uuid

from twisted.internet import defer, reactor
from twisted.trial.unittest import TestCase
from pika.exceptions import ConsumerCancelled

from jasmin.queues.configs import AmqpConfig
from jasmin.queues.content import Content
from jasmin.queues.factory import AmqpFactory


@defer.inlineCallbacks
def waitFor(seconds):
    # Wait seconds
    waitDeferred = defer.Deferred()
    reactor.callLater(seconds, waitDeferred.callback, None)
    yield waitDeferred


class AmqpTestCase(TestCase):
    exchange_name = "CONNECTOR-00"
    message = "Any Message"

    configArgs = {
        'reconnectOnConnectionFailure': False,
        'reconnectOnConnectionLoss': False,
    }

    def setUp(self):
        self.config = AmqpConfig()
        self.config.host = self.configArgs.get('amqp_host', 'localhost')
        self.config.port = self.configArgs.get('amqp_port', 5672)
        self.config.username = self.configArgs.get('amqp_username', 'guest')
        self.config.password = self.configArgs.get('amqp_password', 'guest')
        self.config.log_level = self.configArgs.get('amqp_log_level', logging.DEBUG)
        self.config.reconnectOnConnectionFailure = self.configArgs.get('reconnectOnConnectionFailure', True)
        self.config.reconnectOnConnectionLoss = self.configArgs.get('reconnectOnConnectionLoss', True)

        self.amqp = None
        self._stop = False

    def requirement_disclaimer(self):
        print("failed to connect to an AMQP broker; These tests are designed"
              " to run against a running instance of a AMQP broker")

    @defer.inlineCallbacks
    def connect(self):
        self.amqp = AmqpFactory(self.config)

        try:
            yield self.amqp.connect()
        except:
            self.requirement_disclaimer()
            raise

        yield self.amqp.getChannelReadyDeferred()


class ConnectTestCase(AmqpTestCase):
    @defer.inlineCallbacks
    def test_connect(self):
        yield self.connect()

        yield self.amqp.disconnect()

    @defer.inlineCallbacks
    def test_connect_and_exchange_declare(self):
        yield self.connect()

        exchange_name = '%s_randomName' % time.time()

        yield self.amqp.chan.exchange_declare(exchange=exchange_name, exchange_type="fanout")

        yield self.amqp.disconnect()


class PublishTestCase(AmqpTestCase):
    @defer.inlineCallbacks
    def test_publish_to_topic_exchange(self):
        yield self.connect()

        yield self.amqp.chan.exchange_declare(
            exchange='%s_topic' % self.exchange_name, exchange_type="topic", durable=True)

        yield self.amqp.publish(exchange=self.exchange_name, routing_key="submit.sm", content=Content(self.message))

        yield self.amqp.disconnect()

    @defer.inlineCallbacks
    def test_publish_to_direct_exchange(self):
        yield self.connect()

        yield self.amqp.chan.exchange_declare(
            exchange='%s_direct' % self.exchange_name, exchange_type="direct", durable=True)

        yield self.amqp.publish(exchange=self.exchange_name, routing_key="submit_sm", content=Content(self.message))

        yield self.amqp.disconnect()

    @defer.inlineCallbacks
    def test_publish_to_fanout_exchange(self):
        yield self.connect()

        yield self.amqp.chan.exchange_declare(
            exchange='%s_fanout' % self.exchange_name, exchange_type="fanout", durable=True)

        yield self.amqp.publish(exchange=self.exchange_name, routing_key="submit_sm", content=Content(self.message))

        yield self.amqp.disconnect()

    @defer.inlineCallbacks
    def test_publish_to_queue(self):
        yield self.connect()

        yield self.amqp.named_queue_declare(queue="submit.sm.test_publish_to_queue")

        yield self.amqp.publish(routing_key="submit.sm.test_publish_to_queue", content=Content(self.message))

        yield self.amqp.disconnect()


class ConsumeTools(AmqpTestCase):
    consumedMessages = 0

    def _callback(self, received, queue, ack=False):
        # Re-arm the next pull, then process this delivery.
        queue.get().addCallback(self._callback, queue, ack=ack).addErrback(self._errback)
        self.consumedMessages += 1

        if ack:
            # Ack on the channel that delivered the message (basic_ack is fire-and-forget, returns None).
            received.channel.basic_ack(delivery_tag=received.method.delivery_tag)

    def _errback(self, error):
        # ConsumerCancelled is expected when a consumer is cancelled; a closed queue's pending get()
        # errbacks during teardown. Both are expected noise, swallow them.
        if error.check(ConsumerCancelled) is not None or getattr(self, '_stop', False):
            return None
        print("Error in _errback %s" % (error))
        return error


class ConsumeTestCase(ConsumeTools):
    @defer.inlineCallbacks
    def test_consume_queue(self):
        yield self.connect()

        yield self.amqp.named_queue_declare(queue="submit.sm.test_consume_queue")

        self.queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.sm.test_consume_queue", auto_ack=True)
        self.queue.get().addCallback(self._callback, self.queue).addErrback(self._errback)

        # Wait for 2 seconds
        yield waitFor(2)

        self._stop = True
        yield self.queue.close(Exception('teardown'))
        yield self.amqp.disconnect()


class PublishConsumeTestCase(ConsumeTools):
    @defer.inlineCallbacks
    def test_simple_publish_consume(self):
        yield self.connect()

        yield self.amqp.named_queue_declare(queue="submit.sm.test_simple_publish_consume")

        # Consume
        queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.sm.test_simple_publish_consume", auto_ack=True)
        queue.get().addCallback(self._callback, queue).addErrback(self._errback)

        # Publish
        yield self.amqp.publish(routing_key="submit.sm.test_simple_publish_consume", content=Content(self.message))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        self._stop = True
        yield queue.close(Exception('teardown'))

        yield self.amqp.disconnect()

        self.assertEqual(self.consumedMessages, 1)

    @defer.inlineCallbacks
    def test_simple_publish_consume_by_topic(self):
        yield self.connect()

        yield self.amqp.chan.exchange_declare(exchange='messaging', exchange_type='topic')

        # Consume
        yield self.amqp.named_queue_declare(queue="submit.test_simple_publish_consume_by_topic")
        yield self.amqp.chan.queue_bind(
            queue="submit.test_simple_publish_consume_by_topic", exchange="messaging",
            routing_key="submit.sm.test_simple_publish_consume_by_topic")
        queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.test_simple_publish_consume_by_topic", auto_ack=True)
        queue.get().addCallback(self._callback, queue).addErrback(self._errback)

        # Publish
        yield self.amqp.publish(
            exchange='messaging', routing_key="submit.sm.test_simple_publish_consume_by_topic",
            content=Content(self.message))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        self._stop = True
        yield queue.close(Exception('teardown'))

        yield self.amqp.disconnect()

        self.assertEqual(self.consumedMessages, 1)

    @defer.inlineCallbacks
    def test_publish_consume_from_different_queues(self):
        yield self.connect()

        yield self.amqp.named_queue_declare(queue="submit.sm.test_publish_consume_from_different_queues")
        yield self.amqp.named_queue_declare(queue="deliver.sm.test_publish_consume_from_different_queues")

        # Consume
        self.submit_sm_q, _t1 = yield self.amqp.chan.basic_consume(
            queue="submit.sm.test_publish_consume_from_different_queues", auto_ack=True)
        self.deliver_sm_q, _t2 = yield self.amqp.chan.basic_consume(
            queue="deliver.sm.test_publish_consume_from_different_queues", auto_ack=True)
        self.submit_sm_q.get().addCallback(self._callback, self.submit_sm_q).addErrback(self._errback)
        self.deliver_sm_q.get().addCallback(self._callback, self.deliver_sm_q).addErrback(self._errback)

        # Publish
        yield self.amqp.publish(
            routing_key="submit.sm.test_publish_consume_from_different_queues", content=Content(self.message))
        yield self.amqp.publish(
            routing_key="deliver.sm.test_publish_consume_from_different_queues", content=Content(self.message))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        self._stop = True
        yield self.submit_sm_q.close(Exception('teardown'))
        yield self.deliver_sm_q.close(Exception('teardown'))

        yield self.amqp.disconnect()

        self.assertEqual(self.consumedMessages, 2)

    @defer.inlineCallbacks
    def test_start_consuming_later(self):
        """Related to #67, will start consuming after publishing messages, this will imitate
        starting a connector with some pending messages for it"""
        yield self.connect()

        yield self.amqp.chan.exchange_declare(exchange='messaging', exchange_type='topic')

        # Consume
        yield self.amqp.named_queue_declare(queue="submit.sm.test_start_consuming_later")
        yield self.amqp.chan.queue_bind(
            queue="submit.sm.test_start_consuming_later", exchange="messaging",
            routing_key="submit.sm.test_start_consuming_later")
        queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.sm.test_start_consuming_later", auto_ack=False)

        # Publish
        for i in range(5000):
            yield self.amqp.publish(
                exchange='messaging', routing_key="submit.sm.test_start_consuming_later", content=Content(str(i)))

        # Start consuming (same as starting a connector)
        queue.get().addCallback(self._callback, queue, ack=True).addErrback(self._errback)

        # Wait for 15 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(15)

        self._stop = True
        yield queue.close(Exception('teardown'))

        yield self.amqp.disconnect()

        self.assertEqual(self.consumedMessages, 5000)

    @defer.inlineCallbacks
    def test_publish_pickled_binary_content(self):
        """Refs #640
        As of 12/5/2017 txamqp 0.8 were released and it caused an outage to Jasmin.
        """
        yield self.connect()

        yield self.amqp.chan.exchange_declare(exchange='messaging', exchange_type='topic')

        # Consume
        yield self.amqp.named_queue_declare(queue="submit.test_publish_pickled_binary_content")
        yield self.amqp.chan.queue_bind(
            queue="submit.test_publish_pickled_binary_content", exchange="messaging",
            routing_key="submit.sm.test_publish_pickled_binary_content")
        queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.test_publish_pickled_binary_content", auto_ack=True)
        queue.get().addCallback(self._callback, queue).addErrback(self._errback)

        # Publish a pickled binary content with the highest protocol
        yield self.amqp.publish(
            exchange='messaging', routing_key="submit.sm.test_publish_pickled_binary_content",
            content=Content(pickle.dumps('\x53', pickle.HIGHEST_PROTOCOL)))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        self._stop = True
        yield queue.close(Exception('teardown'))

        yield self.amqp.disconnect()

        self.assertEqual(self.consumedMessages, 1)


class RejectAndRequeueTestCase(ConsumeTools):
    rejectedMessages = 0
    # Used to store rejected messages:
    data = []

    def _callback_reject_once(self, received, queue, reject=False, requeue=1):
        queue.get().addCallback(self._callback_reject_once, queue, reject, requeue).addErrback(self._errback)

        if reject and received.body not in self.data:
            self.rejectedMessages = self.rejectedMessages + 1
            self.data.append(received.body)
            received.channel.basic_reject(delivery_tag=received.method.delivery_tag, requeue=requeue)
        else:
            self.data.remove(received.body)
            self.consumedMessages = self.consumedMessages + 1
            received.channel.basic_ack(delivery_tag=received.method.delivery_tag)

    def _callback_reject_and_requeue_all(self, received, queue, requeue=1):
        queue.get().addCallback(self._callback_reject_and_requeue_all, queue, requeue).addErrback(self._errback)

        self.rejectedMessages = self.rejectedMessages + 1
        received.channel.basic_reject(delivery_tag=received.method.delivery_tag, requeue=requeue)

    @defer.inlineCallbacks
    def test_consume_all_requeued_messages(self):
        "Related to #67, test for consuming all requeued messages"
        yield self.connect()

        yield self.amqp.chan.exchange_declare(exchange='messaging', exchange_type='topic')

        # Consume
        yield self.amqp.named_queue_declare(queue="submit.test_consume_all_requeued_messages")
        yield self.amqp.chan.queue_bind(
            queue="submit.test_consume_all_requeued_messages", exchange="messaging",
            routing_key="submit.sm.test_consume_all_requeued_messages")
        queue, _consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.test_consume_all_requeued_messages", auto_ack=False)
        queue.get().addCallback(self._callback_reject_once, queue, reject=True).addErrback(self._errback)

        # Publish
        for i in range(50):
            yield self.amqp.publish(
                exchange='messaging', routing_key="submit.sm.test_consume_all_requeued_messages",
                content=Content(str(i)))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        self._stop = True
        yield queue.close(Exception('teardown'))
        yield self.amqp.disconnect()

        self.assertEqual(self.rejectedMessages, 50)
        self.assertEqual(self.consumedMessages, 50)

    @defer.inlineCallbacks
    def test_requeue_all_restart_then_reconsume(self):
        """Related to #67, Starting consuming with a """
        yield self.connect()

        yield self.amqp.chan.exchange_declare(exchange='messaging', exchange_type='topic')

        # Setup Consumer
        yield self.amqp.named_queue_declare(queue="submit.test_requeue_all_restart_then_reconsume")
        yield self.amqp.chan.queue_bind(
            queue="submit.test_requeue_all_restart_then_reconsume", exchange="messaging",
            routing_key="submit.sm.test_requeue_all_restart_then_reconsume")
        queue, consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.test_requeue_all_restart_then_reconsume", auto_ack=False)
        # Start consuming through _callback_reject_and_requeue_all
        queue.get().addCallback(self._callback_reject_and_requeue_all, queue).addErrback(self._errback)

        # Publish
        for i in range(50):
            yield self.amqp.publish(
                exchange='messaging', routing_key="submit.sm.test_requeue_all_restart_then_reconsume",
                content=Content(str(i)))

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        # Stop consuming and assert
        yield self.amqp.chan.basic_cancel(consumer_tag=consumer_tag)
        self.assertGreaterEqual(self.rejectedMessages, 50)
        self.assertEqual(self.consumedMessages, 0)

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        # Start consuming again
        queue, consumer_tag = yield self.amqp.chan.basic_consume(
            queue="submit.test_requeue_all_restart_then_reconsume", auto_ack=False)
        # Consuming through _callback
        queue.get().addCallback(self._callback, queue, ack=True).addErrback(self._errback)

        # Wait for 2 seconds
        # (give some time to the consumer to get its work done)
        yield waitFor(2)

        # Stop consuming and assert
        self._stop = True
        yield queue.close(Exception('teardown'))
        self.assertEqual(self.consumedMessages, 50)

        yield self.amqp.disconnect()
