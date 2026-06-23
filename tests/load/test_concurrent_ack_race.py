"""
Acceptance test for the channel-isolation fix.

The old Jasmin AMQP layer multiplexed every consumer + ack onto one shared channel, which raced into
`txamqp.client.ChannelClosed (406, 'PRECONDITION_FAILED - unknown delivery tag N')` under load. The fix
gives each consumer its own channel and acks on the channel that delivered the message.

This test reproduces the load shape that triggered it: K competing consumers on the SAME queue, each on
its OWN channel, acking on the delivering channel, while N messages are published concurrently. It must
drain all N with no channel error. Requires RabbitMQ on localhost:5672 (the all-in-one test image).
"""
from twisted.internet import defer, reactor
from twisted.trial import unittest
from pika.exceptions import ConsumerCancelled

from jasmin.queues.configs import AmqpConfig
from jasmin.queues.content import Content
from jasmin.queues.delivery import DeliveryMessage
from jasmin.queues.factory import AmqpFactory


@defer.inlineCallbacks
def waitFor(seconds):
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, None)
    yield d


class ConcurrentAckRaceTestCase(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        config = AmqpConfig()
        config.host = 'localhost'
        config.reconnectOnConnectionFailure = False
        config.reconnectOnConnectionLoss = False
        self.amqp = AmqpFactory(config)
        yield self.amqp.connect()
        yield self.amqp.getChannelReadyDeferred()
        self.acked = 0
        self.errors = []
        self._stopped = False

    @defer.inlineCallbacks
    def tearDown(self):
        self._stopped = True
        yield self.amqp.disconnect()

    def _consume(self, channel, queue_object):
        def pump(_=None):
            queue_object.get().addCallback(on_msg).addErrback(on_err)

        def on_msg(received):
            message = DeliveryMessage(received)
            # Ack on the channel that delivered the message (the discipline that fixes 406).
            message.channel.basic_ack(delivery_tag=message.delivery_tag)
            self.acked += 1
            pump()

        def on_err(failure):
            if not self._stopped and failure.check(ConsumerCancelled) is None:
                self.errors.append(failure)

        pump()

    @defer.inlineCallbacks
    def test_competing_consumers_ack_on_own_channels(self):
        N = 300
        K = 5
        queue = 'concurrent_ack_race_q'
        yield self.amqp.named_queue_declare(queue=queue, durable=False)

        # K competing consumers, each on its OWN channel, with prefetch so several are in flight at once.
        consumers = []
        for _ in range(K):
            chan = yield self.amqp.newChannel()
            yield chan.basic_qos(prefetch_count=10)
            queue_object, _tag = yield chan.basic_consume(queue=queue, auto_ack=False)
            self._consume(chan, queue_object)
            consumers.append((chan, queue_object))

        # Publish N messages concurrently.
        yield defer.gatherResults([
            defer.maybeDeferred(
                self.amqp.publish, exchange='', routing_key=queue, content=Content(str(i)))
            for i in range(N)
        ])

        # Drain: wait until all N are acked, or give up after 30s.
        for _ in range(300):
            if self.acked >= N:
                break
            yield waitFor(0.1)

        self._stopped = True
        for _chan, q in consumers:
            yield q.close(Exception('test teardown'))

        self.assertEqual(self.errors, [], "channel errors during concurrent ack: %s" % self.errors)
        self.assertEqual(self.acked, N, "expected to ack all %s messages, got %s" % (N, self.acked))
