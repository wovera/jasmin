"""
Integration spike: validate pika's Twisted adapter under the SAME wiring shape Jasmin's AmqpFactory
uses (a twisted ClientFactory + reactor.connectTCP), before porting jasmin/queues/factory.py onto pika.

Proves: connect -> open channel -> declare -> qos -> consume -> publish -> get -> ack (on the
delivering channel) -> cleanup. Requires RabbitMQ on localhost:5672 (the all-in-one test image provides it).
"""
import pika
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet import defer, protocol, reactor
from twisted.trial import unittest


class _PikaClientFactory(protocol.ClientFactory):
    """Mirrors how AmqpFactory builds its protocol: buildProtocol returns the pika connection,
    and we forward pika's `ready` Deferred (fires with the live connection) to factory.ready."""

    def __init__(self, params):
        self._params = params
        self.ready = defer.Deferred()

    def buildProtocol(self, addr):
        p = TwistedProtocolConnection(self._params)
        p.factory = self
        p.ready.chainDeferred(self.ready)
        return p


class PikaTwistedSpikeTestCase(unittest.TestCase):
    @defer.inlineCallbacks
    def test_connect_declare_publish_consume_ack(self):
        params = pika.ConnectionParameters(
            host='127.0.0.1',
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest'),
        )
        factory = _PikaClientFactory(params)
        reactor.connectTCP('127.0.0.1', 5672, factory)

        conn = yield factory.ready
        channel = yield conn.channel()
        yield channel.queue_declare(queue='jasmin_pika_spike', durable=False, auto_delete=True)
        yield channel.basic_qos(prefetch_count=1)
        queue_object, consumer_tag = yield channel.basic_consume(
            queue='jasmin_pika_spike', auto_ack=False)

        yield channel.basic_publish(
            exchange='',
            routing_key='jasmin_pika_spike',
            body=b'hello-from-spike',
            properties=pika.BasicProperties(message_id='spike-1', delivery_mode=2),
        )

        received = yield queue_object.get()
        self.assertEqual(received.body, b'hello-from-spike')
        self.assertEqual(received.properties.message_id, 'spike-1')
        # Ack on the channel that delivered it (the discipline the migration enforces everywhere).
        received.channel.basic_ack(received.method.delivery_tag)

        yield channel.basic_cancel(consumer_tag)
        yield conn.close()
