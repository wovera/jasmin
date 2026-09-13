from twisted.internet import defer

from jasmin.queues.content import Content
from tests.queues.test_amqp import AmqpTestCase


class PriorityQueueTestCase(AmqpTestCase):
    def _content(self, body, priority):
        return Content(body, properties={'priority': priority})

    @defer.inlineCallbacks
    def _consume_one(self, queue):
        q, _consumer_tag = yield self.amqp.chan.basic_consume(queue=queue, auto_ack=True)
        received = yield q.get()
        defer.returnValue(received.body)

    @defer.inlineCallbacks
    def test_higher_priority_is_delivered_first(self):
        yield self.connect()

        yield self.amqp.named_queue_declare(queue='prio-probe', arguments={'x-max-priority': 3})
        yield self.amqp.publish(exchange='', routing_key='prio-probe', content=self._content('low', priority=0))
        yield self.amqp.publish(exchange='', routing_key='prio-probe', content=self._content('high', priority=3))
        first = yield self._consume_one('prio-probe')
        self.assertEqual(first, b'high')

        yield self.amqp.chan.queue_delete(queue='prio-probe')
        yield self.amqp.disconnect()
