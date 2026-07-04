import binascii
import json

from twisted.internet import defer

from jasmin.routing.jasminApi import HttpConnector
from tests.routing.test_throwers_deliver_sm import deliverSmThrowerTestCase, waitFor


class MoForwardTestCase(deliverSmThrowerTestCase):
    forward_queue = 'test.mo.forward'
    routingKey = 'deliver_sm_thrower.http'

    @defer.inlineCallbacks
    def setUp(self):
        yield deliverSmThrowerTestCase.setUp(self)
        yield self.amqpBroker.chan.queue_declare(queue=self.forward_queue, durable=True)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.amqpBroker.chan.queue_delete(queue=self.forward_queue)
        yield deliverSmThrowerTestCase.tearDown(self)

    @defer.inlineCallbacks
    def _get_forward(self):
        result = yield self.amqpBroker.chan.basic_get(queue=self.forward_queue, auto_ack=True)
        if result is None:
            defer.returnValue(None)
        defer.returnValue(json.loads(result.body))

    @defer.inlineCallbacks
    def test_forwards_mo_as_json_and_skips_http(self):
        self.deliverSmThrower.config.mo_forward_queue = self.forward_queue

        message = b'hello mo'
        self.testDeliverSMPdu.params['short_message'] = message
        routedConnector = HttpConnector('dst', 'http://127.0.0.1:1/send', 'POST')
        self.publishRoutedDeliverSmContent(
            self.routingKey, self.testDeliverSMPdu, 'mo-1', 'src', routedConnector
        )
        yield waitFor(1)

        record = yield self._get_forward()
        self.assertIsNotNone(record)
        self.assertEqual(record['id'], 'mo-1')
        self.assertEqual(record['from'], '1234')
        self.assertEqual(record['to'], '4567')
        self.assertEqual(record['binary'], binascii.hexlify(message).decode())

    @defer.inlineCallbacks
    def test_disabled_when_queue_unset(self):
        self.deliverSmThrower.config.mo_forward_queue = ''

        routedConnector = HttpConnector('dst', 'http://127.0.0.1:1/send', 'POST')
        self.publishRoutedDeliverSmContent(
            self.routingKey, self.testDeliverSMPdu, 'mo-2', 'src', routedConnector
        )
        yield waitFor(1)

        self.assertIsNone((yield self._get_forward()))
