from unittest.mock import Mock

from smpp.pdu.operations import DeliverSM
from smpp.pdu.pdu_types import CommandStatus, DataCoding
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import Connector
from tests.managers.listener_fixture import ListenerFixture


class MoIngressTestCase(ListenerFixture, TestCase):
    timeout = 10

    def setUp(self):
        self.listener, self.broker = self._listener('mo-ingress')

    def _deliver(self, short_message=b'YES', destination='4567'):
        pdu = DeliverSM(source_addr='1234', destination_addr=destination, short_message=short_message,
                        data_coding=DataCoding())
        return self.listener.deliver_sm_event_post_interception(
            routable=RoutableDeliverSm(pdu, Connector('mo-ingress')), smpp=Mock())

    @defer.inlineCallbacks
    def test_identical_messages_are_each_forwarded(self):
        # SMPP 3.4 gives an MO no identifier, so equal content proves nothing: a sender may say YES twice.
        first = yield self._deliver()
        second = yield self._deliver()

        self.assertIsNone(first)
        self.assertIsNone(second)
        forwarded = self._published(self.broker, 'deliver.sm.mo-ingress')
        self.assertEqual(len(forwarded), 2)
        self.assertNotEqual(forwarded[0].properties['message-id'], forwarded[1].properties['message-id'])

    @defer.inlineCallbacks
    def test_an_mo_is_handed_over_persistent(self):
        yield self._deliver()

        (forwarded,) = self._published(self.broker, 'deliver.sm.mo-ingress')
        self.assertEqual(forwarded.properties['delivery-mode'], 2)

    @defer.inlineCallbacks
    def test_an_mo_the_broker_did_not_confirm_is_not_acknowledged(self):
        self.broker.publish_confirmed = Mock(return_value=defer.fail(IOError('AMQP Client is not connected')))

        response = yield self._deliver()

        self.assertEqual(response.status, CommandStatus.ESME_RUNKNOWNERR)
        self.broker.publish.assert_not_called()
