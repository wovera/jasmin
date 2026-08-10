import pickle
from datetime import datetime, timedelta
from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.managers.configs import SMPPClientSMListenerConfig
from jasmin.managers.listeners import SMPPClientSMListener
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory


class ExpiredSubmitDiscardTestCase(TestCase):
    """The consumer drops a queued message whose validity elapsed while it waited, reading the instant from the
    AMQP expiration header. Upstream's only behavioural cover for this is skipped, and it asserts an approximate
    count after a fifteen-second sleep; these drive the callback directly so the outcome is exact."""

    def _listener(self):
        client_config = SMPPClientConfig(id='expiry-probe')
        # Zero throughput takes the QoS branch out of the path: the subject here is the expiry guard alone.
        client_config.submit_sm_throughput = 0

        client_factory = Mock()
        client_factory.config = client_config
        # An unbound client sends the message down the not-ready path, which requeues rather than discards, so the
        # two dispositions stay distinguishable without a live SMSC.
        client_factory.smpp = None

        broker = Mock()
        broker.connected = True

        listener = SMPPClientSMListener(SMPPClientSMListenerConfig(), client_factory, broker, Mock())
        listener.submit_sm_q = Mock()
        listener.submit_sm_q.get.return_value = defer.Deferred()
        # Stubbed because the real one arms a reactor timer; the test asserts which disposition was chosen.
        listener.rejectAndRequeueMessage = Mock(return_value=defer.succeed(None))
        return listener

    def _message(self, expiration):
        pdu = SMPPOperationFactory(SMPPClientConfig(id='expiry-probe')).SubmitSM(
            source_addr='1423',
            destination_addr='06155423',
            short_message='Hello world !',
        )

        message = Mock()
        message.content.properties = {
            'message-id': 'expiry-probe',
            'headers': {'expiration': str(expiration), 'created_at': str(datetime.now())},
        }
        message.content.body = pickle.dumps(pdu, pickle.HIGHEST_PROTOCOL)
        message.delivery_tag = 1
        return message

    @defer.inlineCallbacks
    def test_elapsed_validity_is_discarded_not_submitted(self):
        listener = self._listener()
        message = self._message(datetime.now() - timedelta(seconds=1))

        yield listener.submit_sm_callback(message)

        message.channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=0)
        self.assertFalse(listener.rejectAndRequeueMessage.called)

    @defer.inlineCallbacks
    def test_unelapsed_validity_is_left_to_the_send_path(self):
        listener = self._listener()
        message = self._message(datetime.now() + timedelta(hours=1))

        yield listener.submit_sm_callback(message)

        self.assertFalse(message.channel.basic_reject.called)
        self.assertTrue(listener.rejectAndRequeueMessage.called)
