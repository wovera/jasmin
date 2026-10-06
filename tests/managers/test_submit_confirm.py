from unittest.mock import Mock

from pika.exceptions import UnroutableError
from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.protocols.http.errors import SubmitInDoubtError
from jasmin.queues import factory as factory_module
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory


class SubmitConfirmTestCase(TestCase):
    def _manager(self):
        config = SMPPClientPBConfig()
        config.authentication = False
        manager = SMPPClientManagerPB(config)
        sm_listener = Mock()
        sm_listener.last_ready_count = 0
        manager.connectors.append({
            'id': 'confirm-probe',
            'config': SMPPClientConfig(id='confirm-probe'),
            'service': Mock(),
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })
        self.clock = task.Clock()
        self.patch(factory_module, 'reactor', self.clock)
        manager.amqpBroker = AmqpFactory(AmqpConfig())
        manager.amqpBroker.connected = True
        manager.amqpBroker.client = Mock()
        manager.submit_chan = Mock()
        manager.submit_chan.is_closed = False
        return manager

    def _pdu(self):
        return SMPPOperationFactory(SMPPClientConfig(id='confirm-probe')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='Hello')

    @defer.inlineCallbacks
    def test_unconfirmed_publish_is_refused_not_acknowledged(self):
        manager = self._manager()
        manager.submit_chan.basic_publish = Mock(side_effect=UnroutableError([]))

        result = yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False)

        self.assertIs(result, False)
        self.assertEqual(manager.getConnector('confirm-probe')['sm_listener'].last_ready_count, 0)

    @defer.inlineCallbacks
    def test_unconfirmed_publish_releases_the_idempotency_mark(self):
        manager = self._manager()
        manager.submit_chan.basic_publish = Mock(side_effect=UnroutableError([]))
        manager.redisClient = Mock()
        manager.redisClient.set = Mock(return_value=defer.succeed(True))
        manager.redisClient.delete = Mock(return_value=defer.succeed(1))

        result = yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False,
            msgid='77777777-7777-7777-7777-777777777777')

        self.assertIs(result, False)
        manager.redisClient.delete.assert_called_once_with(
            'idem:77777777-7777-7777-7777-777777777777')

    @defer.inlineCallbacks
    def test_a_publish_with_no_open_channel_is_refused_and_releases_the_idempotency_mark(self):
        manager = self._manager()
        manager.submit_chan = None
        manager.redisClient = Mock()
        manager.redisClient.set = Mock(return_value=defer.succeed(True))
        manager.redisClient.delete = Mock(return_value=defer.succeed(1))

        result = yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False,
            msgid='88888888-8888-8888-8888-888888888888')

        self.assertIs(result, False)
        manager.redisClient.delete.assert_called_once_with(
            'idem:88888888-8888-8888-8888-888888888888')
        self.assertEqual(manager.getConnector('confirm-probe')['sm_listener'].last_ready_count, 0)

    @defer.inlineCallbacks
    def test_a_submit_publication_waits_for_the_previous_one_to_be_confirmed(self):
        manager = self._manager()
        first_confirmation = defer.Deferred()
        manager.submit_chan.basic_publish = Mock(side_effect=[first_confirmation, defer.succeed(None)])

        first, second = [manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False) for _ in range(2)]
        self.assertEqual(manager.submit_chan.basic_publish.call_count, 1)

        first_confirmation.callback(None)
        yield defer.gatherResults([first, second])
        self.assertEqual(manager.submit_chan.basic_publish.call_count, 2)

    @defer.inlineCallbacks
    def test_an_unanswered_submit_publication_aborts_the_connection_keeps_its_claim_and_frees_the_lock(self):
        manager = self._manager()
        manager.submit_chan.basic_publish = Mock(side_effect=[defer.Deferred(), defer.succeed(None)])
        manager.redisClient = Mock()
        manager.redisClient.set = Mock(return_value=defer.succeed(True))
        manager.redisClient.delete = Mock(return_value=defer.succeed(1))

        stalled = manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False,
            msgid='99999999-9999-9999-9999-999999999999')
        self.clock.advance(factory_module.PUBLICATION_DEADLINE_SECONDS)

        yield self.assertFailure(stalled, SubmitInDoubtError)
        manager.amqpBroker.client.transport.abortConnection.assert_called_once_with()
        manager.redisClient.delete.assert_not_called()
        result = yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False)
        self.assertIsNot(result, False)

    @defer.inlineCallbacks
    def test_confirmed_publish_increments_the_ready_count(self):
        manager = self._manager()
        manager.submit_chan.basic_publish = Mock(return_value=defer.succeed(None))

        result = yield manager.perspective_submit_sm(
            uid='test-uid',
            cid='confirm-probe',
            SubmitSmPDU=self._pdu(),
            submit_sm_bill=None,
            pickled=False)

        self.assertIsNot(result, False)
        self.assertEqual(manager.getConnector('confirm-probe')['sm_listener'].last_ready_count, 1)

    @defer.inlineCallbacks
    def test_the_confirm_channel_is_reopened_on_every_reconnect(self):
        config = SMPPClientPBConfig()
        config.authentication = False
        manager = SMPPClientManagerPB(config)
        registered_callbacks = []
        channels = [Mock(), Mock()]
        for channel in channels:
            channel.confirm_delivery = Mock(return_value=defer.succeed(None))
        broker = Mock()
        broker.newChannel = Mock(side_effect=[defer.succeed(channels[0]), defer.succeed(channels[1])])
        broker.addChannelReadyCallback = Mock(side_effect=registered_callbacks.append)
        manager.amqpBroker = broker

        yield manager.openSubmitChannel()
        yield manager.openSubmitChannel()

        self.assertIs(manager.submit_chan, channels[1])
        channels[0].confirm_delivery.assert_called_once()
        channels[1].confirm_delivery.assert_called_once()
        self.assertEqual(registered_callbacks, [manager.openSubmitChannel, manager.openSubmitChannel])

    @defer.inlineCallbacks
    def test_a_failed_first_open_still_leaves_the_reconnect_hook_registered(self):
        config = SMPPClientPBConfig()
        config.authentication = False
        manager = SMPPClientManagerPB(config)
        registered_callbacks = []
        channel = Mock()
        channel.confirm_delivery = Mock(return_value=defer.succeed(None))
        broker = Mock()
        broker.newChannel = Mock(side_effect=[defer.fail(Exception('broker down')), defer.succeed(channel)])
        broker.addChannelReadyCallback = Mock(side_effect=registered_callbacks.append)
        manager.amqpBroker = broker

        yield self.assertFailure(manager.openSubmitChannel(), Exception)

        self.assertIsNone(manager.submit_chan)
        self.assertEqual(registered_callbacks, [manager.openSubmitChannel])

        yield registered_callbacks[0]()

        self.assertIs(manager.submit_chan, channel)
        channel.confirm_delivery.assert_called_once()
