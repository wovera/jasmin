from unittest.mock import Mock

from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from jasmin.queues import factory as factory_module
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory


def channel():
    chan = Mock()
    chan.on_closed = defer.Deferred()
    return chan


class AmqpFactoryFixture:
    def setUp(self):
        self.clock = task.Clock()
        self.patch(factory_module, 'reactor', self.clock)
        self.factory = AmqpFactory(AmqpConfig())
        self.factory.preConnect()
        self.connection = Mock()
        self.connection.is_open = True
        self.factory.client = self.connection


class ControlChannelTestCase(AmqpFactoryFixture, TestCase):
    @defer.inlineCallbacks
    def test_a_control_channel_closed_by_the_broker_is_reopened(self):
        first, second = channel(), channel()
        self.connection.channel = Mock(side_effect=[defer.succeed(first), defer.succeed(second)])
        yield self.factory._on_connection_ready(self.connection)

        first.on_closed.callback(Exception('PRECONDITION_FAILED'))

        self.assertIs(self.factory.chan, second)

    @defer.inlineCallbacks
    def test_a_reopened_channel_is_reopened_again(self):
        first, second, third = channel(), channel(), channel()
        self.connection.channel = Mock(side_effect=[defer.succeed(each) for each in (first, second, third)])
        yield self.factory._on_connection_ready(self.connection)

        first.on_closed.callback(Exception('NOT_FOUND'))
        second.on_closed.callback(Exception('NOT_FOUND'))

        self.assertIs(self.factory.chan, third)

    @defer.inlineCallbacks
    def test_a_channel_closing_with_its_connection_is_left_to_the_reconnect(self):
        first = channel()
        self.connection.channel = Mock(side_effect=[defer.succeed(first)])
        yield self.factory._on_connection_ready(self.connection)

        self.connection.is_open = False
        first.on_closed.callback(Exception('connection closed'))

        self.assertEqual(self.connection.channel.call_count, 1)


class AmqpReconnectBackoffTestCase(AmqpFactoryFixture, TestCase):
    def _delays(self, count):
        delays = []
        for _ in range(count):
            self.factory.clientConnectionLost(Mock(), Exception('connection refused'))
            delays.append(self.factory.reconnectTimer.getTime() - self.clock.seconds())
            self.factory.reconnectTimer.cancel()
        return delays

    def test_reconnects_back_off_while_the_broker_stays_away(self):
        base = self.factory.config.reconnectOnConnectionLossDelay

        delays = self._delays(3)

        for attempt, delay in enumerate(delays):
            self.assertTrue(0.75 * base * 2 ** attempt <= delay <= 1.25 * base * 2 ** attempt, delays)

    @defer.inlineCallbacks
    def test_a_ready_connection_restarts_the_backoff(self):
        self._delays(3)
        self.connection.channel = Mock(return_value=defer.succeed(channel()))

        yield self.factory._on_connection_ready(self.connection)

        self.assertEqual(self.factory.reconnectAttempts, 0)


class ConfirmedPublishTestCase(AmqpFactoryFixture, TestCase):
    def setUp(self):
        super().setUp()
        self.factory.connected = True
        self.content = Mock(pika_body=b'body', pika_properties=Mock())

    def _confirming_channel(self, confirmation=None):
        chan = Mock(is_open=True)
        chan.confirm_delivery.return_value = confirmation or defer.succeed(None)
        chan.basic_publish.return_value = defer.succeed(None)
        return chan

    @defer.inlineCallbacks
    def test_an_unavailable_broker_fails_the_publish(self):
        self.factory.connected = False

        yield self.assertFailure(self.factory.publish_confirmed('messaging', 'deliver.sm.c', self.content), IOError)

    @defer.inlineCallbacks
    def test_a_publish_is_mandatory_on_a_confirm_channel(self):
        chan = self._confirming_channel()
        self.connection.channel = Mock(return_value=defer.succeed(chan))

        yield self.factory.publish_confirmed('messaging', 'deliver.sm.c', self.content)

        chan.confirm_delivery.assert_called_once_with()
        chan.basic_publish.assert_called_once_with(exchange='messaging', routing_key='deliver.sm.c', body=b'body',
                                                   properties=self.content.pika_properties, mandatory=True)

    @defer.inlineCallbacks
    def test_a_closed_publisher_is_reopened_and_confirmed_before_reuse(self):
        closed, reopened = self._confirming_channel(), self._confirming_channel()
        closed.is_open = False
        self.factory.confirmChan = closed
        confirmation = defer.Deferred()
        reopened.confirm_delivery.return_value = confirmation
        self.connection.channel = Mock(return_value=defer.succeed(reopened))

        publishing = self.factory.publish_confirmed('', 'outcomes.parked', self.content)
        reopened.basic_publish.assert_not_called()
        confirmation.callback(None)
        yield publishing

        closed.basic_publish.assert_not_called()
        reopened.basic_publish.assert_called_once()
        self.assertIs(self.factory.confirmChan, reopened)

    @defer.inlineCallbacks
    def test_concurrent_publishes_share_one_recovery(self):
        opening = defer.Deferred()
        chan = self._confirming_channel()
        self.connection.channel = Mock(return_value=opening)

        publishes = [self.factory.publish_confirmed('', 'q', self.content) for _ in range(2)]
        opening.callback(chan)
        yield defer.gatherResults(publishes)

        self.connection.channel.assert_called_once_with()
        chan.confirm_delivery.assert_called_once_with()
        self.assertEqual(chan.basic_publish.call_count, 2)

    @defer.inlineCallbacks
    def test_a_publisher_that_cannot_be_confirmed_is_closed_and_never_used(self):
        failed, good = self._confirming_channel(defer.fail(RuntimeError('confirm failed'))), self._confirming_channel()
        self.connection.channel = Mock(side_effect=[defer.succeed(failed), defer.succeed(good)])

        yield self.assertFailure(self.factory.publish_confirmed('', 'q', self.content), RuntimeError)
        failed.close.assert_called_once_with()
        failed.basic_publish.assert_not_called()
        self.assertIsNone(self.factory.confirmChan)

        yield self.factory.publish_confirmed('', 'q', self.content)
        good.basic_publish.assert_called_once()

    @defer.inlineCallbacks
    def test_a_publication_waits_for_the_previous_one_to_be_confirmed(self):
        chan = self._confirming_channel()
        first_confirmation = defer.Deferred()
        chan.basic_publish.side_effect = [first_confirmation, defer.succeed(None)]
        self.connection.channel = Mock(return_value=defer.succeed(chan))

        first = self.factory.publish_confirmed('messaging', 'deliver.sm.c', self.content)
        second = self.factory.publish_confirmed('messaging', 'deliver.sm.c', self.content)
        self.assertEqual(chan.basic_publish.call_count, 1)

        first_confirmation.callback(None)
        yield defer.gatherResults([first, second])
        self.assertEqual(chan.basic_publish.call_count, 2)

    @defer.inlineCallbacks
    def test_an_unanswered_publication_aborts_its_connection_and_frees_the_publisher(self):
        stalled, fresh = self._confirming_channel(), self._confirming_channel()
        stalled.basic_publish.return_value = defer.Deferred()
        self.connection.channel = Mock(side_effect=[defer.succeed(stalled), defer.succeed(fresh)])

        first = self.factory.publish_confirmed('', 'q', self.content)
        self.clock.advance(factory_module.PUBLICATION_DEADLINE_SECONDS)
        yield self.assertFailure(first, defer.TimeoutError)
        self.connection.transport.abortConnection.assert_called_once_with()

        stalled.is_open = False
        yield self.factory.publish_confirmed('', 'q', self.content)
        fresh.basic_publish.assert_called_once()
        self.assertEqual(self.factory._params.blocked_connection_timeout, factory_module.PUBLICATION_DEADLINE_SECONDS)

    @defer.inlineCallbacks
    def test_an_unanswered_channel_setup_aborts_its_connection_and_frees_the_publisher(self):
        fresh = self._confirming_channel()
        self.connection.channel = Mock(side_effect=[defer.Deferred(), defer.succeed(fresh)])

        first = self.factory.publish_confirmed('', 'q', self.content)
        self.clock.advance(factory_module.PUBLICATION_DEADLINE_SECONDS)
        yield self.assertFailure(first, defer.TimeoutError)
        self.connection.transport.abortConnection.assert_called_once_with()

        yield self.factory.publish_confirmed('', 'q', self.content)
        fresh.basic_publish.assert_called_once()

    @defer.inlineCallbacks
    def test_a_publication_whose_guard_fails_under_the_lock_is_never_published(self):
        chan = self._confirming_channel()
        self.connection.channel = Mock(return_value=defer.succeed(chan))

        yield self.assertFailure(self.factory.publish_confirmed(
            '', 'q', self.content, guard=lambda: defer.fail(RuntimeError('lease lost'))), RuntimeError)
        chan.basic_publish.assert_not_called()

        yield self.factory.publish_confirmed('', 'q', self.content)
        chan.basic_publish.assert_called_once()
