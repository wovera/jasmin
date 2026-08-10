from unittest.mock import Mock

from twisted.internet import defer

from tests.managers.test_managers import SMPPClientPBProxyTestCase, waitFor


class ReconnectConsumeTestCase(SMPPClientPBProxyTestCase):
    """A running connector's channel dies with the AMQP connection, and a broker restart takes the exchange,
    queue and binding with it. Reopening a channel is not enough: the resume has to redo the whole setup the
    connector had, or it consumes from a queue that no longer exists and throttles without a prefetch limit."""

    @defer.inlineCallbacks
    def setUp(self):
        yield SMPPClientPBProxyTestCase.setUp(self)
        self.amqpBroker.config.reconnectOnConnectionLoss = True
        self.amqpBroker.config.reconnectOnConnectionLossDelay = 1

    @defer.inlineCallbacks
    def tearDown(self):
        # Left on, a plain disconnect in the parent teardown only triggers another reconnect.
        self.amqpBroker.config.reconnectOnConnectionLoss = False
        yield SMPPClientPBProxyTestCase.tearDown(self)

    @defer.inlineCallbacks
    def _runningConnector(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)

        connector = self.clientManagerPB.getConnector(self.defaultConfig.id)
        # Starting the service for real needs a live SMSC; the resume keys on nothing but this flag.
        connector['service'].running = 1
        defer.returnValue(connector)

    @defer.inlineCallbacks
    def test_resume_redeclares_the_topology_a_broker_restart_took(self):
        yield self._runningConnector()
        queue_name = 'submit.sm.%s' % self.defaultConfig.id
        yield self.amqpBroker.chan.queue_delete(queue=queue_name)

        # Drop the transport rather than disconnecting: only the unexpected path reconnects, and the
        # redeclare memo is reset per connection, so only a real reconnect exercises the setup again.
        self.amqpBroker.client.transport.loseConnection()
        yield waitFor(4)

        # Passive, so it asserts the queue is back rather than creating it: a resume that only reopened a
        # channel leaves nothing here to find.
        chan = yield self.amqpBroker.newChannel()
        yield chan.queue_declare(queue=queue_name, passive=True)

    @defer.inlineCallbacks
    def test_resume_keeps_the_prefetch_limit_that_throttles_the_connector(self):
        connector = yield self._runningConnector()

        openedChannels = []
        newChannel = self.amqpBroker.newChannel

        @defer.inlineCallbacks
        def recordingNewChannel():
            chan = yield newChannel()
            chan.basic_qos = Mock(wraps=chan.basic_qos)
            openedChannels.append(chan)
            defer.returnValue(chan)

        self.amqpBroker.newChannel = recordingNewChannel
        try:
            yield self.clientManagerPB.reconsumeRunningConnectors()
        finally:
            self.amqpBroker.newChannel = newChannel

        self.assertEqual(connector['chan'], openedChannels[0])
        # Unlimited prefetch pushes a whole backlog unacked into memory and removes broker-side flow control.
        openedChannels[0].basic_qos.assert_called_once_with(prefetch_count=1)
