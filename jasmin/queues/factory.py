# pylint: disable=E0203
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

import pika
from twisted.internet import defer, reactor
from twisted.internet.protocol import ClientFactory

from jasmin.queues.protocol import AmqpProtocol
from jasmin.tools.jitter import jittered

LOG_CATEGORY = "jasmin-amqp-factory"


class AmqpFactory(ClientFactory):
    protocol = AmqpProtocol

    def __init__(self, config):
        self.reconnectTimer = None
        self.connectionRetry = True
        self.connected = False
        self.config = config
        self.channelReady = None
        self.exitDeferred = None
        self.connectDeferred = None

        self.client = None  # the pika connection (TwistedProtocolConnection), set in buildProtocol
        self.chan = None  # the control channel (declares + publishes); consumers get their own via newChannel()
        self.queues = []
        self.channelReadyCallbacks = []

        self._params = pika.ConnectionParameters(
            host=config.host,
            port=config.port,
            virtual_host=config.vhost,
            credentials=pika.PlainCredentials(config.username, config.password),
            heartbeat=config.heartbeat,
        )

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file,
                                                   when=self.config.log_rotate)
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def preConnect(self):
        """Initiate deferreds before connecting. Separate from _connect() because the bins call it
        directly when jasmin runs as a twistd plugin."""
        self.connectionRetry = True
        self.exitDeferred = defer.Deferred()
        if self.channelReady is None or self.channelReady is False or self.channelReady.called:
            self.channelReady = defer.Deferred()
        if self.connectDeferred is None or self.connectDeferred.called:
            self.connectDeferred = defer.Deferred()

    def startedConnecting(self, connector):
        self.log.info("Connecting to %s ...", connector.getDestination())

    def getExitDeferred(self):
        """Notified on disconnect+exit without further reconnection retries."""
        return self.exitDeferred

    def getChannelReadyDeferred(self):
        """Notified when the control channel is open and ready. Fresh per connection, so a caller holding a
        spent one is holding the previous connection's signal."""
        return self.channelReady

    def addChannelReadyCallback(self, callback):
        """Invoked on every connection, including reconnections. A consumer's channel dies with the
        connection, so subscribing once at boot leaves the queue filling with nothing draining it. Registering
        is idempotent, so a consumer may re-register from inside its own subscribe path."""
        if callback not in self.channelReadyCallbacks:
            self.channelReadyCallbacks.append(callback)

    def clientConnectionFailed(self, connector, reason):
        self.log.error("Connection failed. Reason: %s", str(reason))
        self.connected = False

        if self.config.reconnectOnConnectionFailure and self.connectionRetry:
            delay = jittered(self.config.reconnectOnConnectionFailureDelay)
            self.log.info("Reconnecting after %.1f seconds ...", delay)
            self.reconnectTimer = reactor.callLater(delay, self.reConnect, connector)
        else:
            if self.connectDeferred is not None and not self.connectDeferred.called:
                self.connectDeferred.errback(reason)
            self.exitDeferred.callback(self)
            self.log.info("Exiting.")

    def clientConnectionLost(self, connector, reason):
        if 'Connection was closed cleanly.' not in str(reason):
            self.log.error("Connection lost. Reason: %s", str(reason))
        else:
            self.log.info("Connection lost. Reason: %s", str(reason))
        self.connected = False
        self.client = None
        self.chan = None

        if self.config.reconnectOnConnectionLoss and self.connectionRetry:
            delay = jittered(self.config.reconnectOnConnectionLossDelay)
            self.log.info("Reconnecting after %.1f seconds ...", delay)
            self.reconnectTimer = reactor.callLater(delay, self.reConnect, connector)
        else:
            self.exitDeferred.callback(self)
            self.log.info("Exiting.")

    def reConnect(self, connector=None):
        if connector is None:
            self.log.error("No connector to retry !")
        else:
            self.preConnect()
            connector.connect()

    def _connect(self):
        self.preConnect()
        self.log.info('Establishing TCP connection to %s:%d', self.config.host, self.config.port)
        reactor.connectTCP(self.config.host, self.config.port, self)
        return self.connectDeferred

    def connect(self):
        return self._connect()

    def buildProtocol(self, addr):
        p = self.protocol(self._params)
        p.factory = self  # Tell the protocol about this factory.
        self.client = p  # Store the connection.
        # pika fires `ready` with the live connection once the AMQP handshake completes.
        p.ready.addCallback(self._on_connection_ready)
        p.ready.addErrback(self._on_connection_failed)
        return p

    @defer.inlineCallbacks
    def _on_connection_ready(self, connection):
        """Open the control channel (for declares + publishes) and signal readiness."""
        self.log.info("AMQP connection ready; opening control channel")
        try:
            self.chan = yield connection.channel()
            self.connected = True
            self.queues = []
            self.channelReady.callback(self)
            if self.connectDeferred is not None and not self.connectDeferred.called:
                self.connectDeferred.callback(self)
        except Exception as e:
            self._on_connection_failed(e)
            return

        for callback in list(self.channelReadyCallbacks):
            try:
                yield defer.maybeDeferred(callback)
            except Exception as e:
                # Swallowed so one consumer cannot strand the others on this connection.
                self.log.error("Channel-ready callback failed; its queue is unattended: %s", e)

    def _on_connection_failed(self, error):
        self.log.error("AMQP connection/channel setup failed: %s", error)
        if self.connectDeferred is not None and not self.connectDeferred.called:
            self.connectDeferred.errback(error)

    def newChannel(self):
        """A fresh, open pika channel. One per consumer/role so delivery-tag spaces stay isolated and acks
        go back on the channel that delivered the message. Returns a Deferred yielding the channel."""
        return self.client.channel()

    def disconnect(self, reason=None):
        self.channelReady = False

        if self.client is not None:
            # pika's close() returns self.closed, which is None until the connection actually closes; the
            # reliable "transport is fully gone" signal is the factory's exitDeferred (fired by
            # clientConnectionLost), so callers can yield this and the reactor is clean afterwards.
            self.client.close()
            return self.exitDeferred

        return None

    def named_queue_declare(self, *args, **keys):
        """Wrapper around the control channel's queue_declare that dedups redeclaration via self.queues."""
        if not self.connected:
            self.log.error("AMQP Client is not connected, cannot queue_declare")
            return None

        for q in self.queues:
            if q == keys.get('queue'):
                self.log.debug('Queue [%s] is already declared, no need to redeclare it', q)
                return None

        return self.chan.queue_declare(*args, **keys).addCallback(self._queue_declared)

    def _queue_declared(self, frame):
        # pika's queue_declare result carries the queue name on the method frame.
        name = frame.method.queue
        self.log.info("A new queue has been successfully declared [%s]", name)
        self.queues.append(name)
        return frame

    def publish(self, exchange='', routing_key='', content=None):
        """Publish a Content (jasmin.queues.content.Content subclass) on the control channel."""
        if not self.connected:
            self.log.error("AMQP Client is not connected, cannot publish to [%s/%s]", exchange, routing_key)
            return None

        return self.chan.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=content.pika_body,
            properties=content.pika_properties,
        )

    def stopConnectionRetrying(self):
        """Stop the factory from reconnecting (reset to True on the next connect())."""
        if self.reconnectTimer and self.reconnectTimer.active():
            self.reconnectTimer.cancel()
            self.reconnectTimer = None

        self.connectionRetry = False

    def disconnectAndDontRetryToConnect(self):
        self.stopConnectionRetrying()
        return self.disconnect()
