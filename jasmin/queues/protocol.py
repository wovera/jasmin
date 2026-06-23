from pika.adapters.twisted_connection import TwistedProtocolConnection


class AmqpProtocol(TwistedProtocolConnection):
    """pika's Twisted connection, wired to AmqpFactory. Logs on connect; pika's own `ready`
    Deferred (set in TwistedProtocolConnection.__init__, fires with the live connection once the
    AMQP handshake completes) is what AmqpFactory waits on to open the control channel."""

    def connectionMade(self):
        self.factory.log.info(
            "Connection made to %s:%s", self.factory.config.host, self.factory.config.port)
        TwistedProtocolConnection.connectionMade(self)
