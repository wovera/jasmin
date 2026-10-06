from unittest.mock import Mock

from smpp.twisted.protocol import SMPPClientProtocol as twistedSMPPClientProtocol
from smpp.twisted.protocol import SMPPSessionStates
from twisted.internet import task
from twisted.trial.unittest import TestCase

from jasmin.protocols.smpp import factory as factory_module
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.factory import SMPPClientFactory
from jasmin.protocols.smpp.protocol import SMPPClientProtocol


class SMPPReconnectBackoffTestCase(TestCase):
    def setUp(self):
        self.clock = task.Clock()
        self.patch(factory_module, 'reactor', self.clock)
        self.factory = SMPPClientFactory(SMPPClientConfig(id='backoff'))

    def _delays(self, count, event):
        delays = []
        for _ in range(count):
            event(Mock(), Exception('bind rejected'))
            delays.append(self.factory.reconnectTimer.getTime() - self.clock.seconds())
            self.factory.reconnectTimer.cancel()
        return delays

    def _assert_doubling(self, base, delays):
        for attempt, delay in enumerate(delays):
            self.assertTrue(0.75 * base * 2 ** attempt <= delay <= 1.25 * base * 2 ** attempt, delays)

    def test_a_connection_lost_again_and_again_backs_off(self):
        self._assert_doubling(self.factory.config.reconnectOnConnectionLossDelay,
                              self._delays(4, self.factory.clientConnectionLost))

    def test_a_connection_refused_again_and_again_backs_off(self):
        self._assert_doubling(self.factory.config.reconnectOnConnectionFailureDelay,
                              self._delays(4, self.factory.clientConnectionFailed))

    def test_only_a_successful_bind_restarts_the_backoff(self):
        self._delays(3, self.factory.clientConnectionLost)
        protocol = SMPPClientProtocol()
        protocol.factory = self.factory
        self.patch(twistedSMPPClientProtocol, 'bindSucceeded', lambda protocol, result, nextState: result)

        protocol.bindSucceeded(None, SMPPSessionStates.BOUND_TRX)

        self.assertEqual(self.factory.reconnectAttempts, 0)
