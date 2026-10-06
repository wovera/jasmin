import tempfile

from twisted.trial.unittest import TestCase

from jasmin.bin.jasmind import Options
from jasmin.managers.configs import SMPPClientSMListenerConfig


class OutcomeInfrastructureTestCase(TestCase):
    def _config(self, dlr_forward_queue):
        config = tempfile.NamedTemporaryFile('w', suffix='.cfg', delete=False)
        config.write('[dlr]\ndlr_forward_queue=%s\n' % dlr_forward_queue)
        config.close()
        return config.name

    def test_forwarded_outcomes_start_without_the_embedded_dlr_lookup(self):
        Options().parseOptions(['-c', self._config('outcomes')])

    def test_the_listener_makes_outcomes_mandatory_only_when_they_are_forwarded(self):
        self.assertTrue(SMPPClientSMListenerConfig(self._config('outcomes')).mandatory_outcomes)
        self.assertFalse(SMPPClientSMListenerConfig(self._config('')).mandatory_outcomes)
