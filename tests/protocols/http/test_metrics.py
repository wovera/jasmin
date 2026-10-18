from unittest.mock import Mock, patch

from prometheus_client import generate_latest
from smpp.pdu.operations import SubmitSM, SubmitSMResp
from smpp.pdu.pdu_types import CommandStatus
from smpp.twisted.protocol import SMPPClientProtocol as twistedSMPPClientProtocol
from twisted.internet import defer
from twisted.trial.unittest import TestCase
from twisted.web.wsgi import WSGIResource

from jasmin.protocols.smpp.protocol import SMPPClientProtocol
from jasmin.protocols.smpp.stats import SMPPServerStatsCollector
from jasmin.tools.metrics import REGISTRY, DISCARDS, RECEIPTS
from .test_server import HTTPApiTestCases

# Every series the Grafana dashboards in docker/grafana/dashboards/Jasmin query: names must never change.
DASHBOARD_HTTPAPI = ['request_count', 'success_count', 'auth_error_count', 'charging_error_count',
                     'interceptor_error_count', 'route_error_count', 'server_error_count', 'throughput_error_count']
DASHBOARD_SMPPC = ['data_sm_count', 'deliver_sm_count', 'interceptor_error_count', 'other_submit_error_count',
                   'submit_sm_count', 'submit_sm_request_count', 'throttling_error_count', 'bound_count',
                   'disconnected_count']
DASHBOARD_SMPPS = ['bind_rx_count', 'bind_trx_count', 'bind_tx_count', 'connect_count', 'data_sm_count',
                   'deliver_sm_count', 'elink_count', 'interceptor_error_count', 'other_submit_error_count',
                   'submit_sm_count', 'submit_sm_request_count', 'throttling_error_count', 'unbind_count',
                   'bound_rx_count', 'bound_trx_count', 'bound_tx_count']


class MetricsTestCases(HTTPApiTestCases):
    def get_metric(self, key=None):
        """
        Invoke /metrics,
         if key is set: return key's value or None if not found
         if not: return metrics in json format
        """
        # prometheus_client.twisted.MetricsResource is a WSGIResource needing a live reactor thread pool, which the
        # in-process DummySite cannot drive: read the registry it serves instead.
        self.assertIsInstance(self.web.resource.children[b'metrics'], WSGIResource)

        metric_map = {}
        for _line in generate_latest(REGISTRY).decode().split('\n'):
            if len(_line) == 0 or _line[:1] == '#':
                continue
            _key, _value = _line.rsplit(' ', 1)
            metric_map[_key] = _value

        return metric_map.get(key) if key is not None else metric_map


class HttpapiTestCases(MetricsTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_send_auth_error(self):
        _before = self.get_metric()

        response = yield self.web.post(b'send', {b'username': self.username,
                                      b'password': 'incorrec',
                                      b'to': b'06155423',
                                      b'content': 'anycontent'})
        self.assertEqual(response.value(),
                         b'Error "Authentication failure for username:%s"' % self.username.encode())
        self.assertEqual(response.responseCode, 403)

        _after = self.get_metric()
        self.assertEqual(int(float(_before['httpapi_request_count'])) + 1,
                         int(float(_after['httpapi_request_count'])))
        self.assertEqual(int(float(_before['httpapi_auth_error_count'])) + 1,
                         int(float(_after['httpapi_auth_error_count'])))

    @defer.inlineCallbacks
    def test_send_other_error(self):
        _before = self.get_metric()

        response = yield self.web.post(b'send', {b'username': 'nathalie',
                                                 b'password': 'correct',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.value(),
                         b'Error "Submit not confirmed by the broker"')
        self.assertEqual(response.responseCode, 503)

        _after = self.get_metric()
        self.assertEqual(int(float(_before['httpapi_request_count'])) + 1,
                         int(float(_after['httpapi_request_count'])))
        self.assertEqual(int(float(_before['httpapi_server_error_count'])) + 1,
                         int(float(_after['httpapi_server_error_count'])))


class SeriesTestCases(MetricsTestCases):
    def setUp(self):
        MetricsTestCases.setUp(self)
        self.clientManager_f.perspective_connector_list = Mock(
            return_value=[{'id': 'c1', 'session_state': 'BOUND_TRX'}])
        SMPPServerStatsCollector().get('smpps_a')
        SMPPServerStatsCollector().get('smpps_b')

    def test_dashboard_series_keep_their_names_and_labels(self):
        exposed = self.get_metric()
        for name in DASHBOARD_HTTPAPI:
            self.assertIn('httpapi_%s' % name, exposed)
        for name in DASHBOARD_SMPPC:
            self.assertIn('smppc_%s{cid="c1"}' % name, exposed)
        for name in DASHBOARD_SMPPS:
            self.assertIn('smppsapi_%s{cid="smpps_a"}' % name, exposed)

    def test_every_smpp_server_is_exposed(self):
        exposed = self.get_metric()
        self.assertIn('smppsapi_connect_count{cid="smpps_a"}', exposed)
        self.assertIn('smppsapi_connect_count{cid="smpps_b"}', exposed)

    def test_bind_state(self):
        exposed = self.get_metric()
        self.assertEqual(exposed['smppc_bind_state{cid="c1",state="BOUND_TRX"}'], '1.0')

    @defer.inlineCallbacks
    def test_send_responses_by_code(self):
        before = REGISTRY.get_sample_value('httpapi_send_responses_total', {'code': '403'}) or 0
        yield self.web.post(b'send', {b'username': 'nathalie', b'password': 'incorrec',
                                      b'to': b'06155423', b'content': 'anycontent'})
        self.assertEqual(REGISTRY.get_sample_value('httpapi_send_responses_total', {'code': '403'}), before + 1)
        exposed = self.get_metric()
        self.assertIn('httpapi_send_responses_total{code="403"}', exposed)

    def test_labelled_counters_are_exposed(self):
        DISCARDS.labels(cid='c1', reason='expired').inc()
        RECEIPTS.labels(cid='c1', state='DELIVRD').inc()
        exposed = self.get_metric()
        self.assertIn('smppc_discards_total{cid="c1",reason="expired"}', exposed)
        self.assertIn('smppc_receipts_total{cid="c1",state="DELIVRD"}', exposed)


class SubmitSmWireTestCases(TestCase):
    def test_submit_sm_resp_status_and_round_trip(self):
        protocol = SMPPClientProtocol()
        protocol.factory = Mock()
        protocol.factory.config.id = 'wire1'
        protocol.encoder = Mock()
        protocol.transport = Mock()
        protocol.onSMPPOperation = Mock()
        labels = {'cid': 'wire1', 'status': 'ESME_RTHROTTLED'}
        before = REGISTRY.get_sample_value('smppc_submit_sm_resp_total', labels) or 0
        rtt_before = REGISTRY.get_sample_value('smppc_submit_sm_roundtrip_seconds_count', {'cid': 'wire1'}) or 0

        protocol.sendPDU(SubmitSM(seqNum=7, source_addr='a', destination_addr='b', short_message=b'x'))
        with patch.object(twistedSMPPClientProtocol, 'PDUResponseReceived'):
            protocol.PDUResponseReceived(SubmitSMResp(seqNum=7, status=CommandStatus.ESME_RTHROTTLED))

        self.assertEqual(REGISTRY.get_sample_value('smppc_submit_sm_resp_total', labels), before + 1)
        self.assertEqual(
            REGISTRY.get_sample_value('smppc_submit_sm_roundtrip_seconds_count', {'cid': 'wire1'}), rtt_before + 1)
        self.assertEqual(protocol.submitSentAt, {})
