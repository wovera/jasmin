import json
from unittest.mock import Mock

from twisted.internet import defer

from jasmin.protocols.smpp.configs import SMPPClientConfig
from .test_server import HTTPApiTestCases


class StatusTestCases(HTTPApiTestCases):
    def setUp(self):
        HTTPApiTestCases.setUp(self)

        config = SMPPClientConfig(id='status-probe', submit_sm_throughput=10)

        session_state = Mock()
        session_state.name = 'BOUND_TRX'
        service = Mock()
        service.running = True
        service.startCounter = 1
        service.stopCounter = 0
        service.SMPPClientFactory.getSessionState.return_value = session_state

        declared = Mock()
        declared.method.message_count = 3
        status_chan = Mock()
        status_chan.queue_declare.return_value = defer.succeed(declared)
        self.clientManager_f.amqpBroker = Mock()
        self.clientManager_f.amqpBroker.newChannel = Mock(return_value=defer.succeed(status_chan))

        sm_listener = Mock()
        sm_listener.oldest_ready_age_seconds.return_value = 12.0

        self.clientManager_f.connectors.append({
            'id': config.id,
            'config': config,
            'service': service,
            'chan': Mock(),
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': sm_listener,
        })

    @defer.inlineCallbacks
    def test_status_lists_every_connector_with_its_depth(self):
        response = yield self.web.get(b'status')
        self.assertEqual(response.responseCode, 200)
        payload = json.loads(response.value())
        self.assertIn('connectors', payload)
        for connector in payload['connectors']:
            for key in ('cid', 'session_state', 'submit_sm_throughput', 'learned_throughput',
                        'messages_ready', 'oldest_ready_age_seconds', 'throttling_error_count'):
                self.assertIn(key, connector)

    @defer.inlineCallbacks
    def test_status_reports_the_stubbed_connector_values(self):
        response = yield self.web.get(b'status')
        payload = json.loads(response.value())
        connector = payload['connectors'][0]
        self.assertEqual(connector['cid'], 'status-probe')
        self.assertEqual(connector['session_state'], 'BOUND_TRX')
        self.assertEqual(connector['submit_sm_throughput'], 10)
        self.assertEqual(connector['learned_throughput'], 10)
        self.assertEqual(connector['messages_ready'], 3)
        self.assertEqual(connector['oldest_ready_age_seconds'], 12.0)
        self.assertEqual(connector['throttling_error_count'], 0)

    @defer.inlineCallbacks
    def test_status_reads_a_profile_persisted_before_learned_throughput_existed(self):
        config = self.clientManager_f.connectors[0]['config']
        del config.learned_throughput
        response = yield self.web.get(b'status')
        self.assertEqual(response.responseCode, 200)
        connector = json.loads(response.value())['connectors'][0]
        self.assertEqual(connector['learned_throughput'], connector['submit_sm_throughput'])


class StatusEmptyTestCases(HTTPApiTestCases):
    @defer.inlineCallbacks
    def test_status_with_no_connectors_returns_an_empty_list(self):
        response = yield self.web.get(b'status')
        self.assertEqual(response.responseCode, 200)
        payload = json.loads(response.value())
        self.assertEqual(payload, {'connectors': []})
