import json

import treq
from twisted.internet import defer

from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.stats import SMPPClientStatsCollector
from jasmin.routing.proxies import RouterPBProxy
from tests.routing.test_router import HappySMSCTestCase, waitFor

TOKEN = 'control-secret'


class ConnectorControlTestCase(RouterPBProxy, HappySMSCTestCase):
    """Connectors are added, replaced, stopped, started and retired over a typed, authenticated endpoint, and the
    running configuration reads back on /status under the same names."""

    @defer.inlineCallbacks
    def setUp(self):
        yield HappySMSCTestCase.setUp(self)
        for child in (b'connectors', b'routes', b'users'):
            self.httpApi.children[child].control_token = TOKEN

    def url(self, *path, resource='connectors'):
        return ('http://127.0.0.1:%d/%s/%s' % (self.httpPort, resource, '/'.join(path))).encode()

    def call(self, method, *path, body=None, token=TOKEN, resource='connectors'):
        headers = {b'Authorization': [b'Bearer ' + token.encode()]} if token else {}
        data = json.dumps(body).encode() if body is not None else None
        return treq.request(method, self.url(*path, resource=resource), data=data, headers=headers,
                            persistent=False)

    @defer.inlineCallbacks
    def answer(self, response):
        defer.returnValue((response.code, response.headers.getRawHeaders('Jasmin-Error-Code', [None])[0],
                           json.loads((yield treq.content(response)))))

    @defer.inlineCallbacks
    def status(self, cid):
        response = yield treq.get(('http://127.0.0.1:%d/status' % self.httpPort).encode(), persistent=False)
        body = json.loads((yield treq.content(response)))
        defer.returnValue(next(c for c in body['connectors'] if c['cid'] == cid))

    def settings(self, **extra):
        return dict({'host': '127.0.0.1', 'port': self.SMSCPort.getHost().port, 'username': 'smppclient1',
                     'password': 'password', 'submit_sm_throughput': 25, 'dlr_expiry': 259200,
                     'source_addr_ton': 'ALPHANUMERIC', 'source_addr_npi': 'UNKNOWN'}, **extra)

    @defer.inlineCallbacks
    def bound(self, cid):
        for _ in range(50):
            if (yield self.status(cid))['session_state'].startswith('BOUND_'):
                defer.returnValue(True)
            yield waitFor(0.2)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def drained(self, cid):
        for _ in range(50):
            if SMPPClientStatsCollector().get(cid).get('submit_sm_count'):
                defer.returnValue(True)
            yield waitFor(0.2)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def retire(self, cid):
        if self.clientManager_f.getConnector(cid) is not None:
            yield self.clientManager_f.perspective_connector_stop(cid, delQueues=True)
            yield self.clientManager_f.perspective_connector_remove(cid)

    @defer.inlineCallbacks
    def queue_one_submit(self, cid):
        pdu = SMPPOperationFactory(SMPPClientConfig(id=cid)).SubmitSM(
            source_addr='1', destination_addr='2', short_message='held')
        yield self.clientManager_f.perspective_submit_sm(
            uid='u', cid=cid, SubmitSmPDU=pdu, submit_sm_bill=None, pickled=False, dlr_level=0)

    @defer.inlineCallbacks
    def test_without_a_configured_token_the_endpoint_refuses_everything(self):
        self.httpApi.children[b'connectors'].control_token = ''
        code, token, _ = yield self.answer((yield self.call('PUT', 'c-off', body=self.settings())))
        self.assertEqual((code, token), (403, 'control_disabled'))
        self.assertIsNone(self.clientManager_f.getConnector('c-off'))

    @defer.inlineCallbacks
    def test_a_wrong_token_is_refused(self):
        code, token, _ = yield self.answer((yield self.call('PUT', 'c-wrong', body=self.settings(), token='nope')))
        self.assertEqual((code, token), (401, 'unauthorized'))

    @defer.inlineCallbacks
    def test_a_new_connector_binds_and_reads_back_its_settings(self):
        self.addCleanup(self.retire, 'c-new')
        code, _, body = yield self.answer((yield self.call('PUT', 'c-new', body=self.settings())))

        self.assertEqual((code, body['created'], body['running']), (200, True, True))
        self.assertTrue((yield self.bound('c-new')))
        config = (yield self.status('c-new'))['config']
        self.assertEqual((config['host'], config['submit_sm_throughput'], config['dlr_expiry'],
                          config['source_addr_ton'], config['bind']),
                         ('127.0.0.1', 25, 259200, 'ALPHANUMERIC', 'transceiver'))
        self.assertNotIn('password', config)
        self.assertFalse(config['log_privacy'])
        self.assertTrue((yield self.status('c-new'))['started'])

    @defer.inlineCallbacks
    def test_an_unknown_setting_or_value_is_refused(self):
        code, token, _ = yield self.answer((yield self.call('PUT', 'c-bad', body=self.settings(throughput=5))))
        self.assertEqual((code, token), (400, 'invalid_connector'))
        code, token, _ = yield self.answer((yield self.call(
            'PUT', 'c-bad', body=self.settings(source_addr_ton='GALACTIC'))))
        self.assertEqual((code, token), (400, 'invalid_connector'))
        self.assertIsNone(self.clientManager_f.getConnector('c-bad'))

    @defer.inlineCallbacks
    def test_replacing_a_connector_keeps_its_backlog(self):
        self.addCleanup(self.retire, 'c-held')
        yield self.call('PUT', 'c-held', body=self.settings(started=False))
        yield self.queue_one_submit('c-held')
        yield waitFor(0.5)

        code, _, body = yield self.answer((yield self.call(
            'PUT', 'c-held', body=self.settings(submit_sm_throughput=40, started=False))))

        self.assertEqual((code, body['created'], body['running']), (200, False, False))
        status = yield self.status('c-held')
        self.assertEqual(status['config']['submit_sm_throughput'], 40)
        self.assertEqual(status['messages_ready'], 1)

    @defer.inlineCallbacks
    def test_a_connector_with_a_backlog_is_not_retired_until_it_drains(self):
        yield self.call('PUT', 'c-retire', body=self.settings(started=False))
        self.clientManager_f.getConnector('c-retire')['sm_listener'].config.submit_retrial_delay_smppc_not_ready = 1
        yield self.queue_one_submit('c-retire')
        yield waitFor(0.5)

        code, token, _ = yield self.answer((yield self.call('DELETE', 'c-retire')))
        self.assertEqual((code, token), (409, 'backlog_not_empty'))

        yield self.call('POST', 'c-retire', 'start')
        self.assertTrue((yield self.bound('c-retire')))
        self.assertTrue((yield self.drained('c-retire')))
        self.assertEqual((yield self.status('c-retire'))['messages_ready'], 0)
        yield self.call('POST', 'c-retire', 'stop')

        code, _, body = yield self.answer((yield self.call('DELETE', 'c-retire')))
        self.assertEqual((code, body['retired']), (200, True))
        self.assertIsNone(self.clientManager_f.getConnector('c-retire'))

    @defer.inlineCallbacks
    def test_stop_and_start_change_the_running_state(self):
        self.addCleanup(self.retire, 'c-cycle')
        yield self.call('PUT', 'c-cycle', body=self.settings())
        self.assertTrue((yield self.bound('c-cycle')))

        code, _, body = yield self.answer((yield self.call('POST', 'c-cycle', 'stop')))
        self.assertEqual((code, body['running']), (200, False))
        code, _, body = yield self.answer((yield self.call('POST', 'c-cycle', 'start')))
        self.assertEqual((code, body['running']), (200, True))

    @defer.inlineCallbacks
    def test_an_unknown_connector_is_not_found(self):
        code, token, _ = yield self.answer((yield self.call('POST', 'c-ghost', 'stop')))
        self.assertEqual((code, token), (404, 'connector_not_found'))

    @defer.inlineCallbacks
    def test_repeating_the_same_settings_keeps_the_bind(self):
        self.addCleanup(self.retire, 'c-same')
        yield self.call('PUT', 'c-same', body=self.settings())
        self.assertTrue((yield self.bound('c-same')))
        service = self.clientManager_f.getConnector('c-same')['service']

        code, _, body = yield self.answer((yield self.call('PUT', 'c-same', body=self.settings())))

        self.assertEqual((code, body['created'], body['running']), (200, False, True))
        self.assertIs(self.clientManager_f.getConnector('c-same')['service'], service)

    @defer.inlineCallbacks
    def test_each_tag_routes_only_over_its_own_connectors(self):
        for cid in ('c-a-1', 'c-a-2', 'c-b-1'):
            self.addCleanup(self.retire, cid)
            yield self.call('PUT', cid, body=self.settings(started=False))
        body = {'routes': [{'tag': 'login-a', 'connectors': ['c-a-1', 'c-a-2']},
                           {'tag': 'login-b', 'connectors': ['c-b-1']}]}

        code, _, _ = yield self.answer((yield self.call('PUT', 'mt', body=body, resource='routes')))
        routes = [list(r.values())[0] for r in self.pbRoot_f.mt_routing_table.getAll()]

        self.assertEqual(code, 200)
        self.assertEqual(sorted(([f.tag for f in r.filters], [c.cid for c in r.connector]) for r in routes),
                         [(['login-a'], ['c-a-1', 'c-a-2']), (['login-b'], ['c-b-1'])])

    @defer.inlineCallbacks
    def test_a_route_to_an_unknown_connector_is_refused_and_the_table_kept(self):
        self.addCleanup(self.retire, 'c-kept')
        yield self.call('PUT', 'c-kept', body=self.settings(started=False))
        yield self.call('PUT', 'mt', body={'routes': [{'tag': 'kept', 'connectors': ['c-kept']}]}, resource='routes')

        code, token, _ = yield self.answer((yield self.call(
            'PUT', 'mt', body={'routes': [{'tag': 'ghost', 'connectors': ['c-ghost']}]}, resource='routes')))

        self.assertEqual((code, token), (400, 'invalid_route'))
        self.assertEqual(len(self.pbRoot_f.mt_routing_table.getAll()), 1)

    @defer.inlineCallbacks
    def test_the_send_user_is_created_once_and_then_left_alone(self):
        body = {'password': 'sendpwd', 'group': 'senders'}

        code, _, first = yield self.answer((yield self.call('PUT', 'sender1', body=body, resource='users')))
        _, _, second = yield self.answer((yield self.call('PUT', 'sender1', body=body, resource='users')))

        self.assertEqual((code, first['created'], second['created']), (200, True, False))
        self.assertIsNotNone(self.pbRoot_f.authenticateUser('sender1', 'sendpwd'))

    @defer.inlineCallbacks
    def test_a_running_connector_is_not_retired_until_stopped(self):
        self.addCleanup(self.retire, 'c-live')
        yield self.call('PUT', 'c-live', body=self.settings())

        code, token, _ = yield self.answer((yield self.call('DELETE', 'c-live')))

        self.assertEqual((code, token), (409, 'connector_running'))
        self.assertIsNotNone(self.clientManager_f.getConnector('c-live'))

    @defer.inlineCallbacks
    def test_repeating_the_settings_with_started_false_stops_it(self):
        self.addCleanup(self.retire, 'c-halt')
        yield self.call('PUT', 'c-halt', body=self.settings())

        code, _, body = yield self.answer((yield self.call('PUT', 'c-halt', body=self.settings(started=False))))

        self.assertEqual((code, body['running']), (200, False))

    @defer.inlineCallbacks
    def test_a_throughput_change_keeps_the_bind(self):
        self.addCleanup(self.retire, 'c-pace')
        yield self.call('PUT', 'c-pace', body=self.settings())
        self.assertTrue((yield self.bound('c-pace')))
        service = self.clientManager_f.getConnector('c-pace')['service']

        code, _, body = yield self.answer((yield self.call(
            'PUT', 'c-pace', body=self.settings(submit_sm_throughput=7.5))))

        self.assertEqual((code, body['created'], body['running']), (200, False, True))
        self.assertIs(self.clientManager_f.getConnector('c-pace')['service'], service)
        self.assertEqual((yield self.status('c-pace'))['config']['submit_sm_throughput'], 7.5)

    @defer.inlineCallbacks
    def test_a_connector_holding_an_unsettled_submit_is_neither_stopped_nor_rebound(self):
        self.addCleanup(self.retire, 'c-busy')
        yield self.call('PUT', 'c-busy', body=self.settings())
        listener = self.clientManager_f.getConnector('c-busy')['sm_listener']
        listener.in_flight.add('unsettled')
        self.addCleanup(listener.in_flight.clear)

        stop, stop_token, _ = yield self.answer((yield self.call('POST', 'c-busy', 'stop')))
        rebind, rebind_token, _ = yield self.answer((yield self.call(
            'PUT', 'c-busy', body=self.settings(port=1))))

        self.assertEqual((stop, stop_token), (409, 'connector_busy'))
        self.assertEqual((rebind, rebind_token), (409, 'connector_busy'))
        self.assertEqual((yield self.status('c-busy'))['messages_in_flight'], 1)

    @defer.inlineCallbacks
    def test_the_first_submit_a_connector_takes_holds_off_a_stop(self):
        self.addCleanup(self.retire, 'c-first')
        yield self.call('PUT', 'c-first', body=self.settings())
        self.assertTrue((yield self.bound('c-first')))
        listener = self.clientManager_f.getConnector('c-first')['sm_listener']
        listener.submit_sm_callback = lambda message: defer.Deferred()
        self.addCleanup(listener.in_flight.clear)
        pdu = SMPPOperationFactory(SMPPClientConfig(id='c-first')).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='first')

        yield self.clientManager_f.perspective_submit_sm(
            uid='u', cid='c-first', SubmitSmPDU=pdu, submit_sm_bill=None, pickled=False)
        yield waitFor(1)
        code, token, _ = yield self.answer((yield self.call('POST', 'c-first', 'stop')))

        self.assertEqual(len(listener.in_flight), 1)
        self.assertEqual((code, token), (409, 'connector_busy'))
        self.assertIsNotNone(self.clientManager_f.getConnector('c-first')['consumer_tag'])

    @defer.inlineCallbacks
    def test_an_empty_route_list_clears_every_route(self):
        self.addCleanup(self.retire, 'c-clear')
        yield self.call('PUT', 'c-clear', body=self.settings(started=False))
        yield self.call('PUT', 'mt', body={'routes': [{'tag': 'clear', 'connectors': ['c-clear']}]}, resource='routes')

        code, _, _ = yield self.answer((yield self.call('PUT', 'mt', body={'routes': []}, resource='routes')))

        self.assertEqual(code, 200)
        self.assertEqual(self.pbRoot_f.mt_routing_table.getAll(), [])

    @defer.inlineCallbacks
    def test_a_disabled_send_group_is_enabled_again(self):
        body = {'password': 'sendpwd', 'group': 'senders-off'}
        yield self.call('PUT', 'sender-off', body=body, resource='users')
        self.pbRoot_f.perspective_group_disable('senders-off')

        yield self.call('PUT', 'sender-off', body=body, resource='users')

        self.assertTrue(self.pbRoot_f.getGroup('senders-off').enabled)
        self.assertIsNotNone(self.pbRoot_f.authenticateUser('sender-off', 'sendpwd'))
