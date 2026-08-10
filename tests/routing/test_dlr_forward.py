import json

from twisted.internet import defer, reactor
from twisted.web import server
from twisted.web.client import Agent
from treq import text_content
from treq.client import HTTPClient

from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.dlr import DLRLookup
from tests.routing.http_server import AckServer
from tests.routing.test_router import HappySMSCTestCase, SubmitSmTestCaseTools
from tests.routing.test_routing_submit_sm_and_dlr import waitFor
from jasmin.routing.proxies import RouterPBProxy


class DlrForwardQueueTestCases(RouterPBProxy, HappySMSCTestCase, SubmitSmTestCaseTools):
    forward_queue = 'test.dlr.forward'

    @defer.inlineCallbacks
    def setUp(self):
        yield HappySMSCTestCase.setUp(self)

        self.AckServerResource = AckServer()
        self.AckServer = reactor.listenTCP(0, server.Site(self.AckServerResource))

        # Enable the JSON outcome forwarder on the running DLRLookup and declare its durable target queue
        # (the downstream AMQP consumer owns declaration in production; here the test plays that role).
        self.dlrlookup.config.dlr_forward_queue = self.forward_queue
        yield self.amqpBroker.chan.queue_declare(queue=self.forward_queue, durable=True)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.amqpBroker.chan.queue_delete(queue=self.forward_queue)
        yield self.AckServer.stopListening()
        yield HappySMSCTestCase.tearDown(self)

    @defer.inlineCallbacks
    def _get_forward(self):
        result = yield self.amqpBroker.chan.basic_get(queue=self.forward_queue, auto_ack=True)
        if result is None:
            defer.returnValue(None)
        defer.returnValue(json.loads(result.body))

    @defer.inlineCallbacks
    def _drain_forwards(self):
        out = []
        while True:
            r = yield self._get_forward()
            if r is None:
                break
            out.append(r)
        defer.returnValue(out)

    @defer.inlineCallbacks
    def _send(self, dlr_level, dlr_url=True):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.prepareRoutingsAndStartConnector()

        if dlr_url:
            self.params['dlr-url'] = self.dlr_url
        else:
            self.params.pop('dlr-url', None)
        self.params['dlr-level'] = dlr_level
        agent = Agent(reactor)
        client = HTTPClient(agent)
        response = yield client.post('http://127.0.0.1:1401/send', data=self.params)
        c = yield text_content(response)
        defer.returnValue(c[9:45])

    @defer.inlineCallbacks
    def test_forwards_level1_and_level2_outcomes(self):
        msgId = yield self._send(dlr_level=3)

        yield waitFor(2)  # level1 (submit_sm_resp)
        yield self.SMSCPort.factory.lastClient.trigger_DLR()
        yield waitFor(1)  # level2 (deliver_sm)
        yield self.stopSmppClientConnectors()

        recs = {}
        for _ in range(2):
            r = yield self._get_forward()
            self.assertIsNotNone(r)
            recs[r['level']] = r

        self.assertEqual(set(recs), {1, 2})

        level1 = recs[1]
        self.assertEqual(level1['msgid'], msgId)
        self.assertEqual(level1['command_status'], 'ESME_ROK')
        self.assertTrue(level1['id_smsc'])  # Message.Id → SMSC id bound at accept
        self.assertFalse(level1['will_be_retried'])
        self.assertIsNone(level1['discard_reason'])

        level2 = recs[2]
        self.assertEqual(level2['msgid'], msgId)
        self.assertEqual(level2['message_state'], 'DELIVRD')
        self.assertEqual(level2['sub'], '001')
        self.assertEqual(level2['dlvrd'], '001')
        self.assertEqual(level2['err'], '000')

    @defer.inlineCallbacks
    def test_forwards_without_dlr_url(self):
        # The outcome forwarder is independent of the HTTP dlr-url: a receipt requested by level alone still
        # forwards both outcomes over AMQP and throws no HTTP receipt.
        msgId = yield self._send(dlr_level=3, dlr_url=False)

        yield waitFor(2)  # level1 (submit_sm_resp)
        yield self.SMSCPort.factory.lastClient.trigger_DLR()
        yield waitFor(1)  # level2 (deliver_sm)
        yield self.stopSmppClientConnectors()

        recs = {}
        for _ in range(2):
            r = yield self._get_forward()
            self.assertIsNotNone(r)
            recs[r['level']] = r

        self.assertEqual(set(recs), {1, 2})
        self.assertEqual(recs[1]['msgid'], msgId)
        self.assertEqual(recs[1]['command_status'], 'ESME_ROK')
        self.assertEqual(recs[2]['msgid'], msgId)
        self.assertEqual(recs[2]['message_state'], 'DELIVRD')
        self.assertIsNone(self.AckServerResource.last_request)  # no HTTP throw for a url-less request

    @defer.inlineCallbacks
    def test_forward_survives_target_queue_outage(self):
        # At-least-once: while the forward target is unroutable the receipt is requeued (not acked away) and lands
        # once the queue exists again — the inbound DLR is settled only after the broker confirms the forward.
        self.dlrlookup.config.dlr_lookup_retry_delay = 1
        yield self.amqpBroker.chan.queue_delete(queue=self.forward_queue)

        msgId = yield self._send(dlr_level=1, dlr_url=False)
        yield waitFor(2)  # level1 forward attempted while the target is absent -> unroutable -> requeued

        yield self.amqpBroker.chan.queue_declare(queue=self.forward_queue, durable=True)

        r = None
        for _ in range(6):
            yield waitFor(1)
            r = yield self._get_forward()
            if r is not None:
                break
        yield self.stopSmppClientConnectors()

        self.assertIsNotNone(r)  # receipt survived the outage
        self.assertEqual(r['msgid'], msgId)
        self.assertEqual(r['level'], 1)

    @defer.inlineCallbacks
    def test_deliver_path_forward_survives_target_queue_outage(self):
        # The level-2 forward runs in deliver_sm_dlr_callback, whose ForwardError branch is a separate mirror of
        # the resp path's; the level-1 outage test above never reaches it.
        self.dlrlookup.config.dlr_lookup_retry_delay = 1

        msgId = yield self._send(dlr_level=3)
        yield waitFor(2)  # level1 lands while the target still exists
        yield self._drain_forwards()  # so the record surviving below can only be the level2 one

        yield self.amqpBroker.chan.queue_delete(queue=self.forward_queue)
        yield self.SMSCPort.factory.lastClient.trigger_DLR()
        yield waitFor(2)  # level2 attempted while the target is absent -> unroutable -> requeued

        yield self.amqpBroker.chan.queue_declare(queue=self.forward_queue, durable=True)

        r = None
        for _ in range(6):
            yield waitFor(1)
            candidate = yield self._get_forward()
            if candidate is not None and candidate['level'] == 2:
                r = candidate
                break
        yield self.stopSmppClientConnectors()

        self.assertIsNotNone(r)  # terminal receipt survived the outage
        self.assertEqual(r['msgid'], msgId)
        self.assertEqual(r['level'], 2)
        self.assertEqual(r['message_state'], 'DELIVRD')

    @defer.inlineCallbacks
    def test_unknown_msgid_receipt_is_dropped_and_pipeline_survives(self):
        # A spurious receipt for a message the engine never submitted (an unmapped smpp id) is not forwarded, and it
        # must not stall the DLR consumer: a following valid receipt still forwards.
        msgId = yield self._send(dlr_level=3)
        yield waitFor(2)  # level1 (submit_sm_resp)
        yield self.SMSCPort.factory.lastClient.trigger_DLR(_id='0000000000', stat='DELIVRD')
        yield waitFor(1)
        yield self.SMSCPort.factory.lastClient.trigger_DLR(stat='DELIVRD')
        yield waitFor(1)
        yield self.stopSmppClientConnectors()
        self.dlrlookup.clearRequeueTimers()  # the unmapped receipt's retry timer would else dirty the reactor

        level2 = [f for f in (yield self._drain_forwards()) if f['level'] == 2]
        self.assertEqual(len(level2), 1)  # only the mapped receipt forwarded; the spurious one dropped
        self.assertEqual(level2[0]['msgid'], msgId)

    @defer.inlineCallbacks
    def test_unrecognized_state_is_forwarded_as_unknown_not_fabricated(self):
        # A receipt whose carrier state the engine does not recognize is forwarded as UNKNOWN (never dropped, never
        # crashed, never a fabricated state), leaving the consumer to decide how to read it.
        msgId = yield self._send(dlr_level=3)
        yield waitFor(2)  # level1 (submit_sm_resp)
        yield self.SMSCPort.factory.lastClient.trigger_DLR(stat='WEIRD')
        yield waitFor(1)
        yield self.stopSmppClientConnectors()

        level2 = [f for f in (yield self._drain_forwards()) if f['level'] == 2]
        self.assertEqual(len(level2), 1)
        self.assertEqual(level2[0]['msgid'], msgId)
        self.assertEqual(level2[0]['message_state'], 'UNKNOWN')

    @defer.inlineCallbacks
    def test_disabled_when_queue_unset(self):
        self.dlrlookup.config.dlr_forward_queue = ''

        yield self._send(dlr_level=1)
        yield waitFor(2)
        yield self.stopSmppClientConnectors()

        self.assertIsNone((yield self._get_forward()))


class DlrForwardQueueDeclarationTestCases(RouterPBProxy, HappySMSCTestCase, SubmitSmTestCaseTools):
    """subscribe() declares the configured forward queue itself, so an outcome published before any consumer
    binds is buffered rather than returned unroutable. The sibling cases declare the queue in their own setUp,
    which would keep this passing with the declaration removed."""

    declared_queue = 'test.dlr.forward.declared'
    probe_pid = 'forward-declaration'

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.amqpBroker.chan.queue_delete(queue=self.declared_queue)
        yield self.amqpBroker.chan.queue_delete(queue='DLRLookup-%s' % self.probe_pid)
        yield HappySMSCTestCase.tearDown(self)

    @defer.inlineCallbacks
    def test_subscribe_declares_the_forward_queue(self):
        config = DLRLookupConfig()
        # A distinct pid keeps this consumer off the harness's own DLRLookup-<pid> queue.
        config.pid = self.probe_pid
        config.dlr_forward_queue = self.declared_queue
        lookup = DLRLookup(config, self.amqpBroker, self.redisClient)
        yield lookup.subscribe()

        # The forward publishes to the default exchange with mandatory=True, so an undeclared queue comes back
        # unroutable and raises: nothing else in this test declares it.
        yield lookup.forward_outcome(msgid='forward-declaration-probe', level=1, message_state='ESME_ROK')

        buffered = yield self.amqpBroker.chan.basic_get(queue=self.declared_queue, auto_ack=True)
        self.assertIsNotNone(buffered)
        self.assertEqual('forward-declaration-probe', json.loads(buffered.body)['msgid'])
