from twisted.internet import defer

from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.routing.Bills import SubmitSmBill
from jasmin.routing.jasminApi import Group, User
from tests.managers.test_managers import SMPPClientPBProxyTestCase, waitFor


class DlrMapOrderingTestCase(SMPPClientPBProxyTestCase):
    """The receipt map has to exist before the PDU can be answered. The SMSC round trip can complete before a
    map written after the publish lands, and a submit_sm_resp that finds no map is discarded, which takes the
    whole receipt chain with it: the later deliver_sm then retries against a map that is never written."""

    @defer.inlineCallbacks
    def setUp(self):
        yield SMPPClientPBProxyTestCase.setUp(self)

        RCInstance = RedisForJasminConfig()
        self.redisClient = yield ConnectionWithConfiguration(RCInstance)
        if RCInstance.password is not None:
            yield self.redisClient.auth(RCInstance.password)
            yield self.redisClient.select(RCInstance.dbid)
        self.clientManagerPB.addRedisClient(self.redisClient)

        opFactory = SMPPOperationFactory(SMPPClientConfig(id='defaultId'))
        self.SubmitSmPDU = opFactory.SubmitSM(
            source_addr='1423',
            destination_addr='06155423',
            short_message='Hello world !',
        )
        self.uid = SubmitSmBill(User('test_user', Group('test_group'), 'test_username', 'pwd')).user.uid

    @defer.inlineCallbacks
    def tearDown(self):
        if getattr(self, 'redisClient', None) is not None:
            yield self.redisClient.disconnect()
        yield SMPPClientPBProxyTestCase.tearDown(self)

    @defer.inlineCallbacks
    def _submitObservingThePublish(self, dlr_level):
        mappedAtPublish = []
        publish = self.amqpBroker.publish

        @defer.inlineCallbacks
        def recordingPublish(**kwargs):
            hashKey = 'dlr:%s' % kwargs['content'].properties['message-id']
            mapped = yield self.redisClient.exists(hashKey)
            mappedAtPublish.append(bool(mapped))
            yield publish(**kwargs)

        self.amqpBroker.publish = recordingPublish
        try:
            yield self.clientManagerPB.perspective_submit_sm(
                uid=self.uid,
                cid=self.defaultConfig.id,
                SubmitSmPDU=self.SubmitSmPDU,
                submit_sm_bill=None,
                pickled=False,
                dlr_url='http://127.0.0.1/receipt',
                dlr_level=dlr_level,
                dlr_method='POST',
                dlr_connector=self.defaultConfig.id,
                source_connector='httpapi',
            )
        finally:
            self.amqpBroker.publish = publish

        defer.returnValue(mappedAtPublish)

    @defer.inlineCallbacks
    def test_the_receipt_map_is_written_before_the_pdu_is_published(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)

        mappedAtPublish = yield self._submitObservingThePublish(dlr_level=1)

        self.assertEqual(mappedAtPublish, [True])

    @defer.inlineCallbacks
    def test_no_map_is_written_when_no_receipt_was_requested(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)

        # Non-vacuity: the observation reports a real absence too, so the assertion above is the ordering and
        # not a key this test would find whenever it looked.
        mappedAtPublish = yield self._submitObservingThePublish(dlr_level=0)

        self.assertEqual(mappedAtPublish, [False])
