from twisted.internet import defer

from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.routing.Bills import SubmitSmBill
from jasmin.routing.jasminApi import Group, User
from tests.managers.test_managers import SMPPClientPBProxyTestCase, waitFor


class IdempotentSubmitTestCases(SMPPClientPBProxyTestCase):
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
    def queued(self, cid):
        # Read the submit queue depth without consuming; the connector is added-but-not-started, so nothing drains it.
        frame = yield self.amqpBroker.chan.queue_declare(queue='submit.sm.%s' % cid, passive=True)
        defer.returnValue(frame.method.message_count)

    @defer.inlineCallbacks
    def test_duplicate_msgid_is_not_reenqueued(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        msgid = '11111111-1111-1111-1111-111111111111'

        first = yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        yield waitFor(1)
        self.assertEqual(first, msgid)
        self.assertEqual((yield self.queued(cid)), 1)

        second = yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        yield waitFor(1)
        self.assertEqual(second, msgid)
        self.assertEqual((yield self.queued(cid)), 1)

    @defer.inlineCallbacks
    def test_missing_redis_fails_closed(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        self.clientManagerPB.redisClient = None

        result = yield self.submit_sm(cid, self.SubmitSmPDU, self.uid,
                                      msgid='22222222-2222-2222-2222-222222222222')
        yield waitFor(1)
        self.assertFalse(result)
        self.assertEqual((yield self.queued(cid)), 0)
