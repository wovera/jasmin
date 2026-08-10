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

    @defer.inlineCallbacks
    def test_claim_expires_with_the_connector_dlr_expiry(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        msgid = '33333333-3333-3333-3333-333333333333'

        yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        yield waitFor(1)

        # -1 is redis for "no expiry": an immortal claim accumulates in a noeviction keyspace until it fills and
        # every subsequent send fails closed.
        ttl = yield self.redisClient.ttl('idem:%s' % msgid)
        self.assertTrue(self.defaultConfig.dlr_expiry - 5 < ttl <= self.defaultConfig.dlr_expiry,
                        'claim ttl was %s' % ttl)

    @defer.inlineCallbacks
    def test_publish_failure_releases_the_claim_so_a_retry_enqueues(self):
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        msgid = '44444444-4444-4444-4444-444444444444'

        publish = self.clientManagerPB.amqpBroker.publish
        self.clientManagerPB.amqpBroker.publish = lambda *a, **k: defer.fail(IOError('broker unavailable'))
        try:
            yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        except Exception:
            pass
        finally:
            self.clientManagerPB.amqpBroker.publish = publish
        # The PB server logs the failure before returning it, and trial fails a test that leaves one logged.
        self.flushLoggedErrors(IOError)
        yield waitFor(1)

        self.assertEqual((yield self.queued(cid)), 0)
        self.assertIsNone((yield self.redisClient.get('idem:%s' % msgid)))

        # Without the release the retry would replay the original accept, reporting a message that never queued
        # as submitted.
        retried = yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        yield waitFor(1)
        self.assertEqual(retried, msgid)
        self.assertEqual((yield self.queued(cid)), 1)


    @defer.inlineCallbacks
    def _submitRequestingAReceipt(self, cid, msgid):
        # The receipt map is only written when a receipt is requested, which the submit_sm proxy cannot express.
        result = yield self.clientManagerPB.perspective_submit_sm(
            uid=self.uid,
            cid=cid,
            SubmitSmPDU=self.SubmitSmPDU,
            submit_sm_bill=None,
            pickled=False,
            dlr_url='http://127.0.0.1/receipt',
            dlr_level=1,
            dlr_method='POST',
            dlr_connector=cid,
            source_connector='httpapi',
            msgid=msgid,
        )
        defer.returnValue(result)

    @defer.inlineCallbacks
    def test_a_failed_receipt_map_write_releases_the_claim(self):
        """The map write sits between the claim and the publish. Left outside the release, a Redis failure there
        holds the claim for its whole expiry while nothing was queued, so the caller's retry - the retry an
        idempotent submit exists to serve - is answered "already accepted" for a message that never sent."""
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        msgid = '55555555-5555-5555-5555-555555555555'
        yield self.redisClient.delete('idem:%s' % msgid)

        hmset = self.redisClient.hmset
        self.redisClient.hmset = lambda *a, **k: defer.fail(IOError('redis unavailable'))
        try:
            yield self._submitRequestingAReceipt(cid, msgid)
        except Exception:
            pass
        finally:
            self.redisClient.hmset = hmset
        self.flushLoggedErrors(IOError)
        yield waitFor(1)

        self.assertEqual((yield self.queued(cid)), 0)
        self.assertIsNone((yield self.redisClient.get('idem:%s' % msgid)))

        retried = yield self.submit_sm(cid, self.SubmitSmPDU, self.uid, msgid=msgid)
        yield waitFor(1)
        self.assertEqual(retried, msgid)
        self.assertEqual((yield self.queued(cid)), 1)

    @defer.inlineCallbacks
    def test_a_failed_content_build_releases_the_claim(self):
        """The content is built after the claim too, and an invalid parameter raises there. Same window, same
        consequence: a held claim for a message that was never enqueued."""
        yield self.connect('127.0.0.1', self.pbPort)
        yield self.add(self.defaultConfig)
        yield waitFor(1)
        cid = self.defaultConfig.id
        msgid = '66666666-6666-6666-6666-666666666666'
        yield self.redisClient.delete('idem:%s' % msgid)

        try:
            # A negative priority is rejected by the content, after the claim has been granted.
            yield self.clientManagerPB.perspective_submit_sm(
                uid=self.uid,
                cid=cid,
                SubmitSmPDU=self.SubmitSmPDU,
                submit_sm_bill=None,
                priority=-1,
                pickled=False,
                source_connector='httpapi',
                msgid=msgid,
            )
        except Exception:
            pass
        yield waitFor(1)

        self.assertEqual((yield self.queued(cid)), 0)
        self.assertIsNone((yield self.redisClient.get('idem:%s' % msgid)))
