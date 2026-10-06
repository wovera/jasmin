import pickle
import time
from unittest.mock import Mock
from uuid import uuid4

from smpp.pdu.operations import DeliverSM
from smpp.pdu.pdu_types import CommandStatus, DataCoding, EsmClass, EsmClassGsmFeatures, EsmClassMode, EsmClassType
from twisted.internet import defer, reactor, task
from twisted.trial.unittest import TestCase

from jasmin.managers import listeners as listeners_module
from jasmin.redis.client import SimpleConnection
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import Connector
from tests.managers.listener_fixture import ListenerFixture


class MoAssemblyTestCase(ListenerFixture, TestCase):
    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.redis = yield SimpleConnection(reconnect=False)
        self.addCleanup(self.redis.disconnect)
        self.listener, self.broker = self._listener('mo-assembly')
        self.listener.redisClient = self.redis
        self.clock = task.Clock()
        self.patch(listeners_module, 'reactor', self.clock)
        # A sender of its own keeps each test's parts out of every other test's hash.
        self.sender = str(uuid4().int)[:12]
        self.key = 'longDeliverSm:%s:%s:%s:%s' % ('mo-assembly', 7, b'4567', self.sender.encode())
        self.addCleanup(self.redis.delete, self.key)
        yield self._store((b'first ', b'second ', b'last'))

    def _part(self, sequence, text, total=3):
        pdu = DeliverSM(source_addr=self.sender, destination_addr='4567', short_message=text, data_coding=DataCoding())
        pdu.params.update(sar_msg_ref_num=7, sar_total_segments=total, sar_segment_seqnum=sequence)
        return pdu

    @defer.inlineCallbacks
    def _store(self, texts):
        for sequence, text in enumerate(texts, start=1):
            yield self.redis.hset(self.key, sequence, pickle.dumps({
                'segment_seqnum': sequence, 'pdu': self._part(sequence, text, len(texts))}))
        yield self.redis.expire(self.key, 300)

    def _deliver(self, pdu):
        return self.listener.deliver_sm_event_post_interception(
            routable=RoutableDeliverSm(pdu, Connector('mo-assembly')), smpp=None)

    def _assembled(self):
        return [content for content in self._published(self.broker, 'deliver.sm.mo-assembly')
                if content.properties['headers']['concatenated']]

    @defer.inlineCallbacks
    def test_a_conflicting_part_under_a_reused_reference_never_replaces_a_stored_one(self):
        yield self.redis.delete(self.key)
        yield self._deliver(self._part(1, b'first a ', total=2))

        response = yield self._deliver(self._part(1, b'other b ', total=2))
        yield self._deliver(self._part(2, b'last a', total=2))

        self.assertEqual(response.status, CommandStatus.ESME_RX_T_APPN)
        self.assertEqual([pickle.loads(content.body).pdu.params['short_message'] for content in self._assembled()],
                         [b'first a last a'])

    @defer.inlineCallbacks
    def test_a_part_of_a_differently_split_message_under_a_reused_reference_is_never_assembled_with_it(self):
        yield self.redis.delete(self.key)
        yield self._deliver(self._part(1, b'prefix '))
        yield self._deliver(self._part(2, b'old middle '))

        response = yield self._deliver(self._part(1, b'prefix ', total=2))

        self.assertEqual(response.status, CommandStatus.ESME_RX_T_APPN)
        self.assertEqual(self._assembled(), [])

    @defer.inlineCallbacks
    def test_an_assembler_that_lost_its_parts_while_waiting_to_publish_never_publishes(self):
        turn, published = defer.Deferred(), []

        @defer.inlineCallbacks
        def publish(guard=None, **kwargs):
            yield turn
            if guard is not None:
                yield guard()
            published.append(kwargs['content'])
        self.broker.publish_confirmed.side_effect = publish
        assembling = self.listener.concatDeliverSMs(self.key, 'sar', 3)
        yield self.redis.hset(self.key, 'claim', 'taken-over')

        turn.callback(None)

        yield self.assertFailure(assembling, RuntimeError)
        self.assertEqual(published, [])
        self.assertEqual((yield self.redis.hget(self.key, 'claim')), 'taken-over')

    @defer.inlineCallbacks
    def test_a_retransmitted_part_restores_an_expiry_its_first_store_missed(self):
        yield self.redis.delete(self.key)
        yield self.redis.hset(self.key, 1, pickle.dumps({'segment_seqnum': 1, 'pdu': self._part(1, b'first ', 2)}))

        yield self._deliver(self._part(1, b'first ', total=2))

        ttl = yield self.redis.ttl(self.key)
        self.assertGreater(ttl, 0)

    @defer.inlineCallbacks
    def test_assembly_is_published_once_and_retires_the_parts(self):
        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)

        (assembled,) = self._assembled()
        self.assertEqual(pickle.loads(assembled.body).pdu.params['short_message'], b'first second last')
        self.assertFalse((yield self.redis.exists(self.key)))

    @defer.inlineCallbacks
    def test_a_reference_reused_while_its_parts_linger_still_assembles(self):
        yield self.redis.delete(self.key)
        for text in (b'one', b'two'):
            yield self._deliver(self._part(1, text + b' a ', total=2))
            yield self._deliver(self._part(2, text + b' b', total=2))

        self.assertEqual([pickle.loads(content.body).pdu.params['short_message'] for content in self._assembled()],
                         [b'one a one b', b'two a two b'])

    @defer.inlineCallbacks
    def test_failed_publication_releases_the_claim_and_a_resend_assembles(self):
        self.broker.publish_confirmed.side_effect = RuntimeError('publication failed')
        yield self.assertFailure(self.listener.concatDeliverSMs(self.key, 'sar', 3), RuntimeError)
        self.assertIsNone((yield self.redis.hget(self.key, 'claim')))

        self.broker.publish_confirmed.side_effect = None
        self.broker.publish_confirmed.reset_mock()
        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)

        self.assertEqual(len(self._assembled()), 1)

    @defer.inlineCallbacks
    def _settle_held(self, delivery):
        while not delivery.called:
            yield task.deferLater(reactor, 0.01, lambda: None)
            self.clock.advance(1)
        return (yield delivery)

    @defer.inlineCallbacks
    def test_a_part_meeting_another_assembler_is_acknowledged_once_that_one_assembles(self):
        publication, started = defer.Deferred(), defer.Deferred()

        def publish(**kwargs):
            if not kwargs['content'].properties['headers']['concatenated']:
                return defer.succeed(None)
            started.callback(None)
            return publication
        self.broker.publish_confirmed.side_effect = publish
        assembling = self.listener.concatDeliverSMs(self.key, 'sar', 3)
        yield started

        delivery = self._deliver(self._part(3, b'last'))
        yield task.deferLater(reactor, 0.05, lambda: None)
        self.assertFalse(delivery.called)
        publication.callback(None)
        yield assembling

        self.assertIsNone((yield self._settle_held(delivery)))
        self.assertEqual(len(self._assembled()), 1)

    @defer.inlineCallbacks
    def test_a_part_meeting_a_crashed_assembler_takes_over_once_its_lease_lapses(self):
        yield self.redis.hset(self.key, 'claim', 'crashed')
        yield self.redis.hset(self.key, 'lease', str(time.time() + 0.1))

        response = yield self._settle_held(self._deliver(self._part(3, b'last')))

        self.assertIsNone(response)
        self.assertEqual(len(self._assembled()), 1)

    @defer.inlineCallbacks
    def test_a_claim_abandoned_by_a_crashed_assembler_is_taken_over_under_the_same_id(self):
        yield self.redis.hset(self.key, 'id', 'assembled-1')
        yield self.redis.hset(self.key, 'claim', 'crashed')
        yield self.redis.hset(self.key, 'lease', str(time.time() - 1))

        response = yield self._deliver(self._part(3, b'last'))

        self.assertIsNone(response)
        (assembled,) = self._assembled()
        self.assertEqual(assembled.properties['message-id'], 'assembled-1')
        self.assertFalse((yield self.redis.exists(self.key)))

    @defer.inlineCallbacks
    def test_a_live_claim_is_not_taken_over(self):
        yield self.redis.hset(self.key, 'claim', 'alive')
        yield self.redis.hset(self.key, 'lease', str(time.time() + 30))

        yield self.redis.hset(self.key, 'lease', str(time.time() + 300))

        response = yield self._settle_held(self._deliver(self._part(3, b'last')))

        self.assertEqual(response.status, CommandStatus.ESME_RX_T_APPN)
        self.assertEqual(self._assembled(), [])
        self.assertEqual((yield self.redis.hget(self.key, 'claim')), 'alive')

    @defer.inlineCallbacks
    def test_lost_claim_reply_leaves_a_lease_that_lapses(self):
        original_eval = self.redis.eval
        lose = [True]

        @defer.inlineCallbacks
        def lose_reply(*args, **kwargs):
            result = yield original_eval(*args, **kwargs)
            if lose:
                lose.clear()
                raise RuntimeError('claim reply lost')
            return result

        self.patch(self.redis, 'eval', lose_reply)
        yield self.assertFailure(self.listener.concatDeliverSMs(self.key, 'sar', 3), RuntimeError)
        self.broker.publish_confirmed.assert_not_called()
        yield self.redis.hset(self.key, 'lease', str(time.time() - 1))

        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)

        self.assertEqual(len(self._assembled()), 1)

    @defer.inlineCallbacks
    def test_parts_expiring_before_claim_are_not_recreated(self):
        original_eval = self.redis.eval

        @defer.inlineCallbacks
        def expire_before_claim(*args, **kwargs):
            yield self.redis.delete(self.key)
            result = yield original_eval(*args, **kwargs)
            return result

        self.patch(self.redis, 'eval', expire_before_claim)
        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)
        self.assertFalse((yield self.redis.exists(self.key)))
        self.broker.publish_confirmed.assert_not_called()

    @defer.inlineCallbacks
    def test_claim_requires_every_part(self):
        yield self.redis.hdel(self.key, ['2'])
        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)
        self.assertIsNone((yield self.redis.hget(self.key, 'claim')))
        self.broker.publish_confirmed.assert_not_called()
        ttl = yield self.redis.ttl(self.key)
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 300)

    @defer.inlineCallbacks
    def test_first_part_without_content_is_dropped_and_retired(self):
        first = pickle.loads((yield self.redis.hget(self.key, '1')))
        del first['pdu'].params['short_message']
        first['pdu'].params.pop('message_payload', None)
        yield self.redis.hset(self.key, 1, pickle.dumps(first))
        log = Mock()
        self.patch(self.listener, 'log', log)

        response = yield self._deliver(self._part(3, b'last'))

        self.assertIsNone(response)
        log.error.assert_called_once_with('Cannot find message content in first pdu params')
        self.assertFalse((yield self.redis.exists(self.key)))
        (part,) = self._published(self.broker, 'deliver.sm.mo-assembly')
        self.assertTrue(part.properties['headers']['will_be_concatenated'])

    @defer.inlineCallbacks
    def test_truncated_malformed_and_empty_udh_are_single_messages(self):
        store = Mock()
        self.patch(self.redis, 'eval', store)
        for body in (
            b'\x05\x00\x03\x07', b'\x02\x00\x03body', b'\x01\x00body', b'',
            bytes([8, 0, 3, 7, 2, 1, 0x70, 4, 1]) + b'body',
            bytes([7, 8, 5, 1, 7, 2, 1, 0]) + b'body',
            bytes([12, 0, 3, 7, 2, 1, 5, 5, 0x12, 0x34, 0x56, 0x78, 0]) + b'body',
        ):
            self.broker.publish_confirmed.reset_mock()
            pdu = DeliverSM(source_addr=self.sender, destination_addr='4567', short_message=body,
                            data_coding=DataCoding())
            pdu.params['esm_class'] = EsmClass(EsmClassMode.DEFAULT, EsmClassType.DEFAULT,
                                               [EsmClassGsmFeatures.UDHI_INDICATOR_SET])

            response = yield self._deliver(pdu)

            self.assertIsNone(response)
            self.broker.publish_confirmed.assert_called_once()
            content = self.broker.publish_confirmed.call_args.kwargs['content']
            self.assertFalse(content.properties['headers']['will_be_concatenated'])
            self.assertFalse(content.properties['headers']['concatenated'])
            self.assertEqual(pickle.loads(content.body).pdu.params['short_message'], body)
        store.assert_not_called()

    @defer.inlineCallbacks
    def test_sar_and_udh_payload_parts_assemble_with_empty_short_message(self):
        for split_method in ('sar', 'udh'):
            yield self.redis.delete(self.key)
            self.broker.publish_confirmed.reset_mock()
            for sequence, text in enumerate((b'first ', b'second ', b'last'), start=1):
                pdu = DeliverSM(source_addr=self.sender, destination_addr='4567',
                                short_message=b'', data_coding=DataCoding())
                if split_method == 'sar':
                    pdu.params.update(sar_msg_ref_num=7, sar_total_segments=3, sar_segment_seqnum=sequence)
                    pdu.params['message_payload'] = text
                else:
                    pdu.params['esm_class'] = EsmClass(EsmClassMode.DEFAULT, EsmClassType.DEFAULT,
                                                       [EsmClassGsmFeatures.UDHI_INDICATOR_SET])
                    pdu.params['message_payload'] = bytes([5, 0, 3, 7, 3, sequence]) + text
                yield self.redis.hset(self.key, sequence, pickle.dumps({
                    'segment_seqnum': sequence, 'pdu': pdu}))

            yield self.listener.concatDeliverSMs(self.key, split_method, 3)

            self.broker.publish_confirmed.assert_called_once()
            content = self.broker.publish_confirmed.call_args.kwargs['content']
            published = pickle.loads(content.body).pdu
            self.assertEqual(published.params['short_message'], b'')
            self.assertEqual(published.params['message_payload'], b'first second last')
            self.assertTrue(content.properties['headers']['concatenated'])

    @defer.inlineCallbacks
    def test_invalid_sar_and_udh_counts_and_sequences_are_single_messages(self):
        store = Mock()
        self.patch(self.redis, 'eval', store)
        for split_method in ('sar', 'udh'):
            for count, sequence in ((0, 1), (2, 3), (2, 0)):
                self.broker.publish_confirmed.reset_mock()
                body = b'body'
                pdu = DeliverSM(source_addr=self.sender, destination_addr='4567', short_message=body,
                                data_coding=DataCoding())
                if split_method == 'sar':
                    pdu.params.update(sar_msg_ref_num=7, sar_total_segments=count, sar_segment_seqnum=sequence)
                else:
                    pdu.params['esm_class'] = EsmClass(EsmClassMode.DEFAULT, EsmClassType.DEFAULT,
                                                       [EsmClassGsmFeatures.UDHI_INDICATOR_SET])
                    body = bytes([5, 0, 3, 7, count, sequence]) + body
                    pdu.params['short_message'] = body

                response = yield self._deliver(pdu)

                self.assertIsNone(response)
                self.broker.publish_confirmed.assert_called_once()
                content = self.broker.publish_confirmed.call_args.kwargs['content']
                self.assertFalse(content.properties['headers']['will_be_concatenated'])
                self.assertFalse(content.properties['headers']['concatenated'])
                self.assertEqual(pickle.loads(content.body).pdu.params['short_message'], body)
        store.assert_not_called()

    @defer.inlineCallbacks
    def test_failed_assembly_returns_smpp_error_and_the_resend_assembles(self):
        self.broker.publish_confirmed.side_effect = RuntimeError('publication failed')
        response = yield self._deliver(self._part(3, b'last'))
        self.assertEqual(response.status, CommandStatus.ESME_RUNKNOWNERR)
        self.assertIsNone((yield self.redis.hget(self.key, 'claim')))

        self.broker.publish_confirmed.side_effect = None
        self.broker.publish_confirmed.reset_mock()
        response = yield self._deliver(self._part(3, b'last'))

        self.assertIsNone(response)
        self.assertEqual(len(self._assembled()), 1)

    @defer.inlineCallbacks
    def test_a_final_part_that_could_not_be_handed_over_never_joins_a_reused_reference(self):
        failed = []

        def publish(**kwargs):
            if kwargs['content'].properties['headers'].get('will_be_concatenated') and not failed:
                failed.append(kwargs['content'])
                return defer.fail(RuntimeError('publication failed'))
            return defer.succeed(None)
        self.broker.publish_confirmed.side_effect = publish

        response = yield self._deliver(self._part(3, b'last'))
        self.assertEqual(response.status, CommandStatus.ESME_RUNKNOWNERR)
        yield self._deliver(self._part(3, b'last'))
        for sequence, text in enumerate((b'new first ', b'new second ', b'new last'), start=1):
            yield self._deliver(self._part(sequence, text))

        self.assertEqual([pickle.loads(content.body).pdu.params['short_message'] for content in self._assembled()],
                         [b'first second last', b'new first new second new last'])

    @defer.inlineCallbacks
    def test_parts_outlive_an_assembly_waiting_for_the_publisher(self):
        yield self.redis.expire(self.key, 5)
        ttls = []

        @defer.inlineCallbacks
        def publish(guard=None, **kwargs):
            yield guard()
            ttls.append((yield self.redis.ttl(self.key)))
        self.broker.publish_confirmed.side_effect = publish

        yield self.listener.concatDeliverSMs(self.key, 'sar', 3)

        self.assertGreater(ttls[0], 5)

    @defer.inlineCallbacks
    def test_a_part_that_cannot_be_kept_is_asked_again_instead_of_acknowledged(self):
        self.listener.redisClient = None

        response = yield self._deliver(self._part(3, b'last'))

        self.assertEqual(response.status, CommandStatus.ESME_RX_T_APPN)
