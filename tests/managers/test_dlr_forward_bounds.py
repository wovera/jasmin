import json
from unittest.mock import Mock, call

from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from jasmin.managers import dlr as dlr_module
from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.content import DLR, Discard
from jasmin.managers.dlr import DLR_FORWARD_MAX_ATTEMPTS, DLR_LOOKUP_PREFETCH_COUNT, DLRLookup
from smpp.pdu.pdu_types import CommandId, CommandStatus


class DLRLookupFixture:
    def setUp(self):
        self.clock = task.Clock()
        self.patch(dlr_module, 'reactor', self.clock)
        config = DLRLookupConfig()
        config.dlr_forward_queue = 'outcomes'
        self.redis = Mock()
        self.redis.delete = Mock(return_value=defer.succeed(1))
        self.redis.hmset = Mock(return_value=defer.succeed(None))
        self.redis.expire = Mock(return_value=defer.succeed(None))
        broker = Mock()
        broker.connected = True
        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        self.lookup = DLRLookup(config, broker, self.redis)
        self.lookup.q = Mock()

    def _discard(self, reason='revoked', msgid='m1'):
        message = Mock()
        message.content = Discard(msgid, reason, 'smppc-1')
        message.routing_key = 'dlr.discard'
        return message

    def _receipt(self, smsc_id='abc', scope='', stat='DELIVRD'):
        message = Mock()
        message.content = DLR(pdu_type=CommandId.deliver_sm, msgid=smsc_id, status=stat, cid='smppc-1',
                              dlr_details={'id': smsc_id, 'sub': '1', 'dlvrd': '1', 'sdate': 's', 'ddate': 'd',
                                           'stat': stat, 'err': '000', 'text': ''},
                              dlr_scope=scope)
        message.routing_key = 'dlr.deliver_sm'
        return message

    def _deliver(self, message):
        return self.lookup.dlr_callback_dispatcher(message)

    def _forwarded_to(self):
        return [each.kwargs['routing_key'] for each in self.lookup.amqpBroker.publish_confirmed.call_args_list]

    def _fail(self, *queues):
        self.lookup.amqpBroker.publish_confirmed = Mock(side_effect=lambda exchange, routing_key, content: defer.fail(
            IOError('%s unroutable' % routing_key)) if routing_key in queues else defer.succeed(None))

    def _acceptance(self, status=CommandStatus.ESME_ROK, **ids):
        message = Mock()
        message.content = DLR(pdu_type=CommandId.submit_sm_resp, msgid='m1', status=status, **ids)
        message.routing_key = 'dlr.submit_sm_resp'
        return message

    @defer.inlineCallbacks
    def _redeliver(self, message, times):
        for _ in range(times):
            yield self._deliver(message)
            self.lookup.clearRequeueTimer(message)


class DLRLookupPrefetchTestCase(DLRLookupFixture, TestCase):
    @defer.inlineCallbacks
    def test_consumer_takes_a_bounded_number_of_receipts_at_once(self):
        chan = Mock()
        chan.basic_consume = Mock(return_value=(Mock(), 'tag'))
        self.lookup.amqpBroker.newChannel = Mock(return_value=chan)

        yield self.lookup.subscribe()

        qos = chan.mock_calls.index(call.basic_qos(prefetch_count=DLR_LOOKUP_PREFETCH_COUNT))
        consume = [name for name, _, _ in chan.mock_calls].index('basic_consume')
        self.assertLess(qos, consume)
        self.lookup.amqpBroker.named_queue_declare.assert_any_call(queue='DLRLookup-%s.durable' % self.lookup.pid,
                                                                   durable=True)
        self.lookup.amqpBroker.named_queue_declare.assert_any_call(queue='outcomes.parked', durable=True)


class ForwardRetryTestCase(DLRLookupFixture, TestCase):
    @defer.inlineCallbacks
    def test_unconfirmed_forwards_are_retried_further_and_further_apart(self):
        self._fail('outcomes')
        message = self._discard()
        delays = []

        for _ in range(3):
            yield self._deliver(message)
            (timer,) = self.lookup.requeue_timers.values()
            delays.append(timer.getTime() - self.clock.seconds())
            self.lookup.clearRequeueTimer(message)

        base = self.lookup.config.dlr_lookup_retry_delay
        for attempt, delay in enumerate(delays):
            self.assertTrue(0.75 * base * 2 ** attempt <= delay <= 1.25 * base * 2 ** attempt, delays)

    @defer.inlineCallbacks
    def test_ten_unconfirmed_forwards_park_the_receipt_which_is_then_acked(self):
        self._fail('outcomes')
        message = self._discard()

        yield self._redeliver(message, DLR_FORWARD_MAX_ATTEMPTS)
        message.channel.basic_ack.assert_not_called()
        yield self._deliver(message)

        self.assertEqual(self._forwarded_to(), ['outcomes'] * DLR_FORWARD_MAX_ATTEMPTS + ['outcomes.parked'])
        self.assertEqual(json.loads(
            self.lookup.amqpBroker.publish_confirmed.call_args.kwargs['content'].body)['msgid'], 'm1')
        message.channel.basic_ack.assert_called_once_with(delivery_tag=message.delivery_tag)
        self.assertEqual(self.lookup.forward_failures, {})

    @defer.inlineCallbacks
    def test_a_receipt_that_cannot_be_parked_either_is_kept_not_dropped(self):
        self._fail('outcomes', 'outcomes.parked')
        message = self._discard()

        yield self._redeliver(message, DLR_FORWARD_MAX_ATTEMPTS + 3)
        yield self._deliver(message)

        self.assertEqual(self._forwarded_to()[-4:], ['outcomes.parked'] * 4)
        message.channel.basic_ack.assert_not_called()
        message.channel.basic_reject.assert_not_called()
        self.assertEqual(len(self.lookup.requeue_timers), 1)
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_a_receipt_failing_only_its_lookup_has_no_forward_failure_counted(self):
        self.redis.hgetall = Mock(return_value=defer.fail(dlr_module.ConnectionError('redis down')))
        message = self._receipt()

        yield self._redeliver(message, 1)

        self.assertEqual(self.lookup.forward_failures, {})

    @defer.inlineCallbacks
    def _exhaust_lookups(self, message):
        self.redis.hgetall = Mock(side_effect=lambda key: defer.fail(dlr_module.ConnectionError('redis down')))
        yield self._redeliver(message, self.lookup.config.dlr_lookup_max_retries - 1)
        message.channel.basic_ack.assert_not_called()
        yield self._deliver(message)

    @defer.inlineCallbacks
    def test_a_receipt_whose_lookups_are_exhausted_is_parked_whole_then_acked(self):
        for message in (self._receipt(), self._acceptance(smpp_msgid=b'7')):
            yield self._exhaust_lookups(message)

            parked = self.lookup.amqpBroker.publish_confirmed.call_args.kwargs
            self.assertEqual(parked['routing_key'], '%s.parked' % message.routing_key)
            self.assertEqual(parked['content'].body, message.content.body)
            self.assertEqual(parked['content'].properties['headers'], message.content.properties['headers'])
            self.assertEqual(parked['content'].properties['delivery-mode'], 2)
            message.channel.basic_ack.assert_called_once_with(delivery_tag=message.delivery_tag)
            message.channel.basic_reject.assert_not_called()

    @defer.inlineCallbacks
    def test_a_receipt_that_cannot_be_parked_after_its_lookups_is_kept_not_dropped(self):
        self._fail('dlr.deliver_sm.parked')
        message = self._receipt()

        yield self._exhaust_lookups(message)

        message.channel.basic_ack.assert_not_called()
        message.channel.basic_reject.assert_not_called()
        self.assertEqual(len(self.lookup.requeue_timers), 1)
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_forward_budgets_are_kept_per_dlr_scope(self):
        self._fail('outcomes')
        first, second = self._discard(), self._discard()
        second.content.properties['headers']['dlr_scope'] = 'other-account'

        yield self._redeliver(first, DLR_FORWARD_MAX_ATTEMPTS)
        self.lookup.amqpBroker.publish_confirmed.reset_mock()
        yield self._deliver(second)

        self.assertEqual(self._forwarded_to(), ['outcomes'])
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_a_sibling_settling_does_not_clear_another_receipts_budget(self):
        self._fail('outcomes')
        delivered, accepted = self._receipt(stat='DELIVRD'), self._receipt(stat='ACCEPTD')
        self.redis.hgetall = Mock(side_effect=lambda key: defer.succeed(
            {'msgid': 'q1', 'connector_type': 'httpapi'} if key.startswith('queue-msgid')
            else {'sc': 'httpapi', 'url': '', 'level': 2, 'method': 'POST'}))

        yield self._redeliver(delivered, DLR_FORWARD_MAX_ATTEMPTS)
        self.lookup.ackMessage(accepted)
        self.lookup.amqpBroker.publish_confirmed.reset_mock()
        yield self._deliver(delivered)

        self.assertEqual(self._forwarded_to(), ['outcomes.parked'])
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_deliveries_sharing_a_message_id_each_settle(self):
        self._fail('outcomes')
        first, second = self._discard(), self._discard()

        yield self._deliver(first)
        yield self._deliver(second)
        self.clock.advance(3600)

        first.channel.basic_reject.assert_called_once_with(delivery_tag=first.delivery_tag, requeue=1)
        second.channel.basic_reject.assert_called_once_with(delivery_tag=second.delivery_tag, requeue=1)

    @defer.inlineCallbacks
    def test_an_unforwarded_receipt_is_not_thrown_over_http(self):
        # Each retry would throw it again, so the throw waits for the forward to be confirmed.
        self._fail('outcomes')
        self.redis.hgetall = Mock(return_value=defer.succeed(
            {'sc': 'httpapi', 'url': 'http://127.0.0.1/receipt', 'level': 1, 'method': 'POST', 'expiry': 86400,
             'connector': 'smppc-1'}))
        message = Mock()
        message.content.body = b'ESME_ROK'
        message.content.properties = {'message-id': 'm1', 'headers': {'smpp_msgid': '01'}}

        yield self.lookup.submit_sm_resp_dlr_callback(message)

        self.lookup.amqpBroker.publish.assert_not_called()
        self.assertEqual(len(self.lookup.requeue_timers), 1)
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_the_smsc_id_is_mapped_even_while_the_outcome_cannot_be_forwarded(self):
        self._fail('outcomes')
        self.redis.hgetall = Mock(return_value=defer.succeed(
            {'sc': 'httpapi', 'url': '', 'level': 3, 'method': 'POST', 'expiry': 86400, 'connector': 'smppc-1'}))
        message = Mock()
        message.content.body = b'ESME_ROK'
        message.content.properties = {'message-id': 'm1', 'headers': {'smpp_msgid': 'ABC', 'dlr_scope': 'acct'}}

        yield self.lookup.submit_sm_resp_dlr_callback(message)

        self.redis.hmset.assert_called_once_with('queue-msgid:acct:ABC', {'msgid': 'm1', 'connector_type': 'httpapi'})
        self.assertEqual(len(self.lookup.requeue_timers), 1)
        self.lookup.clearRequeueTimers()

    @defer.inlineCallbacks
    def test_a_partly_accepted_submit_forwards_its_accepted_segment_count(self):
        self.redis.hgetall = Mock(return_value=defer.succeed(
            {'sc': 'httpapi', 'url': '', 'level': 1, 'method': 'POST', 'expiry': 86400, 'connector': 'smppc-1'}))
        message = Mock()
        message.content = DLR(pdu_type=CommandId.submit_sm_resp, msgid='m1', status='ESME_RTHROTTLED',
                              accepted_segments=2)
        message.routing_key = 'dlr.submit_sm_resp'

        yield self._deliver(message)

        body = json.loads(self.lookup.amqpBroker.publish_confirmed.call_args.kwargs['content'].body)
        self.assertEqual((body['command_status'], body['accepted_segments']), ('ESME_RTHROTTLED', 2))


class SettlementTestCase(DLRLookupFixture, TestCase):
    @defer.inlineCallbacks
    def test_whatever_escapes_a_callback_is_requeued_not_left_unsettled(self):
        message = self._discard()
        self.lookup.discard_dlr_callback = Mock(return_value=defer.fail(RuntimeError('unexpected')))

        yield self._deliver(message)

        self.assertEqual(len(self.lookup.requeue_timers), 1)
        self.clock.advance(3600)
        message.channel.basic_reject.assert_called_once_with(delivery_tag=message.delivery_tag, requeue=1)

    @defer.inlineCallbacks
    def test_an_in_doubt_discard_keeps_the_receipt_map(self):
        self.lookup.ackMessage = Mock(return_value=defer.succeed(None))

        yield self.lookup.discard_dlr_callback(self._discard(reason='in_doubt'))

        body = json.loads(self.lookup.amqpBroker.publish_confirmed.call_args.kwargs['content'].body)
        self.assertEqual(body['discard_reason'], 'in_doubt')
        self.redis.delete.assert_not_called()
        self.lookup.ackMessage.assert_called_once()


class ReceiptMapTestCase(DLRLookupFixture, TestCase):
    def setUp(self):
        super().setUp()
        self.store = {'dlr:m1': {'sc': 'httpapi', 'url': '', 'level': 3, 'method': 'POST', 'expiry': 86400,
                                 'connector': 'smppc-1'}}
        self.redis.hgetall = Mock(side_effect=lambda key: defer.succeed(dict(self.store.get(key, {}))))
        self.redis.hmset = Mock(side_effect=lambda key, values: defer.succeed(self.store.setdefault(key, {}).update(
            values)))
        self.redis.delete = Mock(side_effect=lambda key: defer.succeed(self.store.pop(key, None) and 1))

    @defer.inlineCallbacks
    def test_the_accepted_segments_of_a_partly_refused_long_message_still_resolve_their_receipts(self):
        yield self._deliver(self._acceptance(CommandStatus.ESME_RTHROTTLED, smpp_msgids=[b'1', b'2'],
                                             accepted_segments=2))

        self.assertEqual(self.store['queue-msgid:1']['msgid'], 'm1')
        self.assertEqual(self.store['queue-msgid:2']['msgid'], 'm1')
        self.assertIn('dlr:m1', self.store)

    @defer.inlineCallbacks
    def test_a_terminal_receipt_leaves_a_pending_acceptance_forwardable(self):
        acceptance = self._acceptance(smpp_msgid=b'7')
        self._fail('outcomes')
        yield self._deliver(acceptance)
        self.lookup.clearRequeueTimer(acceptance)

        self._fail()
        receipt = self._receipt('7', stat='DELIVRD')
        yield self._deliver(receipt)
        yield self._deliver(acceptance)

        receipt.channel.basic_ack.assert_called_once_with(delivery_tag=receipt.delivery_tag)
        acceptance.channel.basic_ack.assert_called_once_with(delivery_tag=acceptance.delivery_tag)
        acceptance.channel.basic_reject.assert_not_called()
