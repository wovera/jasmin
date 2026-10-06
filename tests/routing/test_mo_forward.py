import binascii
import json
import os
import pickle
from unittest.mock import Mock, call, patch

from pika.exceptions import NackError, UnroutableError
from smpp.pdu.operations import DeliverSM, DeliverSMResp
from smpp.pdu.pdu_types import (CommandStatus, DataCoding, DataCodingDefault, EsmClass, EsmClassGsmFeatures,
                                 EsmClassMode, EsmClassType)
from twisted.internet import defer, reactor, task
from twisted.trial.unittest import TestCase

from jasmin.managers.content import DeliverSmContent
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.configs import RouterPBConfig, deliverSmThrowerConfig
from jasmin.routing.jasminApi import Connector
from jasmin.routing.proxies import RouterPBProxy
import jasmin.routing.router as router_module
from jasmin.routing.router import RouterPB
from jasmin.tools.jitter import BACKOFF_CEILING_SECONDS
from tests.routing.test_routing_deliver_sm import DeliverSmSMSCTestCase


class MoForwardTestCase(RouterPBProxy, DeliverSmSMSCTestCase):
    forward_queue = 'test.mo.forward'
    routingKey = 'deliver.sm.src'
    wait_timeout = 5
    poll_interval = 0.05

    @defer.inlineCallbacks
    def setUp(self):
        env = patch.dict(os.environ, {
            'ROUTER_MO_FORWARD_QUEUE': self.forward_queue,
            'ROUTER_MO_FORWARD_RETRY_DELAY': '1',
        })
        env.start()
        self.addCleanup(env.stop)
        yield DeliverSmSMSCTestCase.setUp(self)
        self.addCleanup(self.pbRoot_f.stopService)
        self.pbRoot_f.ackMessage = Mock(wraps=self.pbRoot_f.ackMessage)
        self.pbRoot_f.rejectMessage = Mock(wraps=self.pbRoot_f.rejectMessage)
        self.testDeliverSMPdu = DeliverSM(
            source_addr='1234', destination_addr='4567', short_message=b'hello mo')

    @defer.inlineCallbacks
    def _wait_for(self, condition):
        deadline = reactor.seconds() + self.wait_timeout
        while reactor.seconds() < deadline:
            result = yield defer.maybeDeferred(condition)
            if result:
                return result
            yield task.deferLater(reactor, self.poll_interval, lambda: None)
        self.fail('Timed out waiting for MO forwarding condition')

    @defer.inlineCallbacks
    def _get_forward(self):
        result = yield self.amqpBroker.chan.basic_get(queue=self.forward_queue, auto_ack=True)
        if result is None:
            defer.returnValue(None)
        self.assertEqual(result.properties.delivery_mode, 2)
        self.assertEqual(result.properties.content_type, 'application/json')
        defer.returnValue(json.loads(result.body))

    @defer.inlineCallbacks
    def test_forwards_mo_as_json_and_skips_http(self):
        self.testDeliverSMPdu.params['data_coding'] = DataCoding(
            schemeData=DataCodingDefault.UCS2)
        self.testDeliverSMPdu.params['source_port'] = 123
        self.testDeliverSMPdu.custom_tlvs = [(0x1400, 2, 'octets', b'\x01\x02')]
        content = DeliverSmContent(
            RoutableDeliverSm(self.testDeliverSMPdu, Connector('src')), 'src')
        yield self.amqpBroker.publish(exchange='messaging', routing_key=self.routingKey, content=content)
        yield self._wait_for(lambda: self.pbRoot_f.ackMessage.called)

        record = yield self._get_forward()
        self.assertIsNotNone(record)
        self.assertEqual(record['id'], content.properties['message-id'])
        self.assertEqual(record['from'], '1234')
        self.assertEqual(record['to'], '4567')
        self.assertEqual(record['origin-connector'], 'src')
        self.assertEqual(record['coding'], 8)
        self.assertEqual(set(record), {'id', 'from', 'to', 'origin-connector', 'coding', 'binary'})
        self.assertIsInstance(record['coding'], int)
        self.assertEqual(record['binary'], binascii.hexlify(b'hello mo').decode())
        self.assertEqual(self.pbRoot_f.getMORoutingTable().getAll(), [])
        self.pbRoot_f.ackMessage.assert_called_once()
        self.pbRoot_f.rejectMessage.assert_not_called()
        self.assertIsNone((yield self._get_forward()))

    @defer.inlineCallbacks
    def test_disabled_when_queue_unset(self):
        self.pbRoot_f.config.mo_forward_queue = ''
        content = DeliverSmContent(
            RoutableDeliverSm(self.testDeliverSMPdu, Connector('src')), 'src')
        yield self.amqpBroker.publish(exchange='messaging', routing_key=self.routingKey, content=content)
        yield self._wait_for(lambda: self.pbRoot_f.rejectMessage.called)

        self.assertIsNone((yield self._get_forward()))
        self.pbRoot_f.ackMessage.assert_not_called()
        self.pbRoot_f.rejectMessage.assert_called_once()

    @defer.inlineCallbacks
    def _long_mo(self, split_method):
        connector = SMPPClientConfig(id='mo-long-' + split_method, port=self.SMSCPort.getHost().port)
        self.addCleanup(self.redisClient.delete, 'longDeliverSm:%s:7:%s:%s' % (connector.id, b'4567', b'1234'))
        yield self.SMPPClientManagerPBProxy.connect('127.0.0.1', self.CManagerPort)
        yield self.SMPPClientManagerPBProxy.add(connector)
        yield self.SMPPClientManagerPBProxy.start(connector.id)
        yield self._wait_for(lambda: self.SMPPClientManagerPBProxy.session_state(connector.id).addCallback(
            lambda state: state == 'BOUND_TRX'))

        parts = [b'first ', b'second ', b'last']
        client = self.SMSCPort.factory.lastClient
        responses = {}
        client.responseMap[DeliverSMResp] = lambda response: responses.__setitem__(response.seqNum, response.status)
        unsettled, wire_sequence = [3, 1, 2, 2], 0
        for _ in range(5):
            sent = {}
            for sequence in unsettled:
                wire_sequence += 1
                sent[wire_sequence] = sequence
                pdu = DeliverSM(source_addr='1234', destination_addr='4567',
                                short_message=parts[sequence - 1], seqNum=wire_sequence)
                if split_method == 'sar':
                    pdu.params.update(sar_total_segments=3, sar_segment_seqnum=sequence, sar_msg_ref_num=7)
                else:
                    pdu.params['esm_class'] = EsmClass(EsmClassMode.DEFAULT, EsmClassType.DEFAULT,
                                                       [EsmClassGsmFeatures.UDHI_INDICATOR_SET])
                    if split_method == 'udh16':
                        header = bytes([6, 8, 4, 1, 7, 3, sequence])
                    elif split_method == 'udh-ports':
                        header = bytes([11, 5, 4, 0x12, 0x34, 0x56, 0x78, 0, 3, 7, 3, sequence])
                    else:
                        header = bytes([5, 0, 3, 7, 3, sequence])
                    pdu.params['short_message'] = header + parts[sequence - 1]
                client.trigger_deliver_sm(pdu)
            yield self._wait_for(lambda: all(each in responses for each in sent))
            self.assertLessEqual({responses[each] for each in sent},
                                 {CommandStatus.ESME_ROK, CommandStatus.ESME_RX_T_APPN})
            unsettled = [sequence for each, sequence in sent.items() if responses[each] != CommandStatus.ESME_ROK]
            if not unsettled:
                break
        self.assertEqual(unsettled, [])
        yield self._wait_for(lambda: self.pbRoot_f.ackMessage.called)
        yield self.SMPPClientManagerPBProxy.stop(connector.id)
        yield self._wait_for(lambda: self.SMPPClientManagerPBProxy.session_state(connector.id).addCallback(
            lambda state: state == 'NONE'))

        record = yield self._get_forward()
        self.assertIsNotNone(record)
        self.assertEqual(record['binary'], binascii.hexlify(b''.join(parts)).decode())
        self.assertEqual(record['origin-connector'], connector.id)
        self.assertIsNone((yield self._get_forward()))
        self.pbRoot_f.ackMessage.assert_called_once()

    def test_long_sar_last_part_first_forwarded_once(self):
        return self._long_mo('sar')

    def test_long_udh_last_part_first_forwarded_once(self):
        return self._long_mo('udh')

    def test_long_udh16_last_part_first_forwarded_once(self):
        return self._long_mo('udh16')

    def test_long_udh_with_ports_strips_entire_header(self):
        return self._long_mo('udh-ports')

    @defer.inlineCallbacks
    def test_forward_survives_target_queue_outage(self):
        yield self.amqpBroker.chan.queue_delete(queue=self.forward_queue)
        content = DeliverSmContent(
            RoutableDeliverSm(self.testDeliverSMPdu, Connector('src')), 'src')
        yield self.amqpBroker.publish(exchange='messaging', routing_key=self.routingKey, content=content)
        yield self._wait_for(lambda: self.pbRoot_f.rejectMessage.called)
        self.pbRoot_f.ackMessage.assert_not_called()
        self.assertTrue(self.pbRoot_f.rejectMessage.called)
        self.assertEqual(self.pbRoot_f.rejectMessage.call_args.kwargs, {'requeue': 1})
        yield self.amqpBroker.chan.queue_declare(queue=self.forward_queue, durable=True)
        yield self._wait_for(lambda: self.pbRoot_f.ackMessage.called)

        record = yield self._get_forward()
        self.assertIsNotNone(record)
        self.assertEqual(record['id'], content.properties['message-id'])
        self.pbRoot_f.ackMessage.assert_called_once()
        self.assertIsNone((yield self._get_forward()))


class MoForwardConfirmTestCase(TestCase):
    def setUp(self):
        config = RouterPBConfig()
        config.mo_forward_queue = 'test.mo.confirm'
        self.router = RouterPB(config, persistenceTimer=False)
        self.router.deliver_sm_q = Mock()
        self.router.deliver_sm_q.get.return_value = defer.Deferred()
        self.router.amqpBroker = Mock()
        self.router.amqpBroker.publish_confirmed.return_value = defer.succeed(None)
        self.clock = task.Clock()
        self.patch(router_module, 'reactor', self.clock)
        content = DeliverSmContent(RoutableDeliverSm(DeliverSM(
            source_addr='1234', destination_addr='4567', short_message=b'confirm'), Connector('src')), 'src')
        self.message = Mock()
        self.message.content = content

    @defer.inlineCallbacks
    def test_does_not_ack_before_confirmation(self):
        confirmation = defer.Deferred()
        self.router.amqpBroker.publish_confirmed.return_value = confirmation
        delivery = self.router.deliver_sm_callback(self.message)
        self.message.channel.basic_ack.assert_not_called()
        confirmation.callback(None)
        yield delivery
        self.message.channel.basic_ack.assert_called_once_with(delivery_tag=self.message.delivery_tag)
        self.assertEqual(self.router.amqpBroker.publish_confirmed.call_args.kwargs['routing_key'], 'test.mo.confirm')

    @defer.inlineCallbacks
    def test_message_payload_preserves_all_bytes_with_default_coding(self):
        pdu = pickle.loads(self.message.content.body).pdu
        pdu.params['short_message'] = b''
        pdu.params['message_payload'] = b'\x00\x80\xff'
        self.message.content = DeliverSmContent(RoutableDeliverSm(pdu, Connector('src')), 'src')

        yield self.router.deliver_sm_callback(self.message)

        payload = json.loads(self.router.amqpBroker.publish_confirmed.call_args.kwargs['content'].body)
        self.assertEqual(payload, {
            'id': self.message.content.properties['message-id'],
            'from': '1234', 'to': '4567', 'origin-connector': 'src',
            'coding': 0, 'binary': '0080ff'})
        self.message.channel.basic_ack.assert_called_once_with(delivery_tag=self.message.delivery_tag)

    @defer.inlineCallbacks
    def test_nack_and_unroutable_publish_are_retried_without_ack(self):
        for error in (NackError([]), UnroutableError([])):
            self.message.channel.reset_mock()
            self.router.amqpBroker.publish_confirmed.return_value = defer.fail(error)
            yield self.router.deliver_sm_callback(self.message)
            self.message.channel.basic_ack.assert_not_called()
            self.message.channel.basic_reject.assert_not_called()
            self.clock.advance(1.25 * BACKOFF_CEILING_SECONDS)
            self.message.channel.basic_reject.assert_called_once_with(
                delivery_tag=self.message.delivery_tag, requeue=1)

    @defer.inlineCallbacks
    def _assert_payload_build_failure_is_not_requeued(self, pdu):
        self.message.content = DeliverSmContent(RoutableDeliverSm(pdu, Connector('src')), 'src')
        error_log = Mock()
        self.patch(self.router.log, 'error', error_log)

        yield self.router.deliver_sm_callback(self.message)
        self.clock.advance(self.router.config.mo_forward_retry_delay)

        self.message.channel.basic_reject.assert_called_once_with(
            delivery_tag=self.message.delivery_tag, requeue=0)
        self.message.channel.basic_ack.assert_not_called()
        self.router.amqpBroker.publish_confirmed.assert_not_called()
        error_log.assert_called_once()
        self.assertEqual(self.router.requeueTimers, {})
        self.assertEqual(self.clock.getDelayedCalls(), [])

    def test_missing_content_is_rejected_once_without_requeue(self):
        pdu = pickle.loads(self.message.content.body).pdu
        pdu.params.pop('short_message')
        pdu.params.pop('message_payload', None)
        return self._assert_payload_build_failure_is_not_requeued(pdu)

    def test_unserializable_address_is_rejected_once_without_requeue(self):
        pdu = pickle.loads(self.message.content.body).pdu
        pdu.params['source_addr'] = {'1234'}
        return self._assert_payload_build_failure_is_not_requeued(pdu)

    @defer.inlineCallbacks
    def test_every_connection_redeclares_the_durable_queues(self):
        channels = [Mock(is_closed=False) for _ in range(4)]
        for channel in channels:
            queue = Mock()
            queue.get.return_value = defer.Deferred()
            channel.basic_consume.return_value = defer.succeed((queue, 'consumer'))
        broker = Mock()
        broker.newChannel.side_effect = [defer.succeed(channel) for channel in channels]
        self.router.amqpBroker = broker

        yield self.router.subscribe()
        reconnect = broker.addChannelReadyCallback.call_args.args[0]
        yield reconnect()

        for queue in ('test.mo.confirm', 'RouterPB_deliver_sm_all.durable'):
            self.assertEqual(broker.named_queue_declare.call_args_list.count(call(queue=queue, durable=True)), 2)


class MoForwardConfigTestCase(TestCase):
    def test_router_reads_its_own_queue_and_retry_delay(self):
        config_path = self.mktemp()
        with open(config_path, 'w') as config:
            config.write('[router]\nmo_forward_queue = router.mo\nmo_forward_retry_delay = 7\n'
                         '[deliversm-thrower]\nmo_forward_queue = thrower.mo\nretry_delay = 2\n')
        with patch.dict(os.environ, {}, clear=True):
            router = RouterPBConfig(config_path)
            thrower = deliverSmThrowerConfig(config_path)
        self.assertEqual(router.mo_forward_queue, 'router.mo')
        self.assertEqual(router.mo_forward_retry_delay, 7)
        self.assertEqual(thrower.retry_delay, 2)

    def test_router_retry_defaults_independently_of_http_thrower(self):
        with patch.dict(os.environ, {'DELIVERSM_THROWER_MO_FORWARD_QUEUE': 'thrower.mo',
                                    'DELIVERSM_THROWER_RETRY_DELAY': '2'}, clear=True):
            router = RouterPBConfig()
            thrower = deliverSmThrowerConfig()
        self.assertEqual(router.mo_forward_queue, '')
        self.assertEqual(router.mo_forward_retry_delay, 30)
        self.assertEqual(thrower.retry_delay, 2)

    def test_router_environment_overrides_its_own_options(self):
        config_path = self.mktemp()
        with open(config_path, 'w') as config:
            config.write('[router]\nmo_forward_queue = file.mo\nmo_forward_retry_delay = 7\n')
        with patch.dict(os.environ, {'ROUTER_MO_FORWARD_QUEUE': 'environment.mo',
                                    'ROUTER_MO_FORWARD_RETRY_DELAY': '3'}, clear=True):
            router = RouterPBConfig(config_path)
        self.assertEqual(router.mo_forward_queue, 'environment.mo')
        self.assertEqual(router.mo_forward_retry_delay, 3)
