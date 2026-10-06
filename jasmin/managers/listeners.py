# pylint: disable=W0401,W0611
import hashlib
import math
import pickle
import sys
import logging
import time
from datetime import datetime, timedelta
from uuid import uuid4
from logging.handlers import TimedRotatingFileHandler

from dateutil import parser
from messaging.sms.udh import UserDataHeader
from twisted.internet import defer
from twisted.internet import reactor, task
from pika.exceptions import ConsumerCancelled
from smpp.pdu.operations import SubmitSM, DeliverSM
from smpp.pdu.pdu_types import CommandId, CommandStatus, DataCodingScheme, DataCodingGsmMsgClass, EsmClassGsmFeatures
from smpp.twisted.protocol import DataHandlerResponse
from smpp.pdu.error import SMPPRequestTimoutError, SMPPGenericNackTransactionError

from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.managers.content import (SubmitSmRespContent, DeliverSmContent, SubmitSmRespBillContent, DLR, Discard,
                                     IN_DOUBT)
from jasmin.managers.dlr import PARKED_QUEUE
from jasmin.queues.delivery import DeliveryMessage
from jasmin.tools.jitter import backoff, jittered
from jasmin.protocols.smpp.error import *
from jasmin.protocols.smpp.operations import SMPPOperationFactory, count_pdus
from jasmin.tools.tlv import format_tlvs_for_log
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import Connector
from jasmin.tools import qos

LOG_CATEGORY = "jasmin-sm-listener"
UDH_INFORMATION_ELEMENT_HEADER_LENGTH = 2
UDH_INFORMATION_ELEMENT_LENGTHS = {0x00: 3, 0x08: 4, 0x04: 2, 0x05: 4}
IDEMPOTENCY_KEY = 'idem:%s'
WIRE_CLAIM_KEY = 'wire:%s'
OUTCOME_KEY = 'outcome:%s'
OUTCOME_PUBLISH_MAX_ATTEMPTS = 5
LONG_MO_CLAIM_LEASE_SECONDS = 30
# A claim is a lease, so one a crashed assembler abandoned is taken over, under the same assembled-message id.
LONG_MO_CLAIM = '''
local claim = redis.call('HMGET', KEYS[1], 'claim', 'lease')
if claim[1] and tonumber(claim[2]) > tonumber(ARGV[3]) then return {'held'} end
local result = {'claimed', ''}
for i = 1, tonumber(ARGV[1]) do
    local part = redis.call('HGET', KEYS[1], tostring(i))
    if not part then return {} end
    result[i + 2] = part
end
redis.call('HSETNX', KEYS[1], 'id', ARGV[4])
result[2] = redis.call('HGET', KEYS[1], 'id')
redis.call('HSET', KEYS[1], 'claim', ARGV[2], 'lease', tostring(tonumber(ARGV[3]) + tonumber(ARGV[5])))
return result
'''
LONG_MO_RETIRE = '''
if redis.call('HGET', KEYS[1], 'claim') == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
'''
LONG_MO_STORE = '''
local meta = redis.call('HGET', KEYS[1], 'meta')
if meta and meta ~= ARGV[5] then return 0 end
local digest = redis.call('HGET', KEYS[1], 'digest:' .. ARGV[1])
if digest and digest ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], 'digest:' .. ARGV[1], ARGV[3], 'meta', ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
'''
LONG_MO_PARTS_TTL_SECONDS = 300
LONG_MO_RENEW = '''
if redis.call('HGET', KEYS[1], 'claim') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'lease', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
'''
LONG_MO_RELEASE = '''
if redis.call('HGET', KEYS[1], 'claim') == ARGV[1] then
    return redis.call('HDEL', KEYS[1], 'claim', 'lease')
end
return 0
'''


def chained_pdus(reqPDU):
    """The PDU and, for a long message, every further segment, in the order they are sent."""
    pdu = reqPDU
    while True:
        yield pdu
        if not hasattr(pdu, 'nextPdu'):
            return
        pdu = pdu.nextPdu


def segment_responses(reqPDU):
    """Each segment's own submit_sm_resp, in the order the segments were sent; none for a single submit."""
    return [pdu.response for pdu in chained_pdus(reqPDU) if getattr(pdu, 'response', None) is not None]


def segment_smpp_msgids(reqPDU):
    """Every SMSC id an accepted segment was acknowledged with, in the order they were sent.

    Empty for a single submit: only the segments of a long message carry a per-PDU response.
    """
    return [response.params['message_id'] for response in segment_responses(reqPDU)
            if response.status == CommandStatus.ESME_ROK and 'message_id' in response.params]


def settling_response(r):
    """The response a submit is judged by, and how many of its segments the SMSC accepted. A long message with an
    accepted segment is never resent, since that segment is already with the carrier; when another segment was
    refused, it is judged by the first refusal."""
    responses = segment_responses(r.request) or [r.response]
    accepted = sum(1 for response in responses if response.status == CommandStatus.ESME_ROK)
    refused = next((response for response in responses if response.status != CommandStatus.ESME_ROK), None)
    return (r.response if refused is None else refused), accepted


class SMPPClientSMListener:
    """
    This is a listener object instantiated for every new SMPP connection, it is responsible of handling
    SubmitSm, DeliverSm and SubmitSm PDUs for a given SMPP connection
    """

    def __init__(self, config, SMPPClientFactory, amqpBroker, redisClient, RouterPB=None, interceptorpb_client=None):
        self.config = config
        self.SMPPClientFactory = SMPPClientFactory
        self.SMPPOperationFactory = SMPPOperationFactory(self.SMPPClientFactory.config)
        self.amqpBroker = amqpBroker
        self.redisClient = redisClient
        self.RouterPB = RouterPB
        self.interceptorpb_client = interceptorpb_client
        self.submit_sm_q = None
        self.qos_last_submit_sm_at = None
        self.rejectTimers = {}
        self.in_flight = set()
        self.submit_retrials = {}
        self.discard_retrials = {}
        self.handoff_retrials = {}
        self.answered = {}
        self.qosTimer = None
        self.last_dequeued_created_at = None
        self.last_ready_count = 0
        self.learned_rate = None

        # Set pickleProtocol
        self.pickleProtocol = SMPPClientPBConfig(self.config.config_file).pickle_protocol

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file,
                                                   when=self.config.log_rotate)
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def setSubmitSmQ(self, queue):
        self.log.debug('Setting a new submit_sm_q: %s', queue)
        self.submit_sm_q = queue
        self.in_flight.clear()

    def clearRejectTimer(self, msgid):
        if msgid in self.rejectTimers:
            timer = self.rejectTimers[msgid]
            if timer.active():
                timer.cancel()
            del self.rejectTimers[msgid]

    def clearRejectTimers(self):
        for msgid, timer in list(self.rejectTimers.items()):
            if timer.active():
                timer.cancel()
            del self.rejectTimers[msgid]

    def clearQosTimer(self):
        if self.qosTimer is not None and self.qosTimer.called is False:
            self.qosTimer.cancel()
            self.qosTimer = None

    def clearAllTimers(self):
        self.clearQosTimer()
        self.clearRejectTimers()

    @defer.inlineCallbacks
    def rejectAndRequeueMessage(self, message, delay=True):
        msgid = message.content.properties['message-id']

        if delay:
            # Use configured requeue_delay or specific one
            if not isinstance(delay, bool):
                requeue_delay = delay
            else:
                requeue_delay = self.SMPPClientFactory.config.requeue_delay

            requeue_delay = jittered(requeue_delay)
            self.log.debug("Requeuing SubmitSmPDU[%s] in %s seconds",
                           msgid, requeue_delay)

            # Requeue the message with a delay
            timer = reactor.callLater(requeue_delay,
                                      self.rejectMessage,
                                      message=message,
                                      requeue=1)

            # If any, clear timer before setting a new one
            self.clearRejectTimer(msgid)

            self.rejectTimers[msgid] = timer
            defer.returnValue(timer)
        else:
            self.log.debug("Requeuing SubmitSmPDU[%s] without delay", msgid)
            yield self.rejectMessage(message, requeue=1)

    def rejectMessage(self, message, requeue=0):
        msgid = message.content.properties['message-id']
        self.in_flight.discard(msgid)
        if not requeue:
            self.submit_retrials.pop(msgid, None)
        if not self.amqpBroker.connected:
            self.log.error("Cannot reject message, AMQP Broker is not connected !")
            return

        # Reject on the channel that delivered the message. If that channel has since closed, the broker
        # has already requeued the delivery, so there is nothing left to do.
        try:
            message.channel.basic_reject(delivery_tag=message.delivery_tag, requeue=requeue)
        except Exception as e:
            self.log.warning("Could not reject SubmitSmPDU[%s] (delivering channel likely closed): %s", msgid, e)

    def ackMessage(self, message):
        msgid = message.content.properties['message-id']
        self.in_flight.discard(msgid)
        self.submit_retrials.pop(msgid, None)
        if not self.amqpBroker.connected:
            self.log.error("Cannot ack message, AMQP Broker is not connected !")
            return

        try:
            message.channel.basic_ack(delivery_tag=message.delivery_tag)
        except Exception as e:
            self.log.warning("Could not ack SubmitSmPDU[%s] (delivering channel likely closed): %s", msgid, e)

    @defer.inlineCallbacks
    def _discard(self, message, reason):
        msgid = message.content.properties['message-id']
        discard = Discard(msgid, reason, self.SMPPClientFactory.config.id)
        attempts = self.discard_retrials.get(msgid, 0)
        try:
            if self.config.emit_discard_dlr:
                yield self.publishOutcome(msgid, 'messaging', 'dlr.discard', discard, attempts)
            if reason == 'smppc_not_ready' and self.redisClient is not None:
                yield self.redisClient.delete(IDEMPOTENCY_KEY % msgid)
        except Exception as e:
            self.discard_retrials[msgid] = attempts + 1
            self.log.error("Discard of SubmitSmPDU[%s] (%s) failed, message requeued: %s", msgid, reason, e)
            yield self.rejectAndRequeueMessage(
                message, delay=backoff(self.SMPPClientFactory.config.requeue_delay, attempts))
            return
        self.discard_retrials.pop(msgid, None)
        yield self.rejectMessage(message)

    def submissionDeadline(self, headers):
        """The last moment the submit may be sent: its validity, else dlr_expiry after it was queued."""
        if 'expiration' in headers:
            return parser.parse(headers['expiration'])
        return parser.parse(headers['created_at']) + timedelta(seconds=self.SMPPClientFactory.config.dlr_expiry)

    def wireClaimTtl(self, headers):
        """Past the deadline no delivery is sent, so a claim that outlives it covers every delivery of the submit."""
        remaining = (self.submissionDeadline(headers) - datetime.now()).total_seconds()
        return math.ceil(max(0, remaining) + self.SMPPClientFactory.config.dlr_expiry)

    def claimWire(self, msgid, headers):
        return self.redisClient.set(WIRE_CLAIM_KEY % msgid, self.SMPPClientFactory.config.id,
                                    expire=self.wireClaimTtl(headers), only_if_not_exists=True)

    @defer.inlineCallbacks
    def requeueRefused(self, message, delay):
        msgid = message.content.properties['message-id']
        try:
            yield self.redisClient.delete(WIRE_CLAIM_KEY % msgid)
        except Exception as e:
            self.log.error("Wire claim of refused SubmitSmPDU[%s] not released, retrying: %s", msgid, e)
            self.clearRejectTimer(msgid)
            self.rejectTimers[msgid] = reactor.callLater(
                self.SMPPClientFactory.config.requeue_delay, self.requeueRefused, message, delay)
            return
        yield self.rejectAndRequeueMessage(message, delay=delay)

    @defer.inlineCallbacks
    def publishOutcome(self, msgid, exchange, routing_key, content, attempts):
        if attempts < OUTCOME_PUBLISH_MAX_ATTEMPTS:
            yield self.amqpBroker.publish_confirmed(exchange=exchange, routing_key=routing_key, content=content,
                                                    mandatory=self.config.mandatory_outcomes)
            return
        parked = PARKED_QUEUE % routing_key
        yield self.amqpBroker.named_queue_declare(queue=parked, durable=True)
        yield self.amqpBroker.publish_confirmed(exchange='', routing_key=parked, content=content)
        self.log.critical("%s of SubmitSmPDU[%s] parked on %s after %s unconfirmed publications",
                          routing_key, msgid, parked, attempts)

    @defer.inlineCallbacks
    def recordOutcome(self, message, handoffs):
        """What a redelivery of an answered submit replays instead of going to the wire again. Kept in memory too, so a
        redelivery to this process replays it even when Redis did not take it."""
        msgid = message.content.properties['message-id']
        self.answered[msgid] = handoffs
        try:
            yield self.redisClient.set(OUTCOME_KEY % msgid, pickle.dumps(handoffs, self.pickleProtocol),
                                       expire=self.wireClaimTtl(message.content.properties['headers']))
        except Exception as e:
            self.log.error("Outcome of SubmitSmPDU[%s] recorded only in this process: %s", msgid, e)

    @defer.inlineCallbacks
    def handOffOutcome(self, message, handoffs):
        """The submit leaves its queue only once the broker has confirmed every outcome of it. Each confirmed one is
        struck from the record a redelivery replays; a crash between a confirmation and that record publishes it
        again, so outcomes are at-least-once."""
        msgid = message.content.properties['message-id']
        attempts = self.handoff_retrials.get(msgid, 0)
        try:
            while handoffs:
                exchange, routing_key, content = handoffs[0]
                yield self.publishOutcome(msgid, exchange, routing_key, content, attempts)
                handoffs = handoffs[1:]
                yield self.recordOutcome(message, handoffs)
        except Exception as e:
            self.handoff_retrials[msgid] = attempts + 1
            self.log.error("Outcome of SubmitSmPDU[%s] not confirmed, message requeued to replay it: %s", msgid, e)
            yield self.rejectAndRequeueMessage(
                message, delay=backoff(self.SMPPClientFactory.config.requeue_delay, attempts))
            return
        self.handoff_retrials.pop(msgid, None)
        self.answered.pop(msgid, None)
        self.clearRejectTimer(msgid)
        yield self.ackMessage(message)

    @defer.inlineCallbacks
    def settleAnsweredOrInDoubt(self, message):
        """A delivery of a submit that already went to the wire: replay its recorded outcome, or else it is in doubt."""
        msgid = message.content.properties['message-id']
        recorded = self.answered.get(msgid)
        if recorded is None:
            try:
                stored = yield self.redisClient.get(OUTCOME_KEY % msgid)
            except Exception as e:
                self.log.error("SubmitSmPDU[%s] held back, its recorded outcome could not be read: %s", msgid, e)
                yield self.rejectAndRequeueMessage(message)
                return
            if stored is None:
                self.log.error("SubmitSmPDU[%s] already went to the wire once, discarding it in doubt.", msgid)
                yield self._discard(message, IN_DOUBT)
                return
            recorded = pickle.loads(stored)
        self.log.info("SubmitSmPDU[%s] already answered, replaying its outcome without sending it again.", msgid)
        yield self.handOffOutcome(message, recorded)

    def _on_message(self, received):
        # Wrap pika's ReceivedMessage in the txamqp-style shim submit_sm_callback below expects.
        message = DeliveryMessage(received)
        self.in_flight.add(message.content.properties['message-id'])
        return self.submit_sm_callback(message)

    def oldest_ready_age_seconds(self):
        if self.last_dequeued_created_at is None:
            return None
        return max(0.0, (datetime.now() - self.last_dequeued_created_at).total_seconds())

    @defer.inlineCallbacks
    def submit_sm_callback(self, message):
        """This callback is a queue listener
        it is called whenever a message was consumed from queue
        c.f. test_amqp.ConsumeTestCase for use cases
        """
        msgid = None
        SubmitSmPDU = None
        try:
            msgid = message.content.properties['message-id']
            self.submit_sm_q.get().addCallback(self._on_message).addErrback(self.submit_sm_errback)
            SubmitSmPDU = pickle.loads(message.content.body)

            if 'created_at' in message.content.properties.get('headers', {}):
                self.last_dequeued_created_at = parser.parse(
                    message.content.properties['headers']['created_at'])
            self.last_ready_count = max(0, self.last_ready_count - 1)

            self.log.debug("Callbacked a submit_sm with a SubmitSmPDU[%s] (?): %s", msgid, SubmitSmPDU)

            # Update submit_sm retrial tracker
            if msgid in self.submit_retrials:
                self.submit_retrials[msgid] += 1
            else:
                self.submit_retrials[msgid] = 1

            # Verify if message is a SubmitSm PDU
            if isinstance(SubmitSmPDU, SubmitSM) is False:
                self.log.error(
                    "Received object[%s] is not an instance of SubmitSm: discarding this unknown object from queue",
                    msgid)
                yield self._discard(message, 'tlv')
                defer.returnValue(False)

            headers = message.content.properties.get('headers', {})
            try:
                sent_before = yield self.redisClient.get(WIRE_CLAIM_KEY % msgid)
            except Exception as e:
                self.log.error("SubmitSmPDU[%s] held back, its wire claim could not be read: %s", msgid, e)
                yield self.rejectAndRequeueMessage(message)
                defer.returnValue(False)
            if sent_before is not None or msgid in self.answered:
                yield self.settleAnsweredOrInDoubt(message)
                defer.returnValue(False)

            # If the message has expired in the queue
            deadline = self.submissionDeadline(headers)
            if deadline < datetime.now():
                self.log.info("Discarding expired message[%s]: deadline is %s", msgid, deadline)
                yield self._discard(message, 'expired')
                defer.returnValue(False)

            # SMPP Client should be already connected
            if self.SMPPClientFactory.smpp is None:
                created_at = parser.parse(message.content.properties['headers']['created_at'])
                msgAge = datetime.now() - created_at
                if msgAge.total_seconds() > self.config.submit_max_age_smppc_not_ready:
                    self.log.error(
                        "SMPPC [cid:%s] is not connected: Discarding (#%s) SubmitSmPDU[%s], over-aged %s seconds.",
                        self.SMPPClientFactory.config.id, self.submit_retrials[msgid],
                        msgid, msgAge.total_seconds())
                    yield self._discard(message, 'smppc_not_ready')
                    defer.returnValue(False)
                else:
                    if self.config.submit_retrial_delay_smppc_not_ready:
                        delay_str = ' with delay %s seconds' % self.config.submit_retrial_delay_smppc_not_ready
                    else:
                        delay_str = ''
                    self.log.error(
                        "SMPPC [cid:%s] is not connected: Requeuing (#%s) SubmitSmPDU[%s]%s, aged %s seconds.",
                        self.SMPPClientFactory.config.id, self.submit_retrials[msgid],
                        msgid, delay_str, msgAge.total_seconds())
                    yield self.rejectAndRequeueMessage(message,
                                                       delay=self.config.submit_retrial_delay_smppc_not_ready)
                    defer.returnValue(False)

            # SMPP Client should be already bound as transceiver or transmitter
            if self.SMPPClientFactory.smpp.isBound() is False:
                created_at = parser.parse(message.content.properties['headers']['created_at'])
                msgAge = datetime.now() - created_at
                if msgAge.total_seconds() > self.config.submit_max_age_smppc_not_ready:
                    self.log.error(
                        "SMPPC [cid:%s] is not bound: Discarding (#%s) SubmitSmPDU[%s], over-aged %s seconds.",
                        self.SMPPClientFactory.config.id, self.submit_retrials[msgid],
                        msgid, msgAge.total_seconds())
                    yield self._discard(message, 'smppc_not_ready')
                    defer.returnValue(False)
                else:
                    if self.config.submit_retrial_delay_smppc_not_ready:
                        delay_str = ' with delay %s seconds' % self.config.submit_retrial_delay_smppc_not_ready
                    else:
                        delay_str = ''
                    self.log.error("SMPPC [cid:%s] is not bound: Requeuing (#%s) SubmitSmPDU[%s]%s, aged %s seconds.",
                                   self.SMPPClientFactory.config.id, self.submit_retrials[msgid],
                                   msgid, delay_str, msgAge)
                    yield self.rejectAndRequeueMessage(
                        message, delay=self.config.submit_retrial_delay_smppc_not_ready)
                    defer.returnValue(False)

            # Validate per-message TLVs against the connector's rule set
            # (required + max-length), then resolve their wire types from
            # the connector config so the SMPP encoder knows how to
            # serialize (e.g. Int8 vs OctetString).
            from jasmin.tools.tlv_encoder import validate_custom_tlvs, resolve_tlv_types
            connector_tlvs = getattr(self.SMPPClientFactory.config, 'custom_tlvs', [])
            pdu_tlvs_before = getattr(SubmitSmPDU, 'custom_tlvs', []) or []

            # Log what type the caller sent (before resolution)
            for t in pdu_tlvs_before:
                caller_type = t[2] if len(t) >= 3 else None
                tag = t[0] if t else '?'
                self.log.info(
                    "TLV [msgid:%s] [tag:0x%04X] [caller_type:%s] [value_preview:%s]",
                    msgid, int(tag) & 0xFFFF,
                    caller_type if caller_type else 'not-specified',
                    repr(t[3])[:60] if len(t) >= 4 else '?')

            # Resolve None-typed tuples to their declared type; log the resolved type
            SubmitSmPDU.custom_tlvs = resolve_tlv_types(pdu_tlvs_before, connector_tlvs)
            for t in SubmitSmPDU.custom_tlvs:
                tag = t[0] if t else '?'
                resolved_type = t[2] if len(t) >= 3 else None
                self.log.info(
                    "TLV [msgid:%s] [tag:0x%04X] [resolved_type:%s]",
                    msgid, int(tag) & 0xFFFF,
                    resolved_type if resolved_type else 'unknown')

            ok, err = validate_custom_tlvs(SubmitSmPDU.custom_tlvs, connector_tlvs)
            if not ok:
                self.log.error("Rejecting SubmitSmPDU[%s]: %s", msgid, err)
                yield self._discard(message, 'tlv')
                defer.returnValue(False)

            for scope in ('batch', 'account'):
                tag = headers.get('tag_%s' % scope)
                if tag:
                    try:
                        revoked = yield self.redisClient.exists('revoked:%s:%s' % (scope, tag))
                    except Exception as e:
                        self.log.warning(
                            "Revoke check failed for [scope:%s] [tag:%s]: %s, sending as not revoked",
                            scope, tag, e)
                        revoked = False
                    if revoked:
                        yield self._discard(message, 'revoked')
                        defer.returnValue(False)

            if self.qos_last_submit_sm_at is None:
                self.qos_last_submit_sm_at = datetime(1970, 1, 1)

            paced = self.SMPPClientFactory.config.submit_sm_throughput > 0
            if paced:
                throughput = self.SMPPClientFactory.config.submit_sm_throughput
                if self.learned_rate is None or self.learned_rate.ceiling != float(throughput):
                    self.learned_rate = qos.LearnedRate(throughput)
                qos_slow_down = qos.throttle_delay(
                    self.learned_rate.current,
                    self.qos_last_submit_sm_at,
                    datetime.now())
                if qos_slow_down > 0:
                    self.log.debug(
                        "QoS: submit_sm_callback faster than throughput (%s/s), slowing down %ss.",
                        self.learned_rate.current, qos_slow_down)
                    yield qos.slow_down(qos_slow_down)
                SubmitSmPDU.segment_interval = 1 / self.learned_rate.current
            else:
                self.learned_rate = None

            try:
                claimed = yield self.claimWire(msgid, headers)
            except Exception as e:
                self.log.error("SubmitSmPDU[%s] held back, its wire claim could not be taken: %s", msgid, e)
                yield self.rejectAndRequeueMessage(message)
                defer.returnValue(False)
            if not claimed:
                yield self.settleAnsweredOrInDoubt(message)
                defer.returnValue(False)

            schedule = timedelta(seconds=(count_pdus(SubmitSmPDU) - 1) * getattr(SubmitSmPDU, 'segment_interval', 0))
            remaining = (deadline - datetime.now()).total_seconds()
            if remaining < schedule.total_seconds():
                self.log.info("Discarding SubmitSmPDU[%s]: its segments cannot all leave before %s", msgid, deadline)
                yield self.redisClient.delete(WIRE_CLAIM_KEY % msgid)
                yield self._discard(message, 'expired')
                defer.returnValue(False)
            SubmitSmPDU.send_by = reactor.seconds() + remaining
            if paced:
                self.qos_last_submit_sm_at = datetime.now() + schedule

            # Finally: send the sms !
            self.log.debug("Sending SubmitSmPDU[%s] through SMPPClientFactory [cid:%s] after %s requeues.",
                           msgid, self.SMPPClientFactory.config.id, self.submit_retrials[msgid])
            d = self.SMPPClientFactory.smpp.sendDataRequest(SubmitSmPDU)
            d.addCallback(self.submit_sm_resp_event, message)
            yield d
        except Exception as e:
            # Only bytes on the wire can have reached the SMSC, and only a generic_nack answering this very request
            # proves it refused them; anything else after transmission leaves the outcome unknown.
            transmitted = isinstance(SubmitSmPDU, SubmitSM) and any(
                getattr(pdu, 'transmitted', False) for pdu in chained_pdus(SubmitSmPDU))
            accepted = segment_smpp_msgids(SubmitSmPDU) if transmitted else []
            if accepted:
                self.log.error("Long SubmitSmPDU[%s] through [cid:%s] failed after %s accepted segments, the rest in "
                               "doubt, never resent: %s", msgid, self.SMPPClientFactory.config.id, len(accepted), e)
                handoffs = [('messaging', 'dlr.submit_sm_resp', DLR(
                    pdu_type=CommandId.submit_sm_resp, msgid=msgid, status='DISCARDED', smpp_msgids=accepted,
                    dlr_scope=getattr(self.SMPPClientFactory.config, 'dlr_scope', ''),
                    accepted_segments=len(accepted), discard_reason=IN_DOUBT))]
                yield self.recordOutcome(message, handoffs)
                yield self.handOffOutcome(message, handoffs)
                defer.returnValue(False)
            if transmitted and not isinstance(e, SMPPGenericNackTransactionError):
                self.log.error("SubmitSmPDU[%s] got no definite answer through [cid:%s], discarding it in doubt: %s",
                               msgid, self.SMPPClientFactory.config.id, e)
                yield self._discard(message, IN_DOUBT)
                defer.returnValue(False)
            self.log.critical("Rejecting SubmitSmPDU[%s] through [cid:%s] for an unknown error (%s): %s",
                              msgid, self.SMPPClientFactory.config.id, type(e), e)
            yield self._discard(message, 'exception')
            defer.returnValue(False)

    @defer.inlineCallbacks
    def submit_sm_resp_event(self, r, amqpMessage):
        msgid = amqpMessage.content.properties['message-id']
        total_bill_amount = 0.0
        will_be_retried = False

        try:
            response, accepted = settling_response(r)
            if 'submit_sm_bill' in amqpMessage.content.properties['headers']:
                submit_sm_resp_bill = pickle.loads(
                    amqpMessage.content.properties['headers']['submit_sm_bill']).getSubmitSmRespBill()
            else:
                submit_sm_resp_bill = None

            # Each accepted segment is billed, and only those
            if submit_sm_resp_bill is not None and submit_sm_resp_bill.getTotalAmounts() > 0:
                total_bill_amount = submit_sm_resp_bill.getTotalAmounts() * accepted

            if response.status == CommandStatus.ESME_ROK:
                if self.learned_rate:
                    self.learned_rate.clean(datetime.now())

                # UDH is set ?
                UDHI_INDICATOR_SET = False
                if hasattr(r.request.params['esm_class'], 'gsmFeatures'):
                    for gsmFeature in r.request.params['esm_class'].gsmFeatures:
                        if gsmFeature == EsmClassGsmFeatures.UDHI_INDICATOR_SET:
                            UDHI_INDICATOR_SET = True
                            break

                # What type of splitting ?
                splitMethod = None
                if 'sar_msg_ref_num' in r.request.params:
                    splitMethod = 'sar'
                elif UDHI_INDICATOR_SET and r.request.params['short_message'][:3] == b'\x05\x00\x03':
                    splitMethod = 'udh'
                    
                # create _pdu before splitting, so we can use it in both branches (logging the message according the last part of message)
                _pdu = r.request

                # Concatenate short_message
                if splitMethod is not None:
                    if splitMethod == 'sar':
                        short_message = _pdu.params['short_message']
                    else:
                        short_message = _pdu.params['short_message'][6:]

                    while hasattr(_pdu, 'nextPdu'):
                        _pdu = _pdu.nextPdu
                        if splitMethod == 'sar':
                            short_message += _pdu.params['short_message']
                        else:
                            short_message += _pdu.params['short_message'][6:]
                else:
                    short_message = _pdu.params['short_message']

                # Do not log text for privacy reasons
                # Added in #691
                if self.config.log_privacy:
                    logged_content = '** %s byte content **' % len(short_message)
                else:
                    logged_content = '%r' % short_message

                self.log.info(
                    "SMS-MT [cid:%s] [queue-msgid:%s] [smpp-msgid:%s] [status:%s] [prio:%s] [dlr:%s] [validity:%s] \
[from:%s] [to:%s] [content:%s] [tlvs:%s]",
                    self.SMPPClientFactory.config.id,
                    msgid,
                    response.params['message_id'],
                    response.status,
                    amqpMessage.content.properties['priority'],
                    _pdu.params['registered_delivery'].receipt,
                    'none' if ('headers' not in amqpMessage.content.properties or
                               'expiration' not in amqpMessage.content.properties['headers'])
                    else amqpMessage.content.properties['headers']['expiration'],
                    _pdu.params['source_addr'],
                    _pdu.params['destination_addr'],
                    logged_content,
                    format_tlvs_for_log(_pdu, self.config.log_privacy))
            else:
                throttled = response.status == CommandStatus.ESME_RTHROTTLED and self.learned_rate
                if throttled:
                    self.learned_rate.throttled()

                if accepted:
                    self.log.error("Long SubmitSmPDU[%s] through [cid:%s] partly accepted (%s segments), never resent: "
                                   "%s", msgid, self.SMPPClientFactory.config.id, accepted, response.status)
                # Message must be retried ?
                elif response.status.name in self.config.submit_error_retrial:
                    retrial = self.config.submit_error_retrial[response.status.name]
                    delay = False if throttled else retrial['delay']

                    # Still have some retries to go ?
                    if self.submit_retrials[msgid] < retrial['count']:
                        yield self.requeueRefused(amqpMessage, delay)
                        will_be_retried = True

                # Do not log text for privacy reasons
                # Added in #691
                if self.config.log_privacy:
                    logged_content = '** %s byte content **' % len(r.request.params['short_message'])
                else:
                    logged_content = '%r' % r.request.params['short_message']

                # Log the message
                self.log.info(
                    "SMS-MT [cid:%s] [queue-msgid:%s] [status:ERROR/%s] [retry:%s] [prio:%s] [dlr:%s] [validity:%s] \
[from:%s] [to:%s] [content:%s] [tlvs:%s]",
                    self.SMPPClientFactory.config.id,
                    msgid,
                    response.status,
                    will_be_retried,
                    amqpMessage.content.properties['priority'],
                    r.request.params['registered_delivery'].receipt,
                    'none' if ('headers' not in amqpMessage.content.properties or
                               'expiration' not in amqpMessage.content.properties['headers'])
                    else amqpMessage.content.properties['headers']['expiration'],
                    r.request.params['source_addr'],
                    r.request.params['destination_addr'],
                    logged_content,
                    format_tlvs_for_log(r.request, self.config.log_privacy))

            # Only a final outcome: a retryable error's receipt would evict the dlr: map a successful retry needs.
            if not will_be_retried:
                if response.status == CommandStatus.ESME_ROK:
                    dlr = DLR(pdu_type=response.id, msgid=msgid, status=response.status,
                              smpp_msgid=response.params['message_id'],
                              smpp_msgids=segment_smpp_msgids(r.request),
                              dlr_scope=getattr(self.SMPPClientFactory.config, 'dlr_scope', ''))
                else:
                    dlr = DLR(pdu_type=response.id, msgid=msgid, status=response.status,
                              smpp_msgids=segment_smpp_msgids(r.request),
                              dlr_scope=getattr(self.SMPPClientFactory.config, 'dlr_scope', ''),
                              accepted_segments=accepted or None)
                handoffs = [('messaging', 'dlr.submit_sm_resp', dlr)]
                yield self.recordOutcome(amqpMessage, handoffs)
                yield self.handOffOutcome(amqpMessage, handoffs)

                if total_bill_amount > 0:
                    pubQueueName = 'bill_request.submit_sm_resp.%s' % submit_sm_resp_bill.user.uid
                    content = SubmitSmRespBillContent(submit_sm_resp_bill.bid,
                                                      submit_sm_resp_bill.user.uid, total_bill_amount)
                    self.log.debug(
                        "Requesting a SubmitSmRespBillContent from a bill [bid:%s] with routing_key[%s]: %s",
                        submit_sm_resp_bill.bid, pubQueueName, total_bill_amount)
                    yield self.amqpBroker.publish(exchange='billing', routing_key=pubQueueName, content=content)

            if self.config.publish_submit_sm_resp:
                # Send back submit_sm_resp to submit.sm.resp.CID queue
                # There's no actual listeners on this queue, it can be used to
                # track submit_sm_resp messages from a 3rd party app
                content = SubmitSmRespContent(response, msgid, pickleProtocol=self.pickleProtocol)
                self.log.debug("Sending back SubmitSmRespContent[%s] with routing_key[%s]",
                               msgid, amqpMessage.content.properties['reply-to'])
                yield self.amqpBroker.publish(exchange='messaging',
                                              routing_key=amqpMessage.content.properties['reply-to'],
                                              content=content)
        except Exception as e:
            self.log.error('(%s) while handling submit_sm_resp pdu for msgid:%s: %s', type(e), msgid, e)
        else:
            if will_be_retried:
                defer.returnValue(False)

    def submit_sm_errback(self, error):
        """It appears that when closing a queue with the close() method it errbacks with
        a ConsumerCancelled exception, didn't find a clean way to stop consuming a queue
        without errbacking here so this is a workaround to make it clean, it can be considered
        as a @TODO requiring knowledge of the queue api behaviour
        """
        if error.check(ConsumerCancelled) is None:
            # @todo: implement this errback
            # For info, this errback is called whenever:
            # - an error has occurred inside submit_sm_callback
            # - the qosTimer has been cancelled (self.clearQosTimer())
            try:
                error.raiseException()
            except Exception as e:
                self.log.error("Error in submit_sm_errback (%s): %s", type(e), e)

    @defer.inlineCallbacks
    def concatDeliverSMs(self, hashKey, splitMethod, total_segments):
        """Assembles the long MO once every part is stored, then retires its parts so a reused reference starts
        afresh. False while another assembler holds the parts: the carrier must resend this one later."""
        claim = str(uuid4())
        result = yield self.redisClient.eval(
            LONG_MO_CLAIM, keys=[hashKey],
            args=[total_segments, claim, time.time(), str(uuid4()), LONG_MO_CLAIM_LEASE_SECONDS])
        if not result:
            return True
        if result[0] == 'held':
            return False

        try:
            assembled_msgid, parts = result[1], result[2:]
            pdus = {value['segment_seqnum']: value['pdu'] for value in map(pickle.loads, parts)}

            if pdus[1].params.get('short_message'):
                msg_content_key = 'short_message'
            elif 'message_payload' in pdus[1].params:
                msg_content_key = 'message_payload'
            elif 'short_message' in pdus[1].params:
                msg_content_key = 'short_message'
            else:
                self.log.error('Cannot find message content in first pdu params')
                yield self.retireLongMo(hashKey, claim)
                return True

            contents = [pdus[i].params[msg_content_key] for i in range(1, total_segments + 1)]
            concat_message_content = b''.join(
                part if splitMethod == 'sar' else part[part[0] + 1:] for part in contents)

            pdu = pdus[1]
            if splitMethod == 'sar':
                del pdu.params['sar_segment_seqnum']
                del pdu.params['sar_total_segments']
                del pdu.params['sar_msg_ref_num']
            else:
                pdu.params['esm_class'] = None
            pdu.params[msg_content_key] = concat_message_content

            @defer.inlineCallbacks
            def renewed():
                held = yield self.redisClient.eval(LONG_MO_RENEW, keys=[hashKey],
                                                   args=[claim, time.time() + LONG_MO_CLAIM_LEASE_SECONDS,
                                                         LONG_MO_PARTS_TTL_SECONDS])
                if not held:
                    raise RuntimeError('Long MO %s was taken over by another assembler' % hashKey)

            routable = RoutableDeliverSm(pdu, Connector(self.SMPPClientFactory.config.id))
            yield self.deliver_sm_event_post_interception(routable=routable, smpp=None, concatenated=True,
                                                          msgid=assembled_msgid, guard=renewed)
        except Exception:
            yield self.redisClient.eval(LONG_MO_RELEASE, keys=[hashKey], args=[claim])
            raise
        yield self.retireLongMo(hashKey, claim)
        return True

    @defer.inlineCallbacks
    def retireLongMo(self, hashKey, claim):
        try:
            yield self.redisClient.eval(LONG_MO_RETIRE, keys=[hashKey], args=[claim])
        except Exception as e:
            self.log.critical("Assembled long MO [%s] not retired, its parts linger until they expire: %s", hashKey, e)

    def code_dlr_msgid(self, pdu):
        """Code the dlr msg id accordingly to SMPPc's dlr_msg_id_bases value"""

        try:
            if isinstance(pdu, DeliverSM):
                if isinstance(pdu.dlr['id'], bytes):
                    dlr_id = pdu.dlr['id'].decode()
                else:
                    dlr_id = pdu.dlr['id']
                if self.SMPPClientFactory.config.dlr_msg_id_bases == 1:
                    ret = ('%x' % int(dlr_id)).upper().lstrip('0')
                elif self.SMPPClientFactory.config.dlr_msg_id_bases == 2:
                    ret = int(str(dlr_id), 16)
                else:
                    ret = str(dlr_id).upper().lstrip('0')
            else:
                if isinstance(pdu.dlr['id'], bytes):
                    dlr_id = pdu.dlr['id'].decode()
                else:
                    dlr_id = pdu.dlr['id']
                # TODO: code dlr for submit_sm_resp maybe ? TBC
                ret = str(dlr_id).upper().lstrip('0')
        except Exception as e:
            self.log.error('code_dlr_msgid, cannot code msgid [%s] with dlr_msg_id_bases:%s',
                           dlr_id, self.SMPPClientFactory.config.dlr_msg_id_bases)
            self.log.error('code_dlr_msgid, error details: %s', e)
            ret = str(dlr_id).upper().lstrip('0')

        self.log.debug('code_dlr_msgid: %s coded to %s', dlr_id, ret)
        return ret

    def deliver_sm_event_interceptor(self, smpp, pdu):
        self.log.debug('Intercepting deliver_sm event in smppc %s', self.SMPPClientFactory.config.id)

        if self.RouterPB is None:
            self.log.error(
                '(deliver_sm_event_interceptor/%s) RouterPB not set: deliver_sm will not be routed',
                self.SMPPClientFactory.config.id)
            return

        # Prepare for interception
        # this is a temporary routable instance to be used in interception
        routable = RoutableDeliverSm(pdu, Connector(self.SMPPClientFactory.config.id))

        # Interception inline
        # @TODO: make Interception in a thread, just like httpapi interception
        interceptor = self.RouterPB.getMOInterceptionTable().getInterceptorFor(routable)
        if interceptor is not None:
            self.log.debug("RouterPB selected %s interceptor for this DeliverSmPDU", interceptor)
            if self.interceptorpb_client is None:
                smpp.factory.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not set !")
                raise InterceptorNotSetError('InterceptorPB not set !')
            if not self.interceptorpb_client.isConnected:
                smpp.factory.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not connected !")
                raise InterceptorNotConnectedError('InterceptorPB not connected !')

            script = interceptor.getScript()
            self.log.debug("Interceptor script loaded: %s", script)

            # Run !
            d = self.interceptorpb_client.run_script(script, routable)
            d.addCallback(self.deliver_sm_event_post_interception, routable=routable, smpp=smpp)
            d.addErrback(self.deliver_sm_event_post_interception)
            return d
        else:
            return self.deliver_sm_event_post_interception(routable=routable, smpp=smpp)

    @defer.inlineCallbacks
    def deliver_sm_event_post_interception(self, *args, **kw):
        """This event is called whenever a deliver_sm pdu is received through a SMPPc
        It will hand the pdu to the router or a dlr thrower (depending if its a DLR or not).

        Note: this event will catch data_sm pdus as well
        """

        concatenated = kw.get('concatenated', False)
        try:
            # Control args
            if 'smpp' not in kw or 'routable' not in kw:
                self.log.error(
                    'deliver_sm_event_post_interception missing arguments after interception: %s', kw)
                raise InterceptorRunError(
                    'deliver_sm_event_post_interception missing arguments after interception')

            # Set defaults
            smpp = kw['smpp']
            routable = kw['routable']

            # Get message_content
            if 'short_message' in routable.pdu.params and len(routable.pdu.params['short_message']) > 0:
                message_content = routable.pdu.params['short_message']
            elif 'message_payload' in routable.pdu.params:
                message_content = routable.pdu.params['message_payload']
            elif 'short_message' in routable.pdu.params:
                message_content = routable.pdu.params['short_message']
            else:
                message_content = None

            # Post interception:
            if len(args) == 1:
                if isinstance(args[0], bool) and not args[0]:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.error('Failed running interception script, got a False return.')
                    raise InterceptorRunError('Failed running interception script, check log for details')
                elif isinstance(args[0], dict) and args[0]['smpp_status'] > 0:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.info(
                        'Interceptor script returned %s smpp_status error.', args[0]['smpp_status'])
                    raise DeliverSmInterceptionError(code=args[0]['smpp_status'])
                elif isinstance(args[0], (str, bytes)):
                    smpp.factory.stats.inc('interceptor_count')
                    routable = pickle.loads(args[0])
                else:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.error(
                        'Failed running interception script, got the following return: %s', args[0])
                    raise InterceptorRunError(
                        'Failed running interception script, got the following return: %s' % args[0])

            self.log.debug('Handling deliver_sm_event_post_interception event for smppc: %s',
                           self.SMPPClientFactory.config.id)

            # Resolve inbound vendor TLV types from connector config and log them
            from jasmin.tools.tlv_encoder import resolve_tlv_types as _resolve_tlvs
            inbound_tlvs = getattr(routable.pdu, 'custom_tlvs', []) or []
            if inbound_tlvs:
                connector_tlvs = getattr(self.SMPPClientFactory.config, 'custom_tlvs', [])
                routable.pdu.custom_tlvs = _resolve_tlvs(inbound_tlvs, connector_tlvs)
                for t in routable.pdu.custom_tlvs:
                    tag = t[0] if t else '?'
                    resolved_type = t[2] if len(t) >= 3 else None
                    val_preview = repr(t[3])[:60] if len(t) >= 4 else '?'
                    self.log.info(
                        "TLV-RX [cid:%s] [tag:0x%04X] [type:%s] [value_preview:%s]",
                        self.SMPPClientFactory.config.id,
                        int(tag) & 0xFFFF,
                        resolved_type if resolved_type else 'unknown',
                        val_preview)

            routable.pdu.dlr = self.SMPPOperationFactory.isDeliveryReceipt(routable.pdu)
            content = DeliverSmContent(routable,
                                       self.SMPPClientFactory.config.id,
                                       pickleProtocol=self.pickleProtocol,
                                       concatenated=concatenated,
                                       msgid=kw.get('msgid'))
            msgid = content.properties['message-id']

            if routable.pdu.dlr is None:
                # We have a SMS-MO

                # UDH is set ?
                UDHI_INDICATOR_SET = False
                if 'esm_class' in routable.pdu.params and hasattr(routable.pdu.params['esm_class'], 'gsmFeatures'):
                    for gsmFeature in routable.pdu.params['esm_class'].gsmFeatures:
                        if gsmFeature == EsmClassGsmFeatures.UDHI_INDICATOR_SET:
                            UDHI_INDICATOR_SET = True
                            break

                not_class2 = True
                if 'data_coding' in routable.pdu.params:
                    dcs = routable.pdu.params['data_coding']
                    if (dcs.scheme == DataCodingScheme.GSM_MESSAGE_CLASS) and (dcs.schemeData is not None):
                        not_class2 = (dcs.schemeData.msgClass != DataCodingGsmMsgClass.CLASS_2)

                splitMethod = None
                # Is it a part of a long message ?
                if 'sar_msg_ref_num' in routable.pdu.params:
                    splitMethod = 'sar'
                    total_segments = routable.pdu.params['sar_total_segments']
                    segment_seqnum = routable.pdu.params['sar_segment_seqnum']
                    msg_ref_num = routable.pdu.params['sar_msg_ref_num']
                    self.log.debug(
                        'Received SMS-MO part [queue-msgid:%s] using SAR: ttl_segments=%s, segment_sn=%s, msgref=%s',
                        msgid, total_segments, segment_seqnum, msg_ref_num)
                elif UDHI_INDICATOR_SET and not_class2 and message_content and len(message_content) > message_content[0]:
                    try:
                        header = message_content[1:message_content[0] + 1]
                        cursor = 0
                        while cursor < len(header):
                            identifier, length = header[cursor:cursor + UDH_INFORMATION_ELEMENT_HEADER_LENGTH]
                            cursor += UDH_INFORMATION_ELEMENT_HEADER_LENGTH + length
                            if cursor > len(header) or length != UDH_INFORMATION_ELEMENT_LENGTHS.get(identifier, length):
                                raise ValueError('Invalid UDH information element length')
                        udh = UserDataHeader.from_bytes(list(header))
                    except (IndexError, ValueError):
                        udh = None
                    if udh is not None and udh.concat is not None:
                        splitMethod = 'udh'
                        total_segments = udh.concat.cnt
                        segment_seqnum = udh.concat.seq
                        msg_ref_num = udh.concat.ref
                        self.log.debug(
                            'Received SMS-MO part [queue-msgid:%s] using UDH: ttl_segments=%s, segment_sn=%s, msgref=%s',
                            msgid, total_segments, segment_seqnum, msg_ref_num)

                if splitMethod is not None and (total_segments < 1 or not 1 <= segment_seqnum <= total_segments):
                    splitMethod = None

                if splitMethod is None:
                    # It's a simple short message or a part of a concatenated message
                    routing_key = 'deliver.sm.%s' % self.SMPPClientFactory.config.id
                    self.log.debug("Publishing DeliverSmContent[%s] with routing_key[%s]",
                                   msgid, routing_key)
                    yield self.amqpBroker.publish_confirmed(exchange='messaging', routing_key=routing_key,
                                                            content=content, guard=kw.get('guard'))

                    # Get values from data_sm or deliver_sm
                    priority_flag = None
                    if 'priority_flag' in routable.pdu.params:
                        priority_flag = routable.pdu.params['priority_flag']
                    validity_period = None
                    if 'validity_period' in routable.pdu.params:
                        validity_period = routable.pdu.params['validity_period']

                    # Do not log text for privacy reasons
                    # Added in #691
                    if self.config.log_privacy:
                        logged_content = '** %s byte content **' % len(message_content)
                    else:
                        logged_content = '%r' % message_content

                    self.log.info(
                        "SMS-MO [cid:%s] [queue-msgid:%s] [status:%s] [prio:%s] [validity:%s] [from:%s] [to:%s] \
[content:%s]",
                        self.SMPPClientFactory.config.id,
                        msgid,
                        routable.pdu.status,
                        priority_flag,
                        validity_period,
                        routable.pdu.params['source_addr'],
                        routable.pdu.params['destination_addr'],
                        logged_content)
                else:
                    # Long message part received
                    if self.redisClient is None:
                        self.log.critical('Part of long DeliverSm [queue-msgid:%s] cannot be kept without Redis, '
                                          'carrier asked to resend it.', msgid)
                        defer.returnValue(DataHandlerResponse(status=CommandStatus.ESME_RX_T_APPN))
                    else:
                        # source_addr is in the key: for MO the destination is our shared shortcode, so senders
                        # with colliding ref numbers would otherwise interleave into one hash.
                        hashKey = "longDeliverSm:%s:%s:%s:%s" % (
                            self.SMPPClientFactory.config.id,
                            msg_ref_num,
                            routable.pdu.params['destination_addr'],
                            routable.pdu.params['source_addr'])
                        hashValues = {'pdu': routable.pdu,
                                      'total_segments': total_segments,
                                      'msg_ref_num': msg_ref_num,
                                      'segment_seqnum': segment_seqnum}
                        stored = yield self.redisClient.eval(LONG_MO_STORE, keys=[hashKey], args=[
                            segment_seqnum, pickle.dumps(hashValues, self.pickleProtocol),
                            hashlib.sha256(message_content or b"").hexdigest(), LONG_MO_PARTS_TTL_SECONDS,
                            '%s %s %r' % (total_segments, splitMethod, routable.pdu.params.get('data_coding'))])
                        if not stored:
                            self.log.warning("DeliverSmContent[%s] conflicts with a stored part under reused reference "
                                             "%s, carrier asked to resend it.", msgid, msg_ref_num)
                            defer.returnValue(DataHandlerResponse(status=CommandStatus.ESME_RX_T_APPN))
                        self.log.info(
                            "DeliverSmContent[%s] is part of long msg of (%s), will be enqueued after concatenation.",
                            msgid, total_segments)

                        # Flag it as "will_be_concatenated" and publish it to router
                        routing_key = 'deliver.sm.%s' % self.SMPPClientFactory.config.id
                        self.log.debug("Publishing DeliverSmContent[%s](flagged:wbc) with routing_key[%s]",
                                       msgid, routing_key)
                        content.properties['headers']['will_be_concatenated'] = True
                        yield self.amqpBroker.publish_confirmed(exchange='messaging',
                                                                routing_key=routing_key, content=content)

                        for _ in range(LONG_MO_CLAIM_LEASE_SECONDS):
                            if (yield self.concatDeliverSMs(hashKey, splitMethod, total_segments)):
                                break
                            yield task.deferLater(reactor, 1, lambda: None)
                        else:
                            self.log.warning("DeliverSmContent[%s] is part of long msg still being assembled "
                                             "elsewhere, carrier asked to resend it.", msgid)
                            defer.returnValue(DataHandlerResponse(status=CommandStatus.ESME_RX_T_APPN))
            else:
                # This is a DLR !
                # Send DLR to DLRLookup
                yield self.amqpBroker.publish_confirmed(exchange='messaging',
                                                        routing_key='dlr.deliver_sm',
                                                        mandatory=self.config.mandatory_outcomes,
                                                        content=DLR(pdu_type=routable.pdu.id,
                                                                    msgid=self.code_dlr_msgid(routable.pdu),
                                                                    status=routable.pdu.dlr['stat'],
                                                                    cid=self.SMPPClientFactory.config.id,
                                                                    dlr_details=routable.pdu.dlr,
                                                                    dlr_scope=getattr(self.SMPPClientFactory.config,
                                                                                      'dlr_scope', '')))
        except (InterceptorRunError, DeliverSmInterceptionError) as e:
            # Do not log text for privacy reasons
            # Added in #691
            if self.config.log_privacy:
                logged_content = '** %s byte content **' % len(message_content)
            else:
                logged_content = '%r' % message_content

            self.log.info("SMS-MO [cid:%s] [i-status:%s] [from:%s] [to:%s] [content:%s] [tlvs:%s]",
                          self.SMPPClientFactory.config.id,
                          e.status,
                          routable.pdu.params['source_addr'],
                          routable.pdu.params['destination_addr'],
                          logged_content,
                          format_tlvs_for_log(routable.pdu, self.config.log_privacy))

            # Known exception handling
            defer.returnValue(DataHandlerResponse(status=e.status))
        except Exception as e:
            if concatenated:
                raise
            # Unknown exception handling
            self.log.critical('Got an unknown exception (%s): %s', type(e), e)
            defer.returnValue(DataHandlerResponse(status=CommandStatus.ESME_RUNKNOWNERR))
