import json

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.error import AlreadyCalled, AlreadyCancelled
from pika.exceptions import ConsumerCancelled
from txredisapi import ConnectionError
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt

from jasmin.tools.logs import configure_logger
from jasmin.managers.content import DLRContentForHttpapi, DLRContentForSmpps
from jasmin.queues.content import Content
from jasmin.queues.delivery import DeliveryMessage
from jasmin.tools.jitter import backoff
from jasmin.tools.singleton import Singleton
from jasmin.tools import to_enum

LOG_CATEGORY = "dlr"
DLR_LOOKUP_PREFETCH_COUNT = 64
DLR_FORWARD_MAX_ATTEMPTS = 10
PARKED_QUEUE = '%s.parked'
LOOKUP_QUEUE = 'DLRLookup-%s.durable'


def acknowledged_smpp_msgids(message):
    """Every SMSC id the submit was acknowledged with: one per accepted segment for a long message, even one refused
    in part, else the single id; none for a refused submit.

    A receipt may quote any segment of a long message, so each of them has to resolve back to the same message.
    """
    headers = message.content.properties['headers']
    if headers.get('smpp_msgids'):
        return headers['smpp_msgids'].split(',')
    if 'smpp_msgid' in headers:
        return [headers['smpp_msgid']]
    return []


def receipt_identity(message):
    """One receipt across its redeliveries: an SMSC id is unique only per dlr_scope, and one id carries several."""
    headers = message.content.properties['headers']
    return (message.routing_key, headers.get('dlr_scope', ''), message.content.properties['message-id'],
            message.content.body)


def queue_msgid_key(dlr_scope, smpp_msgid):
    # An SMSC message id is unique only per account, and a receipt may arrive on any of that account's binds.
    if dlr_scope:
        return "queue-msgid:%s:%s" % (dlr_scope, smpp_msgid)
    return "queue-msgid:%s" % smpp_msgid


class RedisError(Exception):
    """Raised for any Redis connectivity problem"""


class DLRMapError(Exception):
    """Raised when receiving an invalid dlr content from Redis"""


class DLRMapNotFound(Exception):
    """Raised if no dlr is found in Redis db"""


class ForwardError(Exception):
    """Raised when the broker does not confirm the outcome forward (Nack / unroutable / channel closed)."""


class DLRLookup:
    """
    Will consume dlr pdus (submit_sm, deliver_sm or data_sm), lookup for matching dlr maps in redis db
    and publish dlr for later throwing (http or smpp)
    """

    def __init__(self, config, amqpBroker, redisClient):
        self.pid = config.pid
        self.q = None
        self.chan = None  # this consumer's own channel (per-consumer channel isolates delivery tags)
        self.config = config
        self.amqpBroker = amqpBroker
        self.redisClient = redisClient
        self.requeue_timers = {}
        self.lookup_retrials = {}
        self.forward_failures = {}

        # Set up a dedicated logger
        self.log = configure_logger(LOG_CATEGORY, self.config)

        self.log.info('Started %s #%s.', self.__class__.__name__, self.pid)

    @defer.inlineCallbacks
    def subscribe(self):
        """Subscribe to dlr.* queues"""

        consumerTag = 'DLRLookup-%s' % self.pid
        queueName = LOOKUP_QUEUE % self.pid
        routing_key = 'dlr.*'
        self.chan = yield self.amqpBroker.newChannel()
        yield self.chan.exchange_declare(exchange='messaging', exchange_type='topic')
        yield self.amqpBroker.named_queue_declare(queue=queueName, durable=True)
        yield self.chan.queue_bind(queue=queueName, exchange="messaging", routing_key=routing_key)
        yield self.chan.basic_qos(prefetch_count=DLR_LOOKUP_PREFETCH_COUNT)
        self.q, _consumer_tag = yield self.chan.basic_consume(
            queue=queueName, auto_ack=False, consumer_tag=consumerTag)
        self.setup_callbacks(self.q)

        # Declare the forward queue durably so an outcome published before any consumer binds is buffered, not dropped.
        if self.config.dlr_forward_queue:
            yield self.amqpBroker.named_queue_declare(
                queue=self.config.dlr_forward_queue, durable=True)
            yield self.amqpBroker.named_queue_declare(
                queue=PARKED_QUEUE % self.config.dlr_forward_queue, durable=True)

        # Re-run on every later connection: this channel and its consumer die with the connection, and a broker
        # restart takes the queue too, so receipts would pile up with nothing looking them up.
        self.amqpBroker.addChannelReadyCallback(self.subscribe)

    def clearRequeueTimer(self, message):
        timer = self.requeue_timers.pop(message, None)
        if timer is not None:
            try:
                timer.cancel()
            except (AlreadyCalled, AlreadyCancelled):
                pass

    def clearRequeueTimers(self):
        for message in list(self.requeue_timers):
            self.clearRequeueTimer(message)

    @defer.inlineCallbacks
    def rejectAndRequeueMessage(self, message, delay=True):
        msgid = message.content.properties['message-id']

        if delay:
            requeue_delay = self.config.dlr_lookup_retry_delay if delay is True else delay
            self.log.debug("Requeuing Content[%s] with delay: %s seconds", msgid, requeue_delay)

            # Keyed by delivery: two deliveries sharing a message-id each need their own settlement.
            self.clearRequeueTimer(message)
            timer = reactor.callLater(requeue_delay,
                                      self.rejectMessage,
                                      message=message,
                                      requeue=1)
            self.requeue_timers[message] = timer

            defer.returnValue(timer)
        else:
            self.log.debug("Requeuing Content[%s] without delay", msgid)
            yield self.rejectMessage(message, requeue=1)

    def rejectMessage(self, message, requeue=0):
        msgid = message.content.properties['message-id']
        if requeue == 0 and msgid in self.lookup_retrials:
            # Remove retrial tracker
            del self.lookup_retrials[msgid]
        if requeue == 0:
            self.forward_failures.pop(receipt_identity(message), None)

        self.clearRequeueTimer(message)
        if not self.amqpBroker.connected:
            self.log.error("Cannot reject message, AMQP Broker is not connected !")
            return

        # Reject on the channel that delivered the message. If that channel has since closed, the broker
        # has already requeued the delivery, so there is nothing left to do.
        try:
            message.channel.basic_reject(delivery_tag=message.delivery_tag, requeue=requeue)
        except Exception as e:
            self.log.warning("Could not reject Content[%s] (delivering channel likely closed): %s", msgid, e)

    def ackMessage(self, message):
        msgid = message.content.properties['message-id']
        # Remove retrial tracker
        if msgid in self.lookup_retrials:
            del self.lookup_retrials[msgid]
        self.forward_failures.pop(receipt_identity(message), None)

        self.clearRequeueTimer(message)
        if not self.amqpBroker.connected:
            self.log.error("Cannot ack message, AMQP Broker is not connected !")
            return

        try:
            message.channel.basic_ack(delivery_tag=message.delivery_tag)
        except Exception as e:
            self.log.warning("Could not ack Content[%s] (delivering channel likely closed): %s", msgid, e)

    def setup_callbacks(self, q):
        if self.q is None:
            self.q = q
            self.log.info('DLRLookup (%s) is ready.', self.pid)

        q.get().addCallback(self._on_message).addErrback(self.dlr_errback)

    def _on_message(self, received):
        # Wrap pika's ReceivedMessage in the txamqp-style shim the dispatcher below expects.
        return self.dlr_callback_dispatcher(DeliveryMessage(received))

    @defer.inlineCallbacks
    def dlr_callback_dispatcher(self, message):
        # Again ...
        self.setup_callbacks(self.q)

        # retrial tracking
        if message.content.properties['message-id'] in self.lookup_retrials:
            self.lookup_retrials[message.content.properties['message-id']] += 1
        else:
            self.lookup_retrials[message.content.properties['message-id']] = 1

        # Dispatching
        try:
            if message.routing_key == 'dlr.submit_sm_resp':
                yield self.submit_sm_resp_dlr_callback(message)
            elif message.routing_key == 'dlr.deliver_sm':
                yield self.deliver_sm_dlr_callback(message)
            elif message.routing_key == 'dlr.discard':
                yield self.discard_dlr_callback(message)
            else:
                self.log.error('Unknown routing_key in dlr_callback_dispatcher: %s', message.routing_key)
                yield self.rejectMessage(message)
        except Exception as e:
            # An unsettled delivery holds a prefetch slot for good, so whatever escaped is retried later.
            self.log.error('[msgid:%s] Unsettled after (%s), requeuing: %s',
                           message.content.properties['message-id'], type(e), e)
            yield self.rejectAndRequeueMessage(message)

    def dlr_errback(self, error):
        # ConsumerCancelled fires when the consumer's queue is closed/cancelled (expected on teardown);
        # anything else is a real error inside dlr_callback_dispatcher.
        if error.check(ConsumerCancelled) is None:
            self.log.error("Error in dlr_callback_dispatcher: %s", error)

    @defer.inlineCallbacks
    def forward_outcome(self, msgid, level, message_state='', command_status='', err='',
                        id_smsc='', sub='', dlvrd='', submit_date='', done_date='',
                        connector='', will_be_retried=False, discard_reason=None, accepted_segments=None,
                        delivery=None):
        if not self.config.dlr_forward_queue:
            return

        queue = self.config.dlr_forward_queue
        failures = 0 if delivery is None else self.forward_failures.get(receipt_identity(delivery), 0)
        if failures >= DLR_FORWARD_MAX_ATTEMPTS:
            queue = PARKED_QUEUE % queue
            self.log.critical('Outcome of [msgid:%s] parked on %s after %s unconfirmed forwards',
                              msgid, queue, DLR_FORWARD_MAX_ATTEMPTS)

        payload = {
            'msgid': msgid,
            'level': level,
            'message_state': message_state,
            'command_status': command_status,
            'err': err,
            'id_smsc': id_smsc,
            'sub': sub,
            'dlvrd': dlvrd,
            'submit_date': submit_date,
            'done_date': done_date,
            'connector': connector,
            'will_be_retried': will_be_retried,
            'discard_reason': discard_reason,
            'accepted_segments': accepted_segments,
        }
        # No message-id: one msgid spans several receipts, so it does not identify the AMQP envelope.
        content = Content(
            json.dumps(payload),
            properties={'content-type': 'application/json', 'delivery-mode': 2})
        try:
            # mandatory + confirms: a Nack or an unroutable target errbacks, and any failure here means the
            # receipt was not durably taken, so surface it for the caller to requeue instead of acking a loss.
            yield self.amqpBroker.publish_confirmed(exchange='', routing_key=queue, content=content)
        except Exception as e:
            raise ForwardError(str(e))

    def requeueForward(self, message):
        identity = receipt_identity(message)
        failures = self.forward_failures.get(identity, 0)
        self.forward_failures[identity] = failures + 1
        return self.rejectAndRequeueMessage(message, delay=backoff(self.config.dlr_lookup_retry_delay, failures))

    @defer.inlineCallbacks
    def retryOrParkLookup(self, message, error):
        msgid = message.content.properties['message-id']
        if msgid in self.lookup_retrials and self.lookup_retrials[msgid] < self.config.dlr_lookup_max_retries:
            self.log.error('[msgid:%s] (retrials: %s/%s) RedisError: %s', msgid, self.lookup_retrials[msgid],
                           self.config.dlr_lookup_max_retries, error)
            yield self.rejectAndRequeueMessage(message)
            return
        parked = PARKED_QUEUE % message.routing_key
        yield self.amqpBroker.named_queue_declare(queue=parked, durable=True)
        yield self.amqpBroker.publish_confirmed(exchange='', routing_key=parked, content=Content(
            message.content.body, properties=dict(message.content.properties, **{'delivery-mode': 2})))
        self.log.critical('[msgid:%s] parked on %s after its lookups failed: %s', msgid, parked, error)
        yield self.ackMessage(message)

    @defer.inlineCallbacks
    def discard_dlr_callback(self, message):
        msgid = message.content.properties['message-id']
        reason = message.content.properties['headers']['discard_reason']
        connector = message.content.properties['headers'].get('connector', '')
        try:
            yield self.forward_outcome(msgid, level=1, command_status='DISCARDED', discard_reason=reason,
                                       connector=connector, delivery=message)
        except ForwardError as e:
            self.log.error('Forward of discard[%s] failed, requeuing: %s', msgid, e)
            yield self.requeueForward(message)
            return
        yield self.ackMessage(message)

    @defer.inlineCallbacks
    def submit_sm_resp_dlr_callback(self, message):
        msgid = message.content.properties['message-id']
        dlr_status = message.content.body

        if isinstance(dlr_status, bytes):
            dlr_status = dlr_status.decode()

        try:
            if self.redisClient is None:
                raise RedisError('RC undefined !')

            # Check for DLR request from redis 'dlr' key
            # If there's a pending delivery receipt request then serve it
            # back by publishing a DLRContentForHttpapi to the messaging exchange
            dlr = yield self.redisClient.hgetall("dlr:%s" % msgid)

            if dlr is None or len(dlr) == 0:
                raise DLRMapNotFound('No dlr map for msgid[%s]' % msgid)
            if 'sc' not in dlr or dlr['sc'] not in ['httpapi', 'smppsapi']:
                raise DLRMapError('Fetched unknown dlr: %s' % dlr)

            if dlr['sc'] == 'httpapi':
                self.log.debug('There is a HTTP DLR request for msgid[%s] ...', msgid)
                dlr_url = dlr['url']
                dlr_level = dlr['level']
                dlr_method = dlr['method']
                dlr_expiry = dlr['expiry']
                dlr_connector = dlr.get('connector', 'unknown')

                # Mapped first: a terminal receipt can arrive while the outcome forward below is still failing.
                if dlr_level in [2, 3]:
                    hashValues = {'msgid': msgid, 'connector_type': 'httpapi'}
                    for smpp_msgid in acknowledged_smpp_msgids(message):
                        # Map received submit_sm_resp's message_id to the msg for later receipt handling
                        self.log.debug('Mapping smpp msgid: %s to queue msgid: %s, expiring in %s',
                                       smpp_msgid, msgid, dlr_expiry)
                        hashKey = queue_msgid_key(message.content.properties['headers'].get('dlr_scope', ''), smpp_msgid)
                        yield self.redisClient.hmset(hashKey, hashValues)
                        yield self.redisClient.expire(hashKey, dlr_expiry)

                if dlr['level'] in [1, 3]:
                    self.log.debug('Got DLR information for msgid[%s], url:%s, level:%s, connector:%s',
                                   msgid, dlr_url, dlr_level, dlr_connector)

                    # id_smsc (the SMSC id) is present only on ESME_ROK. Forwarded before the HTTP throw, whose
                    # publish is not retried with it.
                    yield self.forward_outcome(
                        msgid, level=1,
                        command_status=dlr_status,
                        id_smsc=message.content.properties['headers'].get('smpp_msgid', ''),
                        connector=dlr_connector,
                        accepted_segments=message.content.properties['headers'].get('accepted_segments'),
                        discard_reason=message.content.properties['headers'].get('discard_reason'),
                        delivery=message)

                    # An empty url means the receipt was requested for AMQP forwarding only, so skip the HTTP throw.
                    # The dlr_url in DLRContentForHttpapi indicates the level of the actual delivery receipt (1) and
                    # not the requested one (maybe 1 or 3).
                    if dlr_url:
                        self.log.debug("Publishing DLRContentForHttpapi[%s] with routing_key[%s]",
                                       msgid, 'dlr_thrower.http')
                        yield self.amqpBroker.publish(exchange='messaging',
                                                      routing_key='dlr_thrower.http',
                                                      content=DLRContentForHttpapi(dlr_status,
                                                                                   msgid, dlr_url,
                                                                                   dlr_level=1,
                                                                                   dlr_connector=dlr_connector,
                                                                                   method=dlr_method))

                    # DLR request is removed if:
                    # - If level 1 is requested (SMSC level only)
                    # - SubmitSmResp returned an error (no more delivery will be tracked)
                    #
                    # When level 3 is requested, the DLR will be removed when
                    # receiving a deliver_sm (terminal receipt)
                    if dlr_level == 1 or not acknowledged_smpp_msgids(message):
                        self.log.debug('Removing DLR request for msgid[%s]', msgid)
                        yield self.redisClient.delete("dlr:%s" % msgid)
                else:
                    self.log.debug(
                        'Terminal level receipt is requested, will not send any DLR receipt at this level.')

            elif dlr['sc'] == 'smppsapi':
                self.log.debug('There is a SMPPs mapping for msgid[%s] ...', msgid)
                system_id = dlr['system_id']
                source_addr_ton = to_enum(dlr['source_addr_ton'])
                source_addr_npi = to_enum(dlr['source_addr_npi'])
                source_addr = dlr['source_addr']
                dest_addr_ton = to_enum(dlr['dest_addr_ton'])
                dest_addr_npi = to_enum(dlr['dest_addr_npi'])
                destination_addr = dlr['destination_addr']
                sub_date = dlr['sub_date']
                registered_delivery_receipt = to_enum(dlr['rd_receipt'])
                smpps_map_expiry = dlr['expiry']

                if isinstance(source_addr, int):
                    source_addr = str(source_addr)

                if isinstance(destination_addr, int):
                    destination_addr = str(destination_addr)

                hashValues = {'msgid': msgid, 'connector_type': 'smppsapi'}
                for smpp_msgid in acknowledged_smpp_msgids(message):
                    # Map received submit_sm_resp's message_id to the msg for later rceipt handling
                    self.log.debug('Mapping smpp msgid: %s to queue msgid: %s, expiring in %s',
                                   smpp_msgid, msgid, smpps_map_expiry)
                    hashKey = queue_msgid_key(message.content.properties['headers'].get('dlr_scope', ''), smpp_msgid)
                    yield self.redisClient.hmset(hashKey, hashValues)
                    yield self.redisClient.expire(hashKey, smpps_map_expiry)

                # Do we need to forward the receipt to the original sender ?
                if ((dlr_status == 'ESME_ROK' and registered_delivery_receipt in
                    [RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE, RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED]) or
                        (dlr_status != 'ESME_ROK' and
                                 registered_delivery_receipt == RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE)):
                    self.log.debug('Got DLR information for msgid[%s], registered_deliver%s, system_id:%s',
                                   msgid, registered_delivery_receipt, system_id)

                    if (dlr_status != 'ESME_ROK' or (dlr_status == 'ESME_ROK' and
                                                         self.config.smpp_receipt_on_success_submit_sm_resp)):
                        # Send back a receipt (by throwing deliver_sm or data_sm)
                        self.log.debug("Publishing DLRContentForSmpps[%s] with routing_key[%s]",
                                       msgid, 'dlr_thrower.smpps')
                        yield self.amqpBroker.publish(exchange='messaging',
                                                      routing_key='dlr_thrower.smpps',
                                                      content=DLRContentForSmpps(dlr_status, msgid, system_id,
                                                                                 source_addr,
                                                                                 destination_addr, sub_date,
                                                                                 source_addr_ton,
                                                                                 source_addr_npi,
                                                                                 dest_addr_ton,
                                                                                 dest_addr_npi))

        except DLRMapError as e:
            self.log.error('[msgid:%s] DLR Content: %s', msgid, e)
            yield self.rejectMessage(message)
        except (RedisError, ConnectionError) as e:
            yield self.retryOrParkLookup(message, e)
        except DLRMapNotFound as e:
            self.log.debug('[msgid:%s] DLRMapNotFound: %s', msgid, e)
            yield self.rejectMessage(message)
        except ForwardError as e:
            # A delivery outcome must never be dropped: requeue with delay until the broker confirms the forward.
            self.log.error('[msgid:%s] ForwardError (requeuing): %s', msgid, e)
            yield self.requeueForward(message)
        except Exception as e:
            self.log.error('[msgid:%s] Unknown error (%s): %s', msgid, type(e), e)
            yield self.rejectMessage(message)
        else:
            yield self.ackMessage(message)

    @defer.inlineCallbacks
    def deliver_sm_dlr_callback(self, message):
        msgid = message.content.properties['message-id']
        pdu_cid = message.content.properties['headers']['cid']
        pdu_dlr_id = message.content.properties['headers']['dlr_id']
        pdu_dlr_ddate = message.content.properties['headers']['dlr_ddate']
        pdu_dlr_sdate = message.content.properties['headers']['dlr_sdate']
        pdu_dlr_sub = message.content.properties['headers']['dlr_sub']
        pdu_dlr_err = message.content.properties['headers']['dlr_err']
        pdu_dlr_text = message.content.properties['headers']['dlr_text']
        pdu_dlr_dlvrd = message.content.properties['headers']['dlr_dlvrd']
        pdu_dlr_status = message.content.body

        if isinstance(pdu_dlr_status, bytes):
            pdu_dlr_status = pdu_dlr_status.decode()

        try:
            if self.redisClient is None:
                raise RedisError('RC undefined !')

            q = yield self.redisClient.hgetall(
                queue_msgid_key(message.content.properties['headers'].get('dlr_scope', ''), msgid))
            if len(q) != 2 or 'msgid' not in q or 'connector_type' not in q:
                raise DLRMapNotFound('Got a DLR for an unknown message id: %s (coded:%s)' % (pdu_dlr_id, msgid))

            submit_sm_queue_id = q['msgid']
            connector_type = q['connector_type']

            # Get dlr and ensure it's sc (source_connector) is same as q['connector_type']
            dlr = yield self.redisClient.hgetall("dlr:%s" % submit_sm_queue_id)
            if dlr is None or len(dlr) == 0:
                raise DLRMapNotFound('Got a DLR for an unknown message id: %s (coded:%s)' % (pdu_dlr_id, msgid))
            if len(dlr) > 0 and dlr['sc'] != connector_type:
                raise DLRMapError('Found a dlr for msgid:%s with diffrent sc: %s' % (submit_sm_queue_id, dlr['sc']))
            
            success_states = ['ACCEPTD', 'DELIVRD']
            final_states = ['DELIVRD', 'EXPIRED', 'DELETED', 'UNDELIV', 'REJECTD']
            
            if connector_type == 'httpapi':
                self.log.debug('There is a HTTP DLR request for msgid[%s] ...', msgid)
                dlr_url = dlr['url']
                dlr_level = dlr['level']
                dlr_method = dlr['method']

                if dlr_level in [2, 3]:
                    self.log.debug('Got DLR information for msgid[%s], url:%s, level:%s',
                                   submit_sm_queue_id, dlr_url, dlr_level)
                    yield self.forward_outcome(
                        submit_sm_queue_id, level=2,
                        message_state=pdu_dlr_status,
                        err=pdu_dlr_err,
                        id_smsc=msgid,
                        sub=pdu_dlr_sub,
                        dlvrd=pdu_dlr_dlvrd,
                        submit_date=pdu_dlr_sdate,
                        done_date=pdu_dlr_ddate,
                        connector=pdu_cid,
                        delivery=message)

                    # An empty url means the receipt was requested for AMQP forwarding only, so skip the HTTP throw.
                    # The dlr_url in DLRContentForHttpapi indicates the level of the actual delivery receipt (2) and
                    # not the requested one (maybe 2 or 3).
                    if dlr_url:
                        self.log.debug("Publishing DLRContentForHttpapi[%s] with routing_key[%s]",
                                       submit_sm_queue_id, 'dlr_thrower.http')
                        yield self.amqpBroker.publish(exchange='messaging',
                                                      routing_key='dlr_thrower.http',
                                                      content=DLRContentForHttpapi(pdu_dlr_status,
                                                                                   submit_sm_queue_id,
                                                                                   dlr_url, dlr_level=2,
                                                                                   dlr_connector=pdu_cid,
                                                                                   id_smsc=msgid,
                                                                                   sub=pdu_dlr_sub,
                                                                                   dlvrd=pdu_dlr_dlvrd,
                                                                                   subdate=pdu_dlr_sdate,
                                                                                   donedate=pdu_dlr_ddate,
                                                                                   err=pdu_dlr_err,
                                                                                   text=pdu_dlr_text,
                                                                                   method=dlr_method))
            elif connector_type == 'smppsapi':
                self.log.debug('There is a SMPPs mapping for msgid[%s] ...', msgid)
                system_id = dlr['system_id']
                source_addr_ton = to_enum(dlr['source_addr_ton'])
                source_addr_npi = to_enum(dlr['source_addr_npi'])
                source_addr = dlr['source_addr']
                dest_addr_ton = to_enum(dlr['dest_addr_ton'])
                dest_addr_npi = to_enum(dlr['dest_addr_npi'])
                destination_addr = dlr['destination_addr']
                sub_date = dlr['sub_date']
                registered_delivery_receipt = to_enum(dlr['rd_receipt'])

                if isinstance(source_addr, int):
                    source_addr = str(source_addr)

                if isinstance(destination_addr, int):
                    destination_addr = str(destination_addr)

                # Do we need to forward the receipt to the original sender ?
                if ((pdu_dlr_status in success_states and
                             registered_delivery_receipt == RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED) or
                        (pdu_dlr_status not in success_states and
                                 registered_delivery_receipt in [RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED,
                                                                 RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE])):
                    self.log.debug(
                        'Got DLR information for msgid[%s], registered_deliver%s, system_id:%s',
                        submit_sm_queue_id, registered_delivery_receipt, system_id)

                    self.log.debug("Publishing DLRContentForSmpps[%s] with routing_key[%s]",
                                   submit_sm_queue_id, 'dlr_thrower.smpps')
                    yield self.amqpBroker.publish(exchange='messaging',
                                                  routing_key='dlr_thrower.smpps',
                                                  content=DLRContentForSmpps(pdu_dlr_status,
                                                                             submit_sm_queue_id, system_id,
                                                                             source_addr, destination_addr, sub_date,
                                                                             source_addr_ton, source_addr_npi,
                                                                             dest_addr_ton, dest_addr_npi,
                                                                             err=pdu_dlr_err))

                    if pdu_dlr_status in final_states:
                        self.log.debug('Removing SMPPs dlr map for msgid[%s]', submit_sm_queue_id)
                        yield self.redisClient.delete('dlr:%s' % submit_sm_queue_id)
        except DLRMapError as e:
            self.log.error('[msgid:%s] DLRMapError: %s', msgid, e)
            yield self.rejectMessage(message)
        except (RedisError, ConnectionError) as e:
            yield self.retryOrParkLookup(message, e)
        except DLRMapNotFound as e:
            if msgid in self.lookup_retrials and self.lookup_retrials[msgid] < self.config.dlr_lookup_max_retries:
                self.log.error('[msgid:%s] (retrials: %s/%s) DLRMapNotFound: %s', msgid, self.lookup_retrials[msgid],
                               self.config.dlr_lookup_max_retries, e)
                yield self.rejectAndRequeueMessage(message)
            else:
                self.log.error('[msgid:%s] (final) DLRMapNotFound: %s', msgid, e)
                yield self.rejectMessage(message)
        except ForwardError as e:
            # A delivery outcome must never be dropped: requeue with delay until the broker confirms the forward.
            self.log.error('[msgid:%s] ForwardError (requeuing): %s', msgid, e)
            yield self.requeueForward(message)
        except Exception as e:
            self.log.error('[msgid:%s] Unknown error (%s): %s', msgid, type(e), e)
            yield self.rejectMessage(message)
        else:
            yield self.ackMessage(message)

            # Do not log text for privacy reasons
            # Added in #691
            if self.config.log_privacy:
                logged_content = '** %s byte content **' % len(pdu_dlr_text)
            else:
                logged_content = '%r' % pdu_dlr_text

            self.log.info(
                "DLR [cid:%s] [smpp-msgid:%s] [status:%s] [submit date:%s] [done date:%s] [sub/dlvrd messages:%s/%s] \
[err:%s] [content:%s]",
                pdu_cid,
                msgid,
                pdu_dlr_status,
                pdu_dlr_sdate,
                pdu_dlr_ddate,
                pdu_dlr_sub,
                pdu_dlr_dlvrd,
                pdu_dlr_err,
                logged_content,
                extra={'cid': pdu_cid})


class DLRLookupSingleton(metaclass=Singleton):
    """Used to launch only one DLRLookup object"""
    objects = {}

    def get(self, config, amqpBroker, redisClient):
        """Return a DLRLookup object or instanciate a new one"""
        name = 'singleton'
        if name not in self.objects:
            self.objects[name] = DLRLookup(config, amqpBroker, redisClient)

        return self.objects[name]
