import pickle
import datetime
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.internet import defer
from twisted.spread import pb

import jasmin
from jasmin.protocols.smpp.protocol import SMPPServerProtocol
from jasmin.protocols.smpp.services import SMPPClientService
from jasmin.tools.migrations.configuration import ConfigurationMigrator
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt
from smpp.twisted.protocol import SMPPSessionStates
from jasmin.queues.delivery import DeliveryMessage
from .configs import SMPPClientSMListenerConfig
from .content import SubmitSmContent
from .listeners import SMPPClientSMListener

LOG_CATEGORY = "jasmin-pb-client-mgmt"


class ConfigProfileLoadingError(Exception):
    """
    Raised for any error occurring while loading a configuration
    profile with perspective_load
    """


class SMPPClientManagerPB(pb.Avatar):
    def __init__(self, SMPPClientPBConfig):
        self.config = SMPPClientPBConfig
        self.avatar = None
        self.redisClient = None
        self.amqpBroker = None
        self.interceptorpb_client = None
        self.RouterPB = None
        self.connectors = []
        self.declared_queues = []
        self.pickleProtocol = pickle.HIGHEST_PROTOCOL

        # Persistence flag, accessed through perspective_is_persisted
        self.persisted = True

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file,
                                                   when=self.config.log_rotate)
            formatter = logging.Formatter(self.config.log_format,
                                          self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

        # Set pickleProtocol
        self.pickleProtocol = self.config.pickle_protocol

        self.log.info('SMPP Client manager configured and ready.')

    def setAvatar(self, avatar):
        if type(avatar) is str:
            self.log.info('Authenticated Avatar: %s', avatar)
        else:
            self.log.info('Anonymous connection')

        self.avatar = avatar

    def addAmqpBroker(self, amqpBroker):
        self.amqpBroker = amqpBroker
        self.amqpBroker.addChannelReadyCallback(self.reconsumeRunningConnectors)

        self.log.info('Added amqpBroker to SMPPClientManagerPB')

    @defer.inlineCallbacks
    def reconsumeRunningConnectors(self):
        """A connector's channel dies with the AMQP connection and a broker restart takes the topology with it,
        so a reconnect must redeclare and reconsume or the queue fills with nothing draining it."""
        for connector in self.connectors:
            if connector['service'].running != 1:
                continue

            try:
                connector['chan'] = yield self.setupConnectorChannel(connector['id'])
                connector['consumer_tag'] = None
                yield self.consumeSubmitSmQueue(connector)
            except Exception as e:
                # Swallowed so one connector cannot strand the others still to be resumed.
                self.log.error('Could not resume consuming for connector [%s]: %s', connector['id'], e)

    def addRedisClient(self, redisClient):
        self.redisClient = redisClient

        self.log.info('Added Redis Client to SMPPClientManagerPB')

    def addInterceptorPBClient(self, interceptorpb_client):
        self.interceptorpb_client = interceptorpb_client

        self.log.info('Added interceptorpb_client to SMPPClientManagerPB')

    def addRouterPB(self, RouterPB):
        self.RouterPB = RouterPB

        self.log.info('Added RouterPB to SMPPClientManagerPB')

    def getConnector(self, cid):
        for c in self.connectors:
            if str(c['id']) == str(cid):
                self.log.debug('getConnector [%s] returned a connector', cid)
                return c

        self.log.debug('getConnector [%s] returned None', cid)
        return None

    def getConnectorDetails(self, cid):
        c = self.getConnector(cid)
        if c is None:
            self.log.debug('getConnectorDetails [%s] returned None', cid)
            return None

        details = {}
        details['id'] = c['id']
        details['session_state'] = c['service'].SMPPClientFactory.getSessionState().name
        details['service_status'] = c['service'].running
        details['start_count'] = c['service'].startCounter
        details['stop_count'] = c['service'].stopCounter

        self.log.debug('getConnectorDetails [%s] returned details', cid)
        return details

    def delConnector(self, cid):
        for i in range(len(self.connectors)):
            if str(self.connectors[i]['id']) == str(cid):
                del self.connectors[i]
                self.log.debug('Deleted connector [%s].', cid)
                return True

        self.log.debug('Deleting connector [%s] failed.', cid)
        return False

    def perspective_version_release(self):
        return jasmin.get_release()

    def perspective_version(self):
        return jasmin.get_version()

    def perspective_persist(self, profile='jcli-prod'):
        path = '%s/%s.smppccs' % (self.config.store_path, profile)
        self.log.info('Persisting current configuration to [%s] profile in %s', profile, path)

        try:
            # Prepare connectors for persistence
            # Will persist config and service status only
            connectors = []
            for c in self.connectors:
                connectors.append({
                    'id': c['id'],
                    'config': c['config'],
                    'service_status': c['service'].running})

            # Write configuration with datetime stamp
            fh = open(path, 'wb')
            fh.write(('Persisted on %s [Jasmin %s]\n' % (time.strftime("%c"), jasmin.get_release())).encode('ascii'))
            fh.write(pickle.dumps(connectors, self.pickleProtocol))
            fh.close()

            # Set persistance state to True
            self.persisted = True
        except IOError:
            self.log.error('Cannot persist to %s', path)
            return False
        except Exception as e:
            self.log.error('Unknown error occurred while persisting configuration: %s', e)
            return False

        return True

    @defer.inlineCallbacks
    def perspective_load(self, profile='jcli-prod'):
        path = '%s/%s.smppccs' % (self.config.store_path, profile)
        self.log.info('Loading/Activating [%s] profile configuration from %s', profile, path)

        try:
            # Load configuration from file
            fh = open(path, 'rb')
            lines = fh.readlines()
            fh.close()

            # Init migrator
            cf = ConfigurationMigrator(context='smppccs', header=lines[0].decode('ascii'), data=b''.join(lines[1:]))

            # Remove current configuration
            CIDs = []
            for c in self.connectors:
                CIDs.append(c['id'])
            for cid in CIDs:
                remRet = yield self.perspective_connector_remove(cid)
                if not remRet:
                    raise ConfigProfileLoadingError('Error removing connector %s' % cid)
                self.log.info('Removed connector [%s]', cid)

            # Apply configuration
            loadedConnectors = cf.getMigratedData()
            for loadedConnector in loadedConnectors:
                # Add connector
                addRet = yield self.perspective_connector_add(
                    pickle.dumps(loadedConnector['config'],
                                 self.pickleProtocol))
                if not addRet:
                    raise ConfigProfileLoadingError('Error adding connector %s' % loadedConnector['id'])

                # Start it if it's service where started when persisted
                if loadedConnector['service_status'] == 1:
                    startRet = yield self.perspective_connector_start(loadedConnector['id'])
                    if not startRet:
                        self.log.error('Error starting connector %s', loadedConnector['id'])

            # Set persistance state to True
            self.persisted = True
        except IOError as e:
            self.log.error('Cannot load configuration from %s: %s', path, str(e))
            defer.returnValue(False)
        except ConfigProfileLoadingError as e:
            self.log.error('Error while loading configuration: %s', e)
            defer.returnValue(False)
        except Exception as e:
            self.log.error('Unknown error occurred while loading configuration: %s', e)
            defer.returnValue(False)

        defer.returnValue(True)

    def perspective_is_persisted(self):
        return self.persisted

    @defer.inlineCallbacks
    def perspective_connector_add(self, ClientConfig):
        """This will add a new connector to self.connectors
        and get a listener on submit.sm.%cid queue, this listener will be
        started and stopped when the connector will get started and stopped
        through this API"""

        c = pickle.loads(ClientConfig)

        self.log.debug('Adding a new connector %s', c.id)

        if self.getConnector(c.id) is not None:
            self.log.error('Trying to add a new connector with an already existant cid: %s', c.id)
            defer.returnValue(False)
        if self.amqpBroker is None:
            self.log.error('AMQP Broker is not added')
            defer.returnValue(False)
        if self.amqpBroker.connected == False:
            self.log.error('AMQP Broker channel is not yet ready')
            defer.returnValue(False)

        chan = yield self.setupConnectorChannel(c.id)

        # Instanciate smpp client service manager
        serviceManager = SMPPClientService(c, self.config)

        # Instanciate a SM listener
        smListener = SMPPClientSMListener(
            config=SMPPClientSMListenerConfig(self.config.config_file),
            SMPPClientFactory=serviceManager.SMPPClientFactory,
            amqpBroker=self.amqpBroker,
            redisClient=self.redisClient,
            RouterPB=self.RouterPB,
            interceptorpb_client=self.interceptorpb_client)

        # Deliver_sm are sent to smListener's deliver_sm callback method
        serviceManager.SMPPClientFactory.msgHandler = smListener.deliver_sm_event_interceptor

        self.connectors.append({
            'id': c.id,
            'config': c,
            'service': serviceManager,
            'chan': chan,
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': smListener})

        self.log.info('Added a new connector: %s', c.id)

        # Set persistance state to False (pending for persistance)
        self.persisted = False

        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_remove(self, cid):
        """This will stop and remove a connector from self.connectors"""

        self.log.debug('Removing connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to remove a connector with an unknown cid: %s', cid)
            defer.returnValue(False)
        if connector['service'].running == 1:
            self.log.debug('Stopping service for connector [%s] before removing it', cid)
            connector['service'].stopService()

        # Stop the queue consumer
        self.log.debug('Stopping submit_sm_q consumer in connector [%s]', cid)
        yield self.perspective_connector_stop(cid)

        if self.delConnector(cid):
            self.log.info('Removed connector [%s]', cid)
            # Set persistance state to False (pending for persistance)
            self.persisted = False
            defer.returnValue(True)
        else:
            self.log.error('Error removing connector [%s], cid not found', cid)
            defer.returnValue(False)

        # Set persistance state to False (pending for persistance)
        self.persisted = False
        defer.returnValue(True)

    def perspective_connector_list(self):
        """This will return only connector IDs since returning an already copyed SMPPClientConfig
        would be a headache"""

        self.log.debug('Connector list requested, returning %s', self.connectors)

        connectorList = []
        for connector in self.connectors:
            c = self.getConnectorDetails(connector['id'])

            connectorList.append(c)

        self.log.info('Returning a list of %s connectors', len(connectorList))
        return connectorList

    @defer.inlineCallbacks
    def perspective_connector_start(self, cid):
        """This will start a service by adding IService to IServiceCollection
        """

        self.log.debug('Starting connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to start a connector with an unknown cid: %s', cid)
            defer.returnValue(False)
        if self.amqpBroker is None:
            self.log.error('AMQP Broker is not added')
            defer.returnValue(False)
        if self.amqpBroker.connected == False:
            self.log.error('AMQP Broker channel is not yet ready')
            defer.returnValue(False)
        if connector['service'].running == 1:
            self.log.error('Connector [%s] is already running.', cid)
            defer.returnValue(False)
        acceptedStartStates = [None, SMPPSessionStates.NONE, SMPPSessionStates.UNBOUND]
        if connector['service'].SMPPClientFactory.getSessionState() not in acceptedStartStates:
            self.log.error(
                'Connector [%s] cannot be started when in session_state: %s',
                cid,
                connector['service'].SMPPClientFactory.getSessionState())
            defer.returnValue(False)

        connector['service'].startService()

        if not (yield self.consumeSubmitSmQueue(connector)):
            defer.returnValue(False)

        self.log.info('Started connector [%s]', cid)

        # Set persistance state to False (pending for persistance)
        self.persisted = False

        defer.returnValue(True)

    @defer.inlineCallbacks
    def setupConnectorChannel(self, cid):
        """The connector's own channel isolates its delivery-tag space, so acks/rejects go back on the channel
        that delivered the message. Re-run on every connection: a broker restart loses exchange and queue."""
        chan = yield self.amqpBroker.newChannel()

        # Fix prefetch limit per consumer to 1 to get correct throttling
        yield chan.basic_qos(prefetch_count=1)

        # Declare queues
        # First declare the messaging exchange (has no effect if its already declared)
        yield chan.exchange_declare(exchange='messaging', exchange_type='topic')
        # submit.sm queue declaration and binding
        submit_sm_queue = 'submit.sm.%s' % cid
        routing_key = 'submit.sm.%s' % cid
        self.log.info('Binding %s queue to %s route_key', submit_sm_queue, routing_key)
        yield self.amqpBroker.named_queue_declare(queue=submit_sm_queue)
        yield chan.queue_bind(queue=submit_sm_queue,
                              exchange="messaging",
                              routing_key=routing_key)

        defer.returnValue(chan)

    @defer.inlineCallbacks
    def consumeSubmitSmQueue(self, connector):
        cid = connector['id']
        self.log.debug('Starting submit_sm_q consumer in connector [%s]', cid)

        submit_sm_queue = 'submit.sm.%s' % cid
        chan = connector['chan']

        try:
            # Cancel the current consumer (if any) first, so starting a connector twice never leaves two
            # consumers on the same queue (#234). pika rejects re-using a just-cancelled consumer tag
            # (DuplicateConsumerTag), so we let it assign a fresh unique tag for the new consumer.
            if connector['consumer_tag'] is not None:
                self.log.debug('Stopping submit_sm_q consumer in connector [%s]', cid)
                yield chan.basic_cancel(consumer_tag=connector['consumer_tag'])

            # Start a new consumer
            submit_sm_q, consumerTag = yield chan.basic_consume(queue=submit_sm_queue, auto_ack=False)
        except Exception as e:
            self.log.error('Error consuming from queue %s: %s', submit_sm_queue, e)
            defer.returnValue(False)

        self.log.info('%s is consuming from queue: %s', consumerTag, submit_sm_queue)

        # Set callbacks for every consumed message from submit_sm_queue queue.
        # The delivery is wrapped in the txamqp-style shim the listener's submit_sm_callback expects.
        sm_listener = connector['sm_listener']
        d = submit_sm_q.get()
        d.addCallback(lambda received: sm_listener.submit_sm_callback(DeliveryMessage(received))).addErrback(
            sm_listener.submit_sm_errback)

        connector['sm_listener'].setSubmitSmQ(submit_sm_q)
        connector['consumer_tag'] = consumerTag
        connector['submit_sm_q'] = submit_sm_q

        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_stop(self, cid, delQueues=False):
        """This will stop a service by detaching IService to IServiceCollection
        """

        self.log.debug('Stopping connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to stop a connector with an unknown cid: %s', cid)
            defer.returnValue(False)

        # Stop the queue consumer
        if connector['consumer_tag'] is not None:
            self.log.debug('Stopping submit_sm_q consumer in connector [%s]', cid)
            yield connector['chan'].basic_cancel(consumer_tag=connector['consumer_tag'])

            # Cleaning
            self.log.debug('Cleaning objects in connector [%s]', cid)
            connector['submit_sm_q'] = None
            connector['consumer_tag'] = None

        if connector['service'].running == 0:
            self.log.error('Connector [%s] is already stopped.', cid)
            defer.returnValue(False)

        if delQueues:
            submitSmQueueName = 'submit.sm.%s' % cid
            self.log.debug('Deleting queue [%s]', submitSmQueueName)
            yield connector['chan'].queue_delete(queue=submitSmQueueName)

        # Reject & requeue any pending message to avoid loosing messages after
        # clearing timers
        if len(connector['sm_listener'].rejectTimers) > 0:
            for msgid, timer in list(connector['sm_listener'].rejectTimers.items()):
                if timer.active():
                    func = timer.func
                    kw = timer.kw
                    timer.cancel()
                    del connector['sm_listener'].rejectTimers[msgid]

                    self.log.debug('Rejecting/requeuing msgid [%s] before stopping connector', msgid)
                    yield func(**kw)

        # Stop timers in message listeners
        self.log.debug('Clearing sm_listener timers in connector [%s]', cid)
        connector['sm_listener'].clearAllTimers()
        connector['sm_listener'].submit_sm_q = None

        # Stop SMPP connector
        connector['service'].stopService()

        self.log.info('Stopped connector [%s]', cid)

        # Set persistance state to False (pending for persistance)
        self.persisted = False

        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_stopall(self, delQueues=False):
        """This will stop all services by detaching IService to IServiceCollection
        """

        self.log.debug('Stopping all connectors')

        for connector in self.connectors:
            yield self.perspective_connector_stop(connector['id'], delQueues)

        # Set persistance state to False (pending for persistance)
        self.persisted = False

        defer.returnValue(True)

    def perspective_service_status(self, cid):
        """This will return the IService running status
        """

        self.log.debug('Requested service status %s', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to get service status of a connector with an unknown cid: %s', cid)
            return False

        service_status = connector['service'].running
        self.log.info('Connector [%s] service status is: %s', cid, str(service_status))

        return service_status

    def perspective_session_state(self, cid):
        """This will return the session state of a client connector
        """

        self.log.debug('Requested session state for connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to get session state of a connector with an unknown cid: %s', cid)
            return False

        session_state = connector['service'].SMPPClientFactory.getSessionState()
        self.log.info('Connector [%s] session state is: %s', cid, session_state)

        if session_state is None:
            return None
        else:
            # returning Enum would raise this on the client side:
            # Unpersistable data: instance of class enum.EnumValue deemed insecure
            # So we just return back the string of it
            return session_state.name

    def perspective_connector_details(self, cid):
        """This will return the connector details
        """

        self.log.debug('Requested details for connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to get details of a connector with an unknown cid: %s', cid)
            return False

        return self.getConnectorDetails(cid)

    def perspective_connector_config(self, cid):
        """This will return the connector SMPPClientConfig object
        """

        self.log.debug('Requested config for connector [%s]', cid)

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to get config of a connector with an unknown cid: %s', cid)
            return False

        return pickle.dumps(connector['config'], self.pickleProtocol)

    @defer.inlineCallbacks
    def perspective_submit_sm(self, uid, cid, SubmitSmPDU, submit_sm_bill, priority=1, validity_period=None,
                              pickled=True, dlr_url=None, dlr_level=1, dlr_method='POST', dlr_connector=None,
                              source_connector='httpapi', msgid=None):
        """This will enqueue a submit_sm to a connector
        """
        connector = self.getConnector(cid)
        if connector is None:
            self.log.error('Trying to enqueue a SUBMIT_SM to a connector with an unknown cid: %s', cid)
            defer.returnValue(False)
        if self.amqpBroker is None:
            self.log.error('AMQP Broker is not added')
            defer.returnValue(False)
        if self.amqpBroker is None:
            self.log.error('Trying to enqueue a SUBMIT_SM when no broker were added')
            defer.returnValue(False)

        # TODO: Future implementation, submitting a sm to a disconnected broker would be possible
        #    through a local in memory queue
        if self.amqpBroker.connected == False:
            self.log.error('AMQP Broker is not connected')
            defer.returnValue(False)

        # Define the destination and response queue names
        pubQueueName = "submit.sm.%s" % cid
        responseQueueName = "submit.sm.resp.%s" % cid

        # Pickle SubmitSmPDU if it's not pickled
        if not pickled:
            PickledSubmitSmPDU = pickle.dumps(SubmitSmPDU, self.pickleProtocol)
            if submit_sm_bill is not None:
                submit_sm_bill = pickle.dumps(submit_sm_bill, self.pickleProtocol)
        else:
            PickledSubmitSmPDU = SubmitSmPDU
            SubmitSmPDU = pickle.loads(PickledSubmitSmPDU)

        idem_key = None
        if msgid is not None:
            if self.redisClient is None or str(self.redisClient) == '<Redis Connection: Not connected>':
                # fail closed: without the dedup store a caller retry could double-submit to the wire
                self.log.error('Idempotent submit [msgid:%s] refused: Redis not connected', msgid)
                defer.returnValue(False)
            idem_key = "idem:%s" % msgid
            claimed = yield self.redisClient.set(
                idem_key, '1', expire=connector['config'].dlr_expiry, only_if_not_exists=True)
            if not claimed:
                self.log.info('Idempotent submit [msgid:%s] deduplicated; replaying original accept', msgid)
                defer.returnValue(msgid)

        try:
            # Publishing a pickled PDU
            self.log.debug('Publishing SubmitSmPDU with routing_key=%s, priority=%s', pubQueueName, priority)
            c = SubmitSmContent(
                uid=uid,
                body=PickledSubmitSmPDU,
                replyto=responseQueueName,
                submit_sm_bill=submit_sm_bill,
                priority=priority,
                expiration=validity_period,
                msgid=msgid,
                source_connector='httpapi' if source_connector == 'httpapi' else 'smppsapi',
                destination_cid=cid)
            # Written before the publish, and awaited on both ingresses: the SMSC round trip can complete
            # before a write issued after it lands, and a submit_sm_resp that finds no map is discarded, taking
            # the receipt chain with it.
            if source_connector == 'httpapi' and dlr_level in [1, 2, 3]:
                # A requested receipt (dlr_level > 0) enqueues the redis 'dlr' map regardless of dlr_url: the map also
                # drives the AMQP outcome forwarder, which must fire for a url-less receipt request too.
                if self.redisClient is None or str(self.redisClient) == '<Redis Connection: Not connected>':
                    self.log.warning("DLR is not enqueued for SubmitSmPDU [msgid:%s], RC is not connected.",
                                  c.properties['message-id'])
                else:
                    self.log.debug('Setting DLR url (%s) and level (%s) for message id:%s, expiring in %s',
                                   dlr_url,
                                   dlr_level,
                                   c.properties['message-id'],
                                   connector['config'].dlr_expiry)
                    # Set values and callback expiration setting
                    hashKey = "dlr:%s" % (c.properties['message-id'])
                    hashValues = {'sc': 'httpapi',
                                  'url': dlr_url if dlr_url is not None else '',
                                  'level': dlr_level,
                                  'method': dlr_method,
                                  'connector': dlr_connector,
                                  'expiry': connector['config'].dlr_expiry}
                    yield self.redisClient.hmset(hashKey, hashValues)
                    yield self.redisClient.expire(hashKey, connector['config'].dlr_expiry)
            elif (isinstance(source_connector, SMPPServerProtocol) and
                  SubmitSmPDU.params['registered_delivery'].receipt != RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED):
                # If submit_sm is successfully sent from a SMPPServerProtocol connector and DLR is
                # requested, then map message-id to the source_connector to permit related deliver_sm
                # messages holding further receipts to be sent back to the right connector
                if self.redisClient is None or str(self.redisClient) == '<Redis Connection: Not connected>':
                    self.log.warning("SMPPs mapping is not done for SubmitSmPDU [msgid:%s], RC is not connected.",
                                  c.properties['message-id'])
                else:
                    self.log.debug(
                        'Setting SMPPs connector (%s) mapping for msgid:%s, registered_dlr: %s, expiring in %s',
                        source_connector.system_id,
                        c.properties['message-id'],
                        SubmitSmPDU.params['registered_delivery'],
                        source_connector.factory.config.dlr_expiry)
                    # Set values and callback expiration setting
                    hashKey = "dlr:%s" % (c.properties['message-id'])
                    hashValues = {'sc': 'smppsapi',
                                  'system_id': source_connector.system_id,
                                  'source_addr_ton': SubmitSmPDU.params['source_addr_ton'],
                                  'source_addr_npi': SubmitSmPDU.params['source_addr_npi'],
                                  'source_addr': SubmitSmPDU.params['source_addr'],
                                  'dest_addr_ton': SubmitSmPDU.params['dest_addr_ton'],
                                  'dest_addr_npi': SubmitSmPDU.params['dest_addr_npi'],
                                  'destination_addr': SubmitSmPDU.params['destination_addr'],
                                  'sub_date': datetime.datetime.now(),
                                  'rd_receipt': SubmitSmPDU.params['registered_delivery'].receipt,
                                  'expiry': source_connector.factory.config.dlr_expiry}
                    yield self.redisClient.hmset(hashKey, hashValues)
                    yield self.redisClient.expire(hashKey, source_connector.factory.config.dlr_expiry)

            yield self.amqpBroker.publish(exchange='messaging', routing_key=pubQueueName, content=c)
        except Exception:
            # Release the claim on ANY post-claim failure, not only a failed publish: left held, the caller's
            # retry is answered "already accepted" for a message that never reached the queue.
            if idem_key is not None:
                yield self.redisClient.delete(idem_key)
            raise

        defer.returnValue(c.properties['message-id'])
