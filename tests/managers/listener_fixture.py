import pickle
from datetime import datetime, timedelta
from unittest.mock import Mock

from smpp.pdu.pdu_types import CommandId
from twisted.internet import defer

from jasmin.managers.configs import SMPPClientSMListenerConfig
from jasmin.managers.listeners import SMPPClientSMListener
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory


class ListenerFixture:
    def _listener(self, probe, submit_sm_throughput=0):
        client_config = SMPPClientConfig(id=probe)
        client_config.submit_sm_throughput = submit_sm_throughput
        client_factory = Mock()
        client_factory.config = client_config
        client_factory.smpp = None
        broker = Mock()
        broker.connected = True
        broker.publish = Mock(return_value=defer.succeed(None))
        broker.publish_confirmed = Mock(return_value=defer.succeed(None))
        broker.named_queue_declare = Mock(return_value=defer.succeed(None))
        redis = Mock()
        redis.get = Mock(side_effect=lambda key: defer.succeed(None))
        listener = SMPPClientSMListener(SMPPClientSMListenerConfig(), client_factory, broker, redis)
        listener.submit_sm_q = Mock()
        listener.submit_sm_q.get.return_value = defer.Deferred()
        return listener, broker

    def _message(self, probe, expiration=None, created_at=None, body=None):
        if expiration is None:
            expiration = datetime.now() + timedelta(hours=1)
        if created_at is None:
            created_at = datetime.now()
        if body is None:
            pdu = SMPPOperationFactory(SMPPClientConfig(id=probe)).SubmitSM(
                source_addr='1423', destination_addr='06155423', short_message='Hello')
            body = pickle.dumps(pdu)
        message = Mock()
        message.content.properties = {
            'message-id': probe,
            'headers': {'expiration': str(expiration), 'created_at': str(created_at)},
        }
        message.content.body = body
        return message

    def _published(self, broker, routing_key):
        return [call.kwargs['content'] for call in broker.publish_confirmed.call_args_list
                if call.kwargs.get('routing_key') == routing_key]

    def _resp(self, probe, status):
        pdu = SMPPOperationFactory(SMPPClientConfig(id=probe)).SubmitSM(
            source_addr='1423', destination_addr='06155423', short_message='Hello')
        response = Mock()
        response.request = pdu
        response.response = Mock()
        response.response.status = status
        response.response.params = {'message_id': b'01'}
        response.response.id = CommandId.submit_sm_resp
        return response
