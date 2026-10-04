import json

from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from jasmin.protocols.smpp.stats import SMPPClientStatsCollector


class Status(Resource):
    isLeaf = True

    def __init__(self, SMPPClientManagerPB, log):
        Resource.__init__(self)
        self.SMPPClientManagerPB = SMPPClientManagerPB
        self.log = log

    def render_GET(self, request):
        self._render(request)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def _render(self, request):
        request.responseHeaders.addRawHeader(b'content-type', b'application/json')
        try:
            connectors = []
            for listed in self.SMPPClientManagerPB.perspective_connector_list():
                cid = listed['id']
                connector = self.SMPPClientManagerPB.getConnector(cid)
                if connector is None:
                    continue
                config = connector['config']
                listener = connector['sm_listener']
                depth = yield self.SMPPClientManagerPB.perspective_connector_depth(cid)
                stats = SMPPClientStatsCollector().get(cid)
                connectors.append({
                    'cid': cid,
                    'session_state': listed['session_state'],
                    'submit_sm_throughput': config.submit_sm_throughput,
                    'learned_throughput': (listener.learned_rate.current if listener.learned_rate
                                           else config.submit_sm_throughput),
                    'messages_ready': depth['messages_ready'],
                    'messages_in_flight': len(listener.in_flight),
                    'oldest_ready_age_seconds': depth['oldest_ready_age_seconds'],
                    'throttling_error_count': stats.get('throttling_error_count'),
                    'config': {
                        'host': config.host,
                        'port': config.port,
                        'username': config.username,
                        'bind': config.bindOperation,
                        'submit_sm_throughput': config.submit_sm_throughput,
                        'dlr_expiry': config.dlr_expiry,
                        'dlr_msg_id_bases': config.dlr_msg_id_bases,
                        'dlr_scope': getattr(config, 'dlr_scope', ''),
                        'priority_flag_max': config.priority_flag_max,
                        'requeue_delay': config.requeue_delay,
                        'log_privacy': config.log_privacy,
                        'source_addr_ton': config.source_addr_ton.name,
                        'source_addr_npi': config.source_addr_npi.name,
                        'dest_addr_ton': config.dest_addr_ton.name,
                        'dest_addr_npi': config.dest_addr_npi.name,
                    },
                    'started': connector['service'].running == 1,
                })
            request.setResponseCode(200)
            request.write(json.dumps({'connectors': connectors}).encode())
        except Exception as e:
            self.log.error('Error rendering /status: %s', e)
            request.setResponseCode(500)
            request.write(json.dumps({'error': str(e)}).encode())
        finally:
            request.finish()
