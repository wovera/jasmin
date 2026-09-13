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
                details = self.SMPPClientManagerPB.perspective_connector_details(cid) or {}
                config = connector['config']
                depth = yield self.SMPPClientManagerPB.perspective_connector_depth(cid)
                stats = SMPPClientStatsCollector().get(cid)
                connectors.append({
                    'cid': cid,
                    'session_state': details.get('session_state', ''),
                    'submit_sm_throughput': config.submit_sm_throughput,
                    'learned_throughput': getattr(config, 'learned_throughput', config.submit_sm_throughput),
                    'messages_ready': depth['messages_ready'],
                    'oldest_ready_age_seconds': depth['oldest_ready_age_seconds'],
                    'throttling_error_count': stats.get('throttling_error_count'),
                })
            request.setResponseCode(200)
            request.write(json.dumps({'connectors': connectors}).encode())
        except Exception as e:
            self.log.error('Error rendering /status: %s', e)
            request.setResponseCode(500)
            request.write(json.dumps({'error': str(e)}).encode())
        finally:
            request.finish()
