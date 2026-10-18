from datetime import datetime

from twisted.internet import reactor
from twisted.web.resource import Resource

from jasmin.tools.logs import configure_logger
import jasmin
from jasmin.protocols.http.endpoints.send import Send
from jasmin.protocols.http.endpoints.rate import Rate
from jasmin.protocols.http.endpoints.segments import Segments
from jasmin.protocols.http.endpoints.ping import Ping
from jasmin.protocols.http.endpoints.balance import Balance
from jasmin.protocols.http.endpoints.metrics import Metrics
from jasmin.protocols.http.endpoints.control import Connectors, MtRoutes, SendUsers
from jasmin.protocols.http.endpoints.status import Status
from jasmin.protocols.http.endpoints.revoke import Revoke
from jasmin.protocols.http.stats import HttpAPIStatsCollector

LOG_CATEGORY = "jasmin-http-api"

# @TODO make it configurable
reactor.suggestThreadPoolSize(30)


class HTTPApi(Resource):

    def __init__(self, RouterPB, SMPPClientManagerPB, config, interceptor=None):
        Resource.__init__(self)

        # Setup stats collector
        stats = HttpAPIStatsCollector().get()
        stats.set('created_at', datetime.now())

        # Set up a dedicated logger
        log = configure_logger(LOG_CATEGORY, config)

        self.log = log
        # Set http url routings
        log.debug("Setting http url routing for /send")
        self.putChild(b'send', Send(config, RouterPB, SMPPClientManagerPB, stats, log, interceptor))
        log.debug("Setting http url routing for /rate")
        self.putChild(b'rate', Rate(config, RouterPB, stats, log, interceptor))
        log.debug("Setting http url routing for /segments")
        self.putChild(b'segments', Segments(config, RouterPB, stats, log))
        log.debug("Setting http url routing for /balance")
        self.putChild(b'balance', Balance(RouterPB, stats, log))
        log.debug("Setting http url routing for /ping")
        self.putChild(b'ping', Ping(log))
        log.debug("Setting http url routing for /metrics")
        self.putChild(b'metrics', Metrics(SMPPClientManagerPB))
        log.debug("Setting http url routing for /status")
        self.putChild(b'status', Status(SMPPClientManagerPB, log))
        self.putChild(b'connectors', Connectors(SMPPClientManagerPB, config.control_token, log))
        self.putChild(b'routes', MtRoutes(RouterPB, SMPPClientManagerPB, config.control_token, log))
        self.putChild(b'users', SendUsers(RouterPB, config.control_token, log))
        log.debug("Setting http url routing for /revoke")
        self.putChild(b'revoke', Revoke(RouterPB, SMPPClientManagerPB, stats, log))

    def getChild(self, name, request):
        self.log.debug("Getting child with name %s", name)
        if name == b'':
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        self.log.debug("Rendering / response from %s", request.getClientIP())
        request.setResponseCode(200)
        return jasmin.get_release().encode()
