import hmac
import json
import pickle

from smpp.pdu.pdu_types import AddrNpi, AddrTon
from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from jasmin.protocols.http.errors import ERROR_CODE_HEADER
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.routing import Routes
from jasmin.routing.Filters import TagFilter
from jasmin.routing.jasminApi import Group, SmppClientConnector, User

SETTINGS = {
    'host': 'host',
    'port': 'port',
    'username': 'username',
    'password': 'password',
    'bind': 'bindOperation',
    'submit_sm_throughput': 'submit_sm_throughput',
    'dlr_expiry': 'dlr_expiry',
    'dlr_msg_id_bases': 'dlr_msg_id_bases',
    'dlr_scope': 'dlr_scope',
    'priority_flag_max': 'priority_flag_max',
    'requeue_delay': 'requeue_delay',
    'log_privacy': 'log_privacy',
    'source_addr_ton': 'source_addr_ton',
    'source_addr_npi': 'source_addr_npi',
    'dest_addr_ton': 'dest_addr_ton',
    'dest_addr_npi': 'dest_addr_npi',
}
RECONNECT_SETTINGS = {'host', 'port', 'username', 'password', 'bindOperation'}
ENUMS = {'source_addr_ton': AddrTon, 'dest_addr_ton': AddrTon, 'source_addr_npi': AddrNpi, 'dest_addr_npi': AddrNpi}


class ControlErrorToken:
    CONTROL_DISABLED = 'control_disabled'
    UNAUTHORIZED = 'unauthorized'
    INVALID_CONNECTOR = 'invalid_connector'
    CONNECTOR_NOT_FOUND = 'connector_not_found'
    CONNECTOR_NOT_ADDED = 'connector_not_added'
    BACKLOG_NOT_EMPTY = 'backlog_not_empty'
    NOT_PERSISTED = 'not_persisted'
    SERVER_ERROR = 'server_error'
    INVALID_REQUEST = 'invalid_request'
    CONNECTOR_RUNNING = 'connector_running'
    CONNECTOR_BUSY = 'connector_busy'
    INVALID_ROUTE = 'invalid_route'


class ControlError(Exception):
    def __init__(self, status, token, message):
        Exception.__init__(self, message)
        self.status = status
        self.token = token


def connector_config(cid, body):
    unknown = sorted(set(body) - set(SETTINGS) - {'started'})
    if unknown:
        raise ControlError(400, ControlErrorToken.INVALID_CONNECTOR, 'Unknown settings: %s' % ', '.join(unknown))
    settings = {'id': cid}
    for key, attribute in SETTINGS.items():
        if key in body:
            value = body[key]
            if key in ENUMS:
                try:
                    value = ENUMS[key][value]
                except KeyError:
                    raise ControlError(400, ControlErrorToken.INVALID_CONNECTOR, 'Unknown %s: %s' % (key, value))
            settings[attribute] = value
    try:
        return SMPPClientConfig(**settings)
    except Exception as e:
        raise ControlError(400, ControlErrorToken.INVALID_CONNECTOR, str(e))


class ControlResource(Resource):
    """A bearer-authenticated JSON resource that answers every request with a JSON body or a refusal."""

    isLeaf = True

    def __init__(self, control_token, log):
        Resource.__init__(self)
        self.control_token = control_token
        self.log = log

    def _handle(self, request, action):
        self._answer(request, action)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def _answer(self, request, action):
        request.responseHeaders.addRawHeader(b'content-type', b'application/json')
        try:
            self._authorize(request)
            path = [segment.decode() for segment in request.postpath if segment]
            if not path:
                raise ControlError(404, ControlErrorToken.CONNECTOR_NOT_FOUND, 'No resource named')
            body = yield action(request, path)
            request.setResponseCode(200)
        except ControlError as e:
            request.setResponseCode(e.status)
            request.setHeader(ERROR_CODE_HEADER, e.token)
            body = {'error': e.token, 'message': str(e)}
        except Exception as e:
            self.log.error('Control request failed: %s', e)
            request.setResponseCode(500)
            request.setHeader(ERROR_CODE_HEADER, ControlErrorToken.SERVER_ERROR)
            body = {'error': ControlErrorToken.SERVER_ERROR, 'message': str(e)}
        request.write(json.dumps(body).encode())
        request.finish()

    def _authorize(self, request):
        if not self.control_token:
            raise ControlError(403, ControlErrorToken.CONTROL_DISABLED, 'No control token is configured')
        presented = request.getHeader(b'authorization') or b''
        if not hmac.compare_digest(presented, ('Bearer %s' % self.control_token).encode()):
            raise ControlError(401, ControlErrorToken.UNAUTHORIZED, 'Missing or wrong bearer token')


def json_object(request):
    try:
        body = json.loads(request.content.read() or b'{}')
    except ValueError as e:
        raise ControlError(400, ControlErrorToken.INVALID_REQUEST, 'Body is not JSON: %s' % e)
    if not isinstance(body, dict):
        raise ControlError(400, ControlErrorToken.INVALID_REQUEST, 'Body must be a JSON object')
    return body


class Connectors(ControlResource):
    """PUT /connectors/<cid> adds or replaces a connector, POST /connectors/<cid>/start|stop changes its state,
    DELETE /connectors/<cid> retires it. Every change is persisted, so a restart comes back to it."""

    def __init__(self, SMPPClientManagerPB, control_token, log):
        ControlResource.__init__(self, control_token, log)
        self.manager = SMPPClientManagerPB

    def render_PUT(self, request):
        return self._handle(request, self._put)

    def render_POST(self, request):
        return self._handle(request, self._post)

    def render_DELETE(self, request):
        return self._handle(request, self._delete)

    def _existing(self, cid):
        connector = self.manager.getConnector(cid)
        if connector is None:
            raise ControlError(404, ControlErrorToken.CONNECTOR_NOT_FOUND, 'Unknown connector %s' % cid)
        return connector

    def _busy(self, connector):
        if connector['sm_listener'].in_flight:
            raise ControlError(409, ControlErrorToken.CONNECTOR_BUSY,
                               'A submit taken from the queue is still unsettled; retry once it is')

    @defer.inlineCallbacks
    def _quiesce(self, connector):
        """Detach the queue consumer, then check: a submit taken while the check ran must settle before a stop."""
        self._busy(connector)
        if connector['consumer_tag'] is None:
            return
        yield connector['chan'].basic_cancel(consumer_tag=connector['consumer_tag'])
        connector['consumer_tag'] = None
        connector['submit_sm_q'] = None
        listener = connector['sm_listener']
        if listener.in_flight:
            pending = set(listener.in_flight)
            yield self.manager.consumeSubmitSmQueue(connector)
            listener.in_flight |= pending
            self._busy(connector)

    @defer.inlineCallbacks
    def _put(self, request, path):
        cid = path[0]
        body = json_object(request)
        config = connector_config(cid, body)
        wanted = body.get('started', True)

        existing = self.manager.getConnector(cid)
        if existing is not None:
            changed = [attribute for attribute in SETTINGS.values()
                       if getattr(existing['config'], attribute, None) != getattr(config, attribute, None)]
            if not RECONNECT_SETTINGS.intersection(changed):
                for attribute in changed:
                    setattr(existing['config'], attribute, getattr(config, attribute))
                if changed:
                    self.manager.persisted = False
                if wanted and existing['service'].running != 1:
                    yield self.manager.perspective_connector_start(cid)
                elif not wanted and existing['service'].running == 1:
                    yield self._quiesce(existing)
                    yield self.manager.perspective_connector_stop(cid)
                if not self.manager.perspective_is_persisted():
                    yield self._persist()
                defer.returnValue({'connector': cid, 'created': False,
                                   'running': existing['service'].running == 1})
            yield self._quiesce(existing)
            yield self.manager.perspective_connector_remove(cid)
        if not (yield self.manager.perspective_connector_add(pickle.dumps(config, self.manager.pickleProtocol))):
            raise ControlError(503, ControlErrorToken.CONNECTOR_NOT_ADDED, 'The connector could not be added')
        if wanted:
            yield self.manager.perspective_connector_start(cid)
        yield self._persist()
        defer.returnValue({'connector': cid, 'created': existing is None,
                           'running': self.manager.getConnector(cid)['service'].running == 1})

    @defer.inlineCallbacks
    def _post(self, request, path):
        if len(path) != 2 or path[1] not in ('start', 'stop'):
            raise ControlError(404, ControlErrorToken.CONNECTOR_NOT_FOUND, 'Use /connectors/<cid>/start or /stop')
        cid, verb = path
        connector = self._existing(cid)
        if verb == 'start' and connector['service'].running != 1:
            yield self.manager.perspective_connector_start(cid)
        elif verb == 'stop' and connector['service'].running == 1:
            yield self._quiesce(connector)
            yield self.manager.perspective_connector_stop(cid)
        yield self._persist()
        defer.returnValue({'connector': cid, 'running': connector['service'].running == 1})

    @defer.inlineCallbacks
    def _delete(self, request, path):
        cid = path[0]
        connector = self._existing(cid)
        self._busy(connector)
        if connector['service'].running == 1:
            raise ControlError(409, ControlErrorToken.CONNECTOR_RUNNING,
                               'Stop the connector first, so a submit still awaiting the SMSC can settle')
        depth = yield self.manager.perspective_connector_depth(cid)
        if depth['messages_ready'] is None:
            raise ControlError(409, ControlErrorToken.BACKLOG_NOT_EMPTY, 'The queue depth is unknown')
        if depth['messages_ready']:
            raise ControlError(409, ControlErrorToken.BACKLOG_NOT_EMPTY, '%s messages still wait for this connector'
                               % depth['messages_ready'])
        yield self.manager.perspective_connector_stop(cid, delQueues=True)
        yield self.manager.perspective_connector_remove(cid)
        yield self._persist()
        defer.returnValue({'connector': cid, 'retired': True})

    def _persist(self):
        if not self.manager.perspective_persist():
            raise ControlError(500, ControlErrorToken.NOT_PERSISTED, 'The change is live but was not persisted')
        return defer.succeed(None)


class MtRoutes(ControlResource):
    """PUT /routes/mt {"routes": [{"tag", "connectors"}, ...]} makes the MT routing table one random round-robin route
    per tag over its connectors; a submit whose tags match no route is refused."""

    def __init__(self, RouterPB, SMPPClientManagerPB, control_token, log):
        ControlResource.__init__(self, control_token, log)
        self.router = RouterPB
        self.manager = SMPPClientManagerPB

    def render_PUT(self, request):
        return self._handle(request, self._put)

    def _put(self, request, path):
        if path != ['mt']:
            raise ControlError(404, ControlErrorToken.INVALID_ROUTE, 'Use /routes/mt')
        specs = json_object(request).get('routes')
        if not isinstance(specs, list):
            raise ControlError(400, ControlErrorToken.INVALID_ROUTE, 'routes must be a list')
        routes = []
        for order, spec in enumerate(specs, start=1):
            tag = spec.get('tag') if isinstance(spec, dict) else None
            cids = spec.get('connectors') if isinstance(spec, dict) else None
            if not isinstance(tag, str) or not tag or not isinstance(cids, list) or not cids \
                    or not all(isinstance(cid, str) for cid in cids):
                raise ControlError(400, ControlErrorToken.INVALID_ROUTE,
                                   'Each route needs a tag and at least one connector id')
            unknown = [cid for cid in cids if self.manager.getConnector(cid) is None]
            if unknown:
                raise ControlError(400, ControlErrorToken.INVALID_ROUTE, 'Unknown connectors: %s' % ', '.join(unknown))
            routes.append((order, Routes.RandomRoundrobinMTRoute(
                [TagFilter(tag)], [SmppClientConnector(cid) for cid in cids], 0.0)))
        # No Deferred yields between the flush and the adds, so no submit ever sees a half-built table.
        table = self.router.mt_routing_table
        table.flush()
        for order, route in routes:
            table.add(route, order)
        self.router.persistenceState['mtroutes'] = False
        if not self.router.perspective_persist(scope='mtroutes'):
            raise ControlError(500, ControlErrorToken.NOT_PERSISTED, 'The routes are live but were not persisted')
        return defer.succeed({'routes': specs})


class SendUsers(ControlResource):
    """PUT /users/<username> {"password", "group"} ensures the HTTP API user a caller authenticates as."""

    def __init__(self, RouterPB, control_token, log):
        ControlResource.__init__(self, control_token, log)
        self.router = RouterPB

    def render_PUT(self, request):
        return self._handle(request, self._put)

    def _put(self, request, path):
        username = path[0]
        body = json_object(request)
        password, gid = body.get('password'), body.get('group')
        if not isinstance(password, str) or not password or not isinstance(gid, str) or not gid:
            raise ControlError(400, ControlErrorToken.INVALID_REQUEST, 'password and group are required')
        # Authentication refuses a disabled group, so the group is made present and enabled before the user is checked.
        group = self.router.getGroup(gid)
        if group is None:
            group = Group(gid)
            self.router.perspective_group_add(pickle.dumps(group, self.router.pickleProtocol))
        elif not group.enabled:
            self.router.perspective_group_enable(gid)

        user = self.router.authenticateUser(username, password)
        created = user is None or user.group.gid != gid
        if created:
            try:
                user = User(username, group, username, password)
            except Exception as e:
                raise ControlError(400, ControlErrorToken.INVALID_REQUEST, str(e))
            self.router.perspective_user_add(pickle.dumps(user, self.router.pickleProtocol))
        for scope in ('groups', 'users'):
            if not self.router.persistenceState[scope] and not self.router.perspective_persist(scope=scope):
                raise ControlError(500, ControlErrorToken.NOT_PERSISTED, 'The user is live but was not persisted')
        return defer.succeed({'user': username, 'created': created})
