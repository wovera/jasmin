from datetime import datetime
import re

from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from jasmin.protocols.http.validation import UrlArgsValidator
from jasmin.protocols.http.errors import HttpApiError, UrlArgsValidationError, ServerError, ERROR_CODE_HEADER
from jasmin.protocols.http.endpoints import TAG_PATTERN, authenticate_user


class Revoke(Resource):
    isLeaf = True

    def __init__(self, RouterPB, SMPPClientManagerPB, stats, log):
        Resource.__init__(self)
        self.RouterPB = RouterPB
        self.SMPPClientManagerPB = SMPPClientManagerPB
        self.stats = stats
        self.log = log

    def render_POST(self, request):
        self._render(request)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def _render(self, request):
        request.responseHeaders.addRawHeader(b'content-type', b'text/plain')
        response = {'return': None, 'status': 200}

        self.stats.inc('request_count')
        self.stats.set('last_request_at', datetime.now())

        try:
            fields = {b'username': {'optional': False, 'pattern': re.compile(rb'^.{1,16}$')},
                      b'password': {'optional': False, 'pattern': re.compile(rb'^.{1,16}$')},
                      b'batch': {'optional': True, 'pattern': TAG_PATTERN},
                      b'account': {'optional': True, 'pattern': TAG_PATTERN},
                      b'ttl': {'optional': False, 'pattern': re.compile(rb'^[1-9][0-9]{0,7}$')}}
            UrlArgsValidator(request, fields).validate()
            if b'batch' not in request.args and b'account' not in request.args:
                raise UrlArgsValidationError('A batch or an account is required')

            user = authenticate_user(
                request.args[b'username'][0],
                request.args[b'password'][0],
                self.RouterPB,
                self.stats,
                self.log)
            user.getCnxStatus().httpapi['connects_count'] += 1
            user.getCnxStatus().httpapi['last_activity_at'] = datetime.now()

            ttl = int(request.args[b'ttl'][0])
            for scope in ('batch', 'account'):
                if scope.encode() in request.args:
                    tag = request.args[scope.encode()][0].decode()
                    yield self.SMPPClientManagerPB.redisClient.set(
                        'revoked:%s:%s' % (scope, tag), '1', expire=ttl)

            self.stats.inc('success_count')
            response = {'return': 'Success', 'status': 200}
        except HttpApiError as e:
            self.log.error("Error: %s", e)
            response = {'return': e.message, 'status': e.code, 'token': e.token}
        except Exception as e:
            self.log.error("Error: %s", e)
            response = {'return': "Unknown error: %s" % e, 'status': 500, 'token': ServerError.token}
        finally:
            self.log.debug("Returning %s to %s.", response, request.getClientIP())
            request.setResponseCode(response['status'])
            if 'token' in response:
                request.setHeader(ERROR_CODE_HEADER, response['token'])
            if response['status'] == 200:
                request.write(response['return'].encode())
            else:
                request.write(('Error "%s"' % response['return']).encode())
            request.finish()
