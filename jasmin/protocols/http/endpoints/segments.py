from datetime import datetime
import json
import re

from twisted.web.resource import Resource

from jasmin.protocols.smpp.operations import SMPPOperationFactory, count_pdus
from jasmin.protocols.http.errors import (UrlArgsValidationError, ERROR_CODE_HEADER, HttpApiError,
                                          ServerError)
from jasmin.protocols.http.validation import UrlArgsValidator
from jasmin.protocols.http.endpoints import authenticate_user, encode_short_message

# TS 23.040 9.2.3.24.1 gives the concatenation header a single octet for the total-parts field, so no message can
# be sent as more than 255 segments. Counting up to that ceiling rather than to the connector's configured cap
# answers what a body costs even when it is too long to submit, which is what a caller asks before deciding.
MAX_CONCATENATED_SEGMENTS = 255


class Segments(Resource):
    isleaf = True

    def __init__(self, HTTPApiConfig, RouterPB, stats, log):
        Resource.__init__(self)

        self.RouterPB = RouterPB
        self.stats = stats
        self.log = log

        self.opFactory = SMPPOperationFactory(long_content_max_parts=MAX_CONCATENATED_SEGMENTS,
                                              long_content_split=HTTPApiConfig.long_content_split)

    def render_POST(self, request):
        """
        /segments request processing

        Note: This method answers how many segments /send would split the same content and coding into, and it
        must do so by building the very PDUs /send builds - a second count derived from the length would drift.
        """

        self.log.debug("Rendering /segments response with args: %s from %s",
                       request.args, request.getClientIP())
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
        response = {'return': None, 'status': 200}

        self.stats.inc('request_count')
        self.stats.set('last_request_at', datetime.now())

        try:
            fields = {b'username': {'optional': False, 'pattern': re.compile(rb'^.{1,16}$')},
                      b'password': {'optional': False, 'pattern': re.compile(rb'^.{1,16}$')},
                      b'coding': {'optional': True, 'pattern': re.compile(rb'^(0|1|2|3|4|5|6|7|8|9|10|13|14){1}$')},
                      b'content': {'optional': True},
                      b'hex-content': {'optional': True},
                      }

            # Default coding is 0 when not provided
            if b'coding' not in request.args:
                request.args[b'coding'] = [b'0']

            # Content is optional, defaults to empty content string
            if b'hex-content' not in request.args and b'content' not in request.args:
                request.args[b'content'] = [b'']

            # Make validation
            v = UrlArgsValidator(request, fields)
            v.validate()

            # Check if have content --OR-- hex-content
            if b'content' in request.args and b'hex-content' in request.args:
                raise UrlArgsValidationError("content and hex-content cannot be used both in same request.")

            short_message = encode_short_message(request)

            # Authentication
            user = authenticate_user(
                request.args[b'username'][0],
                request.args[b'password'][0],
                self.RouterPB,
                self.stats,
                self.log
            )

            # Update CnxStatus
            user.getCnxStatus().httpapi['connects_count'] += 1
            user.getCnxStatus().httpapi['last_activity_at'] = datetime.now()

            data_coding = int(request.args[b'coding'][0])
            pdu = self.opFactory.SubmitSM(short_message=short_message, data_coding=data_coding)

            response = {
                'return': {
                    'submit_sm_count': count_pdus(pdu),
                    'data_coding': data_coding},
                'status': 200}
        except HttpApiError as e:
            self.log.error("Error: %s", e)
            response = {'return': e.message, 'status': e.code, 'token': e.token}
        except Exception as e:
            self.log.error("Error: %s", e)
            response = {'return': "Unknown error: %s" % e, 'status': 500, 'token': ServerError.token}
        finally:
            self.log.debug("Returning %s to %s.", response, request.getClientIP())

            # Return message
            if response['return'] is None:
                response['return'] = 'System error'
                request.setResponseCode(500)
            else:
                request.setResponseCode(response['status'])
            if 'token' in response:
                request.setHeader(ERROR_CODE_HEADER, response['token'])

            if isinstance(response['return'], bytes):
                return json.dumps(response['return'].decode()).encode()
            return json.dumps(response['return']).encode()

    def render_GET(self, request):
        """Allow GET /segments, which is what a short body reaches for"""
        return self.render_POST(request)
