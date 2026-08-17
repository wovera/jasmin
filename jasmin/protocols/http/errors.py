# Carries the machine-readable failure name alongside the status, which is ambiguous on its own.
ERROR_CODE_HEADER = 'Jasmin-Error-Code'

# How many segments the accepted message was actually split into, which only the split itself can answer: a
# character carried whole into the next segment can add one that dividing the length never shows. The caller bills
# on this, so it is reported rather than left to be re-derived from a copy of the splitting rules.
SEGMENT_COUNT_HEADER = 'Jasmin-Segment-Count'


class HttpApiError(Exception):
    """Base of the HTTP API's errors.

    `token` is the stable machine-readable name of the failure. Several of these share an HTTP status - 403
    covers authentication, charging and throughput alike - so the status alone cannot tell a caller whether to
    wait or to give up. The token is written verbatim, never derived from the class name, so renaming a class
    cannot silently change a value already on the wire.
    """

    token = 'error'

    def __init__(self, code, message=None):
        Exception.__init__(self)
        self.message = message
        self.code = code

    def __str__(self):
        return '%s: %s (%s)' % (self.code, self.__class__.__name__, self.message)


class UrlArgsValidationError(HttpApiError):
    """"Raised when url validation fails  (jasmin.protocols.http.validation.UrlArgsValidator)"""

    token = 'invalid_args'

    def __init__(self, message):
        HttpApiError.__init__(self, 400, message)


class LongContentExceededError(HttpApiError):
    """Raised when a message needs more segments than the connector is configured to send"""

    token = 'long_content_exceeded'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 400, message)


class CredentialValidationError(HttpApiError):
    """Raised when user credential validation fails

    (jasmin.protocols.http.validation.HttpAPICredentialValidator)
    """

    token = 'invalid_credentials'

    def __init__(self, message):
        HttpApiError.__init__(self, 400, message)


class ServerError(HttpApiError):
    """Raised on any occuring error inside HTTP Server"""

    token = 'server_error'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 500, message)


class AuthenticationError(HttpApiError):
    """Raised on authentication error"""

    token = 'authentication_failed'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 403, message)


class RouteNotFoundError(HttpApiError):
    """Raised when no routes found for a given Routable"""

    token = 'route_not_found'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 412, message)


class ConnectorNotFoundError(HttpApiError):
    """Raised when no connectors are available for a given Routable"""

    token = 'connector_not_found'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 412, message)


class ChargingError(HttpApiError):
    """Raised on any occuring error while charging user"""

    token = 'charging_failed'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 403, message)


class ThroughputExceededError(HttpApiError):
    """Raised when throughput is exceeded"""

    token = 'throughput_exceeded'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 403, message)


class InterceptorNotSetError(HttpApiError):
    """Raised when message is about to be intercepted and no interceptorpb_client were set"""

    token = 'interceptor_not_set'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 503, message)


class InterceptorNotConnectedError(HttpApiError):
    """Raised when message is about to be intercepted and interceptorpb_client is disconnected"""

    token = 'interceptor_not_connected'

    def __init__(self, message=None):
        HttpApiError.__init__(self, 503, message)


class InterceptorRunError(HttpApiError):
    """Raised when running script returned an error"""

    token = 'interceptor_run_error'

    def __init__(self, code=400, message=None):
        HttpApiError.__init__(self, code, message)
