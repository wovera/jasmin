from twisted.trial.unittest import TestCase

from jasmin.protocols.http.errors import (
    AuthenticationError,
    ChargingError,
    ConnectorNotFoundError,
    CredentialValidationError,
    HttpApiError,
    InterceptorNotConnectedError,
    InterceptorNotSetError,
    InterceptorRunError,
    LongContentExceededError,
    RouteNotFoundError,
    ServerError,
    ThroughputExceededError,
    UrlArgsValidationError,
)


class ErrorTokenTestCase(TestCase):
    """Status 403 covers authentication, charging and throughput alike, and 412 covers both routing failures,
    so the status on its own cannot tell a caller whether to wait or to give up. The token carries that, and is
    pinned here verbatim because it is a wire value: renaming a class must not change what a caller already
    matches on."""

    EXPECTED_TOKENS = {
        UrlArgsValidationError: 'invalid_args',
        LongContentExceededError: 'long_content_exceeded',
        CredentialValidationError: 'invalid_credentials',
        ServerError: 'server_error',
        AuthenticationError: 'authentication_failed',
        RouteNotFoundError: 'route_not_found',
        ConnectorNotFoundError: 'connector_not_found',
        ChargingError: 'charging_failed',
        ThroughputExceededError: 'throughput_exceeded',
        InterceptorNotSetError: 'interceptor_not_set',
        InterceptorNotConnectedError: 'interceptor_not_connected',
        InterceptorRunError: 'interceptor_run_error',
    }

    def test_each_error_carries_the_token_callers_match_on(self):
        for errorClass, token in self.EXPECTED_TOKENS.items():
            self.assertEqual(errorClass.token, token, errorClass.__name__)

    def test_no_two_errors_share_a_token(self):
        tokens = [errorClass.token for errorClass in self.EXPECTED_TOKENS]

        self.assertEqual(len(set(tokens)), len(tokens))

    def test_the_errors_sharing_status_403_stay_distinguishable(self):
        # The stakes: throughput is a wait, authentication and charging are not.
        overloaded = [AuthenticationError, ChargingError, ThroughputExceededError]

        for errorClass in overloaded:
            self.assertEqual(errorClass().code, 403, errorClass.__name__)
        self.assertEqual(len({errorClass.token for errorClass in overloaded}), len(overloaded))

    def test_every_error_declares_its_own_token(self):
        # Discovered, not listed: a subclass added later that forgets a token inherits the base placeholder and
        # would silently serialise as the generic one.
        for errorClass in HttpApiError.__subclasses__():
            self.assertNotEqual(errorClass.token, HttpApiError.token, errorClass.__name__)
            self.assertIn(errorClass, self.EXPECTED_TOKENS, errorClass.__name__)
