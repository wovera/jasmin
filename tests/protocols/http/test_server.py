import json
import re
from datetime import datetime
from dateutil import parser
from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.protocols.http.configs import HTTPApiConfig
from jasmin.protocols.http.endpoints import send as send_module
from jasmin.protocols.http.server import HTTPApi
from smpp.pdu import smpp_time
from jasmin.protocols.http.stats import HttpAPIStatsCollector
from jasmin.routing.Filters import GroupFilter, SourceAddrFilter
from jasmin.routing.Routes import DefaultRoute, RandomRoundrobinMTRoute, StaticMTRoute
from jasmin.routing.router import RouterPB
from jasmin.routing.configs import RouterPBConfig
from jasmin.routing.jasminApi import User, Group, SmppClientConnector
from .twisted_web_test_utils import DummySite


class HTTPApiTestCases(TestCase):
    def setUp(self):
        # Instanciate a RouterPB (a requirement for HTTPApi)
        RouterPBConfigInstance = RouterPBConfig()
        self.RouterPB_f = RouterPB(RouterPBConfigInstance)

        # Provision Router with User and Route
        self.g1 = Group(1)
        self.u1 = User(1, self.g1, 'nathalie', 'correct')
        self.RouterPB_f.groups.append(self.g1)
        self.RouterPB_f.users.append(self.u1)
        self.RouterPB_f.mt_routing_table.add(DefaultRoute(SmppClientConnector('abc')), 0)

        # Instanciate a SMPPClientManagerPB (a requirement for HTTPApi)
        SMPPClientPBConfigInstance = SMPPClientPBConfig()
        SMPPClientPBConfigInstance.authentication = False
        self.clientManager_f = SMPPClientManagerPB(SMPPClientPBConfigInstance)

        httpApiConfigInstance = HTTPApiConfig()
        self.web = DummySite(HTTPApi(self.RouterPB_f, self.clientManager_f, httpApiConfigInstance))

    def tearDown(self):
        self.RouterPB_f.cancelPersistenceTimer()


class PingTestCases(HTTPApiTestCases):
    @defer.inlineCallbacks
    def test_basic_ping(self):
        response = yield self.web.get(b"ping")
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(response.value(), b"Jasmin/PONG")


class AuthenticationTestCases(HTTPApiTestCases):
    @defer.inlineCallbacks
    def test_send_normal(self):
        response = yield self.web.post(b'send', {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 500)
        self.assertNotEqual(response.value(), "Error \"Authentication failure for username:%s\"" % self.u1.username)

    @defer.inlineCallbacks
    def test_rate_normal(self):
        response = yield self.web.get(b"rate", {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(response.value(), b'{"unit_rate": 0.0, "submit_sm_count": 1}')

    @defer.inlineCallbacks
    def test_balance_normal(self):
        response = yield self.web.get(b"balance", {b'username': self.u1.username,
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(response.value(), b'{"balance": "ND", "sms_count": "ND"}')

    @defer.inlineCallbacks
    def test_send_disabled_user(self):
        self.u1.disable()

        response = yield self.web.post(b'send', {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), ("Error \"Authentication failure for username:%s\"" % self.u1.username).encode())

    @defer.inlineCallbacks
    def test_rate_disabled_user(self):
        self.u1.disable()

        response = yield self.web.get(b"rate", {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), b'"Authentication failure for username:nathalie"')

    @defer.inlineCallbacks
    def test_balance_disabled_user(self):
        self.u1.disable()

        response = yield self.web.get(b"balance", {b'username': self.u1.username,
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), b'"Authentication failure for username:nathalie"')

    @defer.inlineCallbacks
    def test_send_disabled_group(self):
        self.g1.disable()

        response = yield self.web.post(b'send', {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), ("Error \"Authentication failure for username:%s\"" % self.u1.username).encode())

    @defer.inlineCallbacks
    def test_rate_disabled_group(self):
        self.g1.disable()

        response = yield self.web.get(b"rate", {b'username': self.u1.username,
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), b'"Authentication failure for username:nathalie"')

    @defer.inlineCallbacks
    def test_balance_disabled_group(self):
        self.g1.disable()

        response = yield self.web.get(b"balance", {b'username': self.u1.username,
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), b'"Authentication failure for username:nathalie"')


class SendTestCases(HTTPApiTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_post_send_json_with_correct_args(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                               'password': 'incorrec',
                                               'to': '06155423',
                                               'content': 'anycontent'}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

    @defer.inlineCallbacks
    def test_post_send_json_with_extra_args(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                                          'password': 'incorrec',
                                                          'to': '06155423',
                                                          'content': 'anycontent',
                                                          'custom_tlvs': [
                                                              (0x3000, None, 'COctetString', 'test1234')
                                                          ]}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

    @defer.inlineCallbacks
    def test_post_send_json_with_auth_success(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                                          'password': 'correct',
                                                          'to': '06155423',
                                                          'content': 'anycontent',
                                                          'custom_tlvs': [
                                                              (0x3000, None, 'COctetString', 'test1234')
                                                          ]}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

    @defer.inlineCallbacks
    def test_send_with_correct_args(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                               b'password': 'incorrec',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), b"Error \"Authentication failure for username:%s\"" % self.username.encode())

    @defer.inlineCallbacks
    def test_send_with_incorrect_args(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                               b'passwd': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(), b"Error \"Mandatory argument [password] is not found.\"")

    @defer.inlineCallbacks
    def test_send_with_auth_success(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

    @defer.inlineCallbacks
    def test_send_with_priority(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Priority definitions
        valid_priorities = {0, 1, 2, 3}

        for params['priority'] in valid_priorities:
            response = yield self.web.post(b'send', params)
            self.assertEqual(response.responseCode, 500)

            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Priority definitions
        invalid_priorities = {-1, 'a', 44, 4}

        for params['priority'] in invalid_priorities:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             ('Error "Argument [priority] has an invalid value: [%s]."' % params['priority']).encode())

    @defer.inlineCallbacks
    def test_send_with_validity_period(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Validity period definitions
        valid_vps = {0, 1, 2, 3, 4000}

        for params[b'validity-period'] in valid_vps:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Validity period definitions
        invalid_vps = {-1, 'a', 1.0}

        for params[b'validity-period'] in invalid_vps:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             ('Error "Argument [validity-period] has an invalid value: [%s]."' % params[
                                 b'validity-period']).encode())

    @defer.inlineCallbacks
    def test_send_with_validity_period_truncates_the_minted_instant(self):
        """A minted validity instant carries no sub-second meaning and must not reach the encoder with one.

        The absolute-time encoder derives a single tenths-of-a-second digit from the microseconds and rejects
        anything above .9, which is about a tenth of all wall-clock instants. That raise surfaces inside the
        submit callback's generic except, which discards without a retry or a receipt, long after /send answered
        200. The clock is pinned into that rejecting band so the guarantee is proven rather than sampled.
        """
        submitted = []
        self.clientManager_f.perspective_submit_sm = Mock(
            side_effect=lambda **kwargs: submitted.append(kwargs['SubmitSmPDU']))

        rejectingInstant = datetime(2026, 1, 1, 12, 0, 0, 950000)

        class PinnedClock:
            @staticmethod
            def today():
                return rejectingInstant

            # The module also stamps stats off now(); pinning both keeps the run free of a real clock.
            @staticmethod
            def now():
                return rejectingInstant

        send_module.datetime = PinnedClock
        self.addCleanup(setattr, send_module, 'datetime', datetime)

        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent',
                  b'validity-period': 60}
        yield self.web.post(b'send', params)

        self.assertEqual(1, len(submitted))
        validity_period = submitted[0].params['validity_period']
        self.assertEqual(0, validity_period.microsecond)
        # The consequence, not just the field: the encoder accepts it where the untruncated instant would raise.
        smpp_time.unparse_absolute_time(validity_period)
        self.assertRaises(ValueError, smpp_time.unparse_absolute_time, rejectingInstant)

    @defer.inlineCallbacks
    def test_validity_period_rides_the_amqp_expiration_header(self):
        """The queue drops a message whose validity elapsed while it waited, but only if it can see the instant.

        The consumer reads the expiration from the AMQP header, not from the PDU, and the header is only written
        when the publish is given one. Passing the PDU alone leaves that discard unreachable, so a message that
        outlived its validity in a backed-up queue is still submitted to the carrier whenever the queue drains.
        """
        submitted = []
        self.clientManager_f.perspective_submit_sm = Mock(
            side_effect=lambda **kwargs: submitted.append(kwargs))

        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent',
                  b'validity-period': 60}
        yield self.web.post(b'send', params)

        self.assertEqual(1, len(submitted))
        expiration = submitted[0]['validity_period']
        self.assertIsNotNone(expiration)
        # The consumer parses the header with dateutil, so it must round-trip to the PDU's own instant.
        self.assertEqual(parser.parse(expiration), submitted[0]['SubmitSmPDU'].params['validity_period'])

    @defer.inlineCallbacks
    def test_no_validity_period_sets_no_expiration_header(self):
        """A submit naming no validity must not mint one: an absent window means unbounded, not expire-now."""
        submitted = []
        self.clientManager_f.perspective_submit_sm = Mock(
            side_effect=lambda **kwargs: submitted.append(kwargs))

        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}
        yield self.web.post(b'send', params)

        self.assertEqual(1, len(submitted))
        self.assertIsNone(submitted[0]['validity_period'])

    def routeOverTwoConnectors(self):
        """Replace the default route with a random pool of two connectors, and report neither as bound."""
        self.RouterPB_f.mt_routing_table.flush()
        self.RouterPB_f.mt_routing_table.add(
            RandomRoundrobinMTRoute(
                [GroupFilter(self.g1)],
                [SmppClientConnector('bound-one'), SmppClientConnector('unbound-one')],
                0.0),
            2)
        self.clientManager_f.perspective_connector_details = Mock(return_value=False)

        submitted = []
        self.clientManager_f.perspective_submit_sm = Mock(
            side_effect=lambda **kwargs: submitted.append(kwargs['cid']))
        return submitted

    @defer.inlineCallbacks
    def test_send_over_a_random_route_never_picks_an_unbound_connector(self):
        """Spreading at random over a pool holding an unbound connector sends a share of the traffic nowhere.

        Those submits are not refused: they sit queued against a connector that cannot deliver and age out
        silently, so the loss is invisible at the API. Only the bound connector may be chosen.
        """
        submitted = self.routeOverTwoConnectors()
        self.clientManager_f.perspective_connector_details = Mock(
            side_effect=lambda cid: {'session_state': 'BOUND_TRX'} if cid == 'bound-one' else False)

        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}
        for _ in range(20):
            yield self.web.post(b'send', params)

        # Enough draws that an unfiltered random choice over two connectors would land on the unbound one.
        self.assertEqual(20, len(submitted))
        self.assertEqual({'bound-one'}, set(submitted))

    @defer.inlineCallbacks
    def test_send_over_a_random_route_refuses_when_no_connector_is_bound(self):
        """An all-unbound pool is refused before the 200, rather than enqueued against nothing."""
        submitted = self.routeOverTwoConnectors()

        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 412)
        self.assertEqual(response.value(), b'Error "Route has no bound connectors"')
        self.assertEqual([], submitted)

    @defer.inlineCallbacks
    def test_send_with_inurl_dlr(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # URL definitions
        valid_urls = {'http://127.0.0.1/receipt',
                      'http://127.0.0.1:99/receipt',
                      'https://127.0.0.1/receipt',
                      'https://127.0.0.1:99/receipt',
                      'https://127.0.0.1/receipt.html',
                      'https://127.0.0.1:99/receipt.html',
                      'http://www.google.com/receipt',
                      'http://www.google.com:99/receipt',
                      'http://www.google.com/receipt.html',
                      'http://www.google.com:99/receipt.html',
                      'http://www.google.com/',
                      'http://www.google.com:99/',
                      'http://www.google.com',
                      'http://www.google.com:99'}

        for params['dlr-url'] in valid_urls:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # URL definitions
        invalid_urls = {'ftp://127.0.0.1/receipt',
                        'smtp://127.0.0.1:99/receipt',
                        'smpp://127.0.0.1/receipt',
                        '127.0.0.1:99',
                        'www.google.com',
                        'www.google.com:99/'}

        for params['dlr-url'] in invalid_urls:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value(),
                             b"Error \"Argument [dlr-url] has an invalid value: [%s].\"" % params['dlr-url'].encode())

    @defer.inlineCallbacks
    def test_send_without_args(self):
        response = yield self.web.post(b'send')
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(), b"Error \"Mandatory argument [to] is not found.\"")

    @defer.inlineCallbacks
    def test_send_with_some_args(self):
        response = yield self.web.post(b'send', {b'to': b'06155423', b'username': self.username})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(), b'Error \"Mandatory argument [password] is not found.\"')

    @defer.inlineCallbacks
    def test_send_with_tags(self):
        """Related to #455"""
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        valid_tags = {'12', '1,2', '1000,2,12123', 'a,b,c', 'A0,22,B4,4e', 'a-b,2'}
        for params['tags'] in valid_tags:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        invalid_tags = {';', '#,.,:', '+++,sh1t,3r='}
        for params['tags'] in invalid_tags:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value(), ('Error "Argument [tags] has an invalid value: [%s]."' % params['tags']).encode())

    @defer.inlineCallbacks
    def test_send_hex_content(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423'}

        # Assert having an error if content and hex_content are not present
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(),
                         b"Error \"content or hex-content not present.\"")

        # Assert having an error if content and hex_content are present
        params['hex-content'] = b''
        params[b'content'] = b''
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.value(),
                         b"Error \"content and hex-content cannot be used both in same request.\"")

        # Assert correct encoding
        del (params[b'content'])
        params['hex-content'] = b''
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 500)
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert incorrect encoding
        params['hex-content'] = 'Clear text'
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value()[:33],
                         b"Error \"Invalid hex-content data: ")

    @defer.inlineCallbacks
    def test_send_with_sdt(self):
        """Related to #541"""
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Assert sdt optional
        response = yield self.web.post(b'send', params)
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert valid sdt
        valid_sdt = {'000000000100000R'}
        for params['sdt'] in valid_sdt:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert invalid sdt
        invalid_sdt = {'', '000000000100000', '00', '00+', '00R', '00-', '0000000001000000R', '00000000100000R'}
        for params['sdt'] in invalid_sdt:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value()[:22], b"Error \"Argument [sdt] ")


class RateTestCases(HTTPApiTestCases):
    def setUp(self):
        HTTPApiTestCases.setUp(self)

        # Provision Router with additional Users and Routes
        u2 = User(2, Group(2), 'user2', 'correct')
        u3 = User(3, Group(2), 'user3', 'correct')
        u3.mt_credential.setQuota('balance', 10)
        self.RouterPB_f.users.append(u2)
        self.RouterPB_f.users.append(u3)
        filters = [GroupFilter(Group(2))]
        route = StaticMTRoute(filters, SmppClientConnector('abc'), 1.5)
        self.RouterPB_f.mt_routing_table.add(route, 2)

    @defer.inlineCallbacks
    def test_rate_with_correct_args(self):
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                               b'password': 'incorrec',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(json.loads(response.value()), 'Authentication failure for username:nathalie')

    @defer.inlineCallbacks
    def test_rate_with_incorrect_args(self):
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                               b'passwd': b'correct',
                                               b'content': 'hello',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(json.loads(response.value()), 'Mandatory argument [password] is not found.')

    @defer.inlineCallbacks
    def test_rate_with_auth_success(self):
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'submit_sm_count': 1, 'unit_rate': 0.0})

    @defer.inlineCallbacks
    def test_rate_rated_route_unlimited_balance(self):
        response = yield self.web.get(b"rate", {b'username': 'user2',
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'submit_sm_count': 1, 'unit_rate': 0.0})

    @defer.inlineCallbacks
    def test_rate_rated_route_unlimited_balance_long_content(self):
        content = 'A' * 200
        response = yield self.web.get(b"rate", {b'username': 'user2',
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': content})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'submit_sm_count': 2, 'unit_rate': 0.0})

    @defer.inlineCallbacks
    def test_rate_rated_route_defined_balance(self):
        response = yield self.web.get(b"rate", {b'username': 'user3',
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'submit_sm_count': 1, 'unit_rate': 1.5})

    @defer.inlineCallbacks
    def test_rate_rated_route_defined_balance_long_content(self):
        content = 'A' * 200
        response = yield self.web.get(b"rate", {b'username': 'user3',
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': content})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'submit_sm_count': 2, 'unit_rate': 1.5})

    @defer.inlineCallbacks
    def test_rate_with_tags(self):
        "Related to #455"
        params = {b'username': 'user3',
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        valid_tags = {'12', '1,2', '1000,2,12123', 'a,b,c', 'A0,22,B4,4e', 'a-b,2'}
        for params['tags'] in valid_tags:
            response = yield self.web.get(b"rate", params)

            self.assertEqual(response.responseCode, 200)

        invalid_tags = {';', '#,.,:', '+++,sh1t,3r='}
        for params['tags'] in invalid_tags:
            response = yield self.web.get(b"rate", params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value(), ('"Argument [tags] has an invalid value: [%s]."' % params['tags']).encode())


class RateSourceAddrTestCases(HTTPApiTestCases):
    """/rate must price the message /send would actually submit. Its PDU was built with a misspelled keyword,
    which SubmitSM(**kwargs) swallowed silently, so the from-address never reached the PDU being priced and any
    routing or credential rule keyed on the sender saw nothing."""

    @defer.inlineCallbacks
    def test_the_from_address_reaches_the_priced_pdu(self):
        rate = self.web.resource.children[b'rate']
        built = []
        SubmitSM = rate.opFactory.SubmitSM
        rate.opFactory.SubmitSM = lambda **kw: built.append(kw) or SubmitSM(**kw)
        try:
            response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                    b'password': b'correct',
                                                    b'from': b'999',
                                                    b'to': b'06155423'})
        finally:
            rate.opFactory.SubmitSM = SubmitSM

        self.assertEqual(response.responseCode, 200)
        # A misspelled keyword is accepted and dropped by **kwargs, so the assertion is on the name itself.
        self.assertEqual(built[0].get('source_addr'), b'999')


class RateSegmentationTestCases(HTTPApiTestCases):
    """It never applied the GSM 03.38 conversion, so it counted segments over raw UTF-8 bytes and could quote a
    different number of segments than the send it is quoting for."""

    @defer.inlineCallbacks
    def test_segments_are_counted_over_gsm_not_raw_utf8(self):
        # 160 septets in GSM 03.38, but 320 bytes as UTF-8, which would be counted as more than one segment.
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                b'password': b'correct',
                                                b'to': b'06155423',
                                                b'coding': b'0',
                                                b'content': 'é' * 160})

        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value())['submit_sm_count'], 1)


class ErrorCodeHeaderTestCases(HTTPApiTestCases):
    """The status alone is ambiguous - 403 covers authentication, charging and throughput - so every error
    answer carries the machine-readable name too, on every endpoint and every path."""

    def _token(self, response):
        header = response.responseHeaders.getRawHeaders(b'jasmin-error-code')
        return None if header is None else header[0]

    @defer.inlineCallbacks
    def test_rate_authentication_failure_names_itself(self):
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                b'password': b'wrong',
                                                b'to': b'06155423'})

        self.assertEqual(response.responseCode, 403)
        self.assertEqual(self._token(response), b'authentication_failed')

    @defer.inlineCallbacks
    def test_rate_missing_argument_names_itself(self):
        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                b'to': b'06155423'})

        self.assertEqual(response.responseCode, 400)
        self.assertEqual(self._token(response), b'invalid_args')

    @defer.inlineCallbacks
    def test_balance_authentication_failure_names_itself(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': b'wrong'})

        self.assertEqual(response.responseCode, 403)
        self.assertEqual(self._token(response), b'authentication_failed')

    @defer.inlineCallbacks
    def test_a_successful_answer_carries_no_error_name(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': b'correct'})

        self.assertEqual(response.responseCode, 200)
        self.assertIsNone(self._token(response))


class BalanceTestCases(HTTPApiTestCases):
    def setUp(self):
        HTTPApiTestCases.setUp(self)

        # Provision Router with additional Users and Routes
        u2 = User(2, Group(2), 'user2', 'correct')
        u2.mt_credential.setQuota('balance', 100.2)
        u2.mt_credential.setQuota('submit_sm_count', 30)
        u3 = User(3, Group(2), 'user3', 'correct')
        u3.mt_credential.setQuota('balance', 10)
        self.RouterPB_f.users.append(u2)
        self.RouterPB_f.users.append(u3)

    @defer.inlineCallbacks
    def test_balance_with_correct_args(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                  b'password': 'incorrec'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(json.loads(response.value()), 'Authentication failure for username:%s' % 'nathalie')

    @defer.inlineCallbacks
    def test_balance_with_incorrect_args(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                  b'passwd': b'correct'})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(json.loads(response.value()), 'Mandatory argument [password] is not found.')

    @defer.inlineCallbacks
    def test_balance_with_auth_success_unlimited_quotas(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'balance': 'ND', 'sms_count': 'ND'})

    @defer.inlineCallbacks
    def test_balance_with_auth_success_defined_quotas_u2(self):
        response = yield self.web.get(b"balance", {b'username': 'user2',
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'balance': 100.2, 'sms_count': 30})

    @defer.inlineCallbacks
    def test_balance_with_auth_success_defined_quotas_u3(self):
        response = yield self.web.get(b"balance", {b'username': 'user3',
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {'balance': 10, 'sms_count': 'ND'})


class UserStatsTestCases(HTTPApiTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_send_failure(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count']

        response = yield self.web.post(b'send', {b'username': 'nathalie',
                                               b'password': 'incorrec',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_submit_sm_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count'])

    @defer.inlineCallbacks
    def test_send_success(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count']

        response = yield self.web.post(b'send', {b'username': 'nathalie',
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 500)
        self.assertEqual(_submit_sm_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count'])

    @defer.inlineCallbacks
    def test_balance_failure(self):
        # Save before
        _balance_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count']

        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                  b'password': 'incorrec'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_balance_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count'])

    @defer.inlineCallbacks
    def test_balance_success(self):
        # Save before
        _balance_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count']

        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                  b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(_balance_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count'])

    @defer.inlineCallbacks
    def test_rate_failure(self):
        # Save before
        _rate_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count']

        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                               b'password': 'incorrec',
                                               b'to': b'06155423'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_rate_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count'])

    @defer.inlineCallbacks
    def test_rate_success(self):
        # Save before
        _rate_request_count = self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count']

        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                               b'password': b'correct',
                                               b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(_rate_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count'])


class StatsTestCases(HTTPApiTestCases):
    username = 'nathalie'

    def setUp(self):
        HTTPApiTestCases.setUp(self)

        # Re-init stats singleton collector
        created_at = HttpAPIStatsCollector().get().get('created_at')
        HttpAPIStatsCollector().get().init()
        HttpAPIStatsCollector().get().set('created_at', created_at)

    @defer.inlineCallbacks
    def test_send_with_auth_failure(self):
        stats = HttpAPIStatsCollector().get()

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 0)
        self.assertEqual(stats.get('last_request_at'), 0)
        self.assertEqual(stats.get('auth_error_count'), 0)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 0)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)

        response = yield self.web.post(b'send', {b'username': self.username,
                                               b'password': 'incorrec',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 1)
        self.assertTrue(type(stats.get('last_request_at')) == datetime)
        self.assertEqual(stats.get('auth_error_count'), 1)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 0)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)

    @defer.inlineCallbacks
    def test_send_with_auth_success(self):
        stats = HttpAPIStatsCollector().get()

        response = yield self.web.post(b'send', {b'username': self.username,
                                               b'password': b'correct',
                                               b'to': b'06155423',
                                               b'content': 'anycontent'})

        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 1)
        self.assertTrue(type(stats.get('last_request_at')) == datetime)
        self.assertEqual(stats.get('auth_error_count'), 0)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 1)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)
