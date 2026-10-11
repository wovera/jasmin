import pickle
from unittest.mock import Mock

from twisted.internet import defer
from smpp.pdu.constants import priority_flag_value_map

from jasmin.protocols.smpp.configs import SMPPClientConfig
from .test_server import HTTPApiTestCases


class LaneTestCases(HTTPApiTestCases):
    username = 'nathalie'

    def setUp(self):
        HTTPApiTestCases.setUp(self)
        self.clientManager_f.perspective_submit_sm = Mock(return_value=defer.succeed(('msgid', 'cid')))

    @defer.inlineCallbacks
    def test_unknown_lane_is_refused(self):
        response = yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                                 b'to': b'06155423', b'content': 'x', b'lane': b'urgent'})
        self.assertEqual(response.responseCode, 400)

    @defer.inlineCallbacks
    def test_bulk_lane_publishes_at_lowest_precedence(self):
        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x', b'lane': b'bulk'})
        self.assertEqual(self.clientManager_f.perspective_submit_sm.call_args.kwargs['priority'], 0)

    @defer.inlineCallbacks
    def test_transactional_is_the_default_lane(self):
        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x'})
        self.assertEqual(self.clientManager_f.perspective_submit_sm.call_args.kwargs['priority'], 2)

    @defer.inlineCallbacks
    def test_express_lane_publishes_ahead_of_transactional(self):
        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x', b'lane': b'express'})
        self.assertEqual(self.clientManager_f.perspective_submit_sm.call_args.kwargs['priority'], 3)

    @defer.inlineCallbacks
    def test_priority_flag_is_clamped_to_the_connectors_default_ceiling(self):
        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x', b'priority': b'3'})
        pdu = self.clientManager_f.perspective_submit_sm.call_args.kwargs['SubmitSmPDU']
        self.assertEqual(pdu.params['priority_flag'], priority_flag_value_map[0])

    @defer.inlineCallbacks
    def test_priority_flag_is_clamped_to_a_raised_connector_ceiling(self):
        self.clientManager_f.perspective_connector_config = Mock(
            return_value=pickle.dumps(SMPPClientConfig(id='abc', priority_flag_max=2)))

        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x', b'priority': b'3'})
        pdu = self.clientManager_f.perspective_submit_sm.call_args.kwargs['SubmitSmPDU']
        self.assertEqual(pdu.params['priority_flag'], priority_flag_value_map[2])

        yield self.web.post(b'send', {b'username': self.username, b'password': b'correct',
                                      b'to': b'06155423', b'content': 'x', b'priority': b'1'})
        pdu = self.clientManager_f.perspective_submit_sm.call_args.kwargs['SubmitSmPDU']
        self.assertEqual(pdu.params['priority_flag'], priority_flag_value_map[1])
