from twisted.internet import defer

from .test_server import HTTPApiTestCases


class RevokeTestCases(HTTPApiTestCases):
    @defer.inlineCallbacks
    def test_revoke_writes_the_key_with_a_ttl(self):
        response = yield self.web.post(b'revoke', {b'username': 'nathalie', b'password': 'correct',
                                                   b'batch': b'b-1', b'ttl': b'3600'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(response.value(), b'Success')
        self.clientManager_f.redisClient.set.assert_called_once_with('revoked:batch:b-1', '1', expire=3600)

    @defer.inlineCallbacks
    def test_revoke_rejects_a_zero_ttl(self):
        response = yield self.web.post(b'revoke', {b'username': 'nathalie', b'password': 'correct',
                                                   b'batch': b'b-1', b'ttl': b'0'})
        self.assertEqual(response.responseCode, 400)
        self.clientManager_f.redisClient.set.assert_not_called()

    @defer.inlineCallbacks
    def test_revoke_needs_a_batch_or_an_account(self):
        response = yield self.web.post(b'revoke', {b'username': 'nathalie', b'password': 'correct',
                                                   b'ttl': b'3600'})
        self.assertEqual(response.responseCode, 400)
