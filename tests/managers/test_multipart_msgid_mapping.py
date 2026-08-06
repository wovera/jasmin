"""
Test cases for mapping every segment of a long message back to its submit
"""

from types import SimpleNamespace
from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from smpp.pdu.pdu_types import CommandId, CommandStatus

from jasmin.managers.content import DLR
from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.dlr import DLRLookup, acknowledged_smpp_msgids
from jasmin.managers.listeners import segment_smpp_msgids


def submit_sm_resp(message_id):
    return SimpleNamespace(params={'message_id': message_id})


def parted_submit(message_ids):
    """A request PDU chain whose segments were each acknowledged with their own id."""
    segments = []
    for message_id in message_ids:
        segment = SimpleNamespace()
        if message_id is not None:
            segment.response = submit_sm_resp(message_id)
        segments.append(segment)

    for current, following in zip(segments, segments[1:]):
        current.nextPdu = following

    return segments[0]


def published_dlr(content):
    return SimpleNamespace(content=SimpleNamespace(properties=content.properties))


class SegmentMsgidCollectionTestCase(TestCase):
    def test_collects_every_segment_in_send_order(self):
        self.assertEqual(
            [b'a', b'b', b'c'],
            segment_smpp_msgids(parted_submit([b'a', b'b', b'c'])))

    def test_single_submit_carries_no_per_segment_response(self):
        self.assertEqual([], segment_smpp_msgids(SimpleNamespace()))

    def test_skips_a_segment_that_was_never_acknowledged(self):
        self.assertEqual(
            [b'a', b'c'],
            segment_smpp_msgids(parted_submit([b'a', None, b'c'])))


class AcknowledgedMsgidsTestCase(TestCase):
    def test_every_segment_maps_back_to_the_submit(self):
        content = DLR(pdu_type=CommandId.submit_sm_resp, msgid=1, status=CommandStatus.ESME_ROK,
                      smpp_msgid=b'0c', smpp_msgids=[b'0a', b'0b', b'0c'])

        # Normalised exactly like the single id, which is the form a receipt is looked up by.
        self.assertEqual('C', content.properties['headers']['smpp_msgid'])
        self.assertEqual(['A', 'B', 'C'], acknowledged_smpp_msgids(published_dlr(content)))

    def test_single_submit_maps_its_only_id(self):
        content = DLR(pdu_type=CommandId.submit_sm_resp, msgid=1, status=CommandStatus.ESME_ROK,
                      smpp_msgid=b'2', smpp_msgids=[])

        self.assertNotIn('smpp_msgids', content.properties['headers'])
        self.assertEqual(['2'], acknowledged_smpp_msgids(published_dlr(content)))


class RedisFanOutTestCase(TestCase):
    """The map write must happen once per acknowledged segment, not once per message.

    Collecting every segment's id is useless if only one map is written: a receipt quoting any other segment
    still finds nothing and is dropped after its retrials.
    """

    def setUp(self):
        config = DLRLookupConfig()
        self.redisClient = Mock()
        self.redisClient.hgetall.return_value = defer.succeed(
            {'sc': 'httpapi', 'url': '', 'level': 2, 'method': 'POST', 'expiry': 86400, 'connector': 'abc'})
        self.redisClient.hmset.return_value = defer.succeed(True)
        self.redisClient.expire.return_value = defer.succeed(True)
        self.redisClient.delete.return_value = defer.succeed(True)

        amqpBroker = Mock()
        amqpBroker.chan.basic_ack.return_value = defer.succeed(None)
        self.dlrLookup = DLRLookup(config, amqpBroker, self.redisClient)
        self.dlrLookup.ackMessage = Mock(return_value=defer.succeed(None))

    def submitSmRespMessage(self, smpp_msgids):
        content = SimpleNamespace(
            body=b'ESME_ROK',
            properties={'message-id': 'the-queue-msgid',
                        'headers': {'smpp_msgids': smpp_msgids, 'smpp_msgid': smpp_msgids.split(',')[-1]}})
        return SimpleNamespace(content=content, delivery_tag=1)

    @defer.inlineCallbacks
    def test_every_acknowledged_segment_gets_its_own_map(self):
        yield self.dlrLookup.submit_sm_resp_dlr_callback(self.submitSmRespMessage('A,B,C'))

        mapped = [call.args[0] for call in self.redisClient.hmset.call_args_list]
        self.assertEqual(['queue-msgid:A', 'queue-msgid:B', 'queue-msgid:C'], mapped)
        for call in self.redisClient.hmset.call_args_list:
            self.assertEqual('the-queue-msgid', call.args[1]['msgid'])
        # Every key gets its own expiry, or a mapped segment outlives or predeceases its siblings.
        expired = [call.args[0] for call in self.redisClient.expire.call_args_list]
        self.assertEqual(mapped, expired)

    @defer.inlineCallbacks
    def test_a_single_submit_still_maps_its_one_id(self):
        yield self.dlrLookup.submit_sm_resp_dlr_callback(self.submitSmRespMessage('SOLO'))

        mapped = [call.args[0] for call in self.redisClient.hmset.call_args_list]
        self.assertEqual(['queue-msgid:SOLO'], mapped)
