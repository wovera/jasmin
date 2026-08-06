"""
Test cases for jasmin.protocols.smpp.operations module.
"""

import binascii
from twisted.trial.unittest import TestCase
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.smpp.operations import SMPPOperationFactory, UnknownMessageStatusError
from smpp.pdu.pdu_types import CommandId, CommandStatus, MessageState
from smpp.pdu.operations import SubmitSM, DeliverSM, DataSM


class OperationsTest(TestCase):
    def setUp(self):
        self.opFactory = SMPPOperationFactory(SMPPClientConfig(id='test-id'))


class SubmitTest(OperationsTest):
    source_addr = b'20203060'
    destination_addr = b'06155423'
    latin1_sm = b'6162636465666768696a6b6c6d6e6f707172737475767778797a'
    latin1_long_sm = b'6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e6162636465666768696a6b6c6d6e6f707172737475767778797a2e'

    def buildSubmitSmTest(self, sm):
        """
        Build a SubmitSm pdu and test if:
         - command_id is correct
         - command_status is ESME_ROCK (default value)
         - destination_addr is the same as self.destination_addr
         - source_addr is the same as self.source_addr
        """

        pdu = self.opFactory.SubmitSM(
            source_addr=self.source_addr,
            destination_addr=self.destination_addr,
            short_message=sm,
        )

        self.assertEqual(pdu.id, CommandId.submit_sm)
        self.assertEqual(pdu.status, CommandStatus.ESME_ROK)
        self.assertEqual(pdu.params['destination_addr'], self.destination_addr)
        self.assertEqual(pdu.params['source_addr'], self.source_addr)

        return pdu

    def test_encode_latin1(self):
        """
        Test that a latin1 short message text remain the same after it's getting
        encoded in a PDU object.
        """

        sm = binascii.a2b_hex(self.latin1_sm)
        pdu = self.buildSubmitSmTest(sm)

        # SM shall not be altered since it is not sliced (not too long)
        self.assertEqual(pdu.params['short_message'], sm)

    def test_encode_latin1_long(self):
        """
        Test that a latin1 short message long text gets successfully sliced into
        multiple PDUs (parts)
        """

        sm = binascii.a2b_hex(self.latin1_long_sm)
        pdu = self.buildSubmitSmTest(sm)

        # The first PDU shall have a next one
        self.assertTrue(isinstance(pdu.nextPdu, SubmitSM))
        # These UDH parameters shall be present in all PDUs
        self.assertTrue(pdu.params['sar_msg_ref_num'] > 0)
        self.assertTrue(pdu.params['sar_total_segments'] > 0)
        self.assertTrue(pdu.params['sar_segment_seqnum'] > 0)

        # Iterating through sliced PDUs
        partedSmPdu = pdu
        assembledSm = b''
        lastSeqNum = 0
        while True:
            assembledSm += partedSmPdu.params['short_message']

            self.assertTrue(partedSmPdu.params['sar_msg_ref_num'] == pdu.params['sar_msg_ref_num'])
            self.assertTrue(partedSmPdu.params['sar_total_segments'] == pdu.params['sar_total_segments'])
            self.assertTrue(partedSmPdu.params['sar_segment_seqnum'] > lastSeqNum)
            lastSeqNum = partedSmPdu.params['sar_segment_seqnum']

            try:
                partedSmPdu = partedSmPdu.nextPdu
            except AttributeError:
                break

        # Assembled SM shall be equal to the original SM
        self.assertEqual(assembledSm, sm)

        # The last seqNum shall be equal to total segments
        self.assertEqual(lastSeqNum, pdu.params['sar_total_segments'])

    def collectPdus(self, sm, data_coding=0, opFactory=None):
        """Return every PDU in the chain, in order."""
        pdu = (opFactory or self.opFactory).SubmitSM(
            source_addr=self.source_addr,
            destination_addr=self.destination_addr,
            short_message=sm,
            data_coding=data_coding,
        )

        pdus = []
        while True:
            pdus.append(pdu)
            try:
                pdu = pdu.nextPdu
            except AttributeError:
                return pdus

    def collectSegments(self, sm, data_coding=0):
        """Return the short_message of every PDU in the chain, in order."""
        return [pdu.params['short_message'] for pdu in self.collectPdus(sm, data_coding)]

    def test_encode_gsm0338_escape_not_split(self):
        """A GSM 03.38 escape sequence must stay whole across a segment boundary (TS 23.040 9.2.3.24.1).

        A fixed-offset slice would end segment one on the lone 0x1B and open segment two with the euro's
        extension code, which a handset renders as the basic-table 'e'.
        """
        # 152 basic septets then euro signs, so an escape pair straddles the 153-septet boundary.
        sm = b'a' * 152 + b'\x1b\x65' * 77

        segments = self.collectSegments(sm)

        self.assertEqual(b''.join(segments), sm)
        for segment in segments:
            self.assertFalse(segment.endswith(b'\x1b'), 'segment ends on a bare escape')
        # Backing each boundary off a septet costs a third segment; dividing 306 septets by 153 would say two.
        self.assertEqual(len(segments), 3)

    def test_encode_ucs2_surrogate_pair_not_split(self):
        """A UTF-16 surrogate pair is indivisible, so it must not straddle a segment boundary."""
        # 66 BMP code units then a non-BMP pair, which lands across the 67-code-unit boundary.
        sm = b'\x00a' * 66 + b'\xd8\x3d\xde\x00' + b'\x00b' * 20

        segments = self.collectSegments(sm, data_coding=8)

        self.assertEqual(b''.join(segments), sm)
        for segment in segments:
            units = [segment[i:i + 2] for i in range(0, len(segment), 2)]
            self.assertFalse(
                units and 0xD800 <= int.from_bytes(units[-1], 'big') <= 0xDBFF,
                'segment ends on a high surrogate',
            )
            self.assertFalse(
                units and 0xDC00 <= int.from_bytes(units[0], 'big') <= 0xDFFF,
                'segment starts on a low surrogate',
            )

    def test_encode_binary_coding_splits_at_fixed_offsets(self):
        """Only GSM 03.38 and UCS2 carry multi-unit characters, so a binary coding must not be second-guessed.

        A 0x1B or a 0xD8-0xDB byte pair is ordinary data under an 8-bit or national coding; backing a boundary off
        there would shorten segments for no reason and could push a message past the parts cap.
        """
        sm = b'\x1b' * 400

        segments = self.collectSegments(sm, data_coding=3)

        self.assertEqual(b''.join(segments), sm)
        # 134 octets per segment for an 8-bit coding, sliced at fixed offsets: 400 = 134 + 134 + 132.
        self.assertEqual([len(segment) for segment in segments], [134, 134, 132])

    def test_encode_udh_split_advertises_the_real_segment_count(self):
        """The UDH concatenation header carries the total, and the HTTP API's default split is udh, not sar.

        The count comes from the boundary-aware split, so the header a handset reassembles by must agree with the
        number of PDUs actually chained.
        """
        udhFactory = SMPPOperationFactory(
            SMPPClientConfig(id='test-id'), long_content_split='udh'
        )
        sm = b'a' * 152 + b'\x1b\x65' * 77

        pdus = self.collectPdus(sm, opFactory=udhFactory)

        self.assertEqual(len(pdus), 3)
        assembled = b''
        for seqnum, pdu in enumerate(pdus, start=1):
            shortMessage = pdu.params['short_message']
            udh, payload = shortMessage[:6], shortMessage[6:]
            self.assertEqual(udh[0:3], b'\x05\x00\x03')  # UDH length, concatenation IEI, IE length
            self.assertEqual(udh[4], len(pdus))  # total parts, as the handset reads it
            self.assertEqual(udh[5], seqnum)
            self.assertFalse(payload.endswith(b'\x1b'), 'segment ends on a bare escape')
            assembled += payload

        self.assertEqual(assembled, sm)

    def test_encode_sar_split_advertises_the_real_segment_count(self):
        """The SAR total must equal the chain length, or reassembly waits forever for a part that never comes."""
        sm = b'a' * 152 + b'\x1b\x65' * 77

        pdus = self.collectPdus(sm)

        # The literal comes first: total and chain length both derive from one variable, so comparing them to each
        # other can never fail. Only a stated count catches a split that stops early.
        self.assertEqual(len(pdus), 3)
        for pdu in pdus:
            self.assertEqual(pdu.params['sar_total_segments'], len(pdus))


class DeliveryParsingTest(OperationsTest):
    def test_is_delivery_standard(self):
        pdu = DeliverSM(
            source_addr='1234',
            destination_addr='4567',
            short_message='id:1891273321 sub:001 dlvrd:001 submit date:1305050826 done date:1305050826 stat:DELIVRD err:000 text:DLVRD TO MOBILE',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], '1891273321')
        self.assertEqual(isDlr['sub'], '001')
        self.assertEqual(isDlr['dlvrd'], '001')
        self.assertEqual(isDlr['sdate'], '1305050826')
        self.assertEqual(isDlr['ddate'], '1305050826')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], '000')
        self.assertEqual(isDlr['text'], 'DLVRD TO MOBILE')

    def test_is_delivery_empty_text(self):
        pdu = DeliverSM(
            source_addr='1234',
            destination_addr='4567',
            short_message='id:1891273321 sub:001 dlvrd:001 submit date:1305050826 done date:1305050826 stat:DELIVRD err:000 text:',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], '1891273321')
        self.assertEqual(isDlr['sub'], '001')
        self.assertEqual(isDlr['dlvrd'], '001')
        self.assertEqual(isDlr['sdate'], '1305050826')
        self.assertEqual(isDlr['ddate'], '1305050826')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], '000')
        self.assertEqual(isDlr['text'], '')

    def test_is_delivery_clickatell_70(self):
        """Related to #70
        Parsing clickatell's DLRs
        """
        pdu = DeliverSM(
            source_addr='1234',
            destination_addr='4567',
            short_message='id:a29f6845555647139e5c8f3b817f2c9a sub:001 dlvrd:001 submit date:141023215253 done date:141023215259 stat:DELIVRD err:000 text:HOLA',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], 'a29f6845555647139e5c8f3b817f2c9a')
        self.assertEqual(isDlr['sub'], '001')
        self.assertEqual(isDlr['dlvrd'], '001')
        self.assertEqual(isDlr['sdate'], '141023215253')
        self.assertEqual(isDlr['ddate'], '141023215259')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], '000')
        self.assertEqual(isDlr['text'], 'HOLA')

    def test_is_delivery_jasmin_153(self):
        """Related to #153
        Parsing jasmin's DLRs
        """
        pdu = DeliverSM(
            source_addr='1234',
            destination_addr='4567',
            short_message='id:4a38dc46-5125-4969-90be-72104c340d5c sub:001 dlvrd:001 submit date:150519232657 done date:150519232657 stat:DELIVRD err:000 text:-',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], '4a38dc46-5125-4969-90be-72104c340d5c')
        self.assertEqual(isDlr['sub'], '001')
        self.assertEqual(isDlr['dlvrd'], '001')
        self.assertEqual(isDlr['sdate'], '150519232657')
        self.assertEqual(isDlr['ddate'], '150519232657')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], '000')
        self.assertEqual(isDlr['text'], '-')

    def test_is_delivery_jasmin_195(self):
        """Related to #195
        Mandatory fields in short_message are parsed and optional fields are set to defaults when
        they dont exist in short_message"""
        pdu = DeliverSM(
            source_addr='1234',
            destination_addr='4567',
            short_message='id:c87c2273-7edb-4bc7-8d3a-7f57f21b625e submit date:201506201641 done date:201506201641 stat:DELIVRD err:000',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], 'c87c2273-7edb-4bc7-8d3a-7f57f21b625e')
        self.assertEqual(isDlr['sub'], 'ND')
        self.assertEqual(isDlr['dlvrd'], 'ND')
        self.assertEqual(isDlr['sdate'], '201506201641')
        self.assertEqual(isDlr['ddate'], '201506201641')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], '000')
        self.assertEqual(isDlr['text'], '')

    def test_is_delivery_goip(self):
        """Received err:0 to err:000, sub:1 to sub:001, dlvrd:13 to dlvrd:013 and Text to text"""

        pdu = DeliverSM(
            source_addr='12345',
            destination_addr='45678',
            short_message='id:68673723 sub:1 dlvrd:13 submit date:1909301545 done date:1909301545 stat:DELIVRD err:0 Text:\x04\x1a',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEquals(isDlr['id'], '68673723')
        self.assertEquals(isDlr['sub'], '001')
        self.assertEquals(isDlr['dlvrd'], '013')
        self.assertEquals(isDlr['sdate'], '1909301545')
        self.assertEquals(isDlr['ddate'], '1909301545')
        self.assertEquals(isDlr['stat'], 'DELIVRD')
        self.assertEquals(isDlr['err'], '000')
        self.assertEquals(isDlr['text'], '\x04\x1a')

    def test_is_delivery_mmg_deliver_sm_224(self):
        """Related to #224, this is a Sicap's MMG deliver_sm receipt"""
        pdu = DeliverSM(
            source_addr='24206155423',
            destination_addr='JOOKIES',
            short_message='362d9701 2',
            message_state=MessageState.DELIVERED,
            receipted_message_id='362d9701',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], b'362d9701')
        self.assertEqual(isDlr['sub'], 'ND')
        self.assertEqual(isDlr['dlvrd'], 'ND')
        self.assertEqual(isDlr['sdate'], 'ND')
        self.assertEqual(isDlr['ddate'], 'ND')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], 'ND')
        self.assertEqual(isDlr['text'], '')

    def test_is_delivery_mmg_data_sm_92(self):
        """Related to #92, this is a Sicap's MMG data_sm receipt"""
        pdu = DataSM(
            source_addr='24206155423',
            destination_addr='JOOKIES',
            message_state=MessageState.DELIVERED,
            receipted_message_id='362d9701',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertTrue(isDlr is not None)
        self.assertEqual(isDlr['id'], b'362d9701')
        self.assertEqual(isDlr['sub'], 'ND')
        self.assertEqual(isDlr['dlvrd'], 'ND')
        self.assertEqual(isDlr['sdate'], 'ND')
        self.assertEqual(isDlr['ddate'], 'ND')
        self.assertEqual(isDlr['stat'], 'DELIVRD')
        self.assertEqual(isDlr['err'], 'ND')
        self.assertEqual(isDlr['text'], '')

    def test_take_msgid_from_tlv_first(self):
        """Related to #427
        When reciept_message_id is provided in short_message and in TLV param, consider the latter"""
        pdu = DataSM(
            source_addr='24206155423',
            destination_addr='JOOKIES',
            message_state=MessageState.DELIVERED,
            receipted_message_id='6000',
            short_message='id:5000 submit date:201506201641 done date:201506201641 stat:DELIVRD err:000',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertEqual(isDlr['id'], b'6000')

    def test_take_message_state_from_tlv_first(self):
        """Related to #427
        When message_state is provided in short_message and in TLV param, consider the latter"""
        pdu = DataSM(
            source_addr='24206155423',
            destination_addr='JOOKIES',
            message_state=MessageState.ACCEPTED,
            receipted_message_id='5000',
            short_message='id:5000 submit date:201506201641 done date:201506201641 stat:DELIVRD err:000',
        )

        isDlr = self.opFactory.isDeliveryReceipt(pdu)
        self.assertEqual(isDlr['stat'], 'ACCEPTD')


class ReceiptCreationTestCases(OperationsTest):
    message_state_map = {
        'ESME_ROK': {'sm': 'ACCEPTD', 'state': MessageState.ACCEPTED},
        'UNDELIV': {'sm': 'UNDELIV', 'state': MessageState.UNDELIVERABLE},
        'REJECTD': {'sm': 'REJECTD', 'state': MessageState.REJECTED},
        'DELIVRD': {'sm': 'DELIVRD', 'state': MessageState.DELIVERED},
        'EXPIRED': {'sm': 'EXPIRED', 'state': MessageState.EXPIRED},
        'DELETED': {'sm': 'DELETED', 'state': MessageState.DELETED},
        'ACCEPTD': {'sm': 'ACCEPTD', 'state': MessageState.ACCEPTED},
        'UNKNOWN': {'sm': 'UNKNOWN', 'state': MessageState.UNKNOWN},
    }

    def test_unknown_message_state(self):
        for dlr_pdu in ['deliver_sm', 'data_sm']:
            self.assertRaises(UnknownMessageStatusError, self.opFactory.getReceipt,
                              dlr_pdu,
                              'anyid',
                              'JASMIN',
                              '06155423',
                              'ANY_STATus',
                              1,
                              '2017-07-19 17:50:12',
                              'UNKNOWN',
                              'UNKNOWN',
                              'UNKNOWN',
                              'UNKNOWN')

    def test_deliver_sm(self):
        for message_state, _test in self.message_state_map.items():
            pdu = self.opFactory.getReceipt(
                'deliver_sm',
                'anyid',
                'JASMIN',
                '06155423',
                message_state,
                1,
                '2017-07-19 17:50:12',
                'UNKNOWN',
                'UNKNOWN',
                'UNKNOWN',
                'UNKNOWN')

            self.assertEqual(pdu.params['message_state'], _test['state'])
            self.assertTrue(('stat:%s' % _test['sm']).encode() in pdu.params['short_message'])

        # Test other ESME_* states:
        pdu = self.opFactory.getReceipt(
            'deliver_sm',
            'anyid',
            'JASMIN',
            '06155423',
            'ESME_RTHROTTLED',
            3,
            '2017-07-19 17:50:12',
            'UNKNOWN',
            'UNKNOWN',
            'UNKNOWN',
            'UNKNOWN')

        self.assertEqual(pdu.params['message_state'], MessageState.UNDELIVERABLE)
        self.assertTrue(b'stat:UNDELIV' in pdu.params['short_message'])

    def test_data_sm(self):
        for message_state, _test in self.message_state_map.items():
            pdu = self.opFactory.getReceipt(
                'data_sm',
                'anyid',
                'JASMIN',
                '06155423',
                message_state,
                2,
                '2017-07-19 17:50:12',
                'UNKNOWN',
                'UNKNOWN',
                'UNKNOWN',
                'UNKNOWN')

            self.assertEqual(pdu.params['message_state'], _test['state'])

        # Test other ESME_* states:
        pdu = self.opFactory.getReceipt(
            'data_sm',
            'anyid',
            'JASMIN',
            '06155423',
            'ESME_RTHROTTLED',
            2,
            '2017-07-19 17:50:12',
            'UNKNOWN',
            'UNKNOWN',
            'UNKNOWN',
            'UNKNOWN')

        self.assertEqual(pdu.params['message_state'], MessageState.UNDELIVERABLE)
