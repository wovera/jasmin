import csv
import os

import messaging.sms.gsm0338  # noqa: F401  (registers the 'gsm0338' codec used below)
from twisted.trial.unittest import TestCase

from jasmin.protocols.smpp.operations import SMPPOperationFactory, count_pdus

# The platform counts segments in C# before it calls us, and its web console counts them again in TypeScript to tell
# the writer what the message will cost. All three must agree, or someone is billed for segments the screen never
# showed. Rather than three transcriptions of TS 23.038 drifting apart, they are pinned to one table of worked cases
# that lives in the platform repo; this is the engine's side of that pin.
CASES = os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..', '..', '..',
    'docs', 'reference', 'spec-tables', 'segment-plan-cases.csv',
)

GSM7_CODING = 0
UCS2_CODING = 8


def _body(codepoints):
    """"0041x160 20AC" - hex code points, each optionally repeated, so a case carrying a line feed or an astral
    character stays one CSV cell."""
    text = ''
    for token in codepoints.split(' '):
        if not token:
            continue
        code, _, repeat = token.partition('x')
        text += chr(int(code, 16)) * (int(repeat) if repeat else 1)

    return text


def _cases():
    with open(CASES, encoding='utf-8') as handle:
        rows = [line for line in handle if not line.startswith('#')]

    return list(csv.DictReader(rows))


class SegmentCountConformanceTestCase(TestCase):
    """Every worked case splits into the number of segments the shared table states."""

    def test_every_case_splits_into_the_segment_count_the_table_states(self):
        if not os.path.exists(CASES):
            raise self.skipTest(
                'The shared case table lives in the platform repo; the engine is checked out on its own here.'
            )

        factory = SMPPOperationFactory()
        for case in _cases():
            text = _body(case['codepoints'])
            # Mirrors what the HTTP API does with the caller's content before handing it to the splitter.
            if case['encoding'] == 'gsm7':
                coding, short_message = GSM7_CODING, text.encode('gsm0338', 'replace')
            else:
                coding, short_message = UCS2_CODING, text.encode('utf_16_be')

            pdu = factory.SubmitSM(
                source_addr='1423',
                destination_addr='06155423',
                short_message=short_message,
                data_coding=coding,
            )

            self.assertEqual(
                count_pdus(pdu),
                int(case['segments']),
                'case %s: the platform expects %s segments' % (case['case'], case['segments']),
            )
