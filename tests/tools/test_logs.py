import json
import logging
import os
import tempfile
from types import SimpleNamespace

from twisted.trial import unittest

from jasmin.tools.logs import JsonFormatter, configure_logger, mask_address


def make_record(**extra):
    record = logging.LogRecord('test', logging.INFO, __file__, 1, 'sent %s', ('ok',), None)
    record.__dict__.update(extra)
    return record


class JsonFormatterTestCase(unittest.TestCase):
    def test_one_object_with_core_fields(self):
        entry = json.loads(JsonFormatter().format(make_record()))
        self.assertEqual(entry['level'], 'INFO')
        self.assertEqual(entry['logger'], 'test')
        self.assertEqual(entry['msg'], 'sent ok')
        self.assertIn('ts', entry)
        self.assertNotIn('cid', entry)
        self.assertNotIn('trace_id', entry)

    def test_structured_fields_pass_through_extra(self):
        entry = json.loads(JsonFormatter().format(make_record(cid='c1', msgid='m1', trace_id='t1')))
        self.assertEqual((entry['cid'], entry['msgid'], entry['trace_id']), ('c1', 'm1', 't1'))


class MaskAddressTestCase(unittest.TestCase):
    def test_masks_only_under_privacy(self):
        self.assertEqual(mask_address('251911000000', True), '***')
        self.assertEqual(mask_address('251911000000', False), '251911000000')


class ConfigureLoggerTestCase(unittest.TestCase):
    def test_file_rotation_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            config = SimpleNamespace(
                log_level=logging.INFO,
                log_file=os.path.join(directory, 'x.log'),
                log_rotate='midnight',
                log_backup_count=3)
            logger = configure_logger('test-configure-logger-file', config)
            self.addCleanup(lambda: [logger.removeHandler(h) or h.close() for h in list(logger.handlers)])
            self.assertEqual(logger.handlers[0].backupCount, 3)
            self.assertFalse(logger.propagate)

    def test_second_call_keeps_the_handler(self):
        config = SimpleNamespace(log_level=logging.INFO, log_file='stdout', log_rotate='W6', log_backup_count=7)
        logger = configure_logger('test-configure-logger-stdout', config)
        self.addCleanup(lambda: [logger.removeHandler(h) for h in list(logger.handlers)])
        self.assertIs(configure_logger('test-configure-logger-stdout', config).handlers[0], logger.handlers[0])
