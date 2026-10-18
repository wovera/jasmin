import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

STRUCTURED_FIELDS = ('cid', 'msgid', 'trace_id')
MASKED_ADDRESS = '***'


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            'ts': datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec='milliseconds'),
            'level': record.levelname,
            'logger': record.name,
            'msg': record.getMessage(),
        }
        for field in STRUCTURED_FIELDS:
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        if record.exc_info:
            entry['exc'] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def mask_address(address, log_privacy):
    return MASKED_ADDRESS if log_privacy else address


def configure_logger(name, config):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    if 'stdout' in config.log_file:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = TimedRotatingFileHandler(
            filename=config.log_file, when=config.log_rotate, backupCount=config.log_backup_count)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(config.log_level)
    logger.propagate = False
    return logger
