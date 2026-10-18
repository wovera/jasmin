import logging
from types import SimpleNamespace

from twisted.web import server

from jasmin.tools.logs import configure_logger

LOG_CATEGORY = "jasmin-http-access"


class AccessLogSite(server.Site):
    """
    A Site whose access log goes through the shared logging setup: stdout when config.access_log contains
    'stdout', a bounded rotating file otherwise. The query string is never logged, it carries credentials.
    """

    def __init__(self, resource, config):
        server.Site.__init__(self, resource)
        self.accessLog = configure_logger(LOG_CATEGORY, SimpleNamespace(
            log_level=logging.INFO,
            log_file=config.access_log,
            log_rotate=config.log_rotate,
            log_backup_count=config.log_backup_count))

    def log(self, request):
        self.accessLog.info(
            '%s %s %s %s %s',
            request.getClientAddress().host,
            request.method.decode('ascii', 'replace'),
            request.path.decode('ascii', 'replace'),
            request.code,
            request.sentLength)
