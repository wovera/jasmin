from jasmin.tools.logs import configure_logger
from jasmin.protocols.smpp.factory import SMPPClientFactory
from twisted.application import service
from .configs import SMPPClientServiceConfig

LOG_CATEGORY = "jasmin-service-smpp"


class SMPPClientService(service.Service):
    def __init__(self, SMPPClientConfig, config):
        self.startCounter = 0
        self.stopCounter = 0
        self.config = config
        self.SMPPClientConfig = SMPPClientConfig
        self.SMPPClientFactory = SMPPClientFactory(SMPPClientConfig)
        self.SMPPClientServiceConfig = SMPPClientServiceConfig(self.config.getConfigFile())

        # Set up a dedicated logger
        self.log = configure_logger(LOG_CATEGORY, self.SMPPClientServiceConfig)

        self.log.info('New SMPPClientService for [%s]', self.SMPPClientConfig.id)

    def startService(self):
        self.startCounter += 1
        service.Service.startService(self)

        self.log.info('Started service for [%s]', self.SMPPClientConfig.id)
        return self.SMPPClientFactory.connectAndBind().addErrback(self._startServiceErr)

    def stopService(self):
        self.stopCounter += 1
        service.Service.stopService(self)

        self.log.info('Stopped service for [%s]', self.SMPPClientConfig.id)
        return self.SMPPClientFactory.disconnectAndDontRetryToConnect()

    def _startServiceErr(self, reason):
        self.log.info('Service starting failed with reason: %s', reason)
        self.stopService()
