from prometheus_client.core import GaugeMetricFamily, UnknownMetricFamily
from prometheus_client.twisted import MetricsResource

from jasmin.protocols.http.stats import HttpAPIStatsCollector
from jasmin.protocols.smpp.stats import SMPPClientStatsCollector, SMPPServerStatsCollector
from jasmin.tools.metrics import REGISTRY

# Existing series names are read by the Grafana dashboards, so they keep their exact name. A CounterMetricFamily
# would rename them to <name>_total, so cumulative values are exposed under the old name as untyped (rate() works on
# it); current values are gauges, whose name never changes.
COUNTER, GAUGE = 'counter', 'gauge'

HTTPAPI = {
    'request_count':            (COUNTER, 'Http request count.'),
    'interceptor_count':        (COUNTER, 'Successful http request count.'),
    'auth_error_count':         (COUNTER, 'Authentication error count.'),
    'route_error_count':        (COUNTER, 'Routing error count.'),
    'interceptor_error_count':  (COUNTER, 'Interceptor error count.'),
    'throughput_error_count':   (COUNTER, 'Throughput exceeded error count.'),
    'charging_error_count':     (COUNTER, 'Charging error count.'),
    'server_error_count':       (COUNTER, 'Server error count.'),
    'success_count':            (COUNTER, 'Successful http request count.'),
}
SMPPC = {
    'connected_count':          (COUNTER, 'Cumulated number of successful connections.'),
    'disconnected_count':       (COUNTER, 'Cumulated number of disconnections.'),
    'bound_count':              (COUNTER, 'Cumulated number of successful binds.'),
    'submit_sm_request_count':  (COUNTER, 'SubmitSm pdu requests count.'),
    'submit_sm_count':          (COUNTER, 'Complete SubmitSm transactions count.'),
    'deliver_sm_count':         (COUNTER, 'DeliverSm pdu requests count.'),
    'data_sm_count':            (COUNTER, 'Complete DataSm transactions count.'),
    'interceptor_count':        (COUNTER, 'Interceptor calls count.'),
    'elink_count':              (COUNTER, 'EnquireLinks count.'),
    'throttling_error_count':   (COUNTER, 'Throttling errors count.'),
    'interceptor_error_count':  (COUNTER, 'Interception errors count.'),
    'other_submit_error_count': (COUNTER, 'Other errors count.'),
}
SMPPS_API = {
    'connected_count':          (GAUGE, 'Number of connected sessions.'),
    'connect_count':            (COUNTER, 'Cumulated number of connect requests.'),
    'disconnect_count':         (COUNTER, 'Cumulated number of disconnect requests.'),
    'interceptor_count':        (COUNTER, 'Interceptor calls count.'),
    'bound_trx_count':          (GAUGE, 'Number of bound sessions in transceiver mode.'),
    'bound_rx_count':           (GAUGE, 'Number of bound sessions in receiver mode.'),
    'bound_tx_count':           (GAUGE, 'Number of bound sessions in transmitter mode.'),
    'bind_trx_count':           (COUNTER, 'Number of bind requests in transceiver mode.'),
    'bind_rx_count':            (COUNTER, 'Number of bind requests in receiver mode.'),
    'bind_tx_count':            (COUNTER, 'Number of bind requests in transmitter mode.'),
    'unbind_count':             (COUNTER, 'Cumulated number of unbind requests.'),
    'submit_sm_request_count':  (COUNTER, 'SubmitSm pdu requests count.'),
    'submit_sm_count':          (COUNTER, 'Complete SubmitSm transactions count.'),
    'deliver_sm_count':         (COUNTER, 'DeliverSm pdu requests count.'),
    'data_sm_count':            (COUNTER, 'Complete DataSm transactions count.'),
    'elink_count':              (COUNTER, 'EnquireLinks count.'),
    'throttling_error_count':   (COUNTER, 'Throttling errors count.'),
    'interceptor_error_count':  (COUNTER, 'Interception errors count.'),
    'other_submit_error_count': (COUNTER, 'Other errors count.'),
}


def build_family(prefix, metric, descriptor, labels):
    kind, doc = descriptor
    family = GaugeMetricFamily if kind == GAUGE else UnknownMetricFamily
    return family('%s_%s' % (prefix, metric), doc, labels=labels)


class StatsCollector:
    """Exposes the in-memory dict stats: one family per metric, one sample per connector."""
    SMPPClientManagerPB = None

    def collect(self):
        for metric, descriptor in HTTPAPI.items():
            family = build_family('httpapi', metric, descriptor, [])
            family.add_metric([], HttpAPIStatsCollector().get().get(metric))
            yield family

        connectors = self.SMPPClientManagerPB.perspective_connector_list()
        for metric, descriptor in SMPPC.items():
            family = build_family('smppc', metric, descriptor, ['cid'])
            for connector in connectors:
                family.add_metric([connector['id']], SMPPClientStatsCollector().get(connector['id']).get(metric))
            yield family

        bind_state = GaugeMetricFamily('smppc_bind_state', 'Current SMPP session state of the connector.',
                                       labels=['cid', 'state'])
        for connector in connectors:
            bind_state.add_metric([connector['id'], str(connector['session_state'])], 1)
        yield bind_state

        servers = list(SMPPServerStatsCollector().connectors.items())
        for metric, descriptor in SMPPS_API.items():
            family = build_family('smppsapi', metric, descriptor, ['cid'])
            for cid, server in servers:
                family.add_metric([cid], server.get(metric))
            yield family


STATS_COLLECTOR = StatsCollector()
REGISTRY.register(STATS_COLLECTOR)


def Metrics(SMPPClientManagerPB):
    STATS_COLLECTOR.SMPPClientManagerPB = SMPPClientManagerPB
    return MetricsResource(registry=REGISTRY)
