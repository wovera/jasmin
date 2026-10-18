from prometheus_client import CollectorRegistry, Counter, Histogram

REGISTRY = CollectorRegistry()

SUBMIT_SM_RESP = Counter('smppc_submit_sm_resp', 'submit_sm_resp received, by SMPP command_status.',
                         ['cid', 'status'], registry=REGISTRY)
SUBMIT_SM_ROUNDTRIP = Histogram('smppc_submit_sm_roundtrip_seconds', 'submit_sm to submit_sm_resp round trip.',
                                ['cid'], registry=REGISTRY)
RECEIPTS = Counter('smppc_receipts', 'Delivery receipts received, by message_state.',
                   ['cid', 'state'], registry=REGISTRY)
DISCARDS = Counter('smppc_discards', 'submit_sm discarded before or after the wire, by reason.',
                   ['cid', 'reason'], registry=REGISTRY)
SEND_RESPONSES = Counter('httpapi_send_responses', '/send responses, by HTTP code.', ['code'], registry=REGISTRY)

# Carriers send free text in the receipt stat field: anything else is folded so labels stay bounded.
RECEIPT_STATES = ('DELIVRD', 'EXPIRED', 'DELETED', 'UNDELIV', 'ACCEPTD', 'UNKNOWN', 'REJECTD', 'ENROUTE')
