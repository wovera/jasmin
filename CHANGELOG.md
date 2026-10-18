# Changelog

Notable changes in Wovera's fork of Jasmin, relative to upstream `github.com/jookies/jasmin`. This records
*what* changed; the design rationale and validation live in Wovera's internal engineering docs. General
bug-fixes here are offered upstream where practical. See [`WOVERA.md`](WOVERA.md) for the fork's scope.

The format is based on [Keep a Changelog](https://keepachangelog.com/). This fork is not versioned against
upstream releases, so changes are listed under an open **Unreleased** section, grouped by the date each landed
on the fork mainline (newest first).

## Unreleased

### 2026-10-17

- **Changed — logs are JSON lines, set up in one place, and private.** Every component now configures its logger
  through `jasmin.tools.logs.configure_logger` (`logging.config.dictConfig`) instead of a copied handler block. Each line
  is one JSON object (`ts`, `level`, `logger`, `msg`, plus `cid`, `msgid`, `trace_id` when present). Rotated files are
  bounded by the new `log_backup_count` option (default 7). Under `log_privacy`, the `from` and `to` numbers are masked
  in SMS-MT and SMS-MO lines too, and request arguments (which carry the password) are never logged. `AccessLogSite`
  sends the HTTP access log through the same setup without the query string.

### 2026-10-05

- **Fixed — a submit that reached the wire is not sent again while its claim lives.** Immediately before a
  `submit_sm` leaves, the listener takes a Redis claim `wire:<msgid>` (SET NX). Every submit has a deadline - its
  validity, else the connector's `dlr_expiry` after it was queued - and is not sent past it; the claim lives until
  `dlr_expiry` after that deadline. A delivery whose claim is already held - an AMQP redelivery after a channel or
  process loss - is settled before any expiry, readiness, TLV or revocation check: it replays the recorded outcome
  of an answered submit, else it is discarded with the new `dlr.discard` reason `in_doubt`. Any failure after the
  request was encoded and written to the SMPP session (response timeout, failed long transaction, an unbind
  cancelling the transaction) is discarded `in_doubt` instead of being requeued; only a `generic_nack` answering
  the request or a failure before transmission keeps the `exception` reason. A submit whose segments cannot all
  leave before its deadline, measured once the claim is taken, releases the claim and is discarded `expired`
  unsent; the send deadline is derived from that same measurement. The claim is released only when
  every part of the submit was refused with a retryable status, and the message is requeued only once that
  release succeeded; while Redis refuses it the release is retried every `requeue_delay`. If the claim cannot be
  read or taken the submit is held back and requeued. A listener
  without a Redis client therefore sends nothing: every submit is held back the same way. The guarantee rests on
  Redis keeping the claim; a Redis that loses its data loses it. An `in_doubt` discard keeps the message's
  receipt map, so a late acknowledgement and its receipts still resolve.
- **Fixed — a long message is judged across all its segments.** Once any segment is accepted the message is never
  requeued whole. Accepted in every segment, it settles `ESME_ROK` and maps receipts for each segment. Accepted
  only in part, it settles with its first refused segment's status, the new `accepted_segments` count (header on
  `dlr.submit_sm_resp`, field in the forwarded outcome) and the accepted segments' SMSC ids (`smpp_msgids`
  header), which are mapped so their receipts resolve; its dlr map is then kept until it expires. Billing charges
  accepted segments only. Only a message refused in every segment is retried. On a paced connector the segments
  leave one interval of the learned rate apart instead of back to back, the long transaction's deadline grows by
  the pacing, a message whose last segment would leave after its deadline is discarded `expired` unsent, and a
  segment due after the bind dropped or after the deadline fails the message `in_doubt` without being sent. A
  segment answered `generic_nack` or timing out fails the long transaction at once, so no later segment leaves
  for it. A long message that fails after any segment was accepted is never resent: it settles as a
  `dlr.submit_sm_resp` with body `DISCARDED`, `discard_reason: in_doubt`, the accepted count in
  `accepted_segments` and the accepted SMSC ids in `smpp_msgids`, which are mapped so their receipts resolve;
  the forwarded outcome carries `command_status: DISCARDED`, `discard_reason: in_doubt` and `accepted_segments`.
- **Changed — an unconfirmed `/send` publish keeps its `msgid` claim.** A publish the broker neither confirmed nor
  refused answers `503` with `Jasmin-Error-Code: submit_in_doubt` and keeps `idem:<msgid>`, so a retry replays
  the first outcome instead of queueing the message twice. A publish the broker refused (`Nack`, unroutable) or
  never attempted on a closed channel still releases the claim and answers `503 submit_not_confirmed`.
- **Fixed — an unroutable `/send` publish is never reported confirmed.** The submit channel publishes one submit at
  a time through its confirmation, as `publish_confirmed` does: pika matches a `basic.return` to no particular
  publication, so a return and a combined `Ack` could confirm the unroutable submit and fail a routed one.
- **Fixed — a `smppc_not_ready` discard releases the `msgid` claim** once its receipt is confirmed (or parked),
  so the caller's later retry is submitted rather than answered with the acceptance of a message never sent, and
  an unconfirmed discard retried later cannot release a claim the caller has since taken again. If the claim
  cannot be released the message is requeued and its discard published again; a retry arriving before the
  release succeeds is still answered with the original acceptance.
- **Fixed — an accepted submit is acked only once its receipt is confirmed.** The `dlr.submit_sm_resp` receipt is
  published on a confirm channel before the submit is acked, and recorded in `outcome:<msgid>` (expiring with the
  wire claim) and in the listener's memory, so a redelivery to the same process replays it even when Redis did
  not take the record, and a redelivery to the same process replays it even after the claim expired. An
  unconfirmed receipt requeues the submit with a doubling delay from at least one second, capped at 300 seconds;
  its redelivery replays the receipt instead of sending the SMS. After five unconfirmed publications the receipt
  is published to the durable `dlr.submit_sm_resp.parked` queue and the submit is acked once that is confirmed;
  a receipt that cannot be parked either keeps being retried. A billing request is
  published once per answer, unconfirmed as upstream, and never replayed: the router's charge is not idempotent,
  so a lost request under-charges rather than a replay charging twice.
- **Changed — forwarded outcomes are published mandatory.** With `[dlr] dlr_forward_queue` set, the listener
  publishes submit receipts, discards and delivery receipts mandatory, so one no DLR lookup (embedded with
  `--enable-dlr-lookup`, or the standalone `dlrlookupd`) is bound to receive is retried, then parked. With it
  unset they are confirmed but not mandatory, so a deployment without a DLR lookup drops them as upstream.
- **Fixed — an acknowledged MO survives a broker restart.** The listener publishes inbound messages on a confirm
  channel, mandatory, and answers the carrier `ESME_ROK` only after confirmation; an unavailable broker fails the
  publish, so the carrier resends. MO envelopes are persistent and the router's queue and the DLR lookup's queue
  are durable under new names, `RouterPB_deliver_sm_all.durable` and `DLRLookup-<pid>.durable`; the legacy
  non-durable queues are no longer used. One-time deploy step: delete the legacy queues `RouterPB_deliver_sm_all`
  and `DLRLookup-<pid>` if they exist. Receipt and discard envelopes are persistent. Identical MOs are all
  forwarded: SMPP 3.4 gives an MO no identifier, so no content-based duplicate check is made. Not covered: a part
  of a long MO is acknowledged once stored in Redis, so the incomplete message lives only in its 300-second Redis
  hash, which a Redis restart or that expiry loses; without a Redis client a part is answered `ESME_RX_T_APPN`
  so the carrier resends it; and with no
  `[router] mo_forward_queue` the router's native MO routing acknowledges an MO before its unconfirmed downstream
  publication, as upstream.
- **Fixed — long MO assembly survives a crash and a reused reference.** Each part is stored with its content
  digest and the 300-second expiry in one Redis script; a part whose slot already holds different content, or
  whose total count, split method or data coding differs from the parts already stored, is answered
  `ESME_RX_T_APPN` instead of being stored, and a retransmitted identical part restores a missing expiry. A
  reused reference whose new message matches the stored one in all of these and in a stored part's content can
  still be assembled with the old parts. An assembler takes a 30-second lease on the stored parts and publishes
  under an assembled-message id kept in the parts hash, so a lease abandoned by a crashed process is taken over
  under the same id. Once it holds the publisher the assembler renews its lease and the parts' 300-second expiry
  and publishes only if the lease is still its own. The part completing a message is handed to the router before
  assembly starts, so nothing that can fail runs after a confirmed assembly, which deletes the parts; the next
  message reusing the reference assembles afresh. A part arriving while
  another assembler holds a live lease waits, checking once a second for up to the 30-second lease, and is
  answered `ESME_ROK` once that assembly is retired or after it has taken over a lapsed lease and assembled
  itself; still held after that, it is answered `ESME_RX_T_APPN` so the carrier resends it. Answering it
  `ESME_RX_T_APPN` at once let the carrier's resends rebuild and assemble the same message a second time.
- **Changed — one confirmed publisher per AMQP connection.** `AmqpFactory.publish_confirmed` publishes on a single
  confirm-mode channel, one publication at a time through its confirmation (pika matches a `basic.return` to no
  particular publication), reopened and put back in confirm mode once the broker has closed it, and closed when
  its confirm mode cannot be set. Opening that channel or confirming a publication, and confirming a `/send`
  submit, is bounded by 15 seconds; past it the publication fails with `TimeoutError` and the AMQP connection is
  aborted rather than closed gracefully, since a graceful close waits on the broker that stopped answering. The
  reconnect opens a fresh channel; the broker may still have taken the abandoned publication, which its caller
  then publishes again, and a `/send` submit stays `submit_in_doubt` with its `msgid` claim kept. A connection the
  broker blocks for 15 seconds is closed. MO ingress, submit receipts, discards,
  outcome forwards, parking and the router's MO forward all use it.
- **Fixed — receipt lookups are bounded.** The DLR lookup consumer takes at most 64 unsettled receipts. An
  unconfirmed outcome forward is retried with a doubling delay capped at 300 seconds and, after 10 failed
  forwards of that receipt (counted per routing key, `dlr_scope`, message-id and status), published to the
  durable `<dlr_forward_queue>.parked` queue; a receipt that cannot be parked either keeps being retried. A
  receipt whose Redis lookup fails `dlr_lookup_max_retries` times is published whole and persistent to the
  durable `<routing key>.parked` queue (`dlr.deliver_sm.parked`, `dlr.submit_sm_resp.parked`) and acked once
  that is confirmed, instead of being rejected. The
  SMSC-id mapping is installed before the outcome is forwarded, and the outcome before any HTTP receipt is
  thrown. A terminal receipt for an HTTP-submitted message no longer deletes its dlr map: it expires with
  `dlr_expiry`, so an acceptance still being forwarded and the other segments' receipts resolve. A discard no
  longer deletes the message's dlr map either, so an old discard cannot remove the map of a retry under the same
  `msgid`; it expires with `dlr_expiry`. Any failure escaping a receipt callback requeues the delivery. Two deliveries sharing a message-id each settle on
  their own timer.
- **Fixed — a configured MO forward is bounded.** An unconfirmed forward to `[router] mo_forward_queue` is retried
  with a doubling delay from `mo_forward_retry_delay`, capped at 300 seconds, and after 10 failed forwards of that
  MO published to the durable `<mo_forward_queue>.parked` queue; an MO that cannot be parked either keeps being
  retried. A `short_message` an interceptor set as text is forwarded as its UTF-8 bytes instead of being rejected.
- **Fixed — reconnects back off.** SMPP client and AMQP reconnects double their configured delay (at least one
  second) per consecutive failure up to 300 seconds, and restart from the configured delay after a successful bind or connection.
- **Fixed — a control channel closed by the broker is reopened** while its connection stays up, instead of failing
  every later publish and declaration until the connection drops. A discard whose receipt is not confirmed is
  requeued with a doubling delay capped at 300 seconds; after five attempts it is published to the durable
  `dlr.discard.parked` queue, and the submit is removed only once that is confirmed.
- **Fixed — pacing stays under the connector's ceiling.** A carrier throttle no longer lifts a learned rate below
  one message per second up to one, and switching a connector's throughput off forgets its learned rate, so a
  throttled retry takes the configured delay.
- **Fixed — a submit body that cannot be read no longer stops its connector's consumer.** The consumer is re-armed
  before the body is unpickled.
- **Fixed — a `generic_nack` or mismatched response to a `submit_sm` fails its transaction** with
  `SMPPGenericNackTransactionError` or `SMPPProtocolError` instead of raising `TypeError` in the PDU handler.
- **Fixed — HTTP throwers keep retry state per thrower and stop on `404`.** Retry counters and timers are no longer
  shared class attributes, and a `404` answer is matched by its status code, so it is not retried.
- **Fixed — a settled submit leaves no retry tracking behind.** The listener forgets a submit's retry count when
  it acks it or rejects it without requeue, including an outcome replayed by a redelivery.
- **Delivery semantics — outcomes and forwards are at-least-once.** A confirmed outcome is struck from the replay
  record only after its confirmation, and a forward is acked only after its confirmation, so a crash or a lost
  record or ack between the two publishes it again. Consumers must deduplicate.
- **Upstream compatibility.** Redis is mandatory for every outbound submit: a listener without it holds every
  submit back. The MO forward is configured under `[router] mo_forward_queue` and `mo_forward_retry_delay`, and
  its JSON payload carries only `id`, `from`, `to`, `origin-connector`, `coding` and `binary`. The router and DLR
  lookup consume the renamed durable queues `RouterPB_deliver_sm_all.durable` and `DLRLookup-<pid>.durable`.
  Every confirmed publication of a process (MO ingress, receipts, discards, forwards, parking) goes through one
  publisher one at a time, so its throughput is one publication per broker round trip, and waiting publications
  queue on its lock without bound.
- **Known limits.** These hold only in modes Wovera does not run. With internal billing on, a billing request is
  published unconfirmed after the submit is acked, so a crash or failed publication between them loses the
  charge. With no `[router] mo_forward_queue`, native MO routing acks an MO before its unconfirmed downstream
  publication, so a connection loss between them loses it. Long-MO assembly strips the whole UDH and clears UDHI,
  so port-addressing information elements beside the concatenation one are not forwarded.

### 2026-10-04

- **Changed — configured MO forwards need no MO route.** The router forwards every whole inbound message to
  `[router] mo_forward_queue` directly, without a destination connector. The queue is declared durably;
  JSON messages are persistent and published on a dedicated confirm-enabled channel with mandatory routing.
  JSON carries only `id`, `from`, `to`, `origin-connector`, integer `coding` and the full message bytes as hex
  `binary`. The inbound delivery is acked only after confirmation; channel and publish failures are requeued
  after `[router] mo_forward_retry_delay` (default 30 seconds). Invalid payloads are logged and rejected without
  requeue. Pending retry timers are cancelled when the router stops. A closed publisher channel is reopened
  and re-confirmed through shared recovery. Enabled forwarding limits consumer prefetch to eight deliveries.
  Empty keeps upstream routing.
  The container exposes `MO_FORWARD_QUEUE`.
- **Fixed — multipart MO reassembly accepts the last part first.** Assembly runs when all parts are present,
  regardless of arrival order, with an atomic Redis claim preventing concurrent part callbacks from forwarding
  the same assembled message twice. The sender-qualified hash key and its expiry are retained. Failed assembly
  releases its claim for a duplicate-part retry. Missing content is logged and dropped with its claim released.
  Both UDH reference widths are supported, and each part's full header is stripped. Malformed or empty UDH and
  invalid concatenation counts or sequence numbers pass through as single messages.

- **Added — SMPP client connectors are managed over the HTTP API.** `PUT /connectors/<cid>` adds or replaces a
  connector (replacing keeps its submit queue) and starts it unless `"started": false`; `POST
  /connectors/<cid>/start|stop` changes its state; `DELETE /connectors/<cid>` retires it, refused with `409
  backlog_not_empty` while messages still wait for it. Every change is persisted to the configuration store. The
  endpoint requires `Authorization: Bearer <[http-api] control_token>` (env `HTTP_API_CONTROL_TOKEN`) and refuses
  every request while no token is set. Refusals carry a JSON `{"error","message"}` body and the
  `Jasmin-Error-Code` header. `/status` now reads back each connector's settings, except the password, and
  whether it is started.

- **Added — connectors change without dropping work.** A `PUT /connectors/<cid>` that changes only per-submit
  settings (throughput, receipt scope and expiry, priority, requeue delay, logging, source and destination address
  types) applies them in place and the bind stays up; one that changes the host, port, login or bind type opens a new
  session, and is refused with `409 connector_busy` while a submit the connector took from its queue is unsettled.
  Stopping and retiring a connector are refused the same way, after the queue consumer is detached so a
  submit taken during the check is caught too, and `/status` reports each connector's `messages_in_flight`,
  counting the first submit a connector takes as well. `PUT /routes/mt` replaces the whole MT routing table in one synchronous step, and an
  empty list clears it. `PUT /users/<username>` re-enables a disabled group and repairs a user's membership, and an
  unchanged request still persists anything left unsaved.

- **Added — the MT routes and the HTTP send user are managed over the HTTP API too.** `PUT /routes/mt`
  `{"routes": [{"tag", "connectors"}, ...]}` makes one random round-robin route per tag over its connectors, so a
  `/send` carrying that tag goes out only on them; there is no default route, so an untagged submit is refused. A
  request naming an unknown connector is refused and leaves the table as it was.
  `PUT /users/<username>` `{"password", "group"}` creates the user (and its group) once and leaves an existing,
  matching user untouched. A `PUT /connectors/<cid>` that repeats the current settings no longer drops the bind.
  Each change is persisted, and each endpoint takes the same bearer token as `/connectors`.

- **Added — `/send` names the connector that accepted the submit.** A `Jasmin-Connector` header carries its id, and a
  submit deduplicated by its `msgid` answers the connector the first one was accepted for.

- **Added — receipts are correlated per receipt scope.** A connector's `dlr_scope` (jCli `dlr_scope`, the control
  endpoint and `/status`) namespaces the map from an SMSC message id to the submit, so two SMPP accounts that issue
  the same id cannot overwrite each other, while binds of one account share a scope and keep matching a receipt that
  arrives on a sibling. An empty scope keeps the previous key.

- **Fixed — outcomes name the connector that handled them.** A terminal receipt reported the SMSC message id in
  its `connector` field, both on the HTTP receipt and on the forwarded outcome; it now reports the connector
  that received the receipt. A discard outcome carried no connector; it now carries the one that discarded the
  message.

- **Fixed — stopping an already-stopped connector releases its consumer and queue.** `connector_stop` returned
  before cancelling the submit queue consumer and before deleting the queue when the service was already
  stopped, which is always the case when `connector_remove` calls it, so a removed connector left its consumer
  attached.

### 2026-08-07

- **Changed — a message needing more segments than the connector may send is refused, not shortened.** The
  splitter counted the segments the content really needs, then clamped that count to `long_content_max_parts`
  and built only that many, so the tail was dropped: the handset received a message silently missing its end,
  the caller was told the send succeeded, and nothing logged the loss. Boundary-aware splitting made this
  reachable for content that previously fitted, since carrying a character whole into the next segment can add
  one. `SubmitSM` now raises `LongMessageExceedsMaxPartsError`, which the HTTP endpoints answer as a request
  validation failure naming the segments needed and the limit allowed. Refusing is the honest outcome: a caller
  can shorten the message or raise the limit, neither of which is possible when the loss is invisible.

- **Fixed — the SMPP-server submit no longer requires its answer to arrive synchronously.** The handler called
  the client manager without awaiting it and read the resulting `Deferred`'s `.result` directly, guarded by a
  `hasattr` check that only holds while that `Deferred` has already fired. Any I/O awaited on the submit path
  therefore answered every bound ESME `ESME_RSUBMITFAIL` — a whole-plane outage triggered by a change that looks
  unrelated, since the failure surfaces at the ingress rather than where the wait was added. The handler now
  awaits the submit like any other call. With that, the receipt map is awaited on the SMPP-server ingress too,
  as it already was on the HTTP one, so a receipt arriving before the map is written can no longer lose the
  correlation on either path.

- **Fixed — `/rate` priced a message the send path would not have built.** Two faults, both silent. Its PDU was
  built with a misspelled keyword, which the builder's keyword arguments accepted and dropped, so the quoted
  message carried no originating address and any routing keyed on the sender priced against the wrong route. And
  its check for the GSM 03.38 coding compared a byte string against text, which is never equal, so the
  conversion never ran and segments were counted over raw UTF-8: 160 accented characters priced as three
  segments where the send path submits one. The error-name header is now emitted on this endpoint's remaining
  path and on the balance endpoint, which had none.

- **Added — a machine-readable failure name on HTTP API errors.** Several failures share a status: 403 covers
  authentication, charging and throughput alike, and 412 covers both routing failures, so a caller could only
  tell them apart by substring-matching the English prose in the body — and a reworded message silently changed
  how it was classified. Each error now carries a stable token, emitted as a `Jasmin-Error-Code` response
  header. The body is untouched, so existing parsers are unaffected. Tokens are written verbatim rather than
  derived from class names, so renaming a class cannot change a value already on the wire.

- **Fixed — sequence numbers stay inside the range the protocol allows.** SMPP v3.4 §5.1.4 permits
  `sequence_number` 0x00000001 to 0x7FFFFFFF, but the counter only ever incremented. The header encoder accepts
  the full 0xFFFFFFFF, so passing the ceiling put off-spec numbers on the wire raising nothing at all, with
  undefined SMSC behaviour, and only failed at 2^32 — where the encoder's `ValueError` reached a generic
  handler that discarded the message. Both the client and the server protocol now wrap to 1 at the ceiling.

- **Fixed — the receipt map is written before the PDU is published, and awaited.** The map was written after
  the publish and never awaited, so the SMSC round trip could complete before it landed. A `submit_sm_resp`
  arriving first found no map and was discarded, which took the whole receipt chain with it: the later
  `deliver_sm` then retried against a map that was never written, and the sender was left with no outcome for a
  message the handset had received.

- **Fixed — consumers are re-established when the AMQP connection comes back.** The readiness signal was only
  renewed when it was unset, so after startup it was an already-fired `Deferred`: an unexpected reconnect fired it
  again, and the resulting `AlreadyCalledError` was reported as a connection failure that had not happened.
  Publishing reopens the control channel on its own, so the damage was silent and one-sided — a consumer's channel
  dies with the connection, and nothing re-subscribed it, leaving its queue filling with nothing draining it until
  the process was restarted. The signal is now renewed per connection, and consumers register a callback that runs
  on every connection instead of subscribing once at boot: the client manager re-runs each running connector's
  full channel setup (prefetch limit, exchange, queue and binding, then the consumer), and `DLRLookup` and the
  throwers re-run their own subscribe. Because the setup is redone rather than only the channel reopened, a broker
  restart that took the topology with it is recovered too, not just a dropped connection. A failing callback is
  logged and skipped so one consumer cannot strand the others.

- **Added — coverage for carried behaviour that no test could previously fail on.** `subscribe()`'s own durable
  declaration of the forward queue (the existing cases declared it themselves, after subscribe had already run with
  an empty config); the `ForwardError` requeue on the deliver path, which only a level-2 receipt reaches; the
  consumer-side discard of a message whose validity elapsed in the queue, driven directly rather than through the
  skipped timing-dependent case; the idempotency claim's expiry and its release when the publish fails; and the
  bind-filtered random route on the SMPP-server interface, both refusing an all-unbound pool and accepting over the
  bound connector, since that predicate is a separate copy of the HTTP one.

- **Fixed — a queued message that outlives its validity is now discarded rather than sent late.** The consumer
  already dropped a message whose validity had elapsed while it waited, but it reads that instant from the AMQP
  `expiration` header, and the header is only written when the publish supplies one. `perspective_submit_sm`'s
  `validity_period` defaulted to `None` because no caller passed it, so the check was unreachable for every
  message: one that outlived its window in a backed-up queue was still submitted whenever the queue drained, and
  the `SMPPRequestTimoutError` requeue was bounded by nothing. `/send` now forwards the same instant it puts on
  the PDU. The parameter has always existed upstream, unused; upstream's own `test_submitSm_validity` is skipped.
  The SMPP-server ingress has the same gap and is not addressed here.

### 2026-08-06

- **Fixed — concatenated messages split on character boundaries.** Long messages were sliced at fixed byte
  offsets, severing a GSM 03.38 escape pair or a UTF-16 surrogate pair that straddled a boundary: the handset
  rendered the escape's orphaned septet as its basic-table meaning and lone surrogates as replacement characters.
  3GPP TS 23.040 §9.2.3.24.1 forbids both splits. The splitter now backs a boundary off by one unit rather than
  cutting a pair, and the segment count the SAR/UDH headers advertise comes from the split itself, because
  carrying a character whole can add a segment. Inherited from upstream (issue #437, closed with no fix).
- **Fixed — every segment of a concatenated message maps its own SMSC id.** `endLongSubmitSmTransaction` fires per
  segment but called back once, so only the last segment's `submit_sm_resp` reached the transaction callback and a
  concatenated message correlated a single id. Each segment now keeps its own response and the submit publishes
  the whole set in one `smpp_msgids` header — one message, because that message also emits the level-1 outcome and
  publishing per segment would emit duplicate receipts.
- **Fixed — `RandomRoundrobin` routes only to bound connectors.** The route spread submits at random across its
  whole pool, including connectors that were not bound, where the messages aged out silently. Both submit paths
  now choose only among connectors the caller reports bound, and answer "no bound connector" when none is.
- **Fixed — a minted validity instant no longer carries sub-second precision.** A validity window is
  minute-granular but was minted with wall-clock microseconds intact; the absolute-time encoder derives its single
  tenths-of-a-second digit from them and rejects anything above `.9`. That raise landed in the submit callback's
  generic handler, which discarded without a retry or a receipt after `/send` had already answered 200.

### 2026-07-26

- **Fixed — the outcome forward queue is declared durably**, so a receipt published before any consumer binds is
  buffered by the broker instead of dropped.

### 2026-07-08

- **Added — publisher confirms on the outcome forward.** `forward_outcome` publishes on a dedicated
  confirm-enabled channel and the inbound delivery receipt is acked only after the broker confirms, so a lost
  forward requeues the receipt instead of vanishing (an at-least-once return path). A Nack or unroutable publish
  raises `ForwardError`, caught ahead of the generic handler and routed to a requeue rather than the generic
  `requeue=0` discard.
- **Added — adversarial delivery-receipt wire tests** through the real SMPP path: a spurious receipt for an
  unmapped id is dropped without stalling the consumer, and an unrecognized carrier state forwards as `UNKNOWN`
  rather than being fabricated. The bundled SMSC simulator now tolerates an arbitrary stat so these can be driven.
- **Fixed — the outcome forwarder no longer depends on the legacy HTTP `dlr-url`.** The Redis `dlr:` correlation
  map is written whenever a receipt is requested by level, not only when a url is present (an empty url is stored
  when absent); the HTTP throw is gated on a present url at both levels, while the forward and the `queue-msgid`
  correlation stay unconditional. A receipt requested by level alone therefore still forwards.
- **Added — optional `BILLING_FEATURE` toggle** injected by the container entrypoint into the `[smpp-server]` and
  `[http-api]` sections; unset leaves the engine default.

### 2026-07-04

- **Added — config-driven JSON MO forwarder.** With `[deliversm-thrower] mo_forward_queue` set, the
  `deliverSmThrower` republishes each mobile-originated message as a JSON record (`from`, `to`, `content`/`binary`,
  `coding`, `origin-connector`, TLVs) to that queue and acks, instead of throwing it over HTTP — so a downstream
  AMQP consumer owns routing and attribution. Empty = disabled (unchanged HTTP behaviour).
- **Fixed — multipart MO reassembly key includes the sender.** Segments were accumulated under
  `(connector, msg_ref_num, destination_addr)`; for MO the destination is our own shortcode, so two senders with
  colliding 1-byte ref numbers within the segment TTL interleaved into one hash and garbled/lost reassembly.
  `source_addr` is now part of the key.
- **Added — config-driven JSON outcome forwarder.** `DLRLookup` optionally republishes each delivery-receipt
  outcome as a JSON record to a configurable queue (`[dlr] dlr_forward_queue`, empty = disabled) alongside the
  existing HTTP thrower path, at both the `submit_sm_resp` (level 1) and `deliver_sm` (level 2) points, so a
  downstream AMQP consumer can ingest structured outcomes instead of the token-in-URL HTTP callback. The Redis
  `dlr:`/`queue-msgid` correlation is untouched, and the forwarded message carries no AMQP `message-id` (one
  msgid spans several receipts, so it does not identify the envelope). The container entrypoint exposes a
  `DLR_FORWARD_QUEUE` env hook (empty = disabled).
- **Added — reconnect and submit-requeue jitter** to avoid thundering-herd stampedes when many connectors
  reconnect or requeue at once.
- **Added — `throttle_delay` QoS helper**, extracted and unit-tested for the sub-1/s throughput case.
- **Fixed — level-1 DLR published only on a final outcome**, not per retryable attempt: a throttled-then-retried
  submit no longer emits a false terminal `submit_sm_resp` receipt while a retry is still pending, and the
  outcome carries whether the message will be retried.
- **Fixed — QoS throttle and validity/age checks read the full duration** via `timedelta.total_seconds()`. The
  previous code used only `.microseconds`/`.seconds`, dropping whole seconds — breaking sub-1/s throttling and
  wrapping the validity/age math at day boundaries.
- **Fixed — config-header validation error** was missing a `%s` placeholder, raising a formatting error instead
  of the intended message.

### 2026-07-03

- **Added — idempotent `/send`.** `/send` accepts an optional client-supplied `msgid`; the connector claims it in
  Redis (`SETNX`, recorded before publishing to the submit queue) and replays the original accept on a duplicate
  within TTL, releasing the claim if the publish fails. Absent `msgid` keeps the server-minted id. Fail-closed:
  `/send` returns 5xx when Redis is unavailable rather than sending undeduped. Lets a caller safely retry after an
  uncertain outcome without double-submitting to the wire.

### 2026-06-23

- **Changed — AMQP substrate: replaced the unmaintained txAMQP with pika.** txAMQP3 is abandoned and races on
  shared-channel acks, surfacing as `406 PRECONDITION_FAILED` (unknown delivery tag) and malformed
  `Success "[Failure instance: ChannelClosed]"` `/send` responses. The queues layer (factory/protocol/configs) is
  rewritten on pika's `TwistedProtocolConnection` with a per-consumer channel model (`newChannel()`), and every
  ack/reject goes back on the channel that delivered the message. A pika `Content` base maps the txamqp-style
  property dict onto `BasicProperties`, and a `DeliveryMessage` shim adapts pika's `ReceivedMessage`, so the
  manager/routing consumers port over with minimal churn. Dependency: drop `txAMQP3`, add `pika[twisted]`.
