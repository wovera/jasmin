# Changelog

Notable changes in Wovera's fork of Jasmin, relative to upstream `github.com/jookies/jasmin`. This records
*what* changed; the design rationale and validation live in Wovera's internal engineering docs. General
bug-fixes here are offered upstream where practical. See [`WOVERA.md`](WOVERA.md) for the fork's scope.

The format is based on [Keep a Changelog](https://keepachangelog.com/). This fork is not versioned against
upstream releases, so changes are listed under an open **Unreleased** section, grouped by the date each landed
on the fork mainline (newest first).

## Unreleased

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
