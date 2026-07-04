# Changelog

Notable changes in Wovera's fork of Jasmin, relative to upstream `github.com/jookies/jasmin`. This records
*what* changed; the design rationale and validation live in Wovera's internal engineering docs. General
bug-fixes here are offered upstream where practical. See [`WOVERA.md`](WOVERA.md) for the fork's scope.

The format is based on [Keep a Changelog](https://keepachangelog.com/). This fork is not versioned against
upstream releases, so changes are listed under an open **Unreleased** section, grouped by the date each landed
on the fork mainline (newest first).

## Unreleased

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
