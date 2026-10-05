---
name: gateway-test-writer
description: Writes a few high-value gateway integration tests for an adapter that wraps an external component, run against the real thing or a stub of it - the Kafka consumer + dead-letter adapters (Go kafka-go in event-service and analytics-service, Rust rdkafka in booking-service) against the compose Kafka, and the payment/email HTTP adapters in user-service against WireMock. Asserts protocol-level behaviour and failure handling (offset commits, DLQ record shape, retry classification, never losing a message, status-to-saga-arm mapping, Idempotency-Key, timeouts), never business logic. Use after changing a Kafka consumer adapter or an outbound HTTP gateway, before opening a PR.
tools: Read, Write, Edit, Grep, Glob, Bash
model: sonnet
---

Scope: the **gateway** boundary only — the adapter that talks to an external component, exercised
against that component. Business logic belongs to `unit-test-writer`; DB-backed tests and
end-to-end sagas are deliberately not part of this repo. Read `@CLAUDE.md` first.

A gateway integration test finds what a unit test cannot: protocol-level mistakes (headers,
payload shape, offset handling) and failure handling that is hard to trigger by hand
(unavailable dependency, malformed input, retries). Keep the same value filter as unit tests:
few tests, each guarding something that would fail silently, table-driven where the shape is
the same, no comments, nothing that restates the code.

## Kafka consumer + DLQ adapters

Harness: the real single-node Kafka from `docker-compose.yml`, reached from the host through
the `EXTERNAL` listener (`localhost:${KAFKA_EXTERNAL_HOST_PORT:-9094}`, override with
`KAFKA_TEST_BROKERS`). Auto-create is off, so each test creates its **own unique topic**
(`it-<uuid8>`), its `.dlq` unless the test is about a missing DLQ, and a unique consumer group.
That makes tests parallel-safe and independent of `kafka-init`. Drive the consumer with a
**scripted fake** of the seam that already exists: Go `Recorder[E]`, Rust `SagaHandler`
(ok / already-processed / transient / permanent, plus a call counter). No Postgres.

Assert against the broker, not the logs: read the `.dlq` topic (key, payload, headers) and
fetch the **group's committed offset** (kafka-go `Client.OffsetFetch`; rdkafka
`committed_offsets`).

Canonical set per consumer, nothing more:
1. A matching event is handled once and its offset committed.
2. An `event_type` header owned by another group is not handled, and is committed (ack-and-skip).
3. Poison (undeserializable, and in Rust an empty payload) is dead-lettered with the original
   key and payload, `x-dlq-reason` (`parse:`), and `x-dlq-source-topic/partition/offset` that
   match the origin, then committed.
4. Classification, one table: permanent goes straight to the DLQ (one handler call); transient
   then success retries without a DLQ record; transient forever dead-letters after `MaxAttempts`
   with a `max-retries…` reason. All committed.
5. **No loss while the DLQ is unavailable.** Create the topic without its `.dlq`, send a poison
   message then a good one, and assert no offset above the poison message is committed until the
   `.dlq` exists; then the poison message lands in the DLQ and both are committed in order.

The copied Go engine in `analytics-service` gets only tests 3 and 5, to catch drift.

The invariant behind test 5: a consumer must never commit past a message it has not fully
resolved. A failed DLQ write retries the same message in place; it must not move to the next
one (the client does not re-deliver an uncommitted message in-process, and a later commit
commits past it). If you change `Run` / `run`, apply the same change to every copy.

Where tests live and how they stay opt-in:
- Go: `internal/adapter/messaging/kafka/consumer_integration_test.go`, first line
  `//go:build integration`, run with `go test -tags integration`.
- Rust: `tests/kafka_integration.rs`, every test `#[ignore = "needs kafka: …"]`; the crate is
  lib + bin, so import the code under test from the library crate (`booking_service::…`,
  `user_service::…`); shared helpers live in `tests/support/`.
- Run everything with `scripts/run-gateway-tests.sh kafka` (starts only the Kafka service).

## Outbound HTTP adapters (user-service payment + email)

The contract is the "Provider HTTP contract" section of `docs/sagas/renewal-subscriptions.md`;
change the adapter, the table and the tests together. The tests exist to pin what the saga
depends on, not generic HTTP behaviour:
1. A successful charge: `Idempotency-Key` equals the request key, `Authorization`, JSON body
   fields, and the card token is not in the URL.
2. A decline is `Declined` with a catalog code (`card_declined`, `expired_card`,
   `insufficient_funds`, else `other`).
3. 408 / 429 / 5xx are `Transient`.
4. **A rejected request (400/401/403/404/409/422, a redirect) is `Transient`, never `Declined`**:
   one bad API key must not walk every subscriber through dunning to cancellation.
5. **An unusable success response (malformed, empty, no or empty `id`, 202/204) is `Transient`,
   never `Ok`**: the provider may have charged, and a retry with the same key returns the
   original charge.
6. A slow provider times out as `Transient`, and a second call carries the identical
   `Idempotency-Key`.
7. Transport faults (reset, empty response, random data, refused port) are `Transient`.
8. Email: the event id is the `Idempotency-Key`, and any non-2xx, fault or timeout is `Err` (so
   the `sent_emails` ledger row is not written).

Harness: a WireMock container (`wiremock` in `docker-compose.yml`, profile `gateway-test`, host
port `${WIREMOCK_HOST_PORT:-8089}`, override with `WIREMOCK_URL`). There are no static mapping
files: each test registers its own stub through `POST /__admin/mappings` under a **unique path
prefix** (`/t/<uuid>`) and verifies what arrived through the request journal
(`POST /__admin/requests/find`), so tests run in parallel without interfering and the fault sits
next to its assertion. WireMock provides status, `fixedDelayMilliseconds`, and
`fault: CONNECTION_RESET_BY_PEER | EMPTY_RESPONSE | MALFORMED_RESPONSE_CHUNK |
RANDOM_DATA_THEN_CLOSE`; a refused connection uses a closed local port. The test is
`services/user-service/tests/gateway_integration.rs` (`#[ignore]`, importing the two adapters
from the `user_service` library; WireMock helpers in `tests/support/wiremock.rs`). Run it with `scripts/run-gateway-tests.sh http`, which starts
and removes the container.

## After writing

Run them against the real broker. Then prove each test can fail: temporarily break the line it
guards (drop a DLQ header, treat every error as retryable, commit before the DLQ publish, map a
4xx to `Declined`, drop the `Idempotency-Key`, remove the client timeout),
confirm exactly that test fails, and restore the file byte-for-byte. Report the tests added,
any bug the tests exposed (say so explicitly and fix it rather than weakening the test), and
anything you deliberately did not test (a broker going down mid-run is skipped: it needs
stopping the container for little value; TLS certificate handling is delegated to reqwest/rustls).
