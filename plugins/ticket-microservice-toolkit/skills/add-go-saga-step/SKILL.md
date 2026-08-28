---
name: add-go-saga-step
description: Wire one step of a choreography saga over Kafka for a Go service in this repo - either publishing a domain event via the transactional outbox, or consuming an event to trigger a local (possibly compensating) use case. Use after a use case exists that needs to announce, or react to, a cross-service state change (e.g. booking creation triggering seat reservation).
argument-hint: "<service-name> publish <EventName> <aggregate_type>  OR  <service-name> consume <EventName> <topic> <UseCaseToTrigger>"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add Go Saga Step (Kafka, Choreography)

## Context
- Project conventions (Clean Architecture, saga pattern): @CLAUDE.md
- Existing outbox/messaging code in this service: !`grep -rl "outbox_events\|kafka" services 2>/dev/null | head -20`
- Debezium connector defs (CDC publish side): !`ls debezium/ 2>/dev/null`

## Why choreography, not orchestration
This repo uses **choreography**: there is no central saga-coordinator service. Each service
publishes events about its own state changes and independently decides how to react to events
from other services — including how to compensate its own earlier step if something downstream
fails. Every piece of code you generate here must stay consistent with that: no service should
call another service's saga step directly, and no service should need to know the full saga
sequence, only "what do I do when I see event X."

## Arguments
`$ARGUMENTS` is one of:
- `<service-name> publish <EventName> <aggregate_type>` — the CDC connector routes the event to
  the `<aggregate_type>.events` topic (e.g. `booking` → `booking.events`).
- `<service-name> consume <EventName> <topic> <UseCaseToTrigger>`

Verify `services/<service-name>/go.mod` exists first — if not, suggest `/new-go-service`.

## Instructions

### Mode: publish

The publish side is **log-tailing CDC, not an in-process relay**: a Debezium PostgreSQL
connector on Kafka Connect tails `outbox_events` from the WAL and publishes each insert via the
Outbox Event Router SMT. You write the outbox row (and delete it in the same txn); you do
**not** write any relay/producer code.

1. `internal/domain/` — define `<EventName>` as a plain struct (e.g. `BookingRequested{
   BookingID, EventID, UserID, RequestedAt}`), no Kafka-specific types. The struct is
   `json`-serializable with snake_case field names (it becomes the Kafka message value
   verbatim). Give the aggregate or usecase result a way to carry "pending events" produced by
   a successful operation, and a method returning the event's `aggregate_type` string.
2. `internal/adapter/repository/postgres/` — in the **same DB transaction** as the
   state-changing write: `INSERT` a row into `outbox_events` (`id, aggregate_id,
   aggregate_type, event_type, payload JSONB, created_at`) **then `DELETE` that same row**
   before the transaction commits. The `INSERT` still lands in the WAL for Debezium to capture;
   the `DELETE` (which the connector ignores) keeps the table empty. Create the migration for
   this table if it doesn't exist yet for this service — copy the shape from
   `services/user-service/migrations/*_create_outbox_events.sql` +
   `*_outbox_debezium.sql` (no `published_at` column). This is the transactional outbox
   pattern: it avoids the dual-write problem without two-phase commit with Kafka.
3. `debezium/<service-name>-outbox.json` — if this service has no connector yet, add one
   (copy `debezium/user-service-outbox.json`): point `database.*` at the service's Postgres,
   pick a unique `slot.name` / `publication.name`, keep `table.include.list=public.outbox_events`,
   `skipped.operations=u,d,t`, and the `EventRouter` transform (`route.by.field=aggregate_type`,
   `route.topic.replacement=${routedByValue}.events`, `aggregate_id` → key,
   `event_type`/`id` → headers, `table.expand.json.payload=true`). Register it by adding the
   file to the `connect-init` step in `docker-compose` (it POSTs every def in `debezium/`).
   The service's Postgres also needs `wal_level=logical` (set via its `command:` in compose).
   If the connector already exists, nothing to do here — a new `aggregate_type` just starts
   routing to its own `<aggregate_type>.events` topic automatically.
4. Add `<aggregate_type>.events` and `<aggregate_type>.events.dlq` to the `kafka-init`
   topic-creation step. Tell the user which other service(s) should now add a `consume` step
   for `<aggregate_type>.events`.

### Mode: consume

1. `internal/adapter/messaging/kafka/consumer.go` — subscribe to `<topic>` with consumer group
   `<service-name>-<EventName>` via `segmentio/kafka-go`. Use `FetchMessage` + `CommitMessages`
   (manual commit — `CommitInterval: 0`) so the offset only advances after a message is fully
   processed. Deserialize into the `<EventName>` domain event type.
2. Idempotency — Kafka is at-least-once, duplicates will happen. Before invoking
   `<UseCaseToTrigger>`, check a `processed_events` table (unique constraint on event ID) inside
   the same transaction as the usecase's side effect, and skip if already processed. This is
   the same requirement `/review-concurrency` checks for booking writes — don't treat it as
   optional here either.
3. Call `<UseCaseToTrigger>` in `internal/usecase/`. If `<EventName>` is a failure signal from
   downstream (e.g. `SeatReservationFailed`), `<UseCaseToTrigger>` must be a **compensating**
   action that undoes this service's own earlier step (e.g. `CancelBookingUseCase` releasing a
   pending booking) — not a retry of the original request.
4. **Failure handling is mandatory — every consume step must terminate every message and
   never wedge a partition.** Classify the outcome and route it:
   - **Success / idempotent no-op** (event id already in `processed_events`) → commit the
     offset.
   - **Transient error** (DB down, broker/network blip — in this repo an
     `*domain.RepositoryError`, detected with `errors.As`) → do **not** commit the offset;
     retry in-process with capped exponential backoff up to `KAFKA_CONSUMER_MAX_ATTEMPTS`
     (env, default 5).
   - **Poison message** (payload won't unmarshal, missing / non-UUID fields) → publish
     straight to the dead-letter topic `<topic>.dlq` (a `kafka.Writer`), then commit. Never
     retry a parse failure.
   - **Permanent domain rejection** (a domain invariant refused it) → `<topic>.dlq`, then commit.
   - **Retries exhausted** (still transient after `MAX_ATTEMPTS`) → `<topic>.dlq` as a last
     resort, then commit, so one wedged message can't stall the partition forever.
   The DLQ record keeps the original key + value and adds headers `x-dlq-reason` and the
   source topic / partition / offset. If the DLQ write itself fails, leave the offset
   uncommitted (it redelivers on restart) rather than dropping the message. Add `<topic>.dlq`
   to the topic-init / `docker-compose` step next to `<topic>`.

## Concrete example for this repo

**Already implemented — read it as the reference for a new step:** `analytics-service` (Go)
consumes `UserCreated` off `user.events` in `internal/adapter/messaging/kafka/consumer.go`
with group `analytics-service-UserCreated`: manual offset commit, an `event_type` header
guard, `processed_events` idempotency check (migration `*_create_processed_events.sql`), the
`RecordUserRegistrationUseCase` projection into `user_registrations`, bounded
retry-with-backoff on `*domain.RepositoryError`, and a `kafka.Writer` that dead-letters
poison / permanently-failing / retry-exhausted / unexpected-`event_type` messages to
`user.events.dlq`. The publish side lives in `user-service` (Rust): it writes an
`outbox_events` row with `aggregate_type = "user"` and the Debezium `user-service-outbox`
connector (`debezium/user-service-outbox.json`) routes it to `user.events` — there is no
relay code to read.

Sketched next: `booking-service` publishes `BookingRequested` (`aggregate_type = "booking"` →
topic `booking.events`) → `event-service` consumes it, atomically tries to decrement
`available_seats`, and publishes either `SeatReserved` or `SeatReservationFailed`
(`aggregate_type = "seat_reservation"` → `seat_reservation.events`) → `booking-service`
consumes that to confirm or cancel the booking (the compensating path) → `analytics-service`
consumes the final `BookingConfirmed`/`BookingCancelled` events purely as a read model, since
it never mutates another service's state and needs no compensation.
