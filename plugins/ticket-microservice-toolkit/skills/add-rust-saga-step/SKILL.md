---
name: add-rust-saga-step
description: Wire one step of a choreography saga over Kafka for a Rust service in this repo - either publishing a domain event via the transactional outbox, or consuming an event to trigger a local (possibly compensating) use case. Use after a use case exists that needs to announce, or react to, a cross-service state change.
argument-hint: "<service-name> publish <EventName> <topic>  OR  <service-name> consume <EventName> <topic> <UseCaseToTrigger>"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add Rust Saga Step (Kafka, Choreography)

## Context
- Project conventions (Clean Architecture, saga pattern): @CLAUDE.md
- Existing outbox/messaging code in this service: !`grep -rl "outbox_events\|rdkafka" services 2>/dev/null | head -20`

## Why choreography, not orchestration
This repo uses **choreography**: there is no central saga-coordinator service. Each service
publishes events about its own state changes and independently decides how to react to events
from other services — including how to compensate its own earlier step if something downstream
fails. Every piece of code you generate here must stay consistent with that: no service should
call another service's saga step directly, and no service should need to know the full saga
sequence, only "what do I do when I see event X."

## Arguments
`$ARGUMENTS` is one of:
- `<service-name> publish <EventName> <topic>`
- `<service-name> consume <EventName> <topic> <UseCaseToTrigger>`

Verify `services/<service-name>/Cargo.toml` exists first — if not, suggest
`/new-rust-service`.

## Instructions

### Mode: publish

1. `src/domain/` — define `<EventName>` as a plain `serde`-serializable struct (e.g.
   `SeatReserved { event_id, booking_id, reserved_at }`), no Kafka-specific types. Give the
   aggregate or usecase result a way to carry "pending events" produced by a successful
   operation.
2. `src/adapter/repository/postgres.rs` — in the **same DB transaction** as the state-changing
   write (an `sqlx::Transaction`), insert a row into an `outbox_events` table (`id,
   aggregate_id, event_type, payload JSONB, created_at, published_at NULL`); create the
   migration for this table if it doesn't exist yet for this service. This is the transactional
   outbox pattern: it avoids the dual-write problem (DB commit succeeds but the Kafka publish is
   lost, or the reverse) without needing two-phase commit with Kafka.
3. `src/adapter/messaging/kafka/outbox_relay.rs` — a background `tokio::spawn`ed task, started
   from `main.rs` and cancelled via the same shutdown signal as the HTTP server, that polls
   unpublished `outbox_events` rows and publishes each to `<topic>` using `rdkafka`'s
   `FutureProducer`, keyed by `aggregate_id` (so all events for the same booking/event/etc.
   stay ordered within a partition — losing that order breaks the saga), then marks
   `published_at`. This is at-least-once delivery: if the process crashes between publishing
   and marking the row, it republishes on restart — the consumer side must be idempotent (see
   below), this side must not try to be exactly-once. A publish failure on one row must not
   stop the relay: log it, leave `published_at` NULL, move on, retry on the next tick.
4. Add `rdkafka` (with the `tokio` feature) and `serde`/`serde_json` to `Cargo.toml` if not
   already present. Tell the user `<topic>` needs to exist in Kafka and name which other
   service(s) should now add a `consume` step for it.

### Mode: consume

1. `src/adapter/messaging/kafka/consumer.rs` — subscribe to `<topic>` with consumer group
   `<service-name>-<EventName>` using `rdkafka`'s `StreamConsumer`, with
   `enable.auto.commit=false` so the offset is only committed after the message is fully
   processed (manual commit — required for at-least-once correctness here). Deserialize into
   the `<EventName>` domain event type via `serde_json`.
2. Idempotency — Kafka is at-least-once, duplicates will happen. Before invoking
   `<UseCaseToTrigger>`, check a `processed_events` table (unique constraint on event ID) inside
   the same transaction as the usecase's side effect, and skip if already processed. This is
   the same requirement `/review-concurrency` checks for booking writes — don't treat it as
   optional here either.
3. Call `<UseCaseToTrigger>` in `src/usecase/`. If `<EventName>` is a failure signal from
   downstream (e.g. `SeatReservationFailed`), `<UseCaseToTrigger>` must be a **compensating**
   action that undoes this service's own earlier step (e.g. `CancelBookingUseCase` releasing a
   pending booking) — not a retry of the original request.
4. **Failure handling is mandatory — every consume step must terminate every message and
   never wedge a partition.** Classify the outcome and route it:
   - **Success / idempotent no-op** (event id already in `processed_events`) → commit the
     offset.
   - **Transient error** (DB down, broker/network blip — a `UserError::Repository(_)` /
     `RepositoryError`-style variant) → do **not** commit the offset; retry in-process with
     capped exponential backoff up to `KAFKA_CONSUMER_MAX_ATTEMPTS` (env, default 5).
   - **Poison message** (payload won't deserialize, missing / non-UUID fields) → publish
     straight to the dead-letter topic `<topic>.dlq` (a second `FutureProducer`), then commit.
     Never retry a parse failure.
   - **Permanent domain rejection** (a domain invariant refused it) → `<topic>.dlq`, then commit.
   - **Retries exhausted** (still transient after `MAX_ATTEMPTS`) → `<topic>.dlq` as a last
     resort, then commit, so one wedged message can't stall the partition forever.
   The DLQ record keeps the original key + payload and adds headers `x-dlq-reason` and the
   source topic / partition / offset. If the DLQ publish itself fails, leave the offset
   uncommitted (it redelivers on restart) rather than dropping the message. Add `<topic>.dlq`
   to the topic-init / `docker-compose` step next to `<topic>`.

## Concrete example for this repo

**Already implemented — read it as the reference for a new step:** `user-service` publishes
`UserCreated` on `user.created` via the outbox + relay (`src/adapter/messaging/kafka/`,
`migrations/*_create_outbox_events.sql`) → `analytics-service` (Go) consumes it with group
`analytics-service-UserCreated`, projects a `user_registrations` read-model row behind a
`processed_events` idempotency check, and dead-letters poison / permanently-failing /
retry-exhausted messages to `user.created.dlq`.

Sketched next: `booking-service` publishes `BookingRequested` on `booking.requested` →
`event-service` consumes it, atomically tries to decrement `available_seats`, and publishes
either `SeatReserved` or `SeatReservationFailed` on `event.seat-reservation` →
`booking-service` consumes that to confirm or cancel the booking (the compensating path) →
`analytics-service` consumes the final `BookingConfirmed`/`BookingCancelled` events purely as a
read model, since it never mutates another service's state and needs no compensation.
