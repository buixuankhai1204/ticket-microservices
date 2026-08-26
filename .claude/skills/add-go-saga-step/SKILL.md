---
name: add-go-saga-step
description: Wire one step of a choreography saga over Kafka for a Go service in this repo - either publishing a domain event via the transactional outbox, or consuming an event to trigger a local (possibly compensating) use case. Use after a use case exists that needs to announce, or react to, a cross-service state change (e.g. booking creation triggering seat reservation).
argument-hint: "<service-name> publish <EventName> <topic>  OR  <service-name> consume <EventName> <topic> <UseCaseToTrigger>"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add Go Saga Step (Kafka, Choreography)

## Context
- Project conventions (Clean Architecture, saga pattern): @CLAUDE.md
- Existing outbox/messaging code in this service: !`grep -rl "outbox_events\|kafka" services 2>/dev/null | head -20`

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

Verify `services/<service-name>/go.mod` exists first — if not, suggest `/new-go-service`.

## Instructions

### Mode: publish

1. `internal/domain/` — define `<EventName>` as a plain struct (e.g. `BookingRequested{
   BookingID, EventID, UserID, RequestedAt}`), no Kafka-specific types. Give the aggregate or
   usecase result a way to carry "pending events" produced by a successful operation.
2. `internal/adapter/repository/postgres/` — in the **same DB transaction** as the
   state-changing write, insert a row into an `outbox_events` table (`id, aggregate_id,
   event_type, payload JSONB, created_at, published_at NULL`); create the migration for this
   table if it doesn't exist yet for this service. This is the transactional outbox pattern: it
   avoids the dual-write problem (DB commit succeeds but the Kafka publish is lost, or the
   reverse) without needing two-phase commit with Kafka.
3. `internal/adapter/messaging/kafka/outbox_relay.go` — a background loop, started as a
   goroutine from `cmd/main.go` and stopped on the same shutdown context as the HTTP server,
   that polls unpublished `outbox_events` rows, publishes each to `<topic>` via
   `segmentio/kafka-go`, keyed by `aggregate_id` (so all events for the same
   booking/event/etc. stay ordered within a partition — losing that order breaks the saga),
   then marks `published_at`. This is at-least-once delivery: if the process crashes between
   publishing and marking the row, it republishes on restart — the consumer side must be
   idempotent (see below), this side must not try to be exactly-once.
4. Tell the user `<topic>` needs to exist in Kafka and name which other service(s) should now
   add a `consume` step for it.

### Mode: consume

1. `internal/adapter/messaging/kafka/consumer.go` — subscribe to `<topic>` with consumer group
   `<service-name>-<EventName>` via `segmentio/kafka-go`, deserialize into the `<EventName>`
   domain event type.
2. Idempotency — Kafka is at-least-once, duplicates will happen. Before invoking
   `<UseCaseToTrigger>`, check a `processed_events` table (unique constraint on event ID) inside
   the same transaction as the usecase's side effect, and skip if already processed. This is
   the same requirement `/review-concurrency` checks for booking writes — don't treat it as
   optional here either.
3. Call `<UseCaseToTrigger>` in `internal/usecase/`. If `<EventName>` is a failure signal from
   downstream (e.g. `SeatReservationFailed`), `<UseCaseToTrigger>` must be a **compensating**
   action that undoes this service's own earlier step (e.g. `CancelBookingUseCase` releasing a
   pending booking) — not a retry of the original request.
4. Error handling — a retryable error (DB/network blip) should retry with backoff without
   committing the Kafka offset; a message that fails repeatedly (bad payload, permanent domain
   rejection) should go to a dead-letter topic `<topic>.dlq` after N attempts, so one poison
   message can't block the rest of the partition.

## Concrete example for this repo

`booking-service` publishes `BookingRequested` on `booking.requested` → `event-service`
consumes it, atomically tries to decrement `available_seats`, and publishes either
`SeatReserved` or `SeatReservationFailed` on `event.seat-reservation` → `booking-service`
consumes that to confirm or cancel the booking (the compensating path) →
`analytics-service` consumes the final `BookingConfirmed`/`BookingCancelled` events purely as a
read model, since it never mutates another service's state and needs no compensation.
