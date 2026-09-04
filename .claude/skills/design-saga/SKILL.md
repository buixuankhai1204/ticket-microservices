---
name: design-saga
description: Design a cross-service choreography saga before any code is wired — event catalog, happy-path and every failure sequence, the compensation map, the stuck-saga timeout policy, and the Kafka/Debezium infra delta — written to docs/sagas/<saga-name>.md. Use BEFORE /new-go-api-endpoint or /new-rust-api-endpoint with publish:/consume: whenever a single business action changes state in more than one service (booking↔event seat reservation, a payment step, an outbound notification step).
argument-hint: "<saga-name>  (e.g. seat-reservation)"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Design a choreography saga

## Context
- Project conventions (Clean Architecture, choreography saga, delivery semantics, outbox/CDC): @CLAUDE.md
- Gateway routing (which service owns which public route): @kong/kong.yml
- Existing saga designs: !`ls docs/sagas/ 2>/dev/null || echo "(none yet)"`
- Existing events already published in the repo: !`grep -rn "aggregate_type" services --include=*.go --include=*.rs 2>/dev/null | grep -iv test | head -30`
- Topics already created: !`grep -n "topic" docker-compose.yml | grep -i "create\|events" | head -20`
- Debezium connectors already registered: !`ls debezium/*.json 2>/dev/null`

## Arguments
`$ARGUMENTS` — a single kebab-case saga name, e.g. `seat-reservation`, `booking-payment`,
`ticket-delivery`. This becomes `docs/sagas/<saga-name>.md`.

## Why this step exists

Choreography scatters the business transaction across services — no one file describes the
whole flow. Without a written design, three failure modes recur (all seen in the research and
in real saga incidents):

- **Dual-write hole** — a step commits its DB change but its event never reaches Kafka. This
  repo's answer is the transactional outbox + Debezium CDC; the design must say every step
  uses it, never a direct producer for state-carrying events.
- **Missing compensation** — a downstream step fails and an upstream step's committed change
  is never undone, leaving a row wedged in `pending` forever. A compensation is a **new
  forward transaction** (cancel, refund, release), never a DB `ROLLBACK` of the original.
- **Distributed spaghetti** — past ~4 hops or with real conditional branching, choreography
  becomes unfollowable and orchestration is the better tool. The design is where you notice
  this before writing code.

Run this with extended thinking (`/effort high` or "ultrathink" in the prompt) — the failure
sequences and compensation ordering are the hard part.

## Instructions

Produce `docs/sagas/<saga-name>.md` with **every** section below. Read the existing service
code first so the event names, `aggregate_type`s, and entity states you reference are real,
not invented.

### 1. Summary
One paragraph: the single client-visible action that kicks the saga off (e.g. `POST
/api/v1/bookings`), the services it touches, and the end states (`confirmed` / `cancelled`).

### 2. Participants and ordered steps
A numbered list. Each step: the owning service, the local transaction it commits (state
change + outbox row, one DB transaction), and the event it emits. Mark which step is the
**initiator** (has the `http:` edge) and which are **reactors** (`consume:` only).

### 3. Event catalog
A table — one row per event in the saga:

| Event | `aggregate_type` | Topic | Partition key | Producer | Consumer(s) | Delivery guarantee | Payload fields |
|---|---|---|---|---|---|---|---|

- Topic is always `<aggregate_type>.events` (the Debezium Outbox Event Router SMT routes by
  `aggregate_type`). Its DLQ is `<aggregate_type>.events.dlq`.
- Partition key is always the `aggregate_id` (UUID) — this is what preserves per-aggregate
  ordering. State the aggregate.
- **Delivery guarantee**, pick one and justify it in a sentence:
  - **effectively-once** — at-least-once delivery + every consumer dedupes on the event `id`
    via `processed_events`. Required when a duplicate is harmful: seat/stock decrement, a
    charge, an order-state transition, a confirmation. This is the default.
  - **at-least-once, naturally idempotent** — consumers are upsert-by-id / "set state to X" /
    reindex, so a duplicate is a genuine no-op with no `processed_events` table. Say why each
    consumer is naturally idempotent.
  - **at-most-once** — loss is acceptable (best-effort metrics, a "nice to have"
    notification). This is **not an outbox event** — it's a direct Kafka producer call
    outside the DB transaction, documented as lossy. If any event in the saga is this, call
    it out explicitly; it does not get an `outbox_events` row.
- Payload fields: the exact JSON keys (snake_case) and types. The payload becomes the Kafka
  message value verbatim. Include `event_id` and the `aggregate_id`.

### 4. Happy path
A sequence (numbered steps or a `mermaid` `sequenceDiagram`): initiator commits → event →
reactor commits → event → … → terminal state. Show the topic each hop travels on.

### 5. Failure sequences — one per thing that can go wrong
For **each** step that can fail (validation reject, resource unavailable, downstream
timeout), a separate sub-section:
- What fails, and what event the failing service emits instead (e.g. `SeatReservationFailed`
  rather than `SeatReserved`).
- Which upstream service consumes that failure event and what **compensating** transaction it
  runs (a forward correction — `CancelBooking` sets the booking to `cancelled` and emits
  `BookingCancelled`; it does not delete the row).
- Whether the compensation itself can fail, and what happens then (retry via the consumer's
  backoff, then DLQ — never an infinite loop).
- The terminal state after compensation.

### 6. Compensation map
A table making section 5 auditable at a glance:

| Forward step | Committed change | Failure trigger event | Compensating step | Owning service | Terminal state |
|---|---|---|---|---|---|

Every forward step that mutates state must have a row. If a step genuinely needs no
compensation (pure read, or it's the last step), say so explicitly with the reason.

### 7. Stuck-saga policy
Choreography has no coordinator watching for a saga that stalls (a reactor is down, an event
was lost before the outbox pattern was in place, a compensation's own event never arrives).
Specify:
- The maximum wall-clock time a row may sit in a non-terminal state (`pending`, `reserving`).
- The reaper: a periodic job (per service, on its own DB) that finds rows older than that and
  resolves them — usually by driving the same compensation path. Name the service that owns
  it and the query it runs.
- What a client polling the initiator sees while the saga is in flight (`202` + a status
  field, not a hung request).

### 8. Idempotency and DLQ requirements per consumer
For each `consume:` step: the consumer group id (`<service>-<EventName>`), the
`processed_events` check (on the same transaction as the side effect, before it), and the
DLQ classification (commit on success/no-op; capped retry-with-backoff on transient
`RepositoryError`; `<topic>.dlq` on poison / permanent rejection / retries exhausted). If the
topic carries more than one `event_type`, each consumer **acks-and-skips** the others' events
(guard on the `event_type` header) rather than dead-lettering them.

### 9. Infra delta
Exactly what changes outside the service code:
- **Topics** to add to the `kafka-init` step in `docker-compose.yml`: every
  `<aggregate_type>.events` and its `.dlq`.
- **Debezium connectors**: a new `aggregate_type` on a service that already has a connector
  routes automatically — nothing to do. A service publishing for the first time needs a new
  `debezium/<service>-outbox.json` (copy `debezium/user-service-outbox.json`, repoint
  `database.*`, give it a unique `slot.name` / `publication.name`) registered in the
  `connect-init` step, and its Postgres needs `wal_level=logical` in compose.
- **Migrations** each service needs (new `outbox_events` / `processed_events` table, new
  columns, new status enum values) — list them; they get created with `/new-migration`.

### 10. Choreography sanity check
State the hop count and the number of conditional branches. If hops > 4 or branches > 2,
add a paragraph weighing orchestration (a coordinator service owning the sequence) against
staying with choreography, and make a recommendation. If it's within bounds, say so in one
line.

### 11. Next actions
The exact skill invocations to wire it, one per step, in order. For example:

```
/new-go-api-endpoint  booking-service CreateBooking   http:POST:/api/v1/bookings  publish:BookingRequested:booking
/new-go-api-endpoint  event-service   ReserveSeat     consume:BookingRequested:booking.events  publish:SeatReserved:seat_reservation
/new-go-api-endpoint  booking-service ConfirmBooking  consume:SeatReserved:seat_reservation.events
/new-go-api-endpoint  booking-service CancelBooking   consume:SeatReservationFailed:seat_reservation.events
```

Note next to each whether the consuming service already has a consumer for that topic (add a
group) or needs the messaging adapter scaffolded.

## After writing
- `python3 -c "import pathlib,sys; sys.exit(0 if pathlib.Path('docs/sagas/$ARGUMENTS.md').exists() else 1)"` — confirm the file landed.
- Do **not** start wiring code. Hand the file back to the user; the `/new-*-api-endpoint`
  runs are separate, reviewable steps, and `saga-consistency-reviewer` audits the result
  against this design.
