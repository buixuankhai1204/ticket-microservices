---
name: new-go-api-endpoint
description: Add a business operation to an existing Go service — a REST endpoint, a Kafka saga step (publish a domain event through the transactional outbox and/or consume one to trigger a local, possibly compensating, use case), or both — following the service's Clean Architecture layers, paginating list endpoints, documenting with swaggo. Use for any new operation (create booking, reserve seat, react to SeatReservationFailed) on a Go service scaffolded by /new-go-service.
argument-hint: "<service-name> <UseCaseName> [http:<METHOD>:<path>] [publish:<EventName>:<aggregate_type>] [consume:<EventName>:<topic>]"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Go Business Operation (REST endpoint / saga step)

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (Clean Architecture layers, choreography saga, delivery semantics): @CLAUDE.md
- Target service layout: !`ls -R services 2>/dev/null | head -80`
- Existing outbox / messaging code in this repo: !`grep -rl "outbox_events\|kafka" services 2>/dev/null | head -20`
- Debezium connector defs (CDC publish side): !`ls debezium/ 2>/dev/null`

## Arguments
`$ARGUMENTS` — `<service-name> <UseCaseName>` plus **at least one trigger**:

- `http:<METHOD>:<path>` — expose it as a REST endpoint, e.g. `http:POST:/api/v1/bookings`.
- `consume:<EventName>:<topic>` — trigger it from a consumed Kafka event, e.g.
  `consume:SeatReservationFailed:seat_reservation.events`. A **compensating** operation uses this.
- `publish:<EventName>:<aggregate_type>` — *additionally* emit a domain event through the
  transactional outbox, e.g. `publish:BookingRequested:booking`. Combine with `http:` or `consume:`.

Examples:
- `booking-service CreateBooking http:POST:/api/v1/bookings publish:BookingRequested:booking`
- `event-service ReserveSeat consume:BookingRequested:booking.events publish:SeatReserved:seat_reservation`
- `booking-service CancelBooking consume:SeatReservationFailed:seat_reservation.events`

Verify `services/<service-name>/go.mod` exists first — if not, stop and suggest `/new-go-service`.

## Choreography + delivery semantics (when `publish:` / `consume:` is present)
**Choreography, not orchestration:** no central coordinator. A service publishes events about
its own state changes and independently reacts to others' — including compensating its own
earlier step on a downstream failure. Never call another service's step directly; a service
only needs to know "what do I do when I see event X."

**Delivery is at-least-once, made effectively-once by the consumer** — not Kafka EOS (there is
no producer on the publish path; it's CDC). Publish can duplicate (Debezium re-emits an
`outbox_events` insert whose offset wasn't committed before a connector restart); consume can
duplicate (the offset commits only after the side effect commits). The `processed_events`
check (step 7) makes a duplicate a no-op → exactly-once *processing*. Right for a step where a
duplicate is harmful-but-absorbable and a lost event is not (seat decrement, booking
confirmation). Fire-and-forget / loss-tolerant traffic does **not** go through the outbox.

## Instructions

1. **Preconditions.** `go.mod` exists. If `http:` given, verify `<path>` starts with the exact
   prefix this service owns in `kong/kong.yml` (`strip_path: false`, so the full path is what
   the handler registers) — if it matches no route Kong sends here, stop and tell the user to
   add it to `kong.yml` first.

2. **domain → platform/port → usecase → repository** (always; @CLAUDE.md conventions — UUID
   IDs, explicit response mapper, usecase owns the transaction):
   - `internal/domain/` — a new entity's ID field is `uuid.UUID` (`github.com/google/uuid`),
     minted with `uuid.New()` in the entity constructor, never an auto-increment integer. A new
     business rule is a method on the entity that returns a domain error (e.g.
     `ErrSeatUnavailable`) on violation, not an ad hoc check in the usecase.
   - `internal/platform/port/` — add any new port method to the `Repository` interface here; it
     takes `ctx, tx pgx.Tx, …`. Don't define it in the postgres adapter first, and don't put it
     in `domain` (the port names the driver's tx handle).
   - `internal/usecase/` — add `<UseCaseName>UseCase`, constructor-injected with the
     `platform/port` ports it needs **and the `*pgxpool.Pool`**; one exported method (e.g.
     `Execute(ctx, input) (output, error)` — no framework types in the signature). The method
     **owns the transaction**: do all non-DB work first (entity construction, hashing, payload
     building), then open one `tx` — `pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})`
     for a read, `pool.Begin(ctx)` for a write — `defer tx.Rollback(ctx)`, pass `tx` to every
     repository call in the flow, `tx.Commit(ctx)` at the end. Any operation with a `publish:`
     is a write flow (the outbox insert rides this same `tx`).
   - `internal/adapter/repository/postgres/` — implement the new `Repository` method(s). Each
     takes `ctx, tx pgx.Tx, …` and runs its statements on that `tx`; **never** `pool.Begin` /
     `tx.Begin` / `tx.Commit` inside the adapter. The adapter struct holds no pool.

3. **If the operation returns a list** (only meaningful with `http:`), it must be paginated —
   never an unbounded result set:
   - `internal/domain/` — a shared `Pagination` value type (`internal/domain/pagination.go` if
     absent, reused by every list endpoint, not redefined per entity): `Limit, Offset int`,
     built via `NewPagination(limit, offset int) (Pagination, error)` that rejects `offset < 0`
     and `limit < 1` with a domain error and clamps `limit` to `const MaxLimit = 100` rather
     than erroring. Absent input → `limit = 20, offset = 0`.
   - `internal/platform/port/` — the list method takes `tx pgx.Tx` and a `Pagination` and
     returns `(items []T, total int, error)`; `total` is the full match count ignoring
     limit/offset.
   - `internal/adapter/repository/postgres/` — `SELECT ... ORDER BY <stable column> LIMIT $n
     OFFSET $n` plus `SELECT COUNT(*) ...` with the same filter, both on the read-only `tx` the
     usecase opened in step 2 so they can't disagree.
   - `internal/usecase/` — passes the already-validated `Pagination` through, returns `total`
     alongside the items.
   - `internal/adapter/http/` — parse `limit`/`offset` from `r.URL.Query()` as integers; a
     non-integer or negative value is a 400 (only an *absent* param gets the default). Response
     is an envelope, not a bare array:
     ```go
     type PaginatedBookingsResponse struct {
         Data       []BookingResponse `json:"data"`
         Pagination PaginationMeta    `json:"pagination"`
     }
     type PaginationMeta struct {
         Limit   int  `json:"limit"`
         Offset  int  `json:"offset"`
         Total   int  `json:"total"`
         HasMore bool `json:"has_more"`
     }
     ```
     `HasMore = offset+len(data) < total`. Extend the response-mapper convention (step 4) to
     the envelope too.

4. **`http:` — the HTTP edge** (skip entirely if no `http:` argument):
   - `internal/adapter/http/` — request/response DTOs kept separate from domain entities (no
     domain types in JSON tags), and a **named mapper function** (`ToBookingResponse(b
     *domain.Booking) BookingResponse`-style, next to the DTO) — not inline field copying in
     the handler. The handler decodes the request, calls the usecase, calls the mapper, and
     maps domain errors to HTTP status codes explicitly (`errors.Is(err,
     domain.ErrSeatUnavailable) → 409`, `errors.Is(err, domain.ErrNotFound) → 404`, unmapped →
     500). Register the route on the exact `<METHOD>`/`<path>` in the router in `cmd/main.go`.
   - Document it with `swaggo` so `api-doc-sync` can curl the live `/swagger/doc.json`:
     - **Bootstrap once per service, only if missing** (check for `docs/docs.go` or a
       `/swagger` route in `cmd/main.go`): add `github.com/swaggo/swag` and
       `github.com/swaggo/http-swagger` to `go.mod`; general API annotations above `func main()`
       (`// @title <ServiceName> API`, `// @version 1.0`, `// @BasePath /api/v1`,
       `// @securityDefinitions.apikey BearerAuth`, `// @in header`, `// @name Authorization`);
       `mux.Handle("/swagger/", httpSwagger.WrapHandler)`; blank-import `_ "<module>/docs"`.
     - Doc comments above the handler: `@Summary`, `@Description`, `@Tags <service-noun>`,
       `@Accept json`, `@Produce json`, `@Param request body <RequestDTO> true "..."` if it has
       a body, `@Param limit query int false "default 20, max 100"` / `@Param offset query int
       false "default 0"` if it's a list endpoint, `@Success <code> {object} <ResponseDTO>`, one
       `@Failure <code> {object} ErrorResponse "..."` per mapped error, `@Security BearerAuth`
       if the route needs JWT, `@Router <path> [<method-lowercase>]` matching the registered path.
     - Regenerate: `swag init -g cmd/main.go -o docs` if `command -v swag`. If not installed,
       say so and tell the user `go install github.com/swaggo/swag/cmd/swag@latest` — don't skip
       silently; `api-doc-sync` has nothing to curl until `swag init` has run once.

5. **`publish:<EventName>:<aggregate_type>` — emit through the transactional outbox** (skip if
   no `publish:`).

   **First, ask the user two questions and wait for the answers** — the implementation depends
   on them:
   1. **What delivery guarantee does `<EventName>` need?**
      - **(a) effectively-once** — at-least-once delivery + *every* consumer dedupes on the
        event `id` (`processed_events`). The default, and required when a duplicate is harmful:
        seat/stock decrement, a charge/debit, an order-state transition, a confirmation. →
        proceed with the outbox below and, in step 8's summary, list each consumer that must
        add a `processed_events` check.
      - **(b) at-least-once, duplicates harmless** — consumers are naturally idempotent
        (upsert-by-id, "set state to X", cache-invalidate, search reindex). → proceed with the
        outbox below; note in the summary that consumers rely on natural idempotency, no
        `processed_events` required.
      - **(c) at-most-once / loss tolerable** — best-effort metrics, non-critical
        notifications, telemetry the next update supersedes. → **stop**: this is *not* an
        outbox event. It's a direct Kafka producer call made outside the DB transaction and
        explicitly documented as lossy — tell the user this skill covers transactional-outbox
        publishing only, and don't add an `outbox_events` row.
   2. **Which service(s) will consume `<aggregate_type>.events`?** Get the list. For each, note
      whether it already has a consumer for this topic or needs a new `consume:` step, and
      (for guarantee (a)) that it must dedupe on the event `id`. Put this in the step 8 summary
      so the saga is actually wired end to end, not just half-published.

   For guarantee (a) or (b), the publish side is **log-tailing CDC, not an in-process relay**:
   a Debezium connector tails `outbox_events` from the WAL and publishes each insert via the
   Outbox Event Router SMT. You write the outbox row (and delete it in the same txn); you write
   **no** relay/producer code.
   - `internal/domain/` — define `<EventName>` as a plain struct (e.g. `BookingRequested{
     BookingID, EventID, UserID, RequestedAt}`), no Kafka types, `json`-serializable with
     snake_case fields (it becomes the Kafka message value verbatim). Give the usecase result /
     aggregate a way to carry "pending events" and a method returning the `aggregate_type`
     string.
   - `internal/adapter/repository/postgres/` — add `WriteOutbox(ctx context.Context, tx pgx.Tx,
     ev <EventName>) error` (declared on the `internal/platform/port` `Repository`, like every
     other method). It `INSERT`s a row into `outbox_events` (`id, aggregate_id, aggregate_type,
     event_type, payload JSONB, created_at`) **then `DELETE`s that same row** — both on the
     `tx`. The usecase from step 2 calls `repo.<StateWrite>(ctx, tx, …)` then
     `repo.WriteOutbox(ctx, tx, ev)` on its one read-write `tx`, then `Commit`s — the state
     change and its event are one atomic unit. The `INSERT` lands in the WAL for Debezium; the
     `DELETE` (which the connector ignores) keeps the table empty. Create the
     `outbox_events` migration if this service lacks one — copy the shape from
     `services/user-service/migrations/*_create_outbox_events.sql` + `*_outbox_debezium.sql`
     (no `published_at`).
   - `debezium/<service-name>-outbox.json` — if this service has no connector yet, add one
     (copy `debezium/user-service-outbox.json`): point `database.*` at the service's Postgres,
     unique `slot.name` / `publication.name`, `table.include.list=public.outbox_events`,
     `skipped.operations=u,d,t`, and the `EventRouter` transform
     (`route.by.field=aggregate_type`, `route.topic.replacement=${routedByValue}.events`,
     `aggregate_id` → key, `event_type`/`id` → headers, `table.expand.json.payload=true`).
     Register it in the `connect-init` step in `docker-compose`; the service's Postgres needs
     `wal_level=logical` (its `command:` in compose). If the connector already exists, a new
     `aggregate_type` routes to its own topic automatically — nothing to do.
   - Add `<aggregate_type>.events` and `<aggregate_type>.events.dlq` to the `kafka-init`
     topic-creation step. Tell the user which service(s) should now `consume:` it.

6. **`consume:<EventName>:<topic>` — trigger the operation from a Kafka event** (skip if no
   `consume:`; here the operation has no `http:` edge unless one was also requested):
   - `internal/adapter/messaging/kafka/consumer.go` — subscribe to `<topic>` with consumer
     group `<service-name>-<EventName>` via `segmentio/kafka-go`; `FetchMessage` +
     `CommitMessages` with `CommitInterval: 0` (manual commit — the offset advances only after
     a message is fully processed). Deserialize into the `<EventName>` domain type.
   - Idempotency: `<UseCaseName>UseCase` (step 2) opens one read-write `tx` and, on it, checks a
     `processed_events` table (unique constraint on event ID) **before** the side effect,
     skipping if already present — check and side effect on the same `tx`. The consumer calls
     the use case, never a repo method directly. Add the `processed_events` migration if
     missing (`*_create_processed_events.sql`).
   - If `<EventName>` is a downstream failure signal (e.g. `SeatReservationFailed`),
     `<UseCaseName>UseCase` is a **compensating** action that undoes this service's own earlier
     step (e.g. cancel a pending booking) — not a retry.
   - **Failure handling is mandatory — terminate every message, never wedge a partition.**
     Classify: success / idempotent no-op → commit the offset; transient error (an
     `*domain.RepositoryError`, `errors.As`) → do **not** commit, retry in-process with capped
     backoff up to `KAFKA_CONSUMER_MAX_ATTEMPTS` (env, default 5); poison (won't unmarshal,
     missing/non-UUID fields) → publish to `<topic>.dlq` (a `kafka.Writer`) then commit, never
     retry; permanent domain rejection → `<topic>.dlq` then commit; retries exhausted →
     `<topic>.dlq` then commit. DLQ records keep the original key/value plus headers
     `x-dlq-reason` and source topic/partition/offset. If the DLQ write fails, leave the offset
     uncommitted. Add `<topic>.dlq` to the `kafka-init` / `docker-compose` step.

7. **Never wire a synchronous cross-service HTTP call.** If a successful call needs another
   service to react (creating a booking should reserve a seat in `event-service`), that is a
   `publish:` on this same invocation, not an HTTP call from the usecase.

8. Check the dependency rule (`domain` imports nothing local and no driver; `usecase` imports
   `domain` + `platform/port` + `pgx`/`pgxpool` but never `adapter`), then summarize what was
   added and what the user must still fill in (persistence columns, validation rules).

## Reference (already implemented)
`analytics-service` consumes `UserCreated` off `user.events` in
`internal/adapter/messaging/kafka/consumer.go` (group `analytics-service-UserCreated`): manual
offset commit, `event_type` header guard, `processed_events` idempotency check,
`RecordUserRegistrationUseCase` projecting into `user_registrations`, bounded
retry-with-backoff on `*domain.RepositoryError`, and a `kafka.Writer` dead-lettering poison /
permanently-failing / retry-exhausted / unexpected-`event_type` messages to
`user.events.dlq`. Publish side: `user-service` (Rust) writes an `outbox_events` row with
`aggregate_type = "user"`; the Debezium `user-service-outbox` connector routes it to
`user.events` — no relay code.

Sketched next: `booking-service` `publish:BookingRequested:booking` → `event-service`
`consume:BookingRequested:booking.events publish:SeatReserved:seat_reservation` (or
`SeatReservationFailed`) → `booking-service`
`consume:SeatReservationFailed:seat_reservation.events` compensates by cancelling the pending
booking.

Do not write tests unless asked — this command wires the operation through the layers; testing
is a separate pass.
