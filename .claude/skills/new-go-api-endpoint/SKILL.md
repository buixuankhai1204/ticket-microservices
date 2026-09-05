---
name: new-go-api-endpoint
description: Add one business operation to an existing Go service — a REST endpoint (http:), a Kafka saga step that publishes a domain event through the transactional outbox (publish:) and/or consumes one to run a local, possibly compensating, use case (consume:), or a combination — through the service's Clean Architecture layers, with UUID ids, an explicit response mapper, the usecase owning one transaction, paginated lists, swaggo docs, and (for saga steps) idempotency + DLQ handling. Use for any new operation on a Go service; for anything crossing services, run /design-saga first.
argument-hint: "<service-name> <UseCaseName> [http:<METHOD>:<path>] [publish:<EventName>:<aggregate_type>] [consume:<EventName>:<topic>]"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Go business operation

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (layers, choreography saga, delivery semantics): @CLAUDE.md
- Approved saga designs: !`ls docs/sagas/ 2>/dev/null || echo "(none — run /design-saga for a cross-service flow)"`
- Target service layout: !`ls -R services/$1/internal 2>/dev/null | head -80`
- Existing outbox / consumer code to mirror: !`grep -rl "outbox_events\|kafka-go" services 2>/dev/null | head -20`

## Arguments
`$ARGUMENTS` — `<service-name> <UseCaseName>` plus **at least one trigger**:
- `http:<METHOD>:<path>` — a REST endpoint, e.g. `http:POST:/api/v1/bookings`.
- `consume:<EventName>:<topic>` — run the use case from a consumed event, e.g.
  `consume:SeatReservationFailed:seat_reservation.events`. A compensating step uses this.
- `publish:<EventName>:<aggregate_type>` — *also* emit an event through the outbox, e.g.
  `publish:BookingRequested:booking`. Combine with `http:` or `consume:`.

Verify `services/<service-name>/go.mod` exists — if not, stop and suggest `/new-go-service`.

## Step 0 — design first if this crosses services
If the operation has a `publish:` or `consume:` **and** the end-to-end flow isn't already
captured in `docs/sagas/*.md`, stop and run `/design-saga` first. That artifact decides the
delivery guarantee per event, the compensation map, the topic/DLQ list, and which services
consume — this skill only *wires one step* of it. For a plain `http:` operation on a
well-understood service, skip straight to step 1.

## Step 1 — domain → platform/port → usecase → repository (always)
Follow @CLAUDE.md's layer rules. Concretely:
- **`internal/domain/`** — a new entity's ID field is `uuid.UUID` (`github.com/google/uuid`),
  minted with `uuid.New()` in the **entity constructor**. A new business rule is a method on
  the entity returning a domain error (`ErrSeatUnavailable`), not an ad-hoc check in the
  usecase.
- **`internal/platform/port/`** — add new methods to the `Repository` interface here; each
  takes `ctx context.Context, tx pgx.Tx, …`. Don't define it on the postgres adapter first;
  don't put it in `domain`.
- **`internal/usecase/`** — add `<UseCaseName>UseCase`, constructor-injected with the
  `platform/port` ports it needs **and the `*pgxpool.Pool`**; one exported method
  `Execute(ctx, input) (output, error)` — no framework types in the signature. It **owns the
  transaction**: do all non-DB work first (entity construction, hashing, payload building),
  then open one `tx` — `pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})` for a
  read, `pool.Begin(ctx)` for a write — `defer tx.Rollback(ctx)`, pass `tx` to every repo
  call, `tx.Commit(ctx)` at the end. Any `publish:` makes it a write flow.
- **`internal/adapter/repository/postgres/`** — implement the new `Repository` method(s).
  Each takes `ctx, tx pgx.Tx, …` and runs on that `tx`; **never** `Begin`/`Commit` inside the
  adapter; the adapter holds no pool. If a new method scans the **same entity shape** as an
  existing method (e.g. a new `LockSeatsForReservation` alongside `ListSeatsForEvent`), extract
  a shared `scan<Entity>(row rowScanner) (domain.<Entity>, error)` helper (`rowScanner` =
  `interface{ Scan(dest ...any) error }`, satisfied by both `pgx.Rows` and `pgx.Row`) and
  refactor the existing method onto it too — do not copy the inline `rows.Scan(&x.A, &x.B, …)`
  column list into a second place. Keep failure classification (which missing row means which
  domain outcome) out of the repo where it's a saga-specific judgment — return the raw rows /
  a `bool`, let the `usecase` branch on it; only reuse an already-established sentinel like
  `domain.ErrNotFound` for a plain "parent row absent".

## Step 2 — paginate if it returns a list (with `http:`)
Never an unbounded result set.
- **`domain`** — a shared `Pagination` (`internal/domain/pagination.go` if absent, reused by
  every list endpoint): `Limit, Offset int`, `NewPagination(limit, offset) (Pagination,
  error)` rejecting `offset < 0` / `limit < 1` with a domain error, clamping `limit` to
  `const MaxLimit = 100`; absent input → `limit=20, offset=0`.
- **`platform/port`** — the list method takes `tx pgx.Tx` + `Pagination`, returns
  `(items []T, total int, error)` — `total` is the full match count.
- **`repository`** — `SELECT … ORDER BY <stable col> LIMIT $n OFFSET $n` plus a matching
  `SELECT COUNT(*)`, both on the one read-only `tx`.
- **`adapter/http`** — parse `limit`/`offset` from `r.URL.Query()` as integers; a
  non-integer or negative value is `400` (only an *absent* param gets the default). Response
  is an envelope, never a bare array:
  ```go
  type PaginatedBookingsResponse struct {
      Data       []BookingResponse `json:"data"`
      Pagination PaginationMeta    `json:"pagination"` // Limit, Offset, Total, HasMore
  }
  ```
  `HasMore = offset+len(data) < total`. The response mapper (step 3) extends to the envelope.

## Step 3 — the HTTP edge (`http:` only)
- **`internal/adapter/http/`** — request/response DTOs separate from domain entities (no
  domain types in JSON tags) and a **named mapper** (`ToBookingResponse(b *domain.Booking)
  BookingResponse`) next to the DTO — not inline field copying. The handler decodes the
  request, calls the usecase, calls the mapper, maps domain errors to status codes
  explicitly (`errors.Is(err, domain.ErrSeatUnavailable)` → `409`; `domain.ErrNotFound` →
  `404`; unmapped → `500`). Register the route on the exact `<METHOD> <path>` in the router.
- **Path check** — `<path>` must start with the prefix this service owns in `kong.yml`
  (`strip_path: false`). If Kong wouldn't route it here, stop and tell the user to add it to
  `kong.yml` first.
- **swaggo docs** — bootstrap once per service if missing (no `docs/docs.go` / no `/swagger`
  route): add `github.com/swaggo/swag` + `github.com/swaggo/http-swagger`; general
  annotations above `func main()` (`// @title`, `// @version`, `// @BasePath /api/v1`, and
  `// @securityDefinitions.apikey BearerAuth` / `// @in header` / `// @name Authorization` if
  the service has JWT routes); `mux.Handle("/swagger/", httpSwagger.WrapHandler)`;
  blank-import `_ "<module>/docs"`. Then annotate the handler: `@Summary`, `@Description`,
  `@Tags`, `@Accept`/`@Produce json`, `@Param` (body DTO; `limit`/`offset` for a list),
  `@Success <code> {object} <ResponseDTO>`, one `@Failure` per mapped error, `@Security
  BearerAuth` if JWT, `@Router <path> [<method>]`. Regenerate: `swag init -g cmd/main.go -o
  docs` (if `command -v swag`; else tell the user `go install
  github.com/swaggo/swag/cmd/swag@latest` — don't skip silently, `docs/` must be committed or
  the build breaks).

## Step 4 — publish through the transactional outbox (`publish:` only)
The delivery guarantee and consumer list come from the `docs/sagas/*.md` design (step 0). The
publish side is **log-tailing CDC, not an in-process relay** — you write the outbox row, no
producer code.
- **`internal/domain/`** — define `<EventName>` as a plain `json`-serializable struct with
  snake_case fields (`BookingRequested{ EventID, BookingID, UserID, RequestedAt }`), no Kafka
  types — it becomes the Kafka message value verbatim. Include `EventID uuid.UUID`. Give the
  usecase result / aggregate a way to carry pending events and a method returning the
  `aggregate_type` string.
- **`repository`** — add `WriteOutbox(ctx, tx pgx.Tx, ev <EventName>) error` (declared on the
  `platform/port` `Repository`). It `INSERT`s a row into `outbox_events` (`id, aggregate_id,
  aggregate_type, event_type, payload JSONB, created_at`) **then `DELETE`s that same row** —
  both on the `tx`. The usecase calls `repo.<StateWrite>(ctx, tx, …)` then `repo.WriteOutbox(ctx,
  tx, ev)` on its one read-write `tx`, then `Commit`s: the state change and its event are one
  atomic unit. Add the `outbox_events` migration via `/new-migration` if the service lacks
  one.
- **`debezium/<service-name>-outbox.json`** — if the service has no connector, add one (copy
  `debezium/user-service-outbox.json`): repoint `database.*`, unique `slot.name` /
  `publication.name`, `table.include.list=public.outbox_events`, `skipped.operations=u,d,t`,
  the `EventRouter` transform (`route.by.field=aggregate_type`,
  `route.topic.replacement=${routedByValue}.events`, `aggregate_id`→key, `event_type`/`id`→
  headers, `table.expand.json.payload=true`). Register it in the `connect-init` step; the
  service's Postgres needs `wal_level=logical`. If the connector exists, a new
  `aggregate_type` routes automatically — nothing to do.
- Add `<aggregate_type>.events` and `<aggregate_type>.events.dlq` to the `kafka-init` step.
- If `/add-observability` has run on this service, also write the current span's `traceparent`
  into the outbox row's `tracecontext` column so the trace follows the event.

## Step 5 — consume an event (`consume:` only)
- **`internal/adapter/messaging/kafka/`** — subscribe to `<topic>`, consumer group
  `<service-name>-<EventName>`, `segmentio/kafka-go` `FetchMessage` + `CommitMessages` with
  `CommitInterval: 0` (manual commit — the offset advances only after the side effect
  commits). Deserialize into the `<EventName>` domain type. Reuse the generic consumer engine
  in `analytics-service` (`consumer.go`) rather than hand-rolling per event type.
- **Multi-type topic** — an `<aggregate_type>.events` topic carries every event about that
  aggregate. Guard on the `event_type` header: not `<EventName>` → **ack and skip** (commit
  the offset), never dead-letter. Only a message whose `event_type` *is* `<EventName>` (or
  header-less) and won't deserialize is poison → DLQ. Adding the *second* consumer to a topic
  ⇒ switch the first's mismatch branch from DLQ to skip in the same pass.
- **Idempotency** — `<UseCaseName>UseCase` opens one read-write `tx` and, on it, checks
  `processed_events` (unique on event id) **before** the side effect, skipping if present.
  The consumer calls the use case, never a repo method directly. Add the `processed_events`
  migration via `/new-migration` if missing.
- **Compensation** — if `<EventName>` is a downstream failure signal, `<UseCaseName>UseCase`
  is a **forward correction** of this service's own earlier step (set the booking to
  `cancelled`, emit `BookingCancelled`), never a delete or a retry.
- **Failure handling (mandatory — terminate every message)** — classify: success / idempotent
  no-op → commit; transient (`*domain.RepositoryError`, `errors.As`) → do **not** commit,
  retry in-process with capped backoff + jitter up to `KAFKA_CONSUMER_MAX_ATTEMPTS` (env,
  default 5); poison (won't unmarshal, missing/non-UUID fields) → `<topic>.dlq` (a
  `kafka.Writer`) then commit, never retry; permanent domain rejection → `<topic>.dlq` then
  commit; retries exhausted → `<topic>.dlq` then commit. DLQ records keep the original
  key/value + headers `x-dlq-reason`, source topic/partition/offset. If the DLQ write fails,
  leave the offset uncommitted. Add `<topic>.dlq` to `kafka-init`.
- If `/add-observability` has run, extract `traceparent` from the message headers and start
  the processing span from that remote context.

## Step 6 — never wire a synchronous cross-service HTTP call
If a successful operation needs another service to react, that's a `publish:` on this same
invocation, not an HTTP call from the usecase.

## Step 7 — verify and hand off
- `go build ./...`, `go vet ./...`, `gofmt -l .` clean.
- Dependency rule: `domain` imports nothing local and no driver; `usecase` imports `domain` +
  `platform/port` + `pgx`/`pgxpool` but never `adapter`.
- Summarize what was added and what the user still fills in (persistence columns via
  `/new-migration`, validation rules). Note that `saga-consistency-reviewer` should audit any
  `publish:`/`consume:` step and `unit-test-writer` / `integration-test-writer` cover the new
  domain / usecase code. Do not write tests here — that's a separate pass.

## Reference (implemented)
`user-service` (Rust) writes `outbox_events` with `aggregate_type = "user"` (`UserCreated`
on register, `UserLoggedIn` on login), each deleted in the same txn; the Debezium
`user-service-outbox` connector routes both to `user.events`. `analytics-service` (Go)
consumes `user.events` with two groups (`analytics-service-UserCreated` → `user_registrations`,
`analytics-service-UserLoggedIn` → `user_logins`), each behind a `processed_events` check,
ack-and-skipping the other group's `event_type`, dead-lettering poison / permanent / retry-
exhausted to `user.events.dlq` — via the generic consumer engine in `consumer.go`.
