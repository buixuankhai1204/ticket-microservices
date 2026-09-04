# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

Three services exist: `user-service` (Rust/axum — register, login, profile, list; publishes
`UserCreated` + `UserLoggedIn` through the outbox), `event-service` (Go — events + seat reads,
paginated), and `analytics-service` (Go — consumes `user.events` into read models). Each has
its own `go.mod` / `Cargo.toml` — build/lint per service (`go build ./...` + `go vet` +
`gofmt` for Go; `cargo build` + `cargo clippy -- -D warnings` + `cargo fmt` for Rust). The
whole stack runs via `docker-compose.yml` (Kong, a Postgres per service, single-node Kafka,
Kafka Connect + Debezium). `booking-service` — the core seat-reservation saga — is **not yet
built**.

## Architecture (as defined by the gateway)

This is a ticket booking platform split into independent microservices, fronted by a single
Kong Gateway (`kong/kong.yml`, declarative config format `3.0`). The gateway is the source of
truth for how services are named, routed, and secured — check it before adding or renaming a
service.

Planned services, each expected to be its own deployable unit reachable at
`http://<service-name>:<port>` from the gateway:

| Service | Port | Routes | Auth / limits |
|---|---|---|---|
| `user-service` | 8081 | `/api/v1/auth` (60 req/min), `/api/v1/users` (100 req/min) | `/api/v1/users` requires JWT (`exp` claim verified) |
| `event-service` | 8082 | `/api/v1/events` (1000 req/min) | none |
| `booking-service` | 8083 | `/api/v1/bookings` (300 req/min) | JWT (`exp` claim verified) |
| `analytics-service` | 8084 | `/api/v1/analytics` (60 req/min) | JWT (`exp` claim verified) |

All routes use `strip_path: false`, so each service must handle the full `/api/v1/...` prefix
itself rather than expecting it stripped.

Global plugins applied gateway-wide: `cors` (all origins, credentials enabled) and per-route
`rate-limiting` (local policy, per-service limits as above).

The stated intent (see `README.md`) is a mixed-language implementation — Rust and Go across
the different services — rather than a single shared stack. When adding a service, confirm
which language it's meant to be in before scaffolding it, and keep its exposed port and route
prefix in sync with `kong/kong.yml`.

## Application architecture: Clean Architecture per service

Every service (Go or Rust) follows Clean Architecture, dependencies pointing inward only:

- `domain` — entities and business invariants (as methods on the entity), plus any *pure*
  outbound-gateway port (one that names no infra type, e.g. a `PasswordHasher`). No imports of
  any framework or driver (no `pgx`/`sqlx`, no `net/http`/`axum`). The `Repository` port does
  **not** live here — see `platform/port`.
- `platform/port` — the port interfaces/traits that name the DB transaction handle, the
  `Repository` above all: `internal/platform/port/` (Go, `package port`) / `src/platform/port.rs`
  (Rust). Allowed to import the driver *for the handle type only* (`pgx.Tx` / `&mut PgConnection`)
  and `domain`; never `usecase`, `adapter`, or `cmd`. Every `Repository` method takes
  `ctx, tx pgx.Tx, …` (Go) / `conn: &mut PgConnection, …` (Rust).
- `usecase` — orchestrates one business flow per type. Holds the `*pgxpool.Pool` / `PgPool` and
  **owns the transaction boundary**: it opens one transaction per flow (read-only for reads,
  read-write for writes), threads that handle through every repository call, and commits. All
  non-DB work (entity construction, hashing, payload building) happens *before* `Begin` so a
  pooled connection is never pinned across CPU-bound work. Depends on `domain` + `platform/port`
  + `pgx`/`pgxpool` (or `sqlx`) for the handle; never imports `adapter`. Ports and the pool are
  constructor-injected.
- `adapter/http` — controllers/handlers and DTOs, translate transport at the edge, depend on
  `usecase`'s public interface.
- `adapter/repository/postgres` — implements the `platform/port` `Repository` against Postgres;
  the only layer allowed to import the DB driver's query APIs. Every method runs on the
  transaction handle the `usecase` passes in and **never opens or commits its own transaction**;
  it holds no pool. Still returns `domain.ErrNotFound` / `RepositoryError` unchanged.
- `cmd/main.go` (Go) / `main.rs` (Rust) — the composition root: the only place that wires
  concrete adapters into interfaces (the pool goes to each `usecase`, not to the repository)
  and owns the process lifecycle (server startup, graceful shutdown).

Use `/new-go-service` or `/new-rust-service` to scaffold a service in this shape; use
`/new-go-api-endpoint` / `/new-rust-api-endpoint` to add an operation to one afterward
(run `/design-saga` first for anything cross-service); `/new-migration` for a schema change;
`/add-caching`, `/add-resilience`, `/add-observability` to layer in those concerns; and
`/scalability-review` / `/review-concurrency` to audit one.

### Endpoint conventions: entity IDs, response mapping, transactions, pagination

Four rules `/new-go-api-endpoint` and `/new-rust-api-endpoint` enforce on every endpoint they
add, not just the ones that happen to need them:

- **Entity IDs are UUID, never an auto-increment integer.** Go: `github.com/google/uuid`
  (`uuid.UUID` field, `uuid.New()` when an entity is created — in the `domain` constructor, not
  scattered in `usecase`). Rust: the `uuid` crate (`Uuid` field, `Uuid::new_v4()`). Postgres
  columns are `UUID` (`gen_random_uuid()`/`uuid_generate_v4()` default is fine, but the app
  still generates its own so a new entity has an ID before the first insert). Two reasons, not
  just convention: a sequential ID is guessable, which makes IDOR easier to exploit by
  enumeration (see `security-reviewer`); and every saga event already carries an
  `aggregate_id` (see the outbox section below) — UUID entity IDs and event `aggregate_id`s are
  then the same type end to end, no translation at the Kafka boundary.
- **The `adapter/http` layer converts `domain` → response DTO through one named function, not
  ad hoc field copying inline in the handler.** Go: a `ToBookingResponse(b *domain.Booking)
  BookingResponse`-style function next to the DTO. Rust: `impl From<&domain::Booking> for
  BookingResponse` (or an explicit `to_response(&self)` method) next to the DTO. Keeps the
  domain→wire mapping in one place instead of re-derived per handler, and gives
  `api-doc-sync`/tests one clear function to point at.
- **Every endpoint's database access runs inside one transaction, opened by the `usecase`**,
  not only the ones that happen to touch more than one table. The use case holds the pool and
  is the only layer that calls `Begin`/`Commit`/`Rollback`; a read endpoint opens a read-only
  transaction (a consistent snapshot across however many queries it runs), a write endpoint a
  normal read-write one — which is required anyway the moment a `publish:` saga step (see
  `/new-go-api-endpoint` / `/new-rust-api-endpoint`) adds an outbox insert next to the state
  write. The repository method takes the handle (`pgx.Tx` / `&mut PgConnection`) and never
  begins its own; non-DB work (hashing, entity construction) runs before `Begin`, not inside
  the transaction.
- **Every list endpoint is paginated with `limit`/`offset`, never an unbounded result set.** A
  shared `Pagination` domain type per service (not redefined per entity) validates
  `offset >= 0` and `limit >= 1`, defaults to `limit=20, offset=0` when absent, and clamps
  `limit` to a max of 100 rather than erroring on too large a value — an invalid (non-integer
  or negative) value is still a 400. The repository's list method returns the total match count
  alongside the page (one `COUNT(*)` alongside the `LIMIT`/`OFFSET` query, on the same
  read-only transaction the use case opened per the rule above, so the two can't disagree). The
  response is an envelope —
  `{ "data": [...], "pagination": { "limit", "offset", "total", "has_more" } }` — never a bare
  array, so a client can tell it's paginated without reading the docs.

### Schema migrations: zero-downtime, expand/contract

Migrations are timestamped SQL files under `services/<svc>/migrations/`, applied on startup
(Go: an embedded runner wrapping each file in one transaction + `schema_migrations`; Rust:
`sqlx::migrate!`). A deploy runs the new schema while old-version pods still serve, so a
migration must be safe for the **currently running** code:

- **Additive changes ship in one file** — a new table; a nullable column, or `NOT NULL` with
  a constant `DEFAULT`; a `CHECK`/`FK` added `NOT VALID` then `VALIDATE`d later; a new index.
- **Breaking changes are expand/contract across ≥2 deploys** — never `DROP`/`RENAME` a
  column/table, narrow a type, or add a validated constraint in the same migration as (or
  before) the code that stops depending on the old shape. Split: expand (add new, keep old) →
  backfill → switch code → contract (drop old).
- Index every FK column and every column a list endpoint filters/sorts on. `CREATE INDEX
  CONCURRENTLY` needs a `-- +migrate NoTransaction` first line and a runner that honours it.
- IDs are `UUID DEFAULT gen_random_uuid()`; money is integer minor units; enums are `TEXT` +
  `CHECK`. `outbox_events` has no `published_at`; `processed_events` is unique on the event id.

Author with `/new-migration`; `migration-reviewer` and the `migration-safety-check.sh` hook
enforce the above.

### Observability: request IDs and trace context that survive the Kafka hop

Every service (per scaffold, or retrofit with `/add-observability`):

- Emits **one structured access-log line per request** with a `request_id` (inbound
  `X-Request-Id` reused or a fresh UUID, echoed back and put on the context) — not just
  process start/stop. Never logs the `Authorization` header, a raw JWT, or a PII/payment body.
- Runs an **OTel server span that continues the inbound W3C `traceparent`** (Kong/the client
  started the trace), plus spans per use case and per repo call, exported over OTLP.
- Carries **trace context across the saga**: the publishing use case writes the current
  `traceparent` into the outbox row's `tracecontext` column; the Debezium SMT maps it to a
  Kafka header; the consumer extracts it and starts its processing span from that remote
  context. Without this, a client action is two disconnected traces, not one.
- Exposes Prometheus `/metrics` (RED on HTTP; processed/retried/dead-lettered counters and
  lag for consumers), separate from `/healthz` and `/readyz`.

## Cross-service communication: choreography saga over Kafka

Cross-service state changes (e.g. a booking needing a seat reserved in `event-service`) go
through **Kafka**, not synchronous HTTP calls between services — Kong only fronts the
synchronous, client-facing REST routes in the table above; it plays no role in
service-to-service messaging.

The saga style is **choreography**: there is no central coordinator service. Each service
publishes events about its own state changes and independently reacts to events from other
services, including compensating its own earlier step on a downstream failure. Adding or
changing a saga step should never require teaching one service the full sequence — only what
it does when it observes a given event.

**Delivery semantics: at-least-once, made effectively-once by the consumer.** Every hop here
is at-least-once. Publish can duplicate — Debezium re-emits an `outbox_events` insert whose
Kafka offset wasn't committed before the connector restarted. Consume can duplicate — the
offset is committed only *after* the side effect commits, so a consumer crash in between
redelivers. This repo does **not** use Kafka's transactional/EOS producer or end-to-end
exactly-once — there is no producer on the publish path at all (CDC only). Instead the
idempotent consumer below dedupes on the unique event `id`, so applying a duplicate is a
no-op and the net effect is exactly-once *processing*. That is the right trade for saga steps
where a duplicate is harmful but absorbable and a lost event is not (seat decrement /
oversell, booking confirmation). At-most-once (fire-and-forget, loss tolerated) has no place
on the outbox path: if a future step genuinely wants it (best-effort metrics or
notifications), that is a *direct* Kafka producer call outside any DB transaction, explicitly
documented as lossy — not an `outbox_events` row.

Three supporting patterns are required, not optional, for any publish/consume code:

- **Transactional outbox (CDC)** — an event is written to an `outbox_events` table in the
  *same* DB transaction as the state change it describes — the one transaction the `usecase`
  opened for that flow, which it passes to both the state-write repo method and the
  `WriteOutbox` repo method before committing. This avoids the dual-write problem (DB commit
  and Kafka publish can't be made atomic any other way without 2PC). Publishing is
  **log-tailing CDC, not an in-process relay**: a Debezium PostgreSQL connector on Kafka
  Connect reads the WAL via a logical replication slot and publishes each insert through the
  **Outbox Event Router SMT** — sub-second latency, near-zero query load on the write DB,
  scaling independently of the write path. Table shape: `id, aggregate_id, aggregate_type,
  event_type, payload JSONB, created_at` (no `published_at` — nothing stamps rows; plus an
  optional nullable `tracecontext TEXT` once `/add-observability` has run). The SMT maps
  `aggregate_id` → Kafka message key (so a partition preserves per-aggregate order),
  `aggregate_type` → topic **`<aggregate_type>.events`**, `event_type`/`id` (and
  `tracecontext` → `traceparent`) → headers, `payload` → the message value (unwrapped). The connector is set `skipped.operations=u,d,t`
  (inserts only), so a service writes the row and **deletes it again in the same transaction**
  to keep the table empty — the WAL still carries the insert. Connector defs live in
  `debezium/`, registered by the `connect-init` one-shot in `docker-compose`.
- **Idempotent consumers** — Kafka delivers at-least-once. The consumer calls the use case
  (which owns the transaction); the use case checks a `processed_events` table (unique event
  ID) before applying an event, on the same transaction as the side effect.
- **Dead-letter handling** — a consume step must terminate *every* message, never wedge a
  partition. Classify the outcome: success / idempotent no-op → commit the offset; transient
  error (DB/broker blip) → do not commit, retry in-process with capped backoff up to
  `MAX_ATTEMPTS`; poison message (undeserializable) or permanent domain rejection → publish to
  the dead-letter topic `<topic>.dlq` then commit; retries exhausted → `<topic>.dlq` then
  commit, as a last resort. DLQ records carry the original key/payload plus diagnostic headers
  (`x-dlq-reason`, source topic/partition/offset). `<topic>.dlq` is created alongside
  `<topic>` in the topic-init / `docker-compose` step.

Client libraries: `segmentio/kafka-go` for Go services, `rdkafka` for Rust services — for
**consumers**. The publish side is CDC (Debezium), so there is no producer client library on
the publish path.

**Design the saga before wiring it.** Any state change that spans services goes through
`/design-saga <name>` first — it writes `docs/sagas/<name>.md` with the event catalog
(name, `aggregate_type`, topic, delivery guarantee, payload), the happy path, **every**
failure sequence, the compensation map (a compensation is a new forward transaction, not a
rollback), the stuck-saga timeout/reaper policy, and the topic/connector infra delta. That
artifact is what `saga-consistency-reviewer` and `e2e-saga-tester` check the implementation
against. Skip it only for a plain `http:` operation on one service.

Wiring each step is then `/new-go-api-endpoint` / `/new-rust-api-endpoint` — pass
`publish:<EventName>:<aggregate_type>` and/or `consume:<EventName>:<topic>` alongside (or
instead of) `http:<METHOD>:<path>`, once per participant step listed in the design.

Implemented so far (reference these when wiring a new step): `user-service` emits two events on
`aggregate_type = "user"` → the `user.events` topic — `UserCreated` on the
`RegisterUserUseCase` transaction (which also inserts the users row) and `UserLoggedIn` on the
`LoginUserUseCase` transaction (a second, read-write txn after the credential check) — each
written+deleted via the repository `write_outbox` on that same transaction before the use case
commits; the Debezium `user-service-outbox` connector tails the WAL and routes both to
`user.events`. `analytics-service` consumes `user.events` with **two** consumer groups —
`analytics-service-UserCreated` (`RecordUserRegistrationUseCase` → `user_registrations`) and
`analytics-service-UserLoggedIn` (`RecordUserLoginUseCase` → `user_logins`) — each on its own
single transaction doing the `processed_events` check before projecting its read-model row.
Because one topic now carries two event types, each consumer **acks-and-skips** the other
group's `event_type` (guard on the header) rather than dead-lettering it; only poison of its
*own* type, permanent failures, and retry-exhausted messages go to `user.events.dlq`. Both Go
services (`event-service`, `analytics-service`) follow the usecase-owns-the-transaction shape
with the `Repository` port in `internal/platform/port/`.

Sketched next (design it with `/design-saga seat-reservation` before wiring): `booking-service`
publishes `BookingRequested` → `event-service` reserves the seat and publishes
`SeatReserved`/`SeatReservationFailed` → `booking-service` confirms or compensates (cancels)
the booking → `analytics-service` consumes the final outcome as a read model only.

## Claude Code tooling set up for this repo

All of it lives in `.claude/` (skills, agents, hooks) — the zero-setup config anyone who
clones this repo gets. There is **no plugin mirror**; a new convention updates CLAUDE.md and
the affected skill(s)/agent(s), nothing else.

### Skills (`.claude/skills/`, invoked as `/name`)

| Skill | Use for |
|---|---|
| `new-go-service` / `new-rust-service` | Scaffold a new service in Clean Architecture, with pooling, health, graceful shutdown, request-ID logging, OTel tracing, `/metrics`, and the migrations runner wired in |
| `design-saga` | **Before** any cross-service `publish:`/`consume:` — writes `docs/sagas/<name>.md`: event catalog, happy + failure sequences, compensation map, stuck-saga policy, infra delta |
| `new-go-api-endpoint` / `new-rust-api-endpoint` | Wire one operation through the layers: a REST endpoint (`http:`), a saga step (`publish:` / `consume:`), or both |
| `new-migration` | Author a timestamped SQL migration, expand/contract-safe, indexed, reversible |
| `add-caching` | Cache-aside Redis behind a `domain` port on a hot read path — TTL+jitter, fail-open, per-user keys, invalidation |
| `add-resilience` | Timeout + capped retry-with-jitter + circuit breaker + bulkhead on an outbound `domain` port |
| `add-observability` | Retrofit request-ID logs + OTel traces + `traceparent` across the outbox→Kafka→consumer hop + RED/consumer metrics |
| `scalability-review` | Read-only audit: statelessness, pool sizing, N+1, missing caching/metrics, tx scope, shutdown |
| `review-concurrency` | Read-only audit: race conditions, oversell/double-booking, tx ownership |

### Subagents (`.claude/agents/`, invoked via the Agent tool or by name)

| Agent | Use for |
|---|---|
| `security-reviewer` | Read-only: injection, secrets, JWT trust boundary, IDOR, unsafe consumers, log/trace leakage, Kong footguns |
| `api-contract-reviewer` | Read-only, whole-repo: HTTP routes vs `kong.yml` (path, method, port, auth, rate-limit plausibility) |
| `saga-consistency-reviewer` | Read-only, whole-repo: the choreography graph — orphan events, missing topics/DLQs/connectors, missing compensations, non-idempotent consumers, partition-wedge risk, stuck sagas |
| `migration-reviewer` | Read-only: migration files for rolling-deploy safety — lock-heavy DDL, breaking changes without expand/contract, `CONCURRENTLY` in a txn, missing indexes |
| `api-doc-sync` | Writer: keeps `docs/openapi/*.yaml`, the Postman collection, and `docs/curl-examples.md` in sync with handler code (code wins) |
| `unit-test-writer` | Writer: `domain`-only unit tests (pure entities/invariants, no mocks, no DB), exhaustive edge cases |
| `integration-test-writer` | Writer: integration tests vs real Postgres — oversell, idempotency, tx atomicity, compensation, reaper |
| `e2e-saga-tester` | Drives an already-running `docker compose` stack through a saga via Kong and asserts DB + DLQ state, happy path and compensation path |

`api-doc-sync` documents the HTTP surface only; the Kafka contract is `design-saga`'s
`docs/sagas/` artifact, checked by `saga-consistency-reviewer`. `api-contract-reviewer`
checks code against `kong.yml`, not against the API docs — the two don't overlap.

`/new-*-api-endpoint` bootstraps `swaggo`/`utoipa` on a service's first endpoint (live
Swagger UI at `/swagger/` for Go, `/swagger-ui` for Rust). `api-doc-sync` still generates
from source, not by curling that live endpoint — ask before changing that.

`unit-test-writer` and `integration-test-writer` split by test *type*, not language.
`usecase` orchestration is `integration-test-writer`'s job (the use case owns the
transaction, so a `pgx.Tx` / `PgConnection` can't be faked). `e2e-saga-tester` is a third
tier — the running multi-service stack, driven through Kong; it assumes the user brought the
stack up. Use `unit-test-writer` after a `domain` change, `integration-test-writer` after a
`usecase` change, `e2e-saga-tester` after a saga's steps are all wired.

### Hooks (`.claude/settings.json` + `.claude/hooks/`)

- `pre-commit-check.sh` (`PreToolUse` on `Bash`) — only acts on a `git commit`. Lints/formats
  the Go and Rust services with staged changes (`gofmt`/`go vet`,
  `cargo fmt --check`/`cargo clippy -- -D warnings`), scoped to each service's own
  `go.mod`/`Cargo.toml`; blocks the commit (exit 2) on failure. Missing toolchains are
  skipped, not failed.
- `clean-architecture-check.sh` (`PostToolUse` on `Write`/`Edit`) — for a file under a
  service's `domain/`, `platform/port/`, `usecase/`, `adapter/http/`, `adapter/repository/`,
  `adapter/cache/`, or `adapter/messaging/`, greps for imports that break the dependency rule
  (`domain/` importing a driver/framework or `platform/port`; `platform/port/` importing
  `usecase/`/`adapter/`; `usecase/` importing `adapter/`; `adapter/http/` or
  `adapter/cache/` reaching into `adapter/repository/`). `cmd/main.go` / `main.rs` (the
  composition root) is exempt.
- `migration-safety-check.sh` (`PostToolUse` on `Write`/`Edit`) — for a just-written
  `services/*/migrations/*.sql`, flags the grep-obvious rolling-deploy hazards (`ADD COLUMN
  … NOT NULL` with no `DEFAULT`, `DROP COLUMN`, `ALTER COLUMN … TYPE`, `RENAME`,
  `ADD CONSTRAINT` without `NOT VALID`, `CONCURRENTLY` without the `-- +migrate
  NoTransaction` marker) with the expand/contract fix. `migration-reviewer` covers the rest.

`PostToolUse` hooks can't undo the write, but exit 2 surfaces the issue to Claude in the same
turn. All three only gate actions Claude takes inside a Claude Code session — not edits or
commits made directly in a terminal; a real `.git/hooks/pre-commit` would be needed for that
and hasn't been added.

There is deliberately no "service builder" subagent — the scaffold skills already cover
guided generation with the repo's conventions as context.

## Recommended Claude Code workflow for this repo

`docs/development-workflow.md` is the full SDLC — which skill/agent/hook runs at each step
for a new service, a new saga, an endpoint, a migration, and the pre-PR review gate. The
notes below are just the built-in Claude Code features it leans on:

- **Plan mode** (`/plan`, or `Shift+Tab` to cycle modes) — use before `/new-go-service`,
  `/new-rust-service`, or a `/new-*-api-endpoint` invocation carrying `publish:` / `consume:`
  (a saga step touches multiple files across layers plus Debezium / topic-init config in one
  action), so reviewing the plan first is cheaper than reviewing the diff after. Not needed for
  a plain `http:` `/new-*-api-endpoint` on an existing, well-understood service.
- **Extended thinking** (`/effort high`, or the word "ultrathink" in a prompt) — `/design-saga`
  runs with it by default (the failure sequences and compensation ordering are the hard part);
  also reach for it on the seat-reservation locking strategy (see `/review-concurrency`).
  Not needed for routine CRUD endpoints or a scaffold.
- **Background tasks** — once services exist, run each service's test suite as a background
  task when working across more than one service at a time (e.g. verifying `booking-service`
  and `event-service` both still pass after a saga change), instead of blocking on one before
  starting the next. `e2e-saga-tester` against a running stack is a good background job while
  you keep editing.
- **Checkpoints** (`Esc` `Esc`, or `/rewind`) — useful for backing out of an exploratory
  scaffold that went the wrong direction, but they don't replace git and don't capture
  filesystem changes made outside Claude Code (`rm`/`mv`/`cp` in a terminal, edits in another
  editor). Commit to git once a change is actually good, the same as always.
