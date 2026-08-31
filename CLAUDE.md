# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This repository is in early scaffolding: it currently contains only the Kong API Gateway
configuration (`kong/kong.yml`). No service code exists yet. There are no build, lint, or
test commands to run until services are added — do not invent any.

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
`/new-go-api-endpoint` / `/new-rust-api-endpoint` to add an endpoint to one afterward, and
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
  event_type, payload JSONB, created_at` (no `published_at` — nothing stamps rows). The SMT
  maps `aggregate_id` → Kafka message key (so a partition preserves per-aggregate order),
  `aggregate_type` → topic **`<aggregate_type>.events`**, `event_type`/`id` → headers,
  `payload` → the message value (unwrapped). The connector is set `skipped.operations=u,d,t`
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

Wiring a publish or consume step is folded into `/new-go-api-endpoint` /
`/new-rust-api-endpoint` — pass `publish:<EventName>:<aggregate_type>` and/or
`consume:<EventName>:<topic>` alongside (or instead of) `http:<METHOD>:<path>`. For a
`publish:` the skill first asks which delivery guarantee the event needs and which services
will consume it.

Implemented so far (reference these when wiring a new step): `user-service`'s
`RegisterUserUseCase` opens one read-write transaction, and on it the repository inserts the
users row and writes+deletes the `UserCreated` `outbox_events` row (`aggregate_type = "user"`)
before the use case commits; the Debezium `user-service-outbox` connector tails the WAL and
routes it to the `user.events` topic → `analytics-service`'s `RecordUserRegistrationUseCase`
consumes `user.events` (group `analytics-service-UserCreated`), and on its own single
transaction the repository does the `processed_events` check and projects a
`user_registrations` read-model row; the consumer dead-letters poison / permanently-failing /
retry-exhausted / unexpected-`event_type` messages to `user.events.dlq`. Both Go services
(`event-service`, `analytics-service`) follow the usecase-owns-the-transaction shape with the
`Repository` port in `internal/platform/port/`.

Sketched next: `booking-service` publishes `BookingRequested` → `event-service` reserves the
seat and publishes `SeatReserved`/`SeatReservationFailed` → `booking-service` confirms or
compensates (cancels) the booking → `analytics-service` consumes the final outcome as a read
model only.

## Claude Code tooling already set up for this repo

Skills (`.claude/skills/`, invoked as `/name`):

| Skill | Use for |
|---|---|
| `new-go-service` / `new-rust-service` | Scaffold a new service in Clean Architecture |
| `new-go-api-endpoint` / `new-rust-api-endpoint` | Add a business operation to a service: a REST endpoint (`http:`), a Kafka saga step (`publish:` / `consume:`), or both |
| `scalability-review` | Read-only audit: statelessness, pool sizing, N+1, missing caching |
| `review-concurrency` | Read-only audit: race conditions, oversell/double-booking |

Subagents (`.claude/agents/`, invoked via the Agent tool or by name):

| Agent | Use for |
|---|---|
| `security-reviewer` | Read-only audit: injection, secrets, JWT/IDOR, log leakage |
| `api-contract-reviewer` | Read-only, whole-repo audit: routes vs `kong.yml` drift |
| `api-doc-sync` | Writer: generates/updates `docs/openapi/*.yaml` and the Postman collection from actual handler code |
| `unit-test-writer` | Writer: unit tests for `domain` only (pure entities/invariants, no mocks, no DB), exhaustive edge cases |
| `integration-test-writer` | Writer: integration tests against real Postgres — concurrency, idempotency, transaction atomicity |

`api-doc-sync` is the only writing agent — it keeps `docs/openapi/<service>.yaml` and
`docs/postman/ticket-platform.postman_collection.json` in sync with implemented handlers,
code always wins over the spec. It doesn't overlap with `api-contract-reviewer`, which checks
code against `kong.yml`, not against API docs.

`/new-go-api-endpoint` and `/new-rust-api-endpoint` also bootstrap `swaggo`/`utoipa` on a
service's first endpoint and annotate every endpoint after — this gives each service its own
live, interactive Swagger UI (`/swagger/` for Go, `/swagger-ui` for Rust) independent of
`api-doc-sync`, which currently reads source instead of that live endpoint. The two aren't
wired together right now; ask before assuming `api-doc-sync` should switch to curling
`swagger/doc.json`/`openapi.json` instead of parsing source.

`unit-test-writer` and `integration-test-writer` split by test *type*, not by language (both
branch internally for Go/Rust, like the review agents do) — the two need genuinely different
infrastructure (pure `domain` calls + no Docker vs. real Postgres via `testcontainers`), so
keeping them separate stops a "quick unit test" request from accidentally pulling in Docker,
and vice versa. `usecase` orchestration is `integration-test-writer`'s job now, not
`unit-test-writer`'s: the use case owns the transaction, so a `pgx.Tx` / `PgConnection` can't
be meaningfully faked. Use `unit-test-writer` after any `domain` change; use
`integration-test-writer` after any `usecase` change and once a service has real endpoints or
saga steps, since it needs something real to hit.

There is deliberately no "service builder" subagent — `new-go-service`/`new-rust-service` and
the `new-*-api-endpoint` skills already cover guided code generation with the repo's
conventions loaded as context; a separate subagent would duplicate them without adding
isolation or safety benefits worth the redundancy.

Hooks (`.claude/settings.json` + `.claude/hooks/`):

- `pre-commit-check.sh` (`PreToolUse` on `Bash`) — only acts when the command is a
  `git commit`. Lints/formats just the Go and Rust services with staged changes
  (`gofmt`/`go vet`, `cargo fmt --check`/`cargo clippy -- -D warnings`), scoped to each
  service's own `go.mod`/`Cargo.toml`, and blocks the commit (exit 2) on failure. Missing
  toolchains are skipped, not treated as failures.
- `clean-architecture-check.sh` (`PostToolUse` on `Write`/`Edit`) — for any file under a
  service's `domain/`, `platform/port/`, `usecase/`, `adapter/http/`, or `adapter/repository/`,
  greps the just-written file for imports that violate the dependency rule above (e.g. `domain/`
  importing `pgx`/`sqlx`/`net/http`/`axum` or `platform/port`, `platform/port/` importing
  `usecase/`/`adapter/`, `usecase/` importing `adapter/` (it *may* import `pgx`/`pgxpool`/`sqlx`
  now — it owns the transaction), `adapter/http/` reaching into `adapter/repository/` instead
  of going through `usecase`).
  Can't undo the edit (it already happened by the time `PostToolUse` fires) but surfaces the
  violation to Claude immediately via exit 2, so it's fixed in the same turn instead of
  surviving until a later review. `cmd/main.go`/`main.rs` (the composition root) is
  intentionally exempt — it's allowed to import everything.

Both only gate actions Claude takes inside a Claude Code session — they do not run for edits
or commits made directly in a terminal/editor outside Claude Code; a real
`.git/hooks/pre-commit` would be needed for that and hasn't been added.

## `plugins/ticket-microservice-toolkit/`: the same tooling, packaged for distribution

`.claude/` above is this repo's own zero-setup config. `plugins/ticket-microservice-toolkit/`
repackages the same skills/agents/hooks as an installable Claude Code plugin
(`.claude-plugin/plugin.json` + `skills/` + `agents/` + `hooks/hooks.json`, hook commands
using `${CLAUDE_PLUGIN_ROOT}` instead of `$CLAUDE_PROJECT_DIR`) so the conventions can be
reused in a sibling repo instead of copy-pasted. See that directory's own `README.md` for
prerequisites and how to test it locally (`claude --plugin-dir
./plugins/ticket-microservice-toolkit`). The two currently duplicate content on purpose —
`.claude/` isn't wired to reference the plugin instead, since that wasn't verified to
auto-load without an explicit `/plugin install` step.

## Recommended Claude Code workflow for this repo

These are built-in Claude Code features, not project config — noted here so the right one
gets reached for instead of skipped:

- **Plan mode** (`/plan`, or `Shift+Tab` to cycle modes) — use before `/new-go-service`,
  `/new-rust-service`, or a `/new-*-api-endpoint` invocation carrying `publish:` / `consume:`
  (a saga step touches multiple files across layers plus Debezium / topic-init config in one
  action), so reviewing the plan first is cheaper than reviewing the diff after. Not needed for
  a plain `http:` `/new-*-api-endpoint` on an existing, well-understood service.
- **Extended thinking** (`/effort high`, or the word "ultrathink" in a prompt) — worth reaching
  for on the genuinely hard design calls in this domain: the seat-reservation locking strategy
  in `booking-service` (see `/review-concurrency`), or working out a new saga's event sequence
  and compensation path before wiring it with a `/new-*-api-endpoint` `publish:` / `consume:`
  step. Not needed for routine CRUD endpoints.
- **Background tasks** — once services exist, run each service's test suite as a background
  task when working across more than one service at a time (e.g. verifying `booking-service`
  and `event-service` both still pass after a saga change), instead of blocking on one before
  starting the next.
- **Checkpoints** (`Esc` `Esc`, or `/rewind`) — useful for backing out of an exploratory
  scaffold that went the wrong direction, but they don't replace git and don't capture
  filesystem changes made outside Claude Code (`rm`/`mv`/`cp` in a terminal, edits in another
  editor). Commit to git once a change is actually good, the same as always.
