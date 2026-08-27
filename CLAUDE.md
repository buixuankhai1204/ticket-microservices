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

- `domain` — entities and business invariants (as methods on the entity), plus the port
  interfaces/traits the service needs (repository, outbound gateways). No imports of any
  framework or driver (no `pgx`/`sqlx`, no `net/http`/`axum`).
- `usecase` — orchestrates one business flow per type, depends on `domain` only, receives its
  ports via constructor injection.
- `adapter/http` — controllers/handlers and DTOs, translate transport at the edge, depend on
  `usecase`'s public interface.
- `adapter/repository/postgres` — implements a `domain` port against Postgres; the only layer
  allowed to import the DB driver.
- `cmd/main.go` (Go) / `main.rs` (Rust) — the composition root: the only place that wires
  concrete adapters into interfaces and owns the process lifecycle (server startup, graceful
  shutdown).

Use `/new-go-service` or `/new-rust-service` to scaffold a service in this shape; use
`/new-go-api-endpoint` / `/new-rust-api-endpoint` to add an endpoint to one afterward, and
`/scalability-review` / `/review-concurrency` to audit one.

### Endpoint conventions: entity IDs, response mapping, transactions

Three rules `/new-go-api-endpoint` and `/new-rust-api-endpoint` enforce on every endpoint they
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
- **Every endpoint's database access runs inside one transaction**, not only the ones that
  happen to touch more than one table. A read endpoint uses a read-only transaction (a
  consistent snapshot across however many queries it runs); a write endpoint uses a normal
  read-write transaction — which is required anyway the moment `/add-go-saga-step` or
  `/add-rust-saga-step` adds an outbox insert next to the state write, so starting every write
  handler in a transaction from day one avoids retrofitting it later.

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

Three supporting patterns are required, not optional, for any publish/consume code:

- **Transactional outbox** — an event is written to an `outbox_events` table in the *same* DB
  transaction as the state change it describes, and a background relay publishes from that
  table to Kafka. This avoids the dual-write problem (DB commit and Kafka publish can't be made
  atomic any other way without 2PC). Events for the same aggregate are keyed by `aggregate_id`
  so a partition preserves their order. The relay must not let one un-publishable row block
  the rest of the outbox — log it, leave `published_at` NULL, move on, retry next tick.
- **Idempotent consumers** — Kafka delivers at-least-once. Every consumer checks a
  `processed_events` table (unique event ID) before applying an event, in the same transaction
  as the side effect.
- **Dead-letter handling** — a consume step must terminate *every* message, never wedge a
  partition. Classify the outcome: success / idempotent no-op → commit the offset; transient
  error (DB/broker blip) → do not commit, retry in-process with capped backoff up to
  `MAX_ATTEMPTS`; poison message (undeserializable) or permanent domain rejection → publish to
  the dead-letter topic `<topic>.dlq` then commit; retries exhausted → `<topic>.dlq` then
  commit, as a last resort. DLQ records carry the original key/payload plus diagnostic headers
  (`x-dlq-reason`, source topic/partition/offset). `<topic>.dlq` is created alongside
  `<topic>` in the topic-init / `docker-compose` step.

Client libraries: `segmentio/kafka-go` for Go services, `rdkafka` for Rust services.

Use `/add-go-saga-step` or `/add-rust-saga-step` to wire a publish or consume step.

Implemented so far (reference these when wiring a new step): `user-service` publishes
`UserCreated` on `user.created` via the outbox + relay → `analytics-service` consumes it
(group `analytics-service-UserCreated`), projects a `user_registrations` read-model row behind
a `processed_events` check, and dead-letters poison / permanently-failing / retry-exhausted
messages to `user.created.dlq`.

Sketched next: `booking-service` publishes `BookingRequested` → `event-service` reserves the
seat and publishes `SeatReserved`/`SeatReservationFailed` → `booking-service` confirms or
compensates (cancels) the booking → `analytics-service` consumes the final outcome as a read
model only.

## Claude Code tooling already set up for this repo

Skills (`.claude/skills/`, invoked as `/name`):

| Skill | Use for |
|---|---|
| `new-go-service` / `new-rust-service` | Scaffold a new service in Clean Architecture |
| `new-go-api-endpoint` / `new-rust-api-endpoint` | Add an endpoint to an existing service |
| `add-go-saga-step` / `add-rust-saga-step` | Wire a Kafka publish/consume saga step |
| `scalability-review` | Read-only audit: statelessness, pool sizing, N+1, missing caching |
| `review-concurrency` | Read-only audit: race conditions, oversell/double-booking |

Subagents (`.claude/agents/`, invoked via the Agent tool or by name):

| Agent | Use for |
|---|---|
| `security-reviewer` | Read-only audit: injection, secrets, JWT/IDOR, log leakage |
| `api-contract-reviewer` | Read-only, whole-repo audit: routes vs `kong.yml` drift |
| `api-doc-sync` | Writer: generates/updates `docs/openapi/*.yaml` and the Postman collection from actual handler code |
| `unit-test-writer` | Writer: unit tests for `domain`/`usecase`, mocked ports, exhaustive edge cases |
| `integration-test-writer` | Writer: integration tests against real Postgres — concurrency, idempotency, transaction atomicity |

`api-doc-sync` is the only writing agent — it keeps `docs/openapi/<service>.yaml` and
`docs/postman/ticket-platform.postman_collection.json` in sync with implemented handlers,
code always wins over the spec. It doesn't overlap with `api-contract-reviewer`, which checks
code against `kong.yml`, not against API docs.

`unit-test-writer` and `integration-test-writer` split by test *type*, not by language (both
branch internally for Go/Rust, like the review agents do) — the two need genuinely different
infrastructure (mocked ports + no Docker vs. real Postgres via `testcontainers`), so keeping
them separate stops a "quick unit test" request from accidentally pulling in Docker, and vice
versa. Use `unit-test-writer` after any `domain`/`usecase` change; use
`integration-test-writer` once a service has real endpoints or saga steps, since it needs
something real to hit.

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
  service's `domain/`, `usecase/`, `adapter/http/`, or `adapter/repository/`, greps the
  just-written file for imports that violate the dependency rule above (e.g. `domain/`
  importing `pgx`/`sqlx`/`net/http`/`axum`, `usecase/` importing `adapter/` directly,
  `adapter/http/` reaching into `adapter/repository/` instead of going through `usecase`).
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
  `/new-rust-service`, or `/add-go-saga-step`/`/add-rust-saga-step`: they each touch multiple
  files across layers in one action, so reviewing the plan first is cheaper than reviewing the
  diff after. Not needed for a single `/new-*-api-endpoint` on an existing, well-understood
  service.
- **Extended thinking** (`/effort high`, or the word "ultrathink" in a prompt) — worth reaching
  for on the genuinely hard design calls in this domain: the seat-reservation locking strategy
  in `booking-service` (see `/review-concurrency`), or working out a new saga's event sequence
  and compensation path before wiring it with `/add-*-saga-step`. Not needed for routine CRUD
  endpoints.
- **Background tasks** — once services exist, run each service's test suite as a background
  task when working across more than one service at a time (e.g. verifying `booking-service`
  and `event-service` both still pass after a saga change), instead of blocking on one before
  starting the next.
- **Checkpoints** (`Esc` `Esc`, or `/rewind`) — useful for backing out of an exploratory
  scaffold that went the wrong direction, but they don't replace git and don't capture
  filesystem changes made outside Claude Code (`rm`/`mv`/`cp` in a terminal, edits in another
  editor). Commit to git once a change is actually good, the same as always.
