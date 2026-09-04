---
name: new-rust-service
description: Scaffold a new Rust microservice for this repo in Clean Architecture (domain / platform/port / usecase / adapter / main) with connection pooling, health + readiness endpoints, graceful shutdown, structured per-request logging with x-request-id, OpenTelemetry tracing, a Prometheus /metrics endpoint, and sqlx migrations already wired in. Use when starting a brand-new Rust service.
argument-hint: <service-name> [port]
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Rust service

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions: @CLAUDE.md
- Existing Rust service to mirror: !`ls -d services/*/Cargo.toml 2>/dev/null | xargs -n1 dirname 2>/dev/null || echo "(none yet)"`

## Arguments
`$ARGUMENTS` — first token is the service name (must match a `name:` under `services:` in
`kong/kong.yml`); optional second token overrides the port.

## Instructions

### 1. Read the gateway contract
Look up the service name in `kong/kong.yml`. Take the port from its `url` and the API prefix
from the route `paths`. Routes are `strip_path: false`, so the router registers the **full**
`/api/v1/...` path. If the name isn't in `kong.yml`, stop and tell the user to add it there
first.

### 2. Scaffold the Clean Architecture layers
Under `services/<service-name>/` as its own crate, dependency arrows pointing inward only —
in Rust: an inner module never `use`s an outer module's concrete types, only traits it
declares itself.

- **`src/domain/`** — entities (plain structs) and domain errors (`enum BookingError {
  SeatUnavailable, NotFound, .. }`), invariants as methods on the entity
  (`Seat::reserve(&mut self) -> Result<(), BookingError>`), a shared `Pagination` value type,
  and **pure** outbound-gateway traits naming no driver type (`trait PasswordHasher`,
  `trait Cache`), `#[async_trait]` where async so they're usable as `Arc<dyn Trait + Send +
  Sync>`. Entity IDs are `uuid::Uuid` (`uuid` crate, feature `v4`), minted with
  `Uuid::new_v4()` in the constructor. Zero `use` of `sqlx`/`axum`/`tokio`.
- **`src/platform/port.rs`** (`mod port`) — the port traits that name the connection handle,
  `Repository` above all. May `use sqlx` (for `PgConnection`) and `crate::domain`; never
  `crate::adapter` or `crate::usecase`. Every method takes `conn: &mut PgConnection`
  alongside its domain args.
- **`src/usecase/`** — one struct per use case (`BookSeatUseCase`), holding `Arc<dyn
  Repository>` (and other ports) **and a `PgPool`**, injected via `new()`. It **owns the
  transaction boundary**: `let mut tx = self.db_pool.begin().await?;` (for a read, then
  `sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;`), threads `&mut *tx`
  through every repository call, then `tx.commit().await?` (drop = rollback on early `?`). All
  non-DB work (entity construction, Argon2 hashing, payload building) runs **before**
  `begin()`. `use`s `domain`, `crate::platform::port`, `sqlx`; never `adapter`.
- **`src/adapter/http/`** — `axum` handlers + request/response DTOs with `serde` derives (no
  domain types on the wire) + a named mapper (`impl From<&domain::Booking> for
  BookingResponse`) next to the DTO. Maps domain error variants to status codes explicitly.
  Transport only.
- **`src/adapter/repository/postgres.rs`** — implements `crate::platform::port::Repository`
  with `sqlx`, on the `&mut PgConnection` it's handed; **never** `pool.begin()` /
  `tx.commit()`. The only module allowed to import `sqlx` query APIs. Holds no pool.
- **`src/platform/`** — cross-cutting infra: `db` (pool + migrations), `logging`,
  `config`, `observability` (tracer + metrics), plus the `port` module above.
- **`src/main.rs`** — the composition root: wires every layer
  (`let repo: Arc<dyn Repository> = Arc::new(PostgresRepository::new());`
  `let uc = BookSeatUseCase::new(pool.clone(), repo);`), builds the `axum::Router`, owns the
  process lifecycle. No business logic.
- **`Cargo.toml`** — `axum`, `tokio` (full), `sqlx` (postgres, `runtime-tokio-rustls`,
  `migrate`), `async-trait`, `serde`, `uuid` (v4), `tracing` + `tracing-subscriber`,
  `tracing-opentelemetry` + `opentelemetry-otlp`, `metrics` + `metrics-exporter-prometheus`,
  `tower-http` (trace, request-id).

### 3. Wire the platform baseline

- **DB pool** — `src/platform/db.rs`, `PgPoolOptions::new().max_connections(N)` with `N` from
  an env var (bounded, not the default). Constructed in `main.rs`, passed into **each use
  case**; the repository holds no pool; `domain` never sees `PgPool`.
- **Migrations** — `migrations/` applied via `sqlx::migrate!()` on startup. Add migrations
  with `/new-migration`. A saga participant's first migrations are the canonical
  `outbox_events` (no `published_at`) and `processed_events` tables.
- **Health** — `GET /healthz` (liveness) and `GET /readyz` (pings the pool) in
  `src/adapter/http`.
- **Graceful shutdown** — `axum::serve(...).with_graceful_shutdown(shutdown_signal())`
  awaiting `ctrl_c()` and SIGTERM; flush the tracer before exit.
- **Structured logging + request id** — `tracing_subscriber::fmt().json()` in
  `src/platform/logging.rs`. `tower_http::request_id` to read/generate `x-request-id` and
  echo it, plus a `tower_http::trace::TraceLayer` (or a small middleware) that records
  `request_id, method, path, status, latency` as one JSON line per request for **every**
  route — never the `authorization` header or body.
- **Tracing** — `src/platform/observability.rs`: an OTLP exporter + `tracing-opentelemetry`
  layer from env (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`). The HTTP server span
  **continues the inbound W3C `traceparent`**. Deeper spans and cross-Kafka propagation are
  `/add-observability`'s job.
- **Metrics** — `GET /metrics` via `metrics-exporter-prometheus`, distinct from health:
  request/error counts + duration histogram, labelled by route pattern (low cardinality).
- **Statelessness** — `usecase` and adapter structs constructed once at startup (`Arc`-wrapped
  where shared); no `Mutex`-guarded field a request mutates in normal handling.
- **Outbound timeouts** — any `reqwest`/`tonic` client is a `domain` port trait implemented
  in `src/adapter/`, with an explicit timeout — never the default. `/add-resilience` adds a
  breaker/retry later.

### 4. Dockerfile
Multi-stage: `cargo build --release` builder, slim `debian` or `distroless/cc` runtime.
Non-root user.

### 5. Verify and hand off
- `cargo build`, `cargo clippy -- -D warnings`, `cargo fmt --check` from the service dir.
- Route prefix matches `kong.yml` exactly.
- Dependency rule intact: `domain` has no `use` of `sqlx`/`axum`/other local modules;
  `platform::port` `use`s only `crate::domain` + `sqlx`; `usecase` `use`s `domain` +
  `crate::platform::port` + `sqlx` but never `adapter`; `adapter::http` and
  `adapter::repository` don't `use` each other. The `clean-architecture-check.sh` hook checks
  this on every write.
- Add the service (and its `postgres-<name>`, plus an `otel-collector` if not present) to
  `docker-compose.yml` on `ticket-network`. If it publishes events, its Postgres needs
  `wal_level=logical` in its `command:`.
- Tell the user what was scaffolded and what they still fill in (entities, use cases, schema
  via `/new-migration`, endpoints via `/new-rust-api-endpoint`).

Do not add a message queue, cache, or circuit breaker here. Use `/add-caching`,
`/add-resilience`, `/add-observability` once there's real logic, and `/new-rust-api-endpoint`
(after `/design-saga` for anything cross-service) to add operations.
