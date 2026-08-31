---
name: new-rust-service
description: Scaffold a new Rust microservice for this repo in Clean Architecture (domain / usecase / adapter / main), with connection pooling, health checks, graceful shutdown, and structured logging already wired in. Use when starting a brand-new Rust service (e.g. event-service).
argument-hint: <service-name> [port]
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Rust Service

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions: @CLAUDE.md
- Existing services: !`ls services 2>/dev/null || echo "(none yet)"`

## Arguments
`$ARGUMENTS` — first token is the service name (must match a `name:` entry under
`services:` in `kong/kong.yml`, e.g. `event-service`); optional second token overrides the
port.

## Instructions

1. Look up `$ARGUMENTS`'s service name in `kong/kong.yml`. Use the `url` there for the port
   and the route `paths` for the API prefix this service must handle itself
   (`strip_path: false`, so do not strip the prefix in your router). If the service name isn't
   in `kong.yml` yet, stop and tell the user to add it there first — the gateway config is the
   source of truth for ports/routes in this repo.
2. Scaffold under `services/<service-name>/` as its own crate, in **Clean Architecture**
   layers. The dependency rule is strict: arrows only point inward, expressed in Rust as
   "inner modules never `use` an outer module's concrete types, only traits they themselves
   declare":
   - `src/domain/` — entities (plain structs) and domain errors (e.g. `enum BookingError {
     SeatUnavailable, .. }`), plus any **pure outbound-gateway trait** that names no driver
     type (e.g. `trait PaymentGateway`), using `#[async_trait]` (crate `async-trait`) where
     async so they're usable as `Arc<dyn Trait + Send + Sync>`. Zero imports of
     `sqlx`/`axum`/`tokio` here. Business invariants live as methods on the entity (e.g.
     `Seat::reserve(&mut self) -> Result<(), BookingError>`) — the entity enforces the rule,
     not the caller. The `SeatRepository` trait does **not** live here: it names the DB
     connection handle (`&mut PgConnection`), so it belongs in `src/platform/port.rs`.
   - `src/platform/port.rs` — the port traits that name the connection handle, `SeatRepository`
     above all. `mod port` may `use sqlx` (for `PgConnection`) and `crate::domain`; it must not
     `use crate::adapter` or `crate::usecase`. Every method takes `&mut self`-free
     `conn: &mut PgConnection` alongside its domain args.
   - `src/usecase/` — one struct per use case (e.g. `BookSeatUseCase`), holding `Arc<dyn
     SeatRepository>` (and other ports) **and a `PgPool`**, injected via `new()`. It **owns the
     transaction boundary**: `let mut tx = self.db_pool.begin().await?;` (for a read, then
     `sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;`), threads `&mut *tx`
     through every repository call, then `tx.commit().await?`. All non-DB work (entity
     construction, Argon2 hashing, payload building) happens **before** `begin()` so a pooled
     connection is never pinned across CPU-bound work. `use`s `domain`, `crate::platform::port`,
     and `sqlx`; never `adapter`. See `/review-concurrency` for why one threaded `tx` matters
     for booking-service specifically.
   - `src/adapter/http/` — `axum` handlers and request/response DTOs. Depend on the `usecase`
     struct through its public method signatures (or a small trait if you need to mock it in
     handler tests); translate transport (status codes, JSON) at the edge, no business rules.
   - `src/adapter/repository/postgres.rs` — implements the `crate::platform::port::SeatRepository`
     trait using `sqlx`. Every method runs on the `&mut PgConnection` the usecase hands it (via
     `&mut *tx`) and **never begins or commits a transaction of its own**. This is the only
     module allowed to import `sqlx` — the DB driver is a detail, not something
     `usecase`/`domain` should know exists (`usecase` names only `PgPool` / `PgConnection`, not
     query APIs).
   - `src/platform/` — cross-cutting infrastructure with no business meaning: DB pool
     construction, tracing/logging setup, config loading (used only from `main.rs`), plus the
     `port` module above (imported by `usecase` and the postgres adapter).
   - `src/main.rs` — the **composition root**: the only file that wires every layer together
     (`let repo: Arc<dyn SeatRepository> = Arc::new(PostgresSeatRepository::new());`,
     `let uc = BookSeatUseCase::new(pool.clone(), repo);`, then builds the `axum::Router` with
     `uc` in its state) plus the process lifecycle (below). No business logic here, only
     construction, routing, and shutdown.
   - `Cargo.toml` — `axum` for HTTP, `tokio` (full features) for the runtime, `sqlx` (postgres,
     runtime-tokio-rustls) for DB access, `async-trait` for the port traits, `tracing` +
     `tracing-subscriber` for logging.
3. Wire in these scalability/stability patterns, placed in the layer that owns them — do not
   let infrastructure concerns leak into `domain`/`usecase`:
   - **DB connection pool**: built once in `src/platform/db.rs` via
     `sqlx::postgres::PgPoolOptions` with an explicit `.max_connections(N)` from an env var
     (bounded, not the default) — never construct raw connections per-request. Constructed in
     `main.rs`, passed into **each use case** (the use case owns the transaction boundary); the
     postgres adapter holds no pool, and `domain` never sees `PgPool`.
   - **Health check endpoints**: `GET /healthz` (liveness, no DB touch) and `GET /readyz`
     (pings the pool) in `src/adapter/http` — needed for safe rolling deploys. `/readyz` may
     reach `platform::db` directly since liveness/readiness is an infra concern, not a business
     use case.
   - **Graceful shutdown**: in `main.rs` —
     `axum::serve(...).with_graceful_shutdown(shutdown_signal())` where `shutdown_signal` awaits
     `tokio::signal::ctrl_c()` and SIGTERM via `tokio::signal::unix::signal`, with in-flight
     requests allowed to finish.
   - **Structured logging**: `tracing_subscriber::fmt().json()` set up in `src/platform/`,
     accessed via the `tracing` macros (already decoupled by design — `tracing` spans work from
     any layer without a passed-in logger object) with a request-scoped span carrying a request
     ID, for later correlation across services.
   - **Statelessness**: `usecase` and adapter structs are constructed once at startup
     (`Arc`-wrapped where shared across requests) — no `Mutex`-guarded field that a request
     mutates as part of normal handling. Anything that looks like shared state belongs in
     Postgres or Redis, accessed through a `domain` port trait, never in process memory.
   - **Timeouts on outbound calls**: any `reqwest`/tonic client to another service is a
     `domain` port trait (e.g. `PaymentGateway`) implemented in `src/adapter/`, and that
     implementation must set an explicit timeout — never rely on the default (which may be
     unbounded).
4. Add a minimal `Dockerfile` (multi-stage: `cargo build --release` builder stage, slim
   `debian` or `distroless/cc` runtime stage).
5. Confirm the route prefix implemented in the router matches `kong.yml` exactly. Also verify
   the dependency rule wasn't violated: `domain` has no `use` of `sqlx`/`axum`/other local
   modules, `usecase` `use`s `domain` + `crate::platform::port` + `sqlx` (for the tx boundary
   it owns) but never `adapter`, `crate::platform::port` `use`s only `crate::domain` + `sqlx`,
   `adapter::http` and `adapter::repository` don't depend on each other — grep for cross-module
   `use` statements if unsure. Then tell the user
   what was scaffolded and what they still need to fill in (actual entities/use cases/DB
   schema).

Do not add a message queue, cache, or circuit breaker here unless asked — this command
produces the base skeleton; use `/add-caching` or `/add-circuit-breaker` for those once the
service has real logic to wrap.
