---
name: new-rust-api-endpoint
description: Add one business operation to an existing Rust service — a REST endpoint (http:), a Kafka saga step that publishes a domain event through the transactional outbox (publish:) and/or consumes one to run a local, possibly compensating, use case (consume:), or a combination — through the service's Clean Architecture layers, with UUID ids, an explicit response mapper, the usecase owning one transaction, paginated lists, utoipa docs, and (for saga steps) idempotency + DLQ handling. Use for any new operation on a Rust service; for anything crossing services, run /design-saga first.
argument-hint: "<service-name> <UseCaseName> [http:<METHOD>:<path>] [publish:<EventName>:<aggregate_type>] [consume:<EventName>:<topic>]"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Rust business operation

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (layers, choreography saga, delivery semantics): @CLAUDE.md
- Approved saga designs: !`ls docs/sagas/ 2>/dev/null || echo "(none — run /design-saga for a cross-service flow)"`
- Target service layout: !`ls -R services/$1/src 2>/dev/null | head -80`
- Existing outbox / consumer code to mirror: !`grep -rl "outbox_events\|rdkafka" services 2>/dev/null | head -20`

## Arguments
`$ARGUMENTS` — `<service-name> <UseCaseName>` plus **at least one trigger**:
- `http:<METHOD>:<path>` — a REST endpoint, e.g. `http:PATCH:/api/v1/events/:id/seats`.
- `consume:<EventName>:<topic>` — run the use case from a consumed event, e.g.
  `consume:BookingRequested:booking.events`. A compensating step uses this.
- `publish:<EventName>:<aggregate_type>` — *also* emit an event through the outbox, e.g.
  `publish:SeatReserved:seat_reservation`. Combine with `http:` or `consume:`.

Verify `services/<service-name>/Cargo.toml` exists — if not, stop and suggest `/new-rust-service`.

## Step 0 — design first if this crosses services
If the operation has a `publish:` or `consume:` **and** the flow isn't already in
`docs/sagas/*.md`, stop and run `/design-saga` first — it decides the delivery guarantee per
event, the compensation map, the topic/DLQ list, and the consumers. This skill wires *one
step*. For a plain `http:` operation, go to step 1.

## Step 1 — domain → platform/port → usecase → repository (always)
- **`src/domain/`** — a new entity's ID field is `uuid::Uuid` (`uuid` crate, feature `v4`),
  minted with `Uuid::new_v4()` in the **entity constructor**. A new business rule is a method
  on the entity returning a domain error variant (`BookingError::SeatUnavailable`), not an
  ad-hoc check in the usecase.
- **`src/platform/port.rs`** — add new methods to the `#[async_trait]` `Repository` trait
  here; each takes `conn: &mut PgConnection` alongside its domain args. Don't define it on the
  postgres adapter first; don't put it in `domain`.
- **`src/usecase/`** — add `<UseCaseName>UseCase` holding `Arc<dyn Trait>` ports **and a
  `PgPool`**, injected via `new()`; one async method `execute(&self, input) -> Result<Output,
  DomainError>` — no `axum`/`sqlx` types in the signature. It **owns the transaction**: do
  all non-DB work first (entity construction, Argon2 hashing, payload building), then
  `let mut tx = self.db_pool.begin().await.map_err(tx_err)?;` (for a read, follow with
  `sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?`), pass `&mut *tx` to
  every repo call, `tx.commit().await?` at the end (drop = rollback on early `?`). Any
  `publish:` makes it a write flow.
- **`src/adapter/repository/postgres.rs`** — implement the new trait method(s). Each takes
  `conn: &mut PgConnection` and runs on `&mut *conn`; **never** `pool.begin()` /
  `tx.commit()` inside the adapter; it holds no pool.

## Step 2 — paginate if it returns a list (with `http:`)
- **`domain`** — a shared `Pagination` (`src/domain/pagination.rs` if absent): `pub struct
  Pagination { pub limit: i64, pub offset: i64 }`, `Pagination::new(limit, offset) ->
  Result<Self, DomainError>` rejecting `offset < 0` / `limit < 1`, clamping `limit` to
  `const MAX_LIMIT: i64 = 100`; absent input → `limit=20, offset=0`.
- **`platform::port`** — the list method takes `conn: &mut PgConnection` + `Pagination`,
  returns `Result<(Vec<T>, i64), RepoError>` — the `i64` is the full match count.
- **`repository`** — `SELECT … ORDER BY <stable col> LIMIT $1 OFFSET $2` plus a matching
  `SELECT COUNT(*)`, both on the one `&mut *tx`.
- **`adapter/http`** — a `Query<ListParams>` extractor (`limit: Option<i64>, offset:
  Option<i64>`); a non-integer or negative value is `400` (only *absent* gets the default).
  Response is an envelope, never a bare array:
  ```rust
  #[derive(Serialize, ToSchema)]
  struct PaginatedResponse<T> { data: Vec<T>, pagination: PaginationMeta }
  #[derive(Serialize, ToSchema)]
  struct PaginationMeta { limit: i64, offset: i64, total: i64, has_more: bool }
  ```
  `has_more = offset + data.len() as i64 < total`. The response mapper (step 3) extends to
  the envelope.

## Step 3 — the HTTP edge (`http:` only)
- **`src/adapter/http/`** — request/response DTOs with `serde` derives, separate from domain
  entities, and a **named mapper** (`impl From<&domain::Booking> for BookingResponse`) next
  to the DTO — not inline field copying. The axum handler extracts the request, calls the
  usecase, calls `.into()`, and maps domain error variants to status codes explicitly
  (`BookingError::SeatUnavailable` → `CONFLICT`; `NotFound` → `NOT_FOUND`; unmapped →
  `INTERNAL_SERVER_ERROR`). Register the route on the exact `<METHOD> <path>` in `main.rs`.
- **Path check** — `<path>` must start with the prefix this service owns in `kong.yml`
  (`strip_path: false`). If Kong wouldn't route it here, stop and tell the user to add it to
  `kong.yml` first.
- **utoipa docs** — bootstrap once per service if missing (no `ApiDoc` in `main.rs`): add
  `utoipa` (feature `axum_extras`) + `utoipa-swagger-ui`; define `ApiDoc` deriving
  `utoipa::OpenApi` with a `bearer_auth` scheme; merge
  `SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())` into the
  router. Then `#[utoipa::path(...)]` above the handler (`method`, `path`, `request_body`,
  `params(...)` for a list, one `responses((status = <code>, body = ...))` per mapped code,
  `security(("bearer_auth" = []))` if JWT), derive `utoipa::ToSchema` on every referenced
  DTO, and add the handler to `ApiDoc`'s `paths(...)` + each DTO to `components(schemas(...))`.
  `cargo check` confirms it compiles.

## Step 4 — publish through the transactional outbox (`publish:` only)
Delivery guarantee and consumer list come from `docs/sagas/*.md` (step 0). Publish is
log-tailing CDC — you write the outbox row, no producer code.
- **`src/domain/`** — define `<EventName>` as a plain `serde`-serializable struct with
  snake_case fields (`SeatReserved { event_id, booking_id, reserved_at }`), no Kafka types —
  it becomes the Kafka message value verbatim. Include `event_id: Uuid`. Give the usecase
  result / aggregate a way to carry pending events and the event enum an `aggregate_type()`
  method (see `services/user-service/src/domain/events.rs`).
- **`src/adapter/repository/postgres.rs`** — add `write_outbox(&self, conn: &mut PgConnection,
  ev: &<EventName>) -> Result<(), RepoError>` (declared on the `crate::platform::port` trait).
  It `INSERT`s a row into `outbox_events` (`id, aggregate_id, aggregate_type, event_type,
  payload JSONB, created_at`) **then `DELETE`s that same row** — both on `&mut *conn`. The
  usecase calls the state-write method then `write_outbox` on its one read-write `tx`, then
  `tx.commit()`s: state change and event are one atomic unit. Add the `outbox_events`
  migration via `/new-migration` if the service lacks one.
- **`debezium/<service-name>-outbox.json`** — if the service has no connector, add one (copy
  `debezium/user-service-outbox.json`): repoint `database.*`, unique `slot.name` /
  `publication.name`, `table.include.list=public.outbox_events`, `skipped.operations=u,d,t`,
  the `EventRouter` transform. Register it in `connect-init`; the service's Postgres needs
  `wal_level=logical`. If the connector exists, a new `aggregate_type` routes automatically.
- Add `<aggregate_type>.events` and `<aggregate_type>.events.dlq` to the `kafka-init` step.
- `rdkafka` stays in `Cargo.toml` for the *consume* side only. If `/add-observability` has
  run, write the current span's `traceparent` into the outbox row's `tracecontext` column.

## Step 5 — consume an event (`consume:` only)
- **`src/adapter/messaging/kafka/consumer.rs`** — subscribe to `<topic>`, consumer group
  `<service-name>-<EventName>`, `rdkafka` `StreamConsumer`, `enable.auto.commit=false`
  (manual commit — the offset advances only after the side effect commits). Deserialize into
  the `<EventName>` domain type via `serde_json`.
- **Multi-type topic** — guard on the `event_type` header: not `<EventName>` → **ack and
  skip** (commit the offset), never dead-letter. Only a message whose `event_type` *is*
  `<EventName>` (or header-less) and won't deserialize is poison → DLQ. Adding the *second*
  consumer to a topic ⇒ switch the first's mismatch branch from DLQ to skip in the same pass.
- **Idempotency** — `<UseCaseName>UseCase` opens one read-write `tx` and, on `&mut *tx`,
  checks `processed_events` (unique on event id) **before** the side effect, skipping if
  present. The consumer calls the use case, never a repo method directly. Add the
  `processed_events` migration via `/new-migration` if missing.
- **Compensation** — if `<EventName>` is a downstream failure signal, `<UseCaseName>UseCase`
  is a **forward correction** of this service's own earlier step, never a delete or a retry.
- **Failure handling (mandatory — terminate every message)** — classify: success / idempotent
  no-op → commit; transient (`RepositoryError`-style variant) → do **not** commit, retry
  in-process with capped backoff + jitter up to `KAFKA_CONSUMER_MAX_ATTEMPTS` (env, default
  5); poison → `<topic>.dlq` (a second `FutureProducer`) then commit, never retry; permanent
  domain rejection → `<topic>.dlq` then commit; retries exhausted → `<topic>.dlq` then
  commit. DLQ records keep the original key/payload + headers `x-dlq-reason`, source
  topic/partition/offset. If the DLQ publish fails, leave the offset uncommitted. Add
  `<topic>.dlq` to `kafka-init`.
- If `/add-observability` has run, extract `traceparent` from the message headers and start
  the processing span from that remote context.

## Step 6 — never wire a synchronous cross-service HTTP call
If a successful operation needs another service to react, that's a `publish:` on this same
invocation, not an HTTP call from the usecase.

## Step 7 — verify and hand off
- `cargo build`, `cargo clippy -- -D warnings`, `cargo fmt --check` clean.
- Dependency rule: `domain` has no `use` of `sqlx`/`axum`; `usecase` `use`s `domain` +
  `crate::platform::port` + `sqlx` but never `adapter`.
- Summarize what was added and what the user still fills in. Note that
  `saga-consistency-reviewer` should audit any `publish:`/`consume:` step and
  `unit-test-writer` / `integration-test-writer` cover the new code. Don't write tests here.

## Reference (implemented)
`user-service` writes `outbox_events` with `aggregate_type = "user"` — `UserCreated` on the
`RegisterUserUseCase` transaction, `UserLoggedIn` on the `LoginUserUseCase` transaction —
each deleted in the same txn; the Debezium `user-service-outbox` connector routes both to
`user.events`. `analytics-service` (Go) consumes it with two groups behind `processed_events`
checks, ack-and-skipping the other group's `event_type`, dead-lettering poison / permanent /
retry-exhausted to `user.events.dlq`.
