---
name: new-rust-api-endpoint
description: Add a business operation to an existing Rust service — a REST endpoint, a Kafka saga step (publish a domain event through the transactional outbox and/or consume one to trigger a local, possibly compensating, use case), or both — following the service's Clean Architecture layers, paginating list endpoints, documenting with utoipa. Use for any new operation on a Rust service scaffolded by /new-rust-service.
argument-hint: "<service-name> <UseCaseName> [http:<METHOD>:<path>] [publish:<EventName>:<aggregate_type>] [consume:<EventName>:<topic>]"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Rust Business Operation (REST endpoint / saga step)

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (Clean Architecture layers, choreography saga, delivery semantics): @CLAUDE.md
- Target service layout: !`ls -R services 2>/dev/null | head -80`
- Existing outbox / messaging code in this repo: !`grep -rl "outbox_events\|rdkafka" services 2>/dev/null | head -20`
- Debezium connector defs (CDC publish side): !`ls debezium/ 2>/dev/null`

## Arguments
`$ARGUMENTS` — `<service-name> <UseCaseName>` plus **at least one trigger**:

- `http:<METHOD>:<path>` — expose it as a REST endpoint, e.g. `http:PATCH:/api/v1/events/:id/seats`.
- `consume:<EventName>:<topic>` — trigger it from a consumed Kafka event, e.g.
  `consume:BookingRequested:booking.events`. A **compensating** operation uses this.
- `publish:<EventName>:<aggregate_type>` — *additionally* emit a domain event through the
  transactional outbox, e.g. `publish:SeatReserved:seat_reservation`. Combine with `http:` or `consume:`.

Examples:
- `user-service RegisterUser http:POST:/api/v1/auth/register publish:UserCreated:user`
- `event-service ReserveSeat consume:BookingRequested:booking.events publish:SeatReserved:seat_reservation`
- `event-service ReleaseSeat consume:BookingCancelled:booking.events`

Verify `services/<service-name>/Cargo.toml` exists first — if not, stop and suggest `/new-rust-service`.

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

1. **Preconditions.** `Cargo.toml` exists. If `http:` given, verify `<path>` starts with the
   exact prefix this service owns in `kong/kong.yml` (`strip_path: false`, so the full path is
   what the router registers) — if it matches no route Kong sends here, stop and tell the user
   to add it to `kong.yml` first.

2. **domain → platform/port → usecase → repository** (always; @CLAUDE.md conventions — UUID
   IDs, explicit response mapper, usecase owns the transaction):
   - `src/domain/` — a new entity's ID field is `uuid::Uuid` (crate `uuid`, feature `v4`),
     minted with `Uuid::new_v4()` in the entity constructor, never an auto-increment integer. A
     new business rule is a method on the entity that returns a domain error variant (e.g.
     `BookingError::SeatUnavailable`) on violation, not an ad hoc check in the usecase.
   - `src/platform/port.rs` — add any new port method to the `#[async_trait]` repository trait
     here; it takes `conn: &mut PgConnection` alongside its domain args. Don't define it on the
     postgres adapter first, and don't put it in `domain` (the trait names the driver's
     connection handle).
   - `src/usecase/` — add `<UseCaseName>UseCase` holding `Arc<dyn Trait>` ports **and a
     `PgPool`**, injected via `new()`; one async method (e.g. `execute(&self, input) ->
     Result<Output, DomainError>` — no `axum`/`sqlx` types in the signature). The method
     **owns the transaction**: do all non-DB work first (entity construction, Argon2 hashing,
     payload building), then `let mut tx = self.db_pool.begin().await.map_err(tx_err)?;` (for a
     read, follow with `sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?`),
     pass `&mut *tx` to every repository call in the flow, `tx.commit().await?` at the end
     (drop = rollback on early `?`). Any operation with a `publish:` is a write flow.
   - `src/adapter/repository/postgres.rs` — implement the new trait method(s). Each takes
     `conn: &mut PgConnection` and runs its queries on `&mut *conn`; **never** `pool.begin()` /
     `tx.commit()` inside the adapter. The adapter struct holds no pool.

3. **If the operation returns a list** (only meaningful with `http:`), it must be paginated —
   never an unbounded result set:
   - `src/domain/` — a shared `Pagination` value type (`src/domain/pagination.rs` if absent,
     reused by every list endpoint, not redefined per entity): `pub struct Pagination { pub
     limit: i64, pub offset: i64 }`, built via `Pagination::new(limit, offset) -> Result<Self,
     DomainError>` that rejects `offset < 0` and `limit < 1` and clamps `limit` to
     `const MAX_LIMIT: i64 = 100`. Absent input → `limit = 20, offset = 0`.
   - `src/platform/port.rs` — the list method takes `conn: &mut PgConnection` and a
     `Pagination` and returns `Result<(Vec<T>, i64), RepoError>`; the `i64` is the full match
     count ignoring limit/offset.
   - `src/adapter/repository/postgres.rs` — `SELECT ... ORDER BY <stable column> LIMIT $1
     OFFSET $2` plus `SELECT COUNT(*) ...` with the same filter, both on the `&mut *tx` the
     usecase opened in step 2 so they can't disagree.
   - `src/usecase/` — passes the already-validated `Pagination` through, returns the total
     alongside the items.
   - `src/adapter/http/` — an axum `Query<ListParams>` extractor (`limit: Option<i64>, offset:
     Option<i64>`); a non-integer or negative value is a 400 (only an *absent* param gets the
     default). Response is an envelope, not a bare array:
     ```rust
     #[derive(Serialize, ToSchema)]
     struct PaginatedResponse<T> { data: Vec<T>, pagination: PaginationMeta }
     #[derive(Serialize, ToSchema)]
     struct PaginationMeta { limit: i64, offset: i64, total: i64, has_more: bool }
     ```
     `has_more = offset + data.len() as i64 < total`. Extend the response-mapper convention
     (step 4) to the envelope too.

4. **`http:` — the HTTP edge** (skip entirely if no `http:` argument):
   - `src/adapter/http/` — request/response DTOs with `serde` derives kept separate from
     domain entities, and a **named mapper** (`impl From<&domain::Booking> for
     BookingResponse`, next to the DTO) — not inline field copying in the handler. The axum
     handler extracts the request, calls the usecase, calls `.into()`/the mapper, and maps
     domain error variants to HTTP status codes explicitly (`BookingError::SeatUnavailable →
     StatusCode::CONFLICT`, `BookingError::NotFound → StatusCode::NOT_FOUND`, unmapped →
     `StatusCode::INTERNAL_SERVER_ERROR`). Register the route on the exact `<METHOD>`/`<path>`
     in the router in `main.rs`.
   - Document it with `utoipa` so `api-doc-sync` can curl the live `/api-docs/openapi.json`:
     - **Bootstrap once per service, only if missing** (check `Cargo.toml` for `utoipa` /
       `utoipa-swagger-ui`, and `main.rs` for an `ApiDoc` struct): add `utoipa` (feature
       `axum_extras`) and `utoipa-swagger-ui`; define `ApiDoc` deriving `utoipa::OpenApi` with
       empty `paths(...)` / `components(schemas(...))` and a `bearer_auth` scheme; merge
       `SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())` into the
       router in `main.rs`.
     - `#[utoipa::path(...)]` above the handler: `method`, `path` (matching the registered
       route), `request_body = <RequestDTO>` if it has a body, `params((\"limit\" =
       Option<i64>, Query, ...), (\"offset\" = Option<i64>, Query, ...))` if it's a list
       endpoint, one `responses((status = <code>, description = \"...\", body = <ResponseDTO or
       ErrorResponse>))` per mapped code, `security((\"bearer_auth\" = []))` if it needs JWT.
       Derive `utoipa::ToSchema` on every DTO referenced.
     - Add the handler to `ApiDoc`'s `paths(...)` and each DTO to `components(schemas(...))` —
       no separate generation step; `cargo build` / `cargo check` confirms it compiles.

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
   - `src/domain/` — define `<EventName>` as a plain `serde`-serializable struct (e.g.
     `SeatReserved { event_id, booking_id, reserved_at }`), no Kafka types, snake_case fields
     (it becomes the Kafka message value verbatim). Give the usecase result / aggregate a way
     to carry "pending events" and the event enum an `aggregate_type()` method (see
     `services/user-service/src/domain/events.rs`).
   - `src/adapter/repository/postgres.rs` — add `write_outbox(&self, conn: &mut PgConnection,
     ev: &<EventName>) -> Result<(), RepoError>` (declared on the `crate::platform::port`
     trait, like every other method). It `INSERT`s a row into `outbox_events` (`id,
     aggregate_id, aggregate_type, event_type, payload JSONB, created_at`) **then `DELETE`s
     that same row** — both on `&mut *conn`. The usecase from step 2 calls the state-write repo
     method then `write_outbox` on its one read-write `tx` (`&mut *tx`), then `tx.commit()`s —
     the state change and its event are one atomic unit. The `INSERT` lands in the WAL for
     Debezium; the `DELETE` (which the connector ignores) keeps the table empty. Create the
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
     `wal_level=logical`. If the connector already exists, a new `aggregate_type` routes to its
     own topic automatically — nothing to do.
   - `rdkafka` stays in `Cargo.toml` for the *consume* side only; publishing needs no client
     library. Add `<aggregate_type>.events` and `<aggregate_type>.events.dlq` to the
     `kafka-init` topic-creation step. Tell the user which service(s) should now `consume:` it.

6. **`consume:<EventName>:<topic>` — trigger the operation from a Kafka event** (skip if no
   `consume:`; here the operation has no `http:` edge unless one was also requested):
   - `src/adapter/messaging/kafka/consumer.rs` — subscribe to `<topic>` with consumer group
     `<service-name>-<EventName>` using `rdkafka`'s `StreamConsumer`,
     `enable.auto.commit=false` (manual commit — the offset advances only after a message is
     fully processed). Deserialize into the `<EventName>` domain type via `serde_json`.
   - Idempotency: `<UseCaseName>UseCase` (step 2) opens one read-write `tx` and, on `&mut *tx`,
     checks a `processed_events` table (unique constraint on event ID) **before** the side
     effect, skipping if already present — check and side effect on the same `tx`. The consumer
     calls the use case, never a repo method directly. Add the `processed_events` migration if
     missing.
   - If `<EventName>` is a downstream failure signal (e.g. `SeatReservationFailed`),
     `<UseCaseName>UseCase` is a **compensating** action that undoes this service's own earlier
     step — not a retry.
   - **Failure handling is mandatory — terminate every message, never wedge a partition.**
     Classify: success / idempotent no-op → commit the offset; transient error (a
     `RepositoryError`-style variant) → do **not** commit, retry in-process with capped backoff
     up to `KAFKA_CONSUMER_MAX_ATTEMPTS` (env, default 5); poison (won't deserialize,
     missing/non-UUID fields) → publish to `<topic>.dlq` (a second `FutureProducer`) then
     commit, never retry; permanent domain rejection → `<topic>.dlq` then commit; retries
     exhausted → `<topic>.dlq` then commit. DLQ records keep the original key/payload plus
     headers `x-dlq-reason` and source topic/partition/offset. If the DLQ publish fails, leave
     the offset uncommitted. Add `<topic>.dlq` to the `kafka-init` / `docker-compose` step.

7. **Never wire a synchronous cross-service HTTP call.** If a successful call needs another
   service to react, that is a `publish:` on this same invocation, not an HTTP call from the
   usecase.

8. Check the dependency rule (`domain` has no `use` of `sqlx`/`axum`; `usecase` `use`s
   `domain` + `crate::platform::port` + `sqlx` but never `adapter`), then summarize what was
   added and what the user must still fill in (persistence columns, validation rules).

## Reference (already implemented)
`user-service` writes a `UserCreated` `outbox_events` row (`aggregate_type = "user"`) on the
`RegisterUserUseCase` transaction, deleting it in the same txn; the Debezium
`user-service-outbox` connector routes it to `user.events` — no relay code. →
`analytics-service` (Go) consumes `user.events` (group `analytics-service-UserCreated`),
projects a `user_registrations` read-model row behind a `processed_events` check, and
dead-letters poison / permanently-failing / retry-exhausted / unexpected-`event_type` messages
to `user.events.dlq`.

Sketched next: `event-service`
`consume:BookingRequested:booking.events publish:SeatReserved:seat_reservation` (or
`SeatReservationFailed`) atomically decrements `available_seats`; `booking-service` consumes
the outcome to confirm or compensate.

Do not write tests unless asked — this command wires the operation through the layers; testing
is a separate pass.
