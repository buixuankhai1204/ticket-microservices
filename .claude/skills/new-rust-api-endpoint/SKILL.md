---
name: new-rust-api-endpoint
description: Add a new REST API endpoint to an existing Rust service in this repo, following the service's Clean Architecture layers (domain/usecase/adapter), paginate it if it's a list endpoint, and document it with utoipa so api-doc-sync can curl it. Use when adding a new business operation to a Rust service already scaffolded by /new-rust-service.
argument-hint: <service-name> <METHOD> <path> <UseCaseName>
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Rust API Endpoint

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (Clean Architecture layers, saga pattern): @CLAUDE.md
- Target service layout: !`ls -R services 2>/dev/null | head -80`

## Arguments
`$ARGUMENTS` — `<service-name> <METHOD> <path> <UseCaseName>`, e.g.
`event-service PATCH /api/v1/events/:id/seats ReserveSeat`.

## Instructions

1. Verify `services/<service-name>/Cargo.toml` exists — if not, this isn't a Rust service yet,
   stop and suggest `/new-rust-service` first.
2. Verify `<path>` starts with the exact prefix this service owns in `kong/kong.yml`
   (`strip_path: false`, so the full path is what the router must register). If it doesn't
   match any route Kong sends to this service, stop and tell the user to add the route to
   `kong.yml` first.
3. Add code layer by layer, respecting the existing dependency rule and the endpoint
   conventions in @CLAUDE.md (UUID IDs, explicit response mapper, transaction per endpoint):
   - `src/domain/` — if this operation creates a new entity, its ID field is `uuid::Uuid`
     (crate `uuid`, feature `v4`), generated with `Uuid::new_v4()` in the entity's constructor
     — never an auto-increment integer. If this operation represents a new business rule,
     add/extend the entity with a method that enforces the invariant (returns a domain error
     variant, e.g. `BookingError::SeatUnavailable`, on violation) rather than letting the
     usecase check ad hoc conditions. Add any new port method the usecase will need to the
     relevant `#[async_trait]` repository trait here — don't define it on the postgres adapter
     first.
   - `src/usecase/` — add a `<UseCaseName>UseCase` struct (holding `Arc<dyn Trait>` ports
     injected via `new()`), one async method (e.g. `execute(&self, input) -> Result<Output,
     DomainError>`). No `axum`/`sqlx` types appear in this signature.
   - `src/adapter/repository/postgres.rs` — implement any new trait method added in the domain
     step using `sqlx::PgPool`, and run it **inside one transaction every time**
     (`pool.begin()`), not only when the operation touches more than one row/table: a read
     starts the transaction with `sqlx::Transaction` at the default (or explicitly `SET
     TRANSACTION READ ONLY`) isolation for a consistent snapshot across its queries, a write
     commits a normal read-write transaction — which becomes required anyway once
     `/add-rust-saga-step` adds an outbox insert alongside the state write, so starting every
     write in a transaction now avoids retrofitting it later.
   - `src/adapter/http/` — request/response DTOs with `serde` derives (kept separate from
     domain entities — don't derive `Serialize`/`Deserialize` on domain types just for this),
     and a **named mapper** (`impl From<&domain::Booking> for BookingResponse`, next to the
     DTO) that converts the `domain` entity into the response DTO — not inline field copying
     scattered in the handler. The `axum` handler itself extracts the request, calls the
     usecase, calls `.into()`/the mapper, and maps domain error variants to HTTP status codes
     explicitly (e.g. `BookingError::SeatUnavailable → StatusCode::CONFLICT`,
     `BookingError::NotFound → StatusCode::NOT_FOUND`, unmapped →
     `StatusCode::INTERNAL_SERVER_ERROR`). Register the route on the exact `<path>`/`<METHOD>`
     in the router built in `main.rs`.
4. **If this endpoint returns a list** (a collection, not a single resource), it must be
   paginated — never return an unbounded result set:
   - `src/domain/` — a shared `Pagination` value type (`src/domain/pagination.rs` if it
     doesn't exist yet, reused by every list endpoint in this service, not redefined per
     entity): `pub struct Pagination { pub limit: i64, pub offset: i64 }`, built via
     `Pagination::new(limit, offset) -> Result<Self, DomainError>` that rejects `offset < 0`
     and `limit < 1` with a domain error, and clamps `limit` down to a max
     (`const MAX_LIMIT: i64 = 100`) rather than erroring on too-large a value. Default when the
     client sends nothing: `limit = 20, offset = 0`.
   - `src/domain/` (repository trait) — the list method takes a `Pagination` and returns
     `Result<(Vec<T>, i64), RepoError>` — the `i64` is the full match count ignoring
     limit/offset, needed for the response envelope below.
   - `src/adapter/repository/postgres.rs` — `SELECT ... ORDER BY <stable column, e.g.
     created_at, id> LIMIT $1 OFFSET $2` plus a `SELECT COUNT(*) ...` with the same filter,
     both inside the same transaction from step 3 (a read-only one) so the count and the page
     can't disagree due to a concurrent write between the two queries.
   - `src/usecase/` — parses nothing itself; just passes the already-validated `Pagination`
     through and returns the total count alongside the items in its output.
   - `src/adapter/http/` — an axum `Query<ListParams>` extractor (`limit: Option<i64>, offset:
     Option<i64>`); a non-integer or negative value is a 400, not a silently-defaulted value
     (only an *absent* param gets the default). Response is an envelope, not a bare array:
     ```rust
     #[derive(Serialize, ToSchema)]
     struct PaginatedResponse<T> {
         data: Vec<T>,
         pagination: PaginationMeta,
     }
     #[derive(Serialize, ToSchema)]
     struct PaginationMeta {
         limit: i64,
         offset: i64,
         total: i64,
         has_more: bool,
     }
     ```
     `has_more = offset + data.len() as i64 < total`. Extend the response-mapper convention
     from step 3 to cover this envelope too, not just the single-item DTO.
5. Document the endpoint with `utoipa` so `api-doc-sync` can curl it later — this is what makes
   the service's live `/api-docs/openapi.json` reflect the new route:
   - **Bootstrap once per service, only if not already done** (check `Cargo.toml` for `utoipa`
     and `utoipa-swagger-ui`, and `main.rs` for an `ApiDoc` struct): add `utoipa` (with the
     `axum_extras` feature) and `utoipa-swagger-ui` to `Cargo.toml`; define an `ApiDoc` struct
     deriving `utoipa::OpenApi` with an empty `paths(...)` and `components(schemas(...))` list
     and a `bearer_auth` security scheme; in `main.rs`, merge
     `SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())` into the
     `axum::Router`.
   - Add a `#[utoipa::path(...)]` attribute directly above the new handler function: `method`,
     `path` (matching the exact path registered in step 3), `request_body = <RequestDTO>` if the
     route has a body, `params(("limit" = Option<i64>, Query, description = "default 20, max
     100"), ("offset" = Option<i64>, Query, description = "default 0"))` if this is a list
     endpoint (step 4), one `responses((status = <code>, description = "...", body =
     <ResponseDTO or ErrorResponse>))` entry per code mapped in step 3, and
     `security(("bearer_auth" = []))` if the route needs JWT. Derive `utoipa::ToSchema` on
     every DTO referenced this way, alongside its existing `serde` derives.
   - Add the new handler to `ApiDoc`'s `paths(...)` list and each new DTO to
     `components(schemas(...))` — unlike swaggo, there's no separate generation step; the
     `OpenApi` derive picks these up at compile time, so a `cargo build` (or `cargo check`) is
     enough to confirm it compiles before moving on.
6. If a successful call needs another service to react asynchronously (e.g. reserving a seat
   should notify `booking-service`), do **not** call that service's HTTP API from the usecase —
   that couples the two services synchronously and defeats the point of having separate
   services communicate via events. Use `/add-rust-saga-step` (publish mode) to emit a domain
   event through the outbox instead, and tell the user that's the next step rather than wiring
   a synchronous call yourself.
7. Check the dependency rule wasn't violated (`domain` still has no `use` of `sqlx`/`axum`,
   `usecase` still only `use`s `domain`), then summarize what was added and what the user still
   needs to fill in (persistence columns, validation rules).

Do not write tests unless asked — this command wires the endpoint through the layers; testing
is a separate pass.
