---
name: new-rust-api-endpoint
description: Add a new REST API endpoint to an existing Rust service in this repo, following the service's Clean Architecture layers (domain/usecase/adapter). Use when adding a new business operation to a Rust service already scaffolded by /new-rust-service.
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
3. Add code layer by layer, respecting the existing dependency rule (see @CLAUDE.md):
   - `src/domain/` — if this operation represents a new business rule, add/extend the entity
     with a method that enforces the invariant (returns a domain error variant, e.g.
     `BookingError::SeatUnavailable`, on violation) rather than letting the usecase check ad hoc
     conditions. Add any new port method the usecase will need to the relevant `#[async_trait]`
     repository trait here — don't define it on the postgres adapter first.
   - `src/usecase/` — add a `<UseCaseName>UseCase` struct (holding `Arc<dyn Trait>` ports
     injected via `new()`), one async method (e.g. `execute(&self, input) -> Result<Output,
     DomainError>`). No `axum`/`sqlx` types appear in this signature.
   - `src/adapter/repository/postgres.rs` — implement any new trait method added in the domain
     step using `sqlx::PgPool`, inside a transaction if the operation touches more than one
     row/table.
   - `src/adapter/http/` — request/response DTOs with `serde` derives (kept separate from
     domain entities — don't derive `Serialize`/`Deserialize` on domain types just for this), an
     `axum` handler that extracts the request, calls the usecase, and maps domain error variants
     to HTTP status codes explicitly (e.g. `BookingError::SeatUnavailable →
     StatusCode::CONFLICT`, `BookingError::NotFound → StatusCode::NOT_FOUND`, unmapped →
     `StatusCode::INTERNAL_SERVER_ERROR`). Register the route on the exact `<path>`/`<METHOD>`
     in the router built in `main.rs`.
4. If a successful call needs another service to react asynchronously (e.g. reserving a seat
   should notify `booking-service`), do **not** call that service's HTTP API from the usecase —
   that couples the two services synchronously and defeats the point of having separate
   services communicate via events. Use `/add-rust-saga-step` (publish mode) to emit a domain
   event through the outbox instead, and tell the user that's the next step rather than wiring
   a synchronous call yourself.
5. Check the dependency rule wasn't violated (`domain` still has no `use` of `sqlx`/`axum`,
   `usecase` still only `use`s `domain`), then summarize what was added and what the user still
   needs to fill in (persistence columns, validation rules).

Do not write tests unless asked — this command wires the endpoint through the layers; testing
is a separate pass.
