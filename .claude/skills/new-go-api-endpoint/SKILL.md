---
name: new-go-api-endpoint
description: Add a new REST API endpoint to an existing Go service in this repo, following the service's Clean Architecture layers (domain/usecase/adapter). Use when adding a new business operation (e.g. create booking, cancel booking) to a Go service already scaffolded by /new-go-service.
argument-hint: <service-name> <METHOD> <path> <UseCaseName>
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Go API Endpoint

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions (Clean Architecture layers, saga pattern): @CLAUDE.md
- Target service layout: !`ls -R services 2>/dev/null | head -80`

## Arguments
`$ARGUMENTS` — `<service-name> <METHOD> <path> <UseCaseName>`, e.g.
`booking-service POST /api/v1/bookings CreateBooking`.

## Instructions

1. Verify `services/<service-name>/go.mod` exists — if not, this isn't a Go service yet, stop
   and suggest `/new-go-service` first.
2. Verify `<path>` starts with the exact prefix this service owns in `kong/kong.yml`
   (`strip_path: false`, so the full path is what the handler must register). If it doesn't
   match any route Kong sends to this service, stop and tell the user to add the route to
   `kong.yml` first.
3. Add code layer by layer, respecting the existing dependency rule (see @CLAUDE.md):
   - `internal/domain/` — if this operation represents a new business rule, add/extend the
     entity with a method that enforces the invariant (returns a domain error, e.g.
     `ErrSeatUnavailable`, on violation) rather than letting the usecase check ad hoc
     conditions. Add any new port method the usecase will need to the `Repository` interface
     here — don't define it in the postgres adapter first.
   - `internal/usecase/` — add `<UseCaseName>UseCase` (constructor-injected with the `domain`
     ports it needs), one exported method (e.g. `Execute(ctx, input) (output, error)`). No
     framework types (`http.Request`, `pgx.Row`) appear in this signature.
   - `internal/adapter/repository/postgres/` — implement any new `Repository` method added in
     the domain step, using `pgxpool`, inside a transaction if the operation touches more than
     one row/table.
   - `internal/adapter/http/` — request/response DTOs (kept separate from domain entities —
     don't leak domain types into JSON tags), a handler that decodes the request, calls the
     usecase, and maps domain errors to HTTP status codes explicitly (e.g.
     `errors.Is(err, domain.ErrSeatUnavailable) → 409`, `errors.Is(err, domain.ErrNotFound) →
     404`, unmapped error → 500). Register the route on the exact `<path>`/`<METHOD>` in the
     router built in `cmd/main.go`.
4. If a successful call needs another service to react asynchronously (e.g. creating a booking
   should eventually reserve a seat in `event-service`), do **not** call that service's HTTP API
   from the usecase — that couples the two services synchronously and defeats the point of
   having separate services communicate via events. Use `/add-go-saga-step` (publish mode) to
   emit a domain event through the outbox instead, and tell the user that's the next step
   rather than wiring a synchronous call yourself.
5. Check the dependency rule wasn't violated (`domain` still imports nothing local, `usecase`
   still imports `domain` only), then summarize what was added and what the user still needs to
   fill in (persistence columns, validation rules).

Do not write tests unless asked — this command wires the endpoint through the layers; testing
is a separate pass.
