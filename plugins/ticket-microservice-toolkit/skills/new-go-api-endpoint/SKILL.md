---
name: new-go-api-endpoint
description: Add a new REST API endpoint to an existing Go service in this repo, following the service's Clean Architecture layers (domain/usecase/adapter), paginate it if it's a list endpoint, and document it with swaggo so api-doc-sync can curl it. Use when adding a new business operation (e.g. create booking, cancel booking) to a Go service already scaffolded by /new-go-service.
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
3. Add code layer by layer, respecting the existing dependency rule and the endpoint
   conventions in @CLAUDE.md (UUID IDs, explicit response mapper, transaction per endpoint):
   - `internal/domain/` — if this operation creates a new entity, its ID field is `uuid.UUID`
     (`github.com/google/uuid`), generated with `uuid.New()` in the entity's constructor —
     never an auto-increment integer. If this operation represents a new business rule,
     add/extend the entity with a method that enforces the invariant (returns a domain error,
     e.g. `ErrSeatUnavailable`, on violation) rather than letting the usecase check ad hoc
     conditions. Add any new port method the usecase will need to the `Repository` interface
     here — don't define it in the postgres adapter first.
   - `internal/usecase/` — add `<UseCaseName>UseCase` (constructor-injected with the `domain`
     ports it needs), one exported method (e.g. `Execute(ctx, input) (output, error)`). No
     framework types (`http.Request`, `pgx.Row`) appear in this signature.
   - `internal/adapter/repository/postgres/` — implement any new `Repository` method added in
     the domain step using `pgxpool`, and run it **inside one transaction (`pgx.Tx`) every
     time**, not only when the operation touches more than one row/table: a read uses a
     read-only transaction (`pgx.TxOptions{AccessMode: pgx.ReadOnly}`) for a consistent
     snapshot across its queries, a write uses a normal read-write transaction — which becomes
     required anyway once `/add-go-saga-step` adds an outbox insert alongside the state write,
     so starting every write in a transaction now avoids retrofitting it later.
   - `internal/adapter/http/` — request/response DTOs (kept separate from domain entities —
     don't leak domain types into JSON tags), and a **named mapper function**
     (`ToBookingResponse(b *domain.Booking) BookingResponse`-style, next to the DTO) that
     converts the `domain` entity into the response DTO — not inline field copying scattered in
     the handler. The handler itself decodes the request, calls the usecase, calls the mapper,
     and maps domain errors to HTTP status codes explicitly (e.g.
     `errors.Is(err, domain.ErrSeatUnavailable) → 409`, `errors.Is(err, domain.ErrNotFound) →
     404`, unmapped error → 500). Register the route on the exact `<path>`/`<METHOD>` in the
     router built in `cmd/main.go`.
4. **If this endpoint returns a list** (a collection, not a single resource), it must be
   paginated — never return an unbounded result set:
   - `internal/domain/` — a shared `Pagination` value type (`internal/domain/pagination.go` if
     it doesn't exist yet, reused by every list endpoint in this service, not redefined per
     entity): `Limit, Offset int`, built via `NewPagination(limit, offset int) (Pagination,
     error)` that rejects `offset < 0` and `limit < 1` with a domain error, and clamps
     `limit` down to a max (`const MaxLimit = 100`) rather than erroring on too-large a value.
     Default when the client sends nothing: `limit = 20, offset = 0`.
   - `internal/domain/` (`Repository` interface) — the list method takes a `Pagination` and
     returns `(items []T, total int, error)`; `total` is the full match count ignoring
     limit/offset, needed for the response envelope below.
   - `internal/adapter/repository/postgres/` — `SELECT ... ORDER BY <stable column, e.g.
     created_at, id> LIMIT $n OFFSET $n` plus a `SELECT COUNT(*) ...` with the same filter,
     both inside the same transaction from step 3 (a read-only one) so the count and the page
     can't disagree due to a concurrent write between the two queries.
   - `internal/usecase/` — parses nothing itself; just passes the already-validated
     `Pagination` through and returns `total` alongside the items in its output.
   - `internal/adapter/http/` — parse `limit`/`offset` from `r.URL.Query()` as integers; a
     non-integer or negative value is a 400, not a silently-defaulted value (only an *absent*
     param gets the default). Response is an envelope, not a bare array:
     ```go
     type PaginatedBookingsResponse struct {
         Data       []BookingResponse `json:"data"`
         Pagination PaginationMeta    `json:"pagination"`
     }
     type PaginationMeta struct {
         Limit   int  `json:"limit"`
         Offset  int  `json:"offset"`
         Total   int  `json:"total"`
         HasMore bool `json:"has_more"`
     }
     ```
     `HasMore = offset+len(data) < total`. Extend the response-mapper convention from step 3 to
     cover this envelope too, not just the single-item DTO.
5. Document the endpoint with `swaggo` so `api-doc-sync` can curl it later — this is what makes
   the service's live `/swagger/doc.json` reflect the new route:
   - **Bootstrap once per service, only if not already done** (check for `docs/docs.go` or a
     `/swagger` route registration in `cmd/main.go`): add `github.com/swaggo/swag` and
     `github.com/swaggo/http-swagger` to `go.mod`; add general API annotations above
     `func main()` in `cmd/main.go` —
     ```go
     // @title        <ServiceName> API
     // @version      1.0
     // @BasePath     /api/v1
     // @securityDefinitions.apikey BearerAuth
     // @in header
     // @name Authorization
     ```
     register `mux.Handle("/swagger/", httpSwagger.WrapHandler)`, and blank-import the
     generated docs package once it exists (`_ "<module-path>/docs"`).
   - Add swaggo doc comments directly above the new handler function: `@Summary`,
     `@Description`, `@Tags <service-noun>`, `@Accept json`, `@Produce json`, a `@Param request
     body <RequestDTO> true "..."` line if the route has a body, `@Param limit query int false
     "default 20, max 100"` and `@Param offset query int false "default 0"` if this is a list
     endpoint (step 4), `@Success <code> {object} <ResponseDTO>`, one `@Failure <code> {object}
     ErrorResponse "..."` per error mapped in step 3, `@Security BearerAuth` if the route needs
     JWT, and `@Router <path> [<method-lowercase>]` matching the exact path registered in step
     3.
   - Regenerate the live spec: run `swag init -g cmd/main.go -o docs` if `swag` is on `PATH`
     (`command -v swag`). If it isn't, say so in your summary and tell the user to install it
     (`go install github.com/swaggo/swag/cmd/swag@latest`) — don't skip this silently, since
     `api-doc-sync` has nothing to curl until `swag init` has run at least once.
6. If a successful call needs another service to react asynchronously (e.g. creating a booking
   should eventually reserve a seat in `event-service`), do **not** call that service's HTTP API
   from the usecase — that couples the two services synchronously and defeats the point of
   having separate services communicate via events. Use `/add-go-saga-step` (publish mode) to
   emit a domain event through the outbox instead, and tell the user that's the next step
   rather than wiring a synchronous call yourself.
7. Check the dependency rule wasn't violated (`domain` still imports nothing local, `usecase`
   still imports `domain` only), then summarize what was added and what the user still needs to
   fill in (persistence columns, validation rules).

Do not write tests unless asked — this command wires the endpoint through the layers; testing
is a separate pass.
