---
name: new-go-service
description: Scaffold a new Go microservice for this repo in Clean Architecture (domain / usecase / adapter / cmd), with connection pooling, health checks, graceful shutdown, and structured logging already wired in. Use when starting a brand-new Go service (e.g. user-service, booking-service).
argument-hint: <service-name> [port]
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Go Service

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions: @CLAUDE.md
- Existing services: !`ls services 2>/dev/null || echo "(none yet)"`

## Arguments
`$ARGUMENTS` — first token is the service name (must match a `name:` entry under
`services:` in `kong/kong.yml`, e.g. `user-service`); optional second token overrides the port.

## Instructions

1. Look up `$ARGUMENTS`'s service name in `kong/kong.yml`. Use the `url` there for the port
   and the route `paths` for the API prefix this service must handle itself
   (`strip_path: false`, so do not strip the prefix in your router). If the service name isn't
   in `kong.yml` yet, stop and tell the user to add it there first — the gateway config is the
   source of truth for ports/routes in this repo.
2. Scaffold under `services/<service-name>/` in **Clean Architecture** layers. The dependency
   rule is strict: arrows only point inward. Enforce it by import discipline, not just folder
   names:
   - `internal/domain/` — entities (structs) and domain errors (e.g. `ErrSeatUnavailable`),
     plus the **port interfaces** this service needs (`Repository`, and any outbound gateway
     like `PaymentGateway`). Zero imports from this repo or from frameworks/drivers (no `pgx`,
     no `net/http`, no `pgxpool`). Business invariants live here as methods on the entity
     (e.g. a `Seat.Reserve()` that returns `ErrSeatUnavailable` if already taken) — the entity
     itself, not the caller, enforces the rule.
   - `internal/usecase/` — one type per use case (e.g. `BookSeatUseCase`), constructor-injected
     with the `domain` interfaces it needs. Imports `domain` only, never `adapter` and never a
     concrete driver. This is where a multi-step business flow is orchestrated inside a single
     repository-driven transaction — see `/review-concurrency` for why that matters for
     booking-service specifically.
   - `internal/adapter/http/` — HTTP handlers/controllers and request/response DTOs. Depend on
     the `usecase` layer's exported interface (not its struct) so handlers stay testable with a
     fake. Translates transport (HTTP status, JSON) at the edge; no business rules here.
   - `internal/adapter/repository/postgres/` — implements the `domain.Repository` port using
     `pgxpool`. This is the only package allowed to import `pgx`/`pgxpool` — the DB driver is a
     detail, not something `usecase` or `domain` should know exists.
   - `internal/platform/` — cross-cutting infrastructure with no business meaning: DB pool
     construction, logger setup, config loading. Used only from `cmd/main.go`.
   - `cmd/main.go` — the **composition root**: the only file that imports every layer and wires
     concrete adapters into interfaces (`repo := postgres.NewRepository(pool)`,
     `uc := usecase.NewBookSeatUseCase(repo)`, `h := http.NewHandler(uc)`). No business logic
     here, only construction, routing, and the process lifecycle (see below).
   - `go.mod` for this service (module per service, not a shared monorepo module).
3. Wire in these scalability/stability patterns, placed in the layer that owns them — do not
   let infrastructure concerns leak into `domain`/`usecase`:
   - **DB connection pool**: built once in `internal/platform/db` using `pgxpool.New`
     (github.com/jackc/pgx/v5/pgxpool) with an explicit `MaxConns` from an env var (default a
     bounded value, e.g. 20) — never the driver's unbounded default. Constructed in
     `cmd/main.go`, passed into the postgres adapter as `*pgxpool.Pool`; `usecase`/`domain`
     never see it directly.
   - **Health check endpoint**: `GET /healthz` (liveness, no DB touch) and `GET /readyz` (pings
     the pool) in `internal/adapter/http` — Kong/orchestrator needs both for safe rolling
     deploys. `/readyz` may reach `platform/db` directly since liveness/readiness is an infra
     concern, not a business use case.
   - **Graceful shutdown**: in `cmd/main.go` — `signal.NotifyContext(context.Background(),
     os.Interrupt, syscall.SIGTERM)` + `http.Server.Shutdown(ctx)` with a bounded timeout, so
     in-flight requests aren't dropped on deploy/scale-down.
   - **Structured logging**: `log/slog` with JSON handler set up in `internal/platform/logger`,
     injected into `usecase`/`adapter` via a small logging interface (not the concrete `slog`
     type) so `domain`/`usecase` stay framework-agnostic; carry a request-scoped request ID for
     later tracing correlation across services.
   - **Statelessness**: `usecase` and `adapter` structs are constructed once at startup and
     reused across every request — no mutable field on them that a request would write to.
     Anything that looks like shared state belongs in the DB or Redis, accessed through a
     `domain` port, never in process memory.
   - **Timeouts on outbound calls**: any HTTP/gRPC client to another service is a `domain` port
     (e.g. `PaymentGateway`) implemented in `internal/adapter/`, and that implementation must
     set an explicit timeout with context propagation — never an unbounded call.
4. Add a minimal `Dockerfile` (multi-stage build, distroless/alpine final stage).
5. Confirm the route prefix implemented in the router matches `kong.yml` exactly. Also verify
   the dependency rule wasn't violated: `domain` imports nothing local, `usecase` imports only
   `domain`, `adapter/http` and `adapter/repository/*` don't import each other — grep for
   cross-layer imports if unsure. Then tell the user what was scaffolded and what they still
   need to fill in (actual entities/use cases/DB schema).

Do not add a message queue, cache, or circuit breaker here unless asked — this command
produces the base skeleton; use `/add-caching` or `/add-circuit-breaker` for those once the
service has real logic to wrap.
