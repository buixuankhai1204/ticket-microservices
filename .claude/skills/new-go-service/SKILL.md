---
name: new-go-service
description: Scaffold a new Go microservice for this repo in Clean Architecture (domain / platform/port / usecase / adapter / cmd) with connection pooling, health + readiness endpoints, graceful shutdown, structured per-request logging with X-Request-Id, OpenTelemetry tracing, a Prometheus /metrics endpoint, and an embedded migrations runner already wired in. Use when starting a brand-new Go service (e.g. booking-service).
argument-hint: <service-name> [port]
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New Go service

## Context
- Gateway routing source of truth: @kong/kong.yml
- Project conventions: @CLAUDE.md
- Existing Go services to mirror: !`ls -d services/*/go.mod 2>/dev/null | xargs -n1 dirname 2>/dev/null || echo "(none yet)"`

## Arguments
`$ARGUMENTS` — first token is the service name (must match a `name:` under `services:` in
`kong/kong.yml`, e.g. `booking-service`); optional second token overrides the port.

## Instructions

### 1. Read the gateway contract
Look up the service name in `kong/kong.yml`. Take the port from its `url` and the API prefix
from the route `paths`. Routes are `strip_path: false`, so the router registers the **full**
`/api/v1/...` path. If the name isn't in `kong.yml`, stop and tell the user to add it there
first — the gateway config is the source of truth for ports and routes.

### 2. Scaffold the Clean Architecture layers
Under `services/<service-name>/`, one Go module (`go.mod`), dependency arrows pointing inward
only — enforce by import discipline, not just folder names:

- **`internal/domain/`** — entities (structs) and domain errors (`ErrSeatUnavailable`,
  `ErrNotFound`), business invariants as methods on the entity (`Seat.Reserve()` returns
  `ErrSeatUnavailable` if taken — the entity enforces the rule, not the caller), a shared
  `Pagination` value type, and **pure** outbound-gateway ports that name no infra type
  (`PasswordHasher`, `Cache`). Entity IDs are `uuid.UUID` (`github.com/google/uuid`), minted
  with `uuid.New()` in the constructor — never an auto-increment integer. Zero imports of
  this repo's other packages, of `pgx`/`pgxpool`, or of `net/http`.
- **`internal/platform/port/`** (`package port`) — the port interfaces that name the DB
  transaction handle, `Repository` above all. May import `pgx` (for `pgx.Tx`) and `domain`;
  never `usecase`, `adapter`, or `cmd`. Every method takes `ctx context.Context, tx pgx.Tx, …`.
- **`internal/usecase/`** — one type per use case (`BookSeatUseCase`), constructor-injected
  with the `platform/port` interfaces it needs **and the `*pgxpool.Pool`**. It **owns the
  transaction boundary**: one `tx` per flow (`pool.BeginTx(ctx, pgx.TxOptions{AccessMode:
  pgx.ReadOnly})` for reads, `pool.Begin(ctx)` for writes), threaded through every repository
  call, then `Commit`. All non-DB work (entity construction, hashing, payload building) runs
  **before** `Begin` so a pooled connection is never pinned across CPU-bound work. Imports
  `domain`, `platform/port`, `pgx`/`pgxpool`; never `adapter`.
- **`internal/adapter/http/`** — handlers + request/response DTOs (no domain types in JSON
  tags) + a named `To<Entity>Response` mapper next to the DTO. Depends on the `usecase`
  layer's exported interface. Maps domain errors to HTTP status codes explicitly. Translates
  transport only; no business rules.
- **`internal/adapter/repository/postgres/`** — implements `platform/port`'s `Repository`
  using `pgx`. Every method runs on the `pgx.Tx` it's handed and **never** opens or commits a
  transaction. The only package allowed to import `pgx`'s query APIs. Holds no pool.
- **`internal/platform/`** — cross-cutting infra with no business meaning: `db` (pool +
  migrate runner), `logger`, `config`, `observability` (tracer + metrics setup), plus the
  `port/` package above.
- **`cmd/main.go`** — the composition root: the only file importing every layer. Wires
  concrete adapters into interfaces (`repo := postgres.NewRepository()`;
  `uc := usecase.NewBookSeatUseCase(pool, repo)`; `h := http.NewHandler(uc)`), owns the
  process lifecycle. No business logic.

### 3. Wire the platform baseline (place each concern in the layer that owns it)

- **DB pool** — `internal/platform/db`, `pgxpool.New` with `MaxConns` from an env var
  (bounded default ~20, never the driver's unbounded default). Constructed in `main.go`,
  passed into **each use case**; the repository holds no pool; `domain` never sees one.
- **Migrations** — `migrations/` with `embed.go` (`//go:embed *.sql`) and a
  `internal/platform/db/migrate.go` runner that applies each `*.sql` file once inside a
  transaction and records it in `schema_migrations` (copy `event-service`'s). Run it from
  `main.go` on startup. Add migrations later with `/new-migration`. If this service will
  publish or consume saga events, its first migrations are the canonical `outbox_events` (no
  `published_at`) and `processed_events` tables — see `/new-migration` step 3.
- **Health** — `GET /healthz` (liveness, no DB) and `GET /readyz` (pings the pool) in
  `internal/adapter/http`.
- **Graceful shutdown** — `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)` +
  `http.Server.Shutdown(ctx)` with a bounded timeout; flush the tracer before exit.
- **Structured logging + request ID** — `log/slog` JSON handler in
  `internal/platform/logger` behind a small `Logger` interface (not the concrete `*slog`).
  A middleware pair in `internal/adapter/http/middleware.go`, applied to **every** route:
  `RequestID()` (reuse inbound `X-Request-Id` or generate a UUID, echo it, put it on the
  context) and `AccessLog()` (one JSON line per request: `request_id, method, path, status,
  duration_ms` — never the `Authorization` header or body). Copy the pattern from
  `services/analytics-service/internal/adapter/http/middleware.go`.
- **Tracing** — `internal/platform/observability`: an OTel `TracerProvider` (OTLP/HTTP
  exporter) from env (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`). Wrap the router so
  each request's server span **continues the inbound W3C `traceparent`** (Kong/the client
  started the trace). Set `request_id` as a span attribute. Deeper spans (per use case, per
  repo call) and cross-Kafka trace propagation are `/add-observability`'s job.
- **Metrics** — a Prometheus `GET /metrics` (`promhttp`), distinct from `/healthz` /
  `/readyz`: request count, 5xx count, duration histogram, labelled by route pattern (low
  cardinality — never a path with ids in it).
- **Statelessness** — `usecase` and adapter structs constructed once at startup, reused
  across every request; no mutable per-request field. Shared state lives in Postgres/Redis
  behind a port, never in process memory.
- **Outbound timeouts** — any client to another service or external API is a `domain` port
  implemented in `internal/adapter/`, with an explicit timeout and the caller's context
  deadline propagated — never an unbounded call. Wrap it with `/add-resilience` when it gains
  a breaker/retry.

### 4. Dockerfile
Multi-stage build, distroless or alpine final stage. Non-root user.

### 5. Verify and hand off
- `go build ./...` and `go vet ./...` from the service dir; `gofmt -l .` clean.
- Route prefix in the router matches `kong.yml` exactly.
- Dependency rule intact: `domain` imports nothing local and no driver; `platform/port`
  imports only `domain` + `pgx`; `usecase` imports `domain` + `platform/port` + `pgx`/`pgxpool`
  but never `adapter`; `adapter/http` and `adapter/repository/*` don't import each other. Grep
  if unsure — the `clean-architecture-check.sh` hook checks this on every write.
- Add the service (and its `postgres-<name>`, plus an `otel-collector` if not already present)
  to `docker-compose.yml` on `ticket-network`.
- Tell the user what was scaffolded and what they still fill in (real entities, use cases, DB
  schema via `/new-migration`, endpoints via `/new-go-api-endpoint`).

Do not add a message queue, cache, or circuit breaker here — this is the base skeleton. Use
`/add-caching`, `/add-resilience`, `/add-observability` once the service has real logic, and
`/new-go-api-endpoint` (after `/design-saga` for anything cross-service) to add operations.
