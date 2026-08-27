# analytics-service

Go microservice, port **8084**, gateway prefix **`/api/v1/analytics`** (`strip_path: false`
in `kong/kong.yml`, so this service handles the full prefix). Kong verifies a JWT (`exp`
claim) before any `/api/v1/analytics` request reaches here.

Per the platform design it is a **read model only**: it *consumes* the final outcome of the
booking saga from Kafka and serves aggregate queries. It never publishes saga events and its
HTTP surface is read-only.

## Layout (Clean Architecture, dependencies point inward)

| Path | Role |
|---|---|
| `internal/domain/` | `BookingOutcome` entity + invariant, `EventBookingStats` value, `Repository` port, domain errors. No framework/driver imports. |
| `internal/usecase/` | `GetEventStatsUseCase` — one flow, constructor-injected `domain.Repository`. |
| `internal/adapter/http/` | Handlers, DTOs + `ToEventStatsResponse` mapper, health probes, request-ID / access-log middleware, router. |
| `internal/adapter/repository/postgres/` | `Repository` against Postgres via `pgxpool`. Only package allowed to import `pgx`. |
| `internal/platform/` | `config`, `db` (bounded pool + minimal migration runner), `logger` (slog JSON behind an interface). |
| `migrations/` | Embedded `.sql`, applied at startup in filename order. |
| `cmd/main.go` | Composition root + lifecycle (graceful shutdown on SIGINT/SIGTERM). |

## Config (env)

| Var | Required | Default |
|---|---|---|
| `DATABASE_URL` | yes | — |
| `PORT` | no | `8084` |
| `DB_MAX_CONNS` | no | `20` |

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness (no DB touch) |
| `GET` | `/readyz` | Readiness (pings the pool) |
| `GET` | `/api/v1/analytics/events/{eventID}` | Confirmed / cancelled booking counts for one event |

## What still needs filling in

- Real analytics entities / use cases / queries — add them with `/new-go-api-endpoint`.
- The Kafka consumer that writes `booking_outcomes` from `BookingConfirmed` /
  `BookingCancelled` events — add it with `/add-go-saga-step` (it also adds a
  `processed_events` idempotency table and the write method on `domain.Repository`).
- `go mod tidy` in this directory to produce `go.sum` (needs network once).

## Local build / run

```bash
cd services/analytics-service
go mod tidy
go vet ./...
DATABASE_URL=postgres://localhost:5432/analytics go run ./cmd
```
