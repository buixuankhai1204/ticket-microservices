# Ticket Booking Platform

A ticket booking system split into independent microservices — mixed Rust and Go on
purpose, each service in Clean Architecture, fronted by a single Kong Gateway, with
cross-service state changes flowing through a Kafka choreography saga instead of
synchronous service-to-service calls.

| Service | Language | Port | Owns |
|---|---|---|---|
| [`user-service`](services/user-service) | Rust / axum | 8081 | register, login, profile, list — publishes `UserCreated` / `UserLoggedIn` |
| [`event-service`](services/event-service) | Go | 8082 | events, seat layouts, performers — paginated reads |
| [`booking-service`](services/booking-service) | Rust / axum | 8083 | create/get booking — publishes `BookingRequested`, drives the seat-reservation saga |
| [`analytics-service`](services/analytics-service) | Go | 8084 | consumes saga events into read models |

Every cross-service route above is also declared in [`kong/kong.yml`](kong/kong.yml) —
that file is the source of truth for paths, ports, JWT requirements, and rate limits.
Check it before adding or renaming a route.

## Architecture

```mermaid
flowchart TB
    client([Client])
    client -->|"http://localhost:8000"| kong["Kong Gateway<br/>proxy :8000 · admin :8001"]

    kong -->|"/api/v1/auth<br/>/api/v1/users<br/>JWT on /users"| us["user-service<br/>Rust · axum · :8081"]
    kong -->|"/api/v1/events<br/>no auth"| es["event-service<br/>Go · :8082"]
    kong -->|"/api/v1/bookings<br/>JWT required"| bs["booking-service<br/>Rust · axum · :8083"]
    kong -->|"/api/v1/analytics<br/>JWT required"| as["analytics-service<br/>Go · :8084"]

    us --> pgu[("postgres-user<br/>wal_level=logical")]
    es --> pge[("postgres-event")]
    bs --> pgb[("postgres-booking<br/>wal_level=logical")]
    as --> pga[("postgres-analytics")]

    pgu -.WAL tail via replication slot.-> connect
    pgb -.WAL tail via replication slot.-> connect
    connect["Kafka Connect<br/>+ Debezium Postgres connector<br/>+ Outbox Event Router SMT"] -->|"user.events"| kafka[("Kafka<br/>single-node KRaft")]
    connect -->|"booking.events"| kafka

    kafka --> es
    kafka --> bs
    kafka --> as

    classDef go fill:#00ADD8,color:#0b1d26,stroke:#00747f
    classDef rust fill:#DEA584,color:#3d1f00,stroke:#a85d2e
    classDef infra fill:#eef1f5,color:#1a1a1a,stroke:#9aa5b1
    class es,as go
    class us,bs rust
    class kong,pgu,pge,pgb,pga,connect,kafka infra
```

**Two independent transport layers, on purpose:**

- **Synchronous / client-facing** — Kong routes REST requests straight to a service.
  `strip_path: false` everywhere, so each service handles its own full `/api/v1/...`
  prefix rather than expecting Kong to strip it.
- **Asynchronous / service-to-service** — a **choreography saga** over Kafka. There is
  no central orchestrator; each service reacts to events from others and publishes
  events about its own state changes. Kong plays no role here.

### How an event actually gets published — the transactional outbox

A service never calls a Kafka producer directly. Instead:

1. The use case writes the domain event to that service's own `outbox_events` table,
   in the **same Postgres transaction** as the state change it describes (e.g. the
   `users` insert on registration). This is what makes "commit the change" and
   "announce it" atomic without two-phase commit against Kafka.
2. The row is inserted **and deleted again in that same transaction** — the insert
   still lands in the WAL (which is all Debezium needs), so the table itself stays
   empty.
3. A **Debezium PostgreSQL connector**, running on Kafka Connect, tails that WAL via
   a logical replication slot and publishes each captured insert through the
   **Outbox Event Router SMT** — routing `aggregate_type` → topic
   (`<aggregate_type>.events`) and `aggregate_id` → the Kafka message key, so every
   event about one aggregate (one user, one booking) lands on the same partition and
   stays in order.
4. Consumers dedupe on the event's id against a `processed_events` table before
   applying it (delivery is **at-least-once**; this makes processing
   *effectively-once*), and dead-letter poison or permanently-failing messages to
   `<topic>.dlq` rather than wedging the partition.

See [`CLAUDE.md`](CLAUDE.md) for the full write-up of this pattern and
[`docs/sagas/seat-reservation.md`](docs/sagas/seat-reservation.md) for the
booking↔event seat-reservation saga's event catalog, failure sequences, and
compensation map.

## Prerequisites

- Docker and Docker Compose
- For running a service outside its container: Go 1.22+ (`event-service`,
  `analytics-service`) or Rust 1.75+ (`user-service`, `booking-service`)

## Quick start

```bash
cp .env.example .env
docker compose up -d --build
```

This brings up, in dependency order: four Postgres instances (each with its own
volume), the Kafka broker, a one-shot job that creates the saga topics
(`kafka-init`), Kafka Connect, a one-shot job that registers the two Debezium
connectors (`connect-init`), the four services, and Kong.

Check everything is healthy:

```bash
docker compose ps
```

Every service exposes `GET /healthz` (liveness) and `GET /readyz` (readiness,
checks its DB pool) — both outside the `/api/v1` prefix and unauthenticated.

## Try it through the gateway

```bash
# Register, then log in
curl -s -X POST http://localhost:8000/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"correct horse battery staple"}'

TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"correct horse battery staple"}' | jq -r .token)

# Browse events (public, no auth)
curl -s http://localhost:8000/api/v1/events | jq

# Create a booking (JWT required)
curl -s -X POST http://localhost:8000/api/v1/bookings \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"seat_ids":["<a seat uuid from the events response>"]}'
```

`/api/v1/users` and `/api/v1/bookings` require the `Authorization: Bearer <token>`
header — Kong's `jwt` plugin verifies the `exp` claim before the request ever
reaches the service.

### Watching the saga

```bash
# Confirm the Debezium connectors came up
curl -s http://localhost:8083/connectors/user-service-outbox/status | jq '.connector.state'
curl -s http://localhost:8083/connectors/booking-service-outbox/status | jq '.connector.state'

# Tail a topic directly
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic user.events --from-beginning \
  --property print.headers=true
```

## Interactive API docs

The Go services serve live Swagger UI directly (generated by `swag`, committed
under each service's `docs/`):

- event-service: http://localhost:8082/swagger/
- analytics-service: http://localhost:8084/swagger/

`docs/openapi/`, a Postman collection, and `docs/curl-examples.md` at the repo root
are kept in sync with the handlers by the `api-doc-sync` tooling — see
[Claude Code tooling](#claude-code-tooling-for-this-repo) below.

## Configuration

Copy [`.env.example`](.env.example) to `.env` and adjust as needed — `docker compose`
reads it automatically. Every value has a sane local default baked into
`docker-compose.yml`, so `docker compose up` works even with no `.env` at all. Two
settings that must stay in sync:

- `JWT_SECRET` / `JWT_ISSUER` — the secret and issuer `user-service` signs tokens
  with must match the matching consumer entry in `kong/kong.yml`, or Kong rejects
  every token on JWT-protected routes.

## Running a single service outside Docker

Point it at its own Postgres (Docker still runs the databases + Kafka):

```bash
docker compose up -d postgres-event
cd services/event-service
DATABASE_URL=postgres://event_service:event_service@localhost:5435/event_service \
PORT=8082 \
go run ./cmd
```

```bash
docker compose up -d postgres-user
cd services/user-service
DATABASE_URL=postgres://user_service:user_service@localhost:5433/user_service \
JWT_SECRET=dev-secret-change-me JWT_ISSUER=user-service \
cargo run
```

Host ports for each service's own Postgres: `postgres-user` 5433, `postgres-analytics`
5434, `postgres-event` 5435, `postgres-booking` 5436 (all map to container port 5432).

> `booking-service`'s container port 8083 collides with `kafka-connect`'s REST API,
> which also publishes to host port 8083 — so in `docker-compose.yml` it's the only
> service remapped on the host side, at **8085:8083**. Inside the Docker network
> (and from Kong) it's still reached at `booking-service:8083`; only a direct
> `localhost` call from the host needs port 8085.

## Testing

Each service follows the same three tiers — enter at the lowest layer that can
express the property under test:

| Tier | What it proves | Needs |
|---|---|---|
| Unit (`domain`, `usecase`) | invariants, pure business logic | nothing — mocked ports |
| Integration | the real HTTP → usecase → repository → Postgres path: atomicity, concurrency, idempotency, pagination | Docker (a throwaway Postgres per test binary) |
| End-to-end | the full choreography saga through Kong, Kafka, and Debezium | `docker compose up -d` already running |

```bash
# Go services
cd services/event-service && go test ./... && go vet ./...
go test -tags=integration ./integration/...

# Rust services
cd services/user-service && cargo test
cargo test --manifest-path services/user-service/Cargo.toml --test '*'
```

## Project layout

Every service follows the same Clean Architecture shape regardless of language —
dependencies point inward only:

```
services/<name>/
├── domain/                  entities, invariants, pure functions, port interfaces
├── usecase/                 one type per business flow, orchestrates via ports
├── adapter/
│   ├── http/                handlers + DTOs, domain ↔ wire mapping
│   ├── repository/postgres/ the only layer allowed to import the DB driver
│   └── messaging/kafka/     consumers / outbox relay adapters
├── migrations/               timestamped SQL, expand/contract-safe
└── cmd/ (Go) or main.rs (Rust)   composition root
```

Full conventions — entity IDs, transaction ownership, pagination envelopes,
migration safety, observability, and the complete saga/outbox design — are written
up in [`CLAUDE.md`](CLAUDE.md).

## Claude Code tooling for this repo

This repo ships a full set of Claude Code skills, subagents, and hooks under
`.claude/` for scaffolding services, wiring saga steps, authoring migrations, and
running read-only architecture/security/concurrency audits — see the tables in
[`CLAUDE.md`](CLAUDE.md#claude-code-tooling-set-up-for-this-repo) and
[`docs/development-workflow.md`](docs/development-workflow.md) for when to use each
one.

## Current status / known limitations

- Kafka runs as a **single broker** in this compose file (`replication-factor: 1`
  everywhere) — fine for local development, not representative of a production
  topology (would need ≥3 brokers and `replication-factor: 3`).
- The seat-reservation saga's `BookingRequested` publish step is wired;
  `event-service`'s seat-reservation consumer step and the confirm/compensate path
  back in `booking-service` are tracked in
  [`docs/sagas/seat-reservation.md`](docs/sagas/seat-reservation.md) — check that
  file's "Next actions" section for what's implemented versus pending.
