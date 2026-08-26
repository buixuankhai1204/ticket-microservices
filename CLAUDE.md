# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This repository is in early scaffolding: it currently contains only the Kong API Gateway
configuration (`kong/kong.yml`). No service code exists yet. There are no build, lint, or
test commands to run until services are added — do not invent any.

## Architecture (as defined by the gateway)

This is a ticket booking platform split into independent microservices, fronted by a single
Kong Gateway (`kong/kong.yml`, declarative config format `3.0`). The gateway is the source of
truth for how services are named, routed, and secured — check it before adding or renaming a
service.

Planned services, each expected to be its own deployable unit reachable at
`http://<service-name>:<port>` from the gateway:

| Service | Port | Routes | Auth / limits |
|---|---|---|---|
| `user-service` | 8081 | `/api/v1/auth` (60 req/min), `/api/v1/users` (100 req/min) | `/api/v1/users` requires JWT (`exp` claim verified) |
| `event-service` | 8082 | `/api/v1/events` (1000 req/min) | none |
| `booking-service` | 8083 | `/api/v1/bookings` (300 req/min) | JWT (`exp` claim verified) |
| `analytics-service` | 8084 | `/api/v1/analytics` (60 req/min) | JWT (`exp` claim verified) |

All routes use `strip_path: false`, so each service must handle the full `/api/v1/...` prefix
itself rather than expecting it stripped.

Global plugins applied gateway-wide: `cors` (all origins, credentials enabled) and per-route
`rate-limiting` (local policy, per-service limits as above).

The stated intent (see `README.md`) is a mixed-language implementation — Rust and Go across
the different services — rather than a single shared stack. When adding a service, confirm
which language it's meant to be in before scaffolding it, and keep its exposed port and route
prefix in sync with `kong/kong.yml`.

## Application architecture: Clean Architecture per service

Every service (Go or Rust) follows Clean Architecture, dependencies pointing inward only:

- `domain` — entities and business invariants (as methods on the entity), plus the port
  interfaces/traits the service needs (repository, outbound gateways). No imports of any
  framework or driver (no `pgx`/`sqlx`, no `net/http`/`axum`).
- `usecase` — orchestrates one business flow per type, depends on `domain` only, receives its
  ports via constructor injection.
- `adapter/http` — controllers/handlers and DTOs, translate transport at the edge, depend on
  `usecase`'s public interface.
- `adapter/repository/postgres` — implements a `domain` port against Postgres; the only layer
  allowed to import the DB driver.
- `cmd/main.go` (Go) / `main.rs` (Rust) — the composition root: the only place that wires
  concrete adapters into interfaces and owns the process lifecycle (server startup, graceful
  shutdown).

Use `/new-go-service` or `/new-rust-service` to scaffold a service in this shape; use
`/new-go-api-endpoint` / `/new-rust-api-endpoint` to add an endpoint to one afterward, and
`/scalability-review` / `/review-concurrency` to audit one.

## Cross-service communication: choreography saga over Kafka

Cross-service state changes (e.g. a booking needing a seat reserved in `event-service`) go
through **Kafka**, not synchronous HTTP calls between services — Kong only fronts the
synchronous, client-facing REST routes in the table above; it plays no role in
service-to-service messaging.

The saga style is **choreography**: there is no central coordinator service. Each service
publishes events about its own state changes and independently reacts to events from other
services, including compensating its own earlier step on a downstream failure. Adding or
changing a saga step should never require teaching one service the full sequence — only what
it does when it observes a given event.

Two supporting patterns are required, not optional, for any publish/consume code:

- **Transactional outbox** — an event is written to an `outbox_events` table in the *same* DB
  transaction as the state change it describes, and a background relay publishes from that
  table to Kafka. This avoids the dual-write problem (DB commit and Kafka publish can't be made
  atomic any other way without 2PC). Events for the same aggregate are keyed by `aggregate_id`
  so a partition preserves their order.
- **Idempotent consumers** — Kafka delivers at-least-once. Every consumer checks a
  `processed_events` table (unique event ID) before applying an event, in the same transaction
  as the side effect.

Client libraries: `segmentio/kafka-go` for Go services, `rdkafka` for Rust services.

Use `/add-go-saga-step` or `/add-rust-saga-step` to wire a publish or consume step. Example
saga already sketched for this repo: `booking-service` publishes `BookingRequested` →
`event-service` reserves the seat and publishes `SeatReserved`/`SeatReservationFailed` →
`booking-service` confirms or compensates (cancels) the booking → `analytics-service` consumes
the final outcome as a read model only.
