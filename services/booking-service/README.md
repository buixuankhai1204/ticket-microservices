# booking-service

Rust (axum) service on port **8083**, gateway prefix **`/api/v1/bookings`** (JWT). It creates a
pending booking, then confirms or cancels it from the `SeatReserved` / `SeatReservationFailed`
events event-service publishes (see `docs/sagas/seat-reservation.md`).

## Testing

| Type | Where | Real | Faked |
|---|---|---|---|
| Unit | `#[cfg(test)]` in `src/` | the code under test | its collaborators (mockall `Transactor` and `BookingRepository`) |
| Integration | `tests/integration_repository.rs`, `tests/integration_kafka.rs` | our adapter plus a real Postgres / Kafka (Testcontainers) | nothing else |
| Component, in-process | `tests/component_in_process.rs` | the `app::App` wiring over real HTTP and a real Postgres | Kafka: events are fed straight to the consumer handlers as bytes |
| Component, out-of-process | `tests/component_out_of_process.rs` | the compiled binary as its own process, real Postgres and Kafka | the other services (the test produces their events) |
| Contract | `tests/contract_consumer.rs` | our event parsing | event-service, as a Pact message (writes `pacts/`) |
| End-to-end | `tests/e2e.rs` | a running stack, through Kong | nothing |

```bash
cargo test                                   # unit + contract, no Docker

cargo test --test integration_repository -- --ignored
cargo test --test integration_kafka -- --ignored
cargo test --test component_in_process -- --ignored
cargo test --test component_out_of_process -- --ignored

E2E_BASE_URL=http://localhost:8000 cargo test --test e2e -- --ignored
```

A bare `cargo test -- --ignored` also runs `e2e`, which needs `E2E_BASE_URL` (the Kong gateway of a
running stack), so select the files with `--test`. Set `TEST_DATABASE_URL` (an admin URL such as
`postgres://postgres:postgres@localhost:5432/postgres`) and `TEST_KAFKA_BROKERS` to reuse
infrastructure you already run instead of starting containers. Every test gets its own throwaway
database and its own topics, so tests run in parallel against one container each.

The use cases take an `Arc<dyn Transactor>` instead of the pool and the repository takes the
`Tx` the use case opened, which lets the unit tests run them against mocks with no database.
