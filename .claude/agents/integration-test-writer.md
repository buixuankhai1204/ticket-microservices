---
name: integration-test-writer
description: Writes a few integration tests in a service's tests/ folder for the adapters that wrap an external component, run against the real thing or a stub of it - the Postgres repository and the Kafka consumer + dead-letter adapter against Testcontainers, and the payment/email HTTP adapters against an in-process stub server. Asserts protocol-level behaviour and failure handling, never business logic. Use after changing a repository, a Kafka consumer adapter or an outbound HTTP gateway, before opening a PR.
tools: Read, Write, Edit, Grep, Glob, Bash
model: sonnet
---

Integration tests prove that **our adapter works against the real component**, not that the
business rules are right. They live in `tests/integration_repository` and
`tests/integration_kafka` (and `integration_gateways` in user-service) and are opt-in for the
Docker ones (Go build tag `integration`; Rust `#[ignore]`).

- **Repository vs Postgres**: a container is shared per test binary and every test gets a
  throwaway database with the real migrations. Cover what SQL decides: idempotency keys
  (`processed_events`, `ON CONFLICT`), row locking (a rival waits), `SKIP LOCKED` claims,
  conditional updates, ordering and pagination, that errors keep their SQLSTATE so the consumer
  can classify them, and that a rolled-back transaction leaves nothing behind.
- **Kafka consumer vs a broker** (`TEST_KAFKA_BROKERS` or Testcontainers, with topic
  auto-creation off): the offset is committed only after the use case ran; an event type owned by
  another group is skipped but committed; a poison message lands on `<topic>.dlq` with the
  original key and value and the `x-dlq-*` origin headers while the next message still gets
  through; a permanent failure is dead-lettered without retries; a transient failure is retried;
  and **no message is ever committed past one that could not be dead-lettered**.
- **HTTP gateways vs a stub server** (the in-process `wiremock` crate, no Docker): status → saga
  arm mapping (approved, declined, transient — other 4xx stay transient), the `Idempotency-Key`
  header, timeouts, malformed bodies.

Rules: unique topics and databases per test; a scripted use case (not the real one) drives the
consumer; few tests; no comments; do not change production code. Run with
`scripts/run-tests.sh integration <service>`.

See `@CLAUDE.md` for the layering and the test tiers.
