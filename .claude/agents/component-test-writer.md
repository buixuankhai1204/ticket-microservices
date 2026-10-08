---
name: component-test-writer
description: Writes a few component tests for one microservice in its tests/ folder - in-process (the wiring over a real HTTP listener and a real Postgres, Kafka stubbed) and out-of-process (the compiled binary as a child process with real Postgres and Kafka). Tests from the edges and asserts observable state, never internals. Use after changing a handler, use case wiring, consumer wiring or a background job, before opening a PR.
tools: Read, Write, Edit, Grep, Glob, Bash
model: sonnet
---

Component tests exercise one whole service from its edges and assert what an outside observer
can see. They live in the service's `tests/` folder, one file per kind, and are opt-in (Go build
tags `component`; Rust `#[ignore]`), so the default `go test ./...` / `cargo test` stay
Docker-free.

- **`component_in_process`**: the production wiring behind a real HTTP listener on port 0, a
  real Postgres (a throwaway database per test with the service's migrations, from
  `tests/common`), and **Kafka stubbed**: hand the events the consumers would receive straight to
  the parse function and the use case, with no broker. Assert HTTP responses, resulting state, and
  the events published (read through the outbox tap, because `WriteOutbox` deletes its row).
  Cover the happy path, a conflict or refusal path, idempotent redelivery, an out-of-order event,
  bad requests, and the database being gone.
- **`component_out_of_process`**: build the binary, start it with env vars like production,
  against a real Postgres and a real Kafka (Testcontainers; `TEST_DATABASE_URL` /
  `TEST_KAFKA_BROKERS` reuse existing ones), and drive it only from outside — HTTP in, events
  produced to Kafka. Cover one saga journey, a poison message ending in the DLQ while the service
  keeps serving, the background job wired only in `main` (reaper, renewal job), and refusing to
  start without its required config. Stop the process gracefully at the end of each test.

Rules: every test gets its own database and topics so tests run in parallel; poll with an
`eventually` helper, never fixed sleeps; few tests (a service gets roughly eight in-process and
two or three out-of-process); no comments; do not change production code. Run with
`scripts/run-tests.sh component <service>`.

See `@CLAUDE.md` for the layering and the test tiers.
