# Development workflow (SDLC) with the `.claude/` toolkit

This is the end-to-end lifecycle for changing this repo, and which skill/agent/hook does what
at each step. It assumes the conventions in `CLAUDE.md` (Clean Architecture layers, usecase
owns the transaction, choreography saga over Kafka, transactional outbox via Debezium CDC).

The toolkit lives entirely in `.claude/` — 11 skills (`/name`), 8 subagents (Agent tool or
by name), 3 hooks (automatic). There is no plugin mirror.

---

## 0. One-time prerequisites

| Need | Why |
|---|---|
| Go toolchain, Rust toolchain (`rustup component add clippy rustfmt`) | Per-service build/lint; the `pre-commit-check.sh` hook needs them (missing = skipped, not failed) |
| Docker + `docker compose` | `docker-compose.yml` runs the whole stack; `integration-test-writer` and `e2e-saga-tester` need it |
| `swag` CLI (`go install github.com/swaggo/swag/cmd/swag@latest`) | Regenerating Go Swagger docs; `docs/` is committed and required to build |
| `.env` (copy from `.env.example`) | `docker compose` reads it |

---

## 1. Pick the right lifecycle

| You are… | Go to |
|---|---|
| Starting a brand-new service | §2 |
| Adding a business action that **crosses services** (a saga) | §3 |
| Adding a plain REST endpoint to an existing service | §4 |
| Only changing the database schema | §5 |
| Adding caching / resilience / observability to an existing service | §6 |
| About to open a PR | §7 (always) |
| Running the stack locally to try it | §8 |

Each lifecycle ends at §7 (review gate) then a PR.

---

## 2. New service

1. **Add the route to `kong/kong.yml` first.** Service name, `url` (`http://<name>:<port>`),
   route `paths` (`strip_path: false`), `rate-limiting`, and `jwt` plugin if protected. Add a
   `consumers[].jwt_secrets` entry if it issues tokens. The gateway config is the source of
   truth — the scaffold skill reads it.
2. **Plan mode** (`Shift+Tab`), then `/new-go-service <name>` or `/new-rust-service <name>`.
   Scaffolds the layers + pool + health + graceful shutdown + request-ID logging + OTel + a
   `/metrics` endpoint + the migrations runner. Review the plan before it writes.
3. **Schema**: `/new-migration <name> create_<entity>` for each table. A saga participant's
   first migrations are the canonical `outbox_events` and `processed_events` tables.
   → `migration-reviewer` (see §5).
4. **Operations**: `/new-go-api-endpoint` / `/new-rust-api-endpoint` per endpoint (§4) or
   saga step (§3).
5. Add the service + its `postgres-<name>` (and `otel-collector` if not present) to
   `docker-compose.yml` on `ticket-network`. A publisher's Postgres needs `wal_level=logical`.
6. **Gate**: `api-contract-reviewer` (route matches `kong.yml`), then §7.

---

## 3. New saga (cross-service state change)

This is the highest-risk change type. Do not skip the design step.

1. **Design first** — `/effort high` then `/design-saga <saga-name>` (e.g. `seat-reservation`).
   Produces `docs/sagas/<saga-name>.md`: event catalog (name, `aggregate_type`, topic,
   partition key, delivery guarantee, payload), happy path, **every** failure sequence, the
   compensation map (a compensation is a *new forward transaction*, never a rollback), the
   stuck-saga timeout/reaper policy, and the topic/connector/migration infra delta. It ends
   with the exact `/new-*-api-endpoint` invocations to run next.
2. **Read the design.** This is the cheapest place to catch "distributed spaghetti" (> ~4
   hops or > 2 branches → reconsider an orchestrator) and a missing compensation.
3. **Migrations** for each participant — `/new-migration` for `outbox_events`,
   `processed_events`, new status enum values, new columns. → `migration-reviewer`.
4. **Wire each step**, one at a time, **in plan mode**:
   `/new-go-api-endpoint <svc> <UseCase> [http:…] [publish:<Event>:<aggregate_type>] [consume:<Event>:<topic>]`
   - `publish:` adds the `WriteOutbox` call on the usecase's transaction, the
     `debezium/<svc>-outbox.json` connector (if new), and the topic + `.dlq` in `kafka-init`.
   - `consume:` adds the consumer group, the `event_type` header guard (ack-and-skip
     siblings), the `processed_events` idempotency check, and the DLQ classification
     (commit / capped-retry-with-backoff / dead-letter).
   - A compensating step is a `consume:<FailureEvent>` whose usecase forward-corrects this
     service's own earlier write.
5. **Gate**:
   - `saga-consistency-reviewer` — every published event has a consumer; every failure has a
     compensation; every topic/DLQ/connector exists; every consumer is idempotent and can't
     wedge a partition; no state can stick in `pending`. Run it after **every** `publish:` /
     `consume:` step and again before the PR.
   - `integration-test-writer` — concurrent oversell (N requests, M<N seats, exactly M
     succeed), idempotent consumer (same event id twice = no-op), transaction atomicity (no
     state without its outbox row), the compensation path, the reaper.
   - `docker compose up -d` (§8), then **`e2e-saga-tester`** — drives the saga through Kong,
     asserts DB + DLQ state on the happy path and the compensation path.
   - Then §7.

---

## 4. New REST endpoint (single service)

1. `/new-go-api-endpoint <svc> <UseCase> http:<METHOD>:<path>` (or the Rust one). Wires
   domain → `platform/port` → usecase (opens one transaction) → repository → handler + DTOs +
   named response mapper + explicit domain-error → status mapping + swaggo/utoipa annotation.
   A list endpoint is auto-paginated with the `limit`/`offset` envelope.
   - `<path>` must start with the prefix the service owns in `kong.yml`, or the skill stops.
2. Regenerate Go docs if needed: `swag init -g cmd/main.go -o docs` (from the service dir).
3. **Gate**:
   - `security-reviewer` — if the endpoint touches auth, user input, or raw SQL (JWT trust
     boundary, IDOR, injection, log leakage).
   - `review-concurrency` — if it reads-then-writes shared state (seat counts especially).
   - `api-doc-sync` — regenerate `docs/openapi/*.yaml` + Postman + curl-examples from the
     handler.
   - `unit-test-writer` (domain changes) and/or `integration-test-writer` (usecase / handler).
   - Then §7.

---

## 5. Schema-only change

1. `/new-migration <svc> <migration_name>` — timestamped SQL in
   `services/<svc>/migrations/`. Enforces: additive-safe in one file, or an **expand/contract
   split** across deploys for anything breaking (`DROP`/`RENAME`/type-narrow/`NOT NULL` with
   no default); an index on every FK and list-filter column; a reversibility note; the
   canonical `outbox_events` / `processed_events` shapes.
2. The **`migration-safety-check.sh` hook** fires automatically on write and blocks (exit 2)
   on the grep-obvious hazards, with the fix.
3. **Gate**: `migration-reviewer` — the full audit (lock duration at realistic scale,
   expand/contract phasing, `CONCURRENTLY` vs the txn-wrapped runner, missing indexes).
4. Apply: restart the service (its startup runner applies pending files), or
   `docker compose up -d --build <svc>`. For a destructive reset: `docker compose down -v`.

---

## 6. Cross-cutting retrofit

| Reach for | When | What it does |
|---|---|---|
| `/add-caching <svc> <ReadUseCase>` | A hot read path can't serve from Postgres under its `kong.yml` rate limit; `scalability-review` flags it | Cache-aside Redis behind a `domain` port — TTL+jitter, fail-open, per-user keys for JWT routes, invalidation in the write path |
| `/add-resilience <svc> <PortName>` | A service gains its first synchronous outbound call (payment, email, third-party) | Timeout + capped retry-with-jitter + circuit breaker + bulkhead on the adapter impl; explicit fail-open/closed |
| `/add-observability <svc>` | A service predates the logging baseline (`user-service`), or a saga needs one trace end to end | Request-ID access logs, OTel spans, `traceparent` carried through the outbox → Kafka header → consumer span, RED + consumer-lag metrics |

Each ends at the relevant §7 reviewers (`scalability-review` for caching, `security-reviewer`
for observability PII, `saga-consistency-reviewer` if the outbox shape changed).

---

## 7. Review gate — before every PR

Run in this order; each is read-only and reports `file:line` + consequence + severity.

| Order | Reviewer | Runs when the change… |
|---|---|---|
| 1 | `security-reviewer` | touches auth, user input, raw SQL, a consumer, or `kong.yml` |
| 2 | `review-concurrency` | reads-then-writes shared state (any seat/booking path) |
| 3 | `saga-consistency-reviewer` | added or changed a `publish:` / `consume:` step |
| 4 | `migration-reviewer` | ships a migration |
| 5 | `api-contract-reviewer` | changed routes in ≥1 service (also: periodically) |
| 6 | `scalability-review` | added or changed a service (statelessness, pool, N+1, tx scope, metrics) |

Then tests:
- `unit-test-writer` after any `domain` change.
- `integration-test-writer` after any `usecase` change or new saga step.
- `e2e-saga-tester` once a saga's steps are all wired and the stack is up (§8).

Then static checks (the `pre-commit-check.sh` hook also runs these on `git commit`, scoped to
staged files' services):
```bash
# per Go service dir
go build ./... && go vet ./... && gofmt -l .
# per Rust service dir
cargo build && cargo clippy -- -D warnings && cargo fmt --check
```

Then commit (branch first if on `main`), open the PR.

---

## 8. Run & verify locally

```bash
docker compose up -d
docker compose ps                     # every service + postgres-* + kafka healthy; *-init completed
curl -s localhost:8083/connectors/user-service-outbox/status   # connector.state + tasks RUNNING (not just registered)
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Drive it through Kong (`http://localhost:8000`), never a service port:
```bash
curl -i -X POST localhost:8000/api/v1/auth/register -H 'content-type: application/json' -d '{"email":"e2e@example.com","password":"correct-horse-battery-staple"}'
```
See `docs/curl-examples.md` for the rest.

Inspect a DLQ (should be empty):
```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user.events.dlq --from-beginning --timeout-ms 5000
```

Reset (wipes volumes — needed after a non-additive migration):
```bash
docker compose down -v && docker compose up -d
```

---

## 9. What blocks you automatically (hooks)

| Hook | Fires on | Blocks when |
|---|---|---|
| `clean-architecture-check.sh` | `Write`/`Edit` of a file under `domain/`, `platform/port/`, `usecase/`, `adapter/{http,repository,cache,messaging}/` | An import violates the dependency rule (e.g. `domain/` importing `pgx`, `usecase/` importing `adapter/`, a cache adapter reaching into the repository). `cmd/main.go` / `main.rs` is exempt. |
| `migration-safety-check.sh` | `Write`/`Edit` of `services/*/migrations/*.sql` | `ADD COLUMN … NOT NULL` with no `DEFAULT`, `DROP COLUMN`, `ALTER COLUMN … TYPE`, `RENAME`, `ADD CONSTRAINT` without `NOT VALID`, `CONCURRENTLY` without the `-- +migrate NoTransaction` marker |
| `pre-commit-check.sh` | `git commit` | `gofmt`/`go vet` or `cargo fmt --check`/`cargo clippy -- -D warnings` fails for a service with staged changes |

These only gate Claude Code sessions — not edits/commits made directly in a terminal.

---

## 10. When to use which Claude Code mode

- **Plan mode** — before `/new-go-service`, `/new-rust-service`, or any `/new-*-api-endpoint`
  carrying `publish:` / `consume:`. Not needed for a plain `http:` endpoint on a known service.
- **Extended thinking** (`/effort high`) — `/design-saga` runs with it by default; also the
  seat-reservation locking strategy. Not for routine CRUD.
- **Background tasks** — run a service's test suite, or `e2e-saga-tester` against a running
  stack, in the background while you keep editing another service.
- **Checkpoints** (`Esc Esc`) — back out of an exploratory scaffold; not a substitute for git.

---

## 11. CI (not yet set up)

There is no `.github/workflows/` yet. When added it should run, per service with changes:
`go build ./... && go vet ./... && gofmt -l . && go test ./...` (Go) /
`cargo build && cargo clippy -- -D warnings && cargo fmt --check && cargo test` (Rust); plus a
`docker compose`-based job for `-tags=integration` / `tests/` integration suites; plus the
review agents as an advisory (non-blocking) step on the diff.
