---
name: new-migration
description: Author a Postgres schema migration for one service in the repo's timestamped-SQL convention, safe for a zero-downtime rolling deploy — expand/contract split, no table-rewriting or lock-heavy DDL in a step the running version can't tolerate, an index on every FK and list-filter column, and a reversibility note. Use whenever a service needs a new table, column, index, constraint, or enum value (new outbox_events / processed_events table for a saga step, a new booking status, a seats index).
argument-hint: "<service-name> <migration_name>  (e.g. booking-service create_bookings)"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# New database migration

## Context
- Project conventions: @CLAUDE.md
- Target service's existing migrations: !`ls services/$1/migrations/ 2>/dev/null || echo "(service not found — check the name)"`
- How this service applies migrations: !`find services/$1 -name migrate.go -o -name 'main.rs' | head -3`

## Arguments
`$ARGUMENTS` — `<service-name>` (must exist under `services/`) then a `snake_case`
`<migration_name>` describing the change (`create_bookings`, `add_booking_status_cancelled`,
`index_seats_event_id`).

## Repo conventions (do not deviate)

- **File**: `services/<service>/migrations/<version>_<migration_name>.sql` where `<version>`
  is a UTC timestamp `YYYYMMDDHHMMSS` — generate it with `date -u +%Y%m%d%H%M%S`. It must
  sort **after** every existing file in that directory (check the `ls` above; bump by a
  second if two are generated in the same run).
- **Go services** embed `migrations/*.sql` via `migrations/embed.go` (`//go:embed *.sql`) and
  apply each file **inside one transaction** in `internal/platform/db/migrate.go`, recording
  the filename in `schema_migrations`. Adding a `.sql` file is all that's needed — `embed.go`
  already globs. **A transaction-wrapped runner cannot run `CREATE INDEX CONCURRENTLY`** or
  any other statement that forbids a txn block — see "Non-transactional migrations" below.
- **Rust services** (`user-service`) use `sqlx` migrations from `migrations/`, also
  transaction-wrapped per file.
- Every migration is **idempotent-safe to re-read**: `CREATE TABLE IF NOT EXISTS`,
  `CREATE INDEX IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `DROP ... IF EXISTS`. The runner
  already skips applied files, but this keeps a hand-run safe too.
- **UUID keys**: `id UUID PRIMARY KEY DEFAULT gen_random_uuid()` — never `SERIAL` /
  `BIGSERIAL` / `IDENTITY` (see the entity-ID rule in @CLAUDE.md).
- **Money** is integer minor units (`price_minor BIGINT NOT NULL CHECK (price_minor >= 0)`),
  never `float`/`numeric` dollars.
- **Enums** are `TEXT` + a `CHECK (col IN (...))` constraint (see `seats.status`), not
  Postgres `CREATE TYPE ... AS ENUM` (adding a value to a real enum can't run in a txn and
  can't be removed).

## Instructions

### 1. Classify the change
State which it is, because it decides whether one file is safe or you need an expand/contract
split:

- **Additive & safe in one step** — a new table; a new **nullable** column, or a column
  `NOT NULL` **with a constant `DEFAULT`** (Postgres ≥11 fills it without a full rewrite); a
  new `CHECK`/`FK` added `NOT VALID` then `VALIDATE`d; a new index (see step 4). The running
  service version ignores what it doesn't know about, so ship it.
- **Breaking — must be expand/contract across ≥2 deploys** — dropping/renaming a column or
  table, narrowing a type, adding a `NOT NULL` column with no default, adding a plain
  (validated-immediately) constraint to a populated table. Do **not** put this in one
  migration. Produce only the **expand** migration now (add the new shape alongside the old),
  and write the ordered follow-up plan into the file's header comment:
  1. *expand* (this file) — add new column/table, keep the old; deploy.
  2. *backfill* — a separate later migration copying old → new in batches; deploy.
  3. *code* — service reads/writes the new shape; deploy.
  4. *contract* — a separate later migration dropping the old shape; deploy.
  `migration-reviewer` will reject a breaking change that skips this.

### 2. Write the file
Header comment first: **what** changed, **why**, and — for a breaking change — the
expand/contract step this file is and what the down path is (the exact `DROP`/`ALTER` to
reverse it; "not reversible without data loss" if true). Then the DDL. Match the house style
in `services/event-service/migrations/20260827000002_create_seats.sql` (lowercase types
aligned, `CHECK` inline, a comment above each non-obvious column).

### 3. Canonical table shapes — copy these exactly when a saga step needs them
- **`outbox_events`** (transactional outbox; see @CLAUDE.md). No `published_at` — nothing
  stamps rows; the app INSERTs then DELETEs each row in the same transaction and Debezium
  tails the WAL:
  ```sql
  CREATE TABLE IF NOT EXISTS outbox_events (
      id             UUID PRIMARY KEY,
      aggregate_id   UUID NOT NULL,
      aggregate_type TEXT NOT NULL,
      event_type     TEXT NOT NULL,
      payload        JSONB NOT NULL,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ```
  No index needed — the table is kept empty by design. The service's Postgres also needs
  `wal_level=logical` (its `command:` in `docker-compose.yml`) and a Debezium connector in
  `debezium/` — that wiring is `/new-go-api-endpoint` / `/new-rust-api-endpoint`, not here.
- **`processed_events`** (idempotent-consumer ledger):
  ```sql
  CREATE TABLE IF NOT EXISTS processed_events (
      event_id     UUID PRIMARY KEY,
      processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ```

### 4. Index every access path
- Every **foreign-key** column gets an index (Postgres does not create one automatically;
  without it, a delete/update on the parent does a seq scan and takes a strong lock).
- Every column a **list endpoint** filters or sorts on (check the service's
  `internal/adapter/http` handlers / `usecase` list methods) gets an index matching the
  `WHERE` + `ORDER BY` shape — a composite index in the same column order as the query.
- Give indexes a stable name (`<table>_<cols>_idx`) so a later migration can drop them by name.

### 5. Non-transactional migrations (`CREATE INDEX CONCURRENTLY` etc.)
The per-file transaction wrapper makes `CONCURRENTLY` error out. For a table large enough
that a plain `CREATE INDEX`'s `ACCESS EXCLUSIVE`-ish lock is unacceptable:
- Put `-- +migrate NoTransaction` as the **first line** of the file.
- Update the service's runner to honour it: in Go's
  `internal/platform/db/migrate.go`, when the script's first line is that marker, run it with
  `pool.Exec(ctx, script)` directly (no `tx`) and record the version in a separate `Exec`
  afterwards; for `sqlx`, use its `-- no-transaction` support. Keep such a migration to a
  **single** statement so a mid-way failure is unambiguous to re-run (`CREATE INDEX
  CONCURRENTLY IF NOT EXISTS`, which leaves an `INVALID` index on failure — the re-run drops
  and recreates).
- In dev/CI against an empty table a plain `CREATE INDEX` is fine; use the marker only when
  the target table is expected to hold real volume.

### 6. Verify
Run the new file against a throwaway Postgres, in isolation and then with the whole
directory, to catch ordering and syntax errors:
```
docker run --rm -d --name mig-check -e POSTGRES_PASSWORD=x -e POSTGRES_DB=app postgres:16-alpine >/dev/null && sleep 3
cat services/<service>/migrations/*.sql | docker exec -i mig-check psql -U postgres -d app -v ON_ERROR_STOP=1 -f -
docker rm -f mig-check >/dev/null
```
For a Go service also `go build ./...` from the service dir (the `embed.FS` re-globs; a
missing file or a syntax error in `embed.go` fails the build).

### 7. Hand off
Summarize: the file, whether it's additive or an expand step (and if so, the remaining
expand/contract steps), the indexes added and why, and any runner change made for a
non-transactional migration. Then stop — `migration-reviewer` audits it and the user applies
it via the service's normal startup migrate path (or `docker compose up`).
