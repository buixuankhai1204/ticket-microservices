---
name: add-caching
description: Add a cache-aside (lazy) Redis layer to one existing read use case, behind a pure domain port, with a bounded TTL plus jitter, explicit per-call timeouts, fail-open behaviour when Redis is down, per-user keying for anything behind JWT, and invalidation wired into the matching write use case. Use for a hot read path a service can't serve from Postgres on every request under its rate limit (e.g. event-service GET /api/v1/events at 1000 req/min), or when /scalability-review flags "hot read path with no caching".
argument-hint: "<service-name> <ReadUseCaseName>  (e.g. event-service GetEvent)"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add cache-aside caching to a read use case

## Context
- Project conventions (Clean Architecture layers, the usecase owns the transaction): @CLAUDE.md
- Gateway rate limits (which read paths are hot): @kong/kong.yml
- Target service layout: !`ls -R services/$1/internal services/$1/src 2>/dev/null | head -60`
- Redis already in compose?: !`grep -n "redis" docker-compose.yml || echo "(no redis service yet)"`

## Arguments
`$ARGUMENTS` — `<service-name>` and the name of an **existing read** use case
(`GetEvent`, `ListEvents`, `GetUserProfile`). If the use case doesn't exist yet, stop and
point at `/new-go-api-endpoint` / `/new-rust-api-endpoint`.

## Design rules

- **Cache-aside, not read-through / write-through.** The use case checks the cache; on a
  miss it does its normal read and then populates the cache. No caching logic in the
  repository or the handler.
- **`Cache` is a pure `domain` port** — it names no Redis type, so it lives in `domain/`
  next to `PasswordHasher` (Go: `internal/domain/cache.go`, `package domain`; Rust:
  `src/domain/ports.rs`). Signature is bytes/string in, bytes/string out, plus `ctx`:
  `Get(ctx, key) ([]byte, bool, error)`, `Set(ctx, key, val []byte, ttl time.Duration) error`,
  `Delete(ctx, key string) error`. Serialization (JSON of the response-shaped struct) is the
  use case's job, not the port's.
- **The Redis adapter is the only place `redis` is imported** —
  `internal/adapter/cache/redis/` (Go, `github.com/redis/go-redis/v9`) /
  `src/adapter/cache/redis.rs` (Rust, `redis` + `deadpool-redis`). It's constructed in the
  composition root and injected into the use case, exactly like a repository. It holds its
  own bounded connection pool; it never touches `pgxpool`.
- **TTL is bounded and jittered.** Never cache forever. Pick a TTL from how stale the data
  may safely be (an event's details: minutes; a seat map: seconds or don't cache it — it
  changes on every booking). Add ±10–20% random jitter per write so a burst of misses
  doesn't create a synchronized expiry stampede later.
- **Every Redis call has an explicit timeout** (50–100 ms), separate from the request
  context, and **fail-open**: a Redis error or timeout on `Get` is logged at `warn` and
  treated as a miss; on `Set`/`Delete` it's logged and swallowed. Redis being down must
  degrade latency, never turn a 200 into a 500.
- **Key scheme**: `<service>:<entity>:v1:<id>` for a single entity;
  `<service>:<entity>:list:v1:<sha1 of the normalized query params>` for a list. The `v1`
  segment lets a shape change invalidate the whole namespace by bumping it.
- **Per-user keying behind JWT.** If the route has the `jwt` plugin in `kong.yml`
  (`/api/v1/users`, `/api/v1/bookings`, `/api/v1/analytics`), the authenticated user id is
  **in the key**. A shared key across users is a cache-borne IDOR — one user's response
  served to another.

## Instructions

1. **Confirm it's worth it.** The use case is a read, it's on a rate-limited-high or
   JWT-light public route, and the underlying data tolerates the staleness window. If it's a
   seat map or anything that changes on every write, say so and stop — stale seat data causes
   overselling UX; better to leave it uncached or cache for 1–2 s only.

2. **`domain` port.** Add the `Cache` interface/trait as above. No `time` import problem in
   Go (`time.Duration` is stdlib, allowed in `domain`); Rust uses `std::time::Duration`.

3. **Redis adapter.** Implement `Cache` in `internal/adapter/cache/redis/`. Constructor takes
   a URL/pool config; set `PoolSize` / pool `max_size` from an env var (`REDIS_POOL_SIZE`,
   default ~10) and dial/read/write timeouts. Map "key not found" to `(nil, false, nil)`, not
   an error.

4. **Use case — the miss path runs before `Begin`.** Reorder `<ReadUseCaseName>.Execute` /
   `execute`:
   - Build the cache key from the input (and the authed user id if applicable).
   - `Get` from the cache. On hit: deserialize to the output struct and return — **no
     transaction opened at all**. This is the win: a hit never touches the pool.
   - On miss: open the read-only transaction as today, call the repository, build the output.
   - After `Commit`, `Set` the serialized output with `ttl + jitter`. A `Set` failure does
     not fail the request.
   The use case gets the `Cache` port constructor-injected alongside its repo and pool.

5. **Invalidation in the write path.** Find the write use case(s) that mutate the same
   aggregate (`CreateEvent`, an `UpdateEvent`, or the saga step that changes seat status).
   After that use case commits, call `cache.Delete` for the affected entity key **and** any
   list-namespace key it belongs to. Document the resulting staleness window (between a
   concurrent read's `Get` miss→`Set` and the writer's `Delete`, a stale value can be
   re-cached for up to one TTL — acceptable for event details, not for balances). If strict
   consistency is needed, invalidate from the outbox consumer instead and note the added
   latency.

6. **Compose + config.** Add a `redis:7-alpine` service to `docker-compose.yml` on
   `ticket-network` with a healthcheck (`redis-cli ping`), and `REDIS_URL` +
   `REDIS_POOL_SIZE` to the target service's environment and its `platform/config`. Add
   `redis` to that service's `depends_on`.

7. **Clean Architecture check.** `domain` still imports nothing but stdlib; `usecase` imports
   `domain` + `platform/port` + `pgx`/`sqlx` (+ `time`), never the redis adapter; the redis
   adapter imports the driver + `domain`, never `usecase`/`http`. The
   `clean-architecture-check.sh` hook knows about `adapter/cache/`.

8. **Hand off.** Summarize the key scheme, TTL + jitter chosen and why, the invalidation
   points, the staleness window, and the fail-open behaviour. Note for the user that
   `integration-test-writer` should add a cache hit/miss/invalidation test and a
   "Redis down ⇒ still 200 from DB" test.
