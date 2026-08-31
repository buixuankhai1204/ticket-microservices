---
name: scalability-review
description: Read-only audit of a service (or the current diff) for scalability/stability issues - statelessness, DB pool sizing, N+1 queries, unbounded outbound calls, missing health checks. Use after implementing or changing a service, before it's considered done.
argument-hint: [path-to-service, defaults to current diff]
allowed-tools: Read, Grep, Glob, Bash(git diff:*), Bash(git status:*)
---

# Scalability Review

## Context
- Project conventions: @CLAUDE.md
- Gateway config (expected ports/routes/limits): @kong/kong.yml
- Target: `$ARGUMENTS` if given, otherwise: !`git status --short`

## Instructions

This is a review, not a fix — do not edit files. Read the target service's code and check
each item below. For every finding, report the concrete file:line, quote the problematic
code, and explain the failure scenario under load (not just "this is bad practice").

1. **Statelessness** — any package-level/`static`/global mutable state (in-memory cache, map,
   counter) that would produce wrong results or diverge once this service runs as N replicas
   behind Kong.
2. **DB connection pool** — is a pool used at all (not a raw connection per request), and is
   its max-size explicitly bounded (env-configurable), not left at a driver default that could
   exhaust Postgres connections under concurrent load?
3. **N+1 queries** — a loop that issues one DB query per iteration instead of a single
   batched/joined query; flag the loop and estimate the query count at realistic list sizes.
4. **Unbounded outbound calls** — any HTTP/gRPC client call to another service or external API
   without an explicit timeout, or without the caller's own context deadline propagated in.
5. **Missing health/readiness endpoints** — `/healthz` and `/readyz` (or equivalent) should
   exist and `/readyz` should actually verify the DB pool, not just return 200 unconditionally.
6. **Hot read paths with no caching** — endpoints that are read-heavy per `kong.yml`'s rate
   limits (e.g. `event-service` at 1000 req/min) but hit the DB on every request with no
   cache-aside layer.
7. **Blocking work on the request path** — synchronous calls to slow/external systems
   (email, analytics, third-party APIs) done inline instead of being pushed to a queue/async
   job, which would inflate p99 latency under load.
8. **CPU-bound work inside an open transaction** — in this repo the usecase opens the tx, so
   check it does all CPU-bound / network work (Argon2 password hashing ≈ 100 ms, an outbound
   gateway call, response building) **before** `pool.Begin` / `db_pool.begin()`, never between
   `Begin` and `Commit`. Holding a pooled connection + open transaction across a slow step
   pins that connection for the whole step; at `MaxConns` concurrent requests the pool
   exhausts and every further request blocks on checkout.
9. **Graceful shutdown** — verify `SIGTERM`/`SIGINT` are handled and in-flight requests are
   drained before exit, not just `os.Exit`/process kill.

## Output

A findings list ordered most-severe first. For each: file:line, one-sentence summary, the
concrete failure scenario (e.g. "under N concurrent bookings this pool exhausts at
MaxConns=5, causing 500s"), and a one-line suggested direction (not a full fix — that's a
separate task). If nothing is wrong in a category, omit it; don't pad the report with clean
bills of health.
