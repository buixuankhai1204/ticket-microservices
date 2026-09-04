---
name: scalability-review
description: Read-only audit of a service (or the current diff) for scalability and stability issues — statelessness, DB pool sizing, N+1 queries, unbounded outbound calls, missing health checks, hot read paths with no caching, cache stampede risk, CPU-bound work inside a transaction, blocking work on the request path, missing RED / consumer-lag metrics, and graceful shutdown. Use after implementing or changing a service, before it's considered done.
argument-hint: "[path-to-service, defaults to current diff]"
allowed-tools: Read, Grep, Glob, Bash(git diff:*), Bash(git status:*)
---

# Scalability review

## Context
- Project conventions: @CLAUDE.md
- Gateway config (expected ports/routes/limits): @kong/kong.yml
- Target: `$ARGUMENTS` if given, otherwise: !`git status --short`

## Instructions

This is a review, not a fix — do not edit files. Read the target service's code and check
each item. For every finding, report the concrete `file:line`, quote the problematic code,
and explain the failure scenario **under load** (not just "this is bad practice").

1. **Statelessness** — any package-level / `static` / global mutable state (in-memory cache,
   map, counter) that would give wrong results or diverge once the service runs as N replicas
   behind Kong.
2. **DB connection pool** — is a pool used at all (not a connection per request), and is its
   max size explicitly bounded from an env var, not left at a driver default that could
   exhaust Postgres connections under concurrent load?
3. **N+1 queries** — a loop issuing one query per iteration instead of a batched/joined
   query; flag the loop and estimate the query count at realistic list sizes.
4. **Unbounded outbound calls** — any HTTP/gRPC client call to another service or external
   API with no explicit timeout, or without the caller's context deadline propagated in. The
   fix is `/add-resilience` (timeout + capped retry + breaker + bulkhead).
5. **Missing health/readiness** — `/healthz` and `/readyz` should both exist and `/readyz`
   should actually verify the DB pool, not return 200 unconditionally.
6. **Hot read path with no caching** — an endpoint that's read-heavy per `kong.yml`'s rate
   limits (e.g. `event-service` at 1000 req/min) hitting Postgres on every request with no
   cache-aside layer. The fix is `/add-caching`.
7. **Cache stampede / staleness** — if a cache-aside layer exists: is the TTL bounded and
   **jittered** (a fixed TTL across a burst of misses creates a synchronized expiry
   thundering-herd later)? Is there a single-flight / lock so a popular key's miss doesn't
   fan out to N identical DB reads? Is the key user-scoped for JWT routes (a shared key is a
   cache-borne IDOR)?
8. **Blocking work on the request path** — synchronous calls to slow/external systems (email,
   third-party APIs, analytics) done inline instead of via an event/async job, inflating p99
   under load. (A saga step is a `publish:` to the outbox — non-blocking by design; flag a
   sync HTTP call standing in for one.)
9. **CPU-bound work inside an open transaction** — the usecase owns the tx, so check it does
   all CPU-bound / network work (Argon2 hashing ≈ 100 ms, an outbound gateway call, response
   building) **before** `pool.Begin` / `db_pool.begin()`, never between `Begin` and `Commit`.
   Holding a pooled connection + open transaction across a slow step pins that connection; at
   `MaxConns` concurrent requests the pool exhausts and every further request blocks on
   checkout.
10. **Transaction span too wide** — `Begin`→`Commit` wrapping a call to Kafka, Redis, or an
    HTTP client. The DB connection is held for the whole thing. Only DB work belongs between
    `Begin` and `Commit`.
11. **Observability under load** — is there a Prometheus `/metrics` with RED (request rate,
    error rate, duration histogram, low-cardinality labels)? For a service that consumes
    Kafka: are messages-processed / retried / dead-lettered counters and consumer lag
    exposed (a silently lagging consumer is invisible without them)? Is a per-request
    `X-Request-Id` logged and a server span continuing the inbound `traceparent`? Missing
    these means an under-load incident can't be diagnosed. The fix is `/add-observability`.
12. **Graceful shutdown** — `SIGTERM`/`SIGINT` handled and in-flight requests drained before
    exit, not `os.Exit` / process kill. Tracer/consumer flushed on the way out.

For the messaging/saga angle (consumer wedging a partition, unbounded retry, missing DLQ),
defer to `saga-consistency-reviewer` — note it in the summary rather than duplicating it here.

## Output

A findings list, most-severe first. Per finding: `file:line`, one-sentence summary, the
concrete failure scenario ("under N concurrent bookings this pool exhausts at MaxConns=5,
causing 500s"), and a one-line suggested direction (or the `/add-*` skill that addresses it)
— not a full fix. If a category is clean, omit it; don't pad with clean bills of health.
