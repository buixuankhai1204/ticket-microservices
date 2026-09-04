---
name: add-resilience
description: Wrap one outbound dependency (a domain gateway port like PaymentGateway, an email/SMS sender, the Redis cache adapter) in timeout + capped retry-with-jittered-backoff + circuit breaker + a concurrency bulkhead, with an explicit documented fallback (fail-open vs fail-closed). Use when a service gains its first synchronous call to an external system or another team's API, or when /scalability-review flags an unbounded outbound call.
argument-hint: "<service-name> <PortName>  (e.g. booking-service PaymentGateway)"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add resilience to an outbound dependency

## Context
- Project conventions (Clean Architecture, no synchronous cross-service calls for saga steps): @CLAUDE.md
- Target service layout: !`ls -R services/$1/internal services/$1/src 2>/dev/null | head -60`
- Existing outbound ports in this service: !`grep -rn "Gateway\|Sender\|Client\|Notifier" services/$1 --include=*.go --include=*.rs 2>/dev/null | grep -i "interface\|trait\|type " | head -20`

## Arguments
`$ARGUMENTS` — `<service-name>` and the `<PortName>` of an **outbound `domain` port** (one
that names an external system, not the `Repository`). If the port doesn't exist yet, define
it first (it's a `domain` port trait/interface named after the capability, e.g.
`PaymentGateway`, implemented in `internal/adapter/<name>/`).

## Scope — what this is and isn't

- **Is**: a decorator around an *adapter implementation* of an outbound `domain` port — an
  HTTP/gRPC call to a payment provider, an email/SMS API, a third-party pricing service, the
  Redis cache adapter.
- **Isn't**: the **Kafka consumer** retry/DLQ path. That's already specified as
  classify-then-commit/retry/dead-letter in `/new-go-api-endpoint` / `/new-rust-api-endpoint`
  and audited by `saga-consistency-reviewer` — do not add a circuit breaker there.
- **Isn't**: a reason to add a synchronous cross-service call. If a saga step needs another
  service to act, that's a `publish:` through the outbox (see `/design-saga`), never an HTTP
  call from a use case.

## The four layers (apply all; skip one only with a stated reason)

1. **Timeout + deadline propagation.** Every call has an explicit per-attempt timeout
   (Go: `context.WithTimeout` derived from the caller's `ctx`; Rust: `tokio::time::timeout`).
   The caller's context deadline is always propagated in — never start a fresh unbounded
   context. Pick the per-attempt timeout from the dependency's real p99, not a round number.
2. **Retry — only for safe operations.** Retry a `GET`/idempotent call or one carrying an
   idempotency key. **Never blind-retry a non-idempotent `POST`** (a charge, a send) without
   an idempotency key the downstream honours — a client timeout + your retry = double charge.
   Cap attempts (2–3 total), exponential backoff **with full jitter**
   (`sleep = random(0, base * 2^attempt)`, capped), and only retry transient classes
   (timeout, connection refused, 429, 5xx) — a 4xx is permanent, return it immediately.
3. **Circuit breaker.** Wrap the adapter impl (Go: `github.com/sony/gobreaker`; Rust: a
   small hand-rolled `Closed`/`Open`/`HalfOpen` state machine or `failsafe`). Trip on a
   consecutive-failure or failure-ratio threshold; while **Open**, fail fast without calling
   the dependency; after a cooldown allow one **HalfOpen** probe and close on success. This
   stops a dead dependency from consuming every worker and inflating p99 for unrelated
   traffic. The breaker state is per-process (fine — services are stateless and replicated).
4. **Bulkhead.** A semaphore bounding concurrent in-flight calls to this dependency
   (Go: buffered channel / `golang.org/x/sync/semaphore`; Rust: `tokio::sync::Semaphore`),
   sized well below the DB pool so one slow dependency can't pin every request. Over the
   limit: fail fast with the same fallback as an open breaker.

## Fallback — decide and write it down

State the dependency's failure policy in a comment on the decorator and in the hand-off:

- **fail-open** (degrade, keep serving) — cache, recommendations, non-critical enrichment.
  Return an empty/default result and log.
- **fail-closed** (reject the request) — payment, fraud check, anything money- or
  safety-relevant. Return a typed domain error the handler maps to `503`/`502` (never a
  silent success).

## Instructions

1. **Locate or define the port.** Confirm `<PortName>` is a `domain` port (interface/trait,
   no infra types) with a concrete adapter under `internal/adapter/` / `src/adapter/`.

2. **Add the decorator** next to the adapter impl — a type that implements the **same
   `domain` port** and holds the real impl plus the breaker, a retry policy struct, and the
   semaphore. Order per call: acquire semaphore → breaker `Execute(` → per-attempt timeout →
   retry loop. Release/record in `defer` / on drop.

3. **Config, not constants.** Timeout, max attempts, backoff base/cap, breaker threshold +
   cooldown, and bulkhead size all come from env via the service's `platform/config`
   (`<DEP>_TIMEOUT_MS`, `<DEP>_MAX_ATTEMPTS`, `<DEP>_BREAKER_*`, `<DEP>_MAX_INFLIGHT`), with
   sane defaults. Document each in `.env.example`.

4. **Wire at the composition root only.** `cmd/main.go` / `main.rs` constructs
   `real := NewXAdapter(...)` then `resilient := NewResilientX(real, cfg)` and injects
   `resilient` where the port is expected. `usecase` and `domain` are unchanged and never
   mention the breaker.

5. **Observability.** The decorator emits: attempt count, final outcome, breaker state
   transitions (log at `warn` on open), and bulkhead rejections — using the service's logger
   with the request id. If `/add-observability` has run, add a span per attempt and a
   counter per outcome.

6. **Clean Architecture check.** The decorator is in `adapter/`, implements a `domain` port,
   imports the breaker lib + `domain` — never `usecase`. `domain` gains no new imports.

7. **Hand off.** Summarize: the four layers' chosen values and why, the fail-open/closed
   decision, the env vars added. Note that `integration-test-writer` should cover: retry stops
   at the cap; a non-idempotent call is not retried; the breaker opens after N failures and
   fails fast; the bulkhead rejects over the limit; the documented fallback fires.
