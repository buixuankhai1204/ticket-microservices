---
name: review-concurrency
description: Read-only audit for race conditions and concurrency bugs, with special focus on seat/ticket reservation logic in booking-service (double-booking, oversell). Use after implementing or changing any code that reads-then-writes shared state under concurrent requests.
argument-hint: [path-to-service, defaults to current diff]
allowed-tools: Read, Grep, Glob, Bash(git diff:*), Bash(git status:*)
---

# Concurrency Review

## Context
- Project conventions: @CLAUDE.md
- Gateway config (rate limits hint at expected concurrency per route): @kong/kong.yml
- Target: `$ARGUMENTS` if given, otherwise: !`git status --short`

## Instructions

This is a review, not a fix — do not edit files. This repo's highest-risk concurrency surface
is `booking-service`: two concurrent requests must never both succeed in reserving the same
seat/ticket. Treat that path as the priority target regardless of what else is in scope.

For each finding, report the concrete file:line, quote the code, and describe the exact
interleaving of two (or more) concurrent requests that breaks it — not just "this could race."

1. **Check-then-act without a lock/transaction** — code that reads available seat count (or
   any shared counter/row) and later writes based on that read, without the read and write
   being inside the same DB transaction with row-level locking (`SELECT ... FOR UPDATE`) or an
   atomic `UPDATE ... WHERE available > 0` pattern. This is the classic oversell bug: two
   requests both read "1 seat left," both proceed to book.
2. **Missing idempotency** — a booking/payment endpoint that isn't safe to retry (client
   timeout + retry, or a duplicate request from a flaky network) without an idempotency key,
   risking a double charge or double booking for the same logical request.
3. **Optimistic locking without a retry/conflict path** — if a version column /
   compare-and-swap pattern is used, verify a conflict actually returns a clear
   "try again"/409 rather than silently overwriting or silently succeeding.
4. **Distributed lock correctness** (if Redis/etcd locks are used) — check the lock has a TTL
   (so a crashed holder doesn't deadlock everyone else) and that the critical section can't run
   longer than the TTL under realistic load.
5. **In-memory state shared across goroutines/tasks without synchronization** — a map/slice/
   counter mutated from multiple request handlers without a mutex or channel, which also
   won't work once this service has more than one replica (see `/scalability-review` for the
   multi-replica angle — this check is about within-process races).
6. **Transaction scope too broad or too narrow** — a transaction held open across a slow
   external call (holding DB locks longer than necessary, hurting throughput), or conversely
   split across multiple statements that should be atomic but aren't.

## Output

A findings list ordered most-severe first (oversell/double-booking bugs always first). For
each: file:line, one-sentence summary, the concrete two-request interleaving that fails, and a
one-line suggested direction (e.g. "wrap in a transaction with `SELECT ... FOR UPDATE` on the
seat row"). If nothing is wrong, say so briefly rather than padding the report.
