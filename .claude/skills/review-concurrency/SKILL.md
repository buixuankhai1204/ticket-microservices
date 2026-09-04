---
name: review-concurrency
description: Read-only audit for race conditions and concurrency bugs, with special focus on seat/ticket reservation logic (double-booking, oversell). Use after implementing or changing any code that reads-then-writes shared state under concurrent requests — booking-service's reserve/confirm path above all.
argument-hint: "[path-to-service, defaults to current diff]"
allowed-tools: Read, Grep, Glob, Bash(git diff:*), Bash(git status:*)
---

# Concurrency review

## Context
- Project conventions: @CLAUDE.md
- Gateway config (rate limits hint at expected concurrency per route): @kong/kong.yml
- Target: `$ARGUMENTS` if given, otherwise: !`git status --short`

## Instructions

This is a review, not a fix — do not edit files. This repo's highest-risk concurrency surface
is the **seat/ticket reservation path** (whichever service owns it — `event-service` decrements
`available_seats`, `booking-service` drives it): two concurrent requests must never both
succeed in taking the same seat. Treat that path as the priority target regardless of what
else is in scope.

For each finding, report the concrete `file:line`, quote the code, and describe the exact
interleaving of two (or more) concurrent requests that breaks it — not just "this could race".

1. **Check-then-act without a lock or atomic write** — code that reads a seat count / status
   and later writes based on that read, without the read and write being in the same DB
   transaction with either:
   - an **atomic conditional update** — `UPDATE seats SET status='reserved' WHERE id=$1 AND
     status='available'` / `UPDATE events SET available_seats = available_seats - $1 WHERE id=$2
     AND available_seats >= $1`, then check the affected-row count (0 rows ⇒ lost the race ⇒
     return `409`), **or**
   - a **pessimistic row lock** — `SELECT ... FOR UPDATE` on the specific seat/event row, then
     the write, both on the one transaction the usecase opened.
   This is the classic oversell bug: two requests both read "1 left", both proceed. Reserved
   seating is a high-contention, non-fungible resource — pessimistic locking or an atomic
   conditional update is the correct tool here, not an after-the-fact version check.
2. **The transaction doesn't actually span the read and the write** — because the usecase
   opens one transaction and threads it through every repo call, a `SELECT ... FOR UPDATE`
   holds its lock until that transaction commits. Flag: a repo method that opens its **own**
   transaction (the lock releases between the `SELECT` and the `UPDATE`); a usecase that calls
   `pool.Begin` more than once in a flow (the second tx can't see the first's uncommitted
   rows, and they can deadlock); a `SELECT ... FOR UPDATE` in a read-only transaction.
3. **Missing idempotency on a mutating endpoint** — a booking/payment request that isn't safe
   to retry (client timeout + retry, duplicate from a flaky network) without an idempotency
   key, risking a double booking / double charge for one logical request. For the Kafka
   *consume* side this is the `processed_events` check — its absence is a `saga-consistency-
   reviewer` finding; here, focus on the synchronous HTTP entry point.
4. **Optimistic locking with no conflict path** — if a version column / CAS is used, verify a
   conflict returns a clear `409` / "retry", not a silent overwrite or a silent success.
5. **Distributed lock correctness** (if Redis/etcd locks appear) — a TTL so a crashed holder
   doesn't deadlock everyone; the critical section provably shorter than the TTL under load;
   fencing so a resumed-after-pause holder can't still write.
6. **In-memory state shared across goroutines/tasks without synchronization** — a
   map/slice/counter mutated from multiple request handlers with no mutex/channel; also wrong
   once the service has >1 replica (that multi-replica angle is `/scalability-review`; this
   check is within-process races).
7. **Transaction scope wrong** — held open across a slow external call (DB locks held longer
   than needed, throughput collapse), or conversely split across statements that should be
   one atomic unit but aren't (e.g. the state write and its `outbox_events` row in separate
   transactions — they must be one, see @CLAUDE.md).
8. **`pgx.Tx` / `&mut PgConnection` shared across goroutines/tasks** — the handle is not safe
   for concurrent use; N concurrent callers each need their own `Begin`.

## Output

A findings list, most-severe first (oversell / double-booking always first). Per finding:
`file:line`, one-sentence summary, the concrete two-request interleaving that fails, and a
one-line suggested direction (e.g. "atomic `UPDATE ... WHERE available_seats >= :n` and check
`RowsAffected`, return 409 on 0"). Note that `integration-test-writer`'s concurrent-oversell
test (fire N requests at M<N seats, assert exactly M succeed) is the regression guard for
any fix. If nothing is wrong, say so briefly rather than padding the report.
