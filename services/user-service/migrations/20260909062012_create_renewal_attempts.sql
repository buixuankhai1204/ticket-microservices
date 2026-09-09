-- One row per (subscription, billing period) renewal attempt -- the idempotency
-- anchor and retry ledger for the renewal-subscriptions flow
-- (docs/sagas/renewal-subscriptions.md sections 2, 7, 8).
--
-- UNIQUE (subscription_id, period_end) is the core guarantee: Job A's
-- INSERT ... SELECT ... ON CONFLICT DO NOTHING can never create a second attempt
-- for a period, so re-running the whole job is a no-op. `idempotency_key`
-- (deterministically 'renew:{subscription_id}:{period_end}') is also the key
-- passed to the payment provider's Idempotency-Key header, so a retried Charge
-- call after a crash returns the original charge instead of charging again --
-- you retry the call, never the charge.
--
-- Status lifecycle (Job B, one row):
--   pending / failed_retryable / failed_permanent  --(TX1)-->  charging
--   charging --(provider ok)-->                    succeeded
--   charging --(provider 5xx/timeout, cap not hit)--> failed_retryable
--   charging --(provider 5xx/timeout, cap hit)-->  given_up        (+ subscription past_due)
--   charging --(card declined, dunning slots left)--> failed_permanent (+ SubscriptionPaymentFailed)
--   charging --(card declined, dunning exhausted)--> failed_permanent (+ subscription canceled + SubscriptionCanceled)
-- `charging` is committed BEFORE the provider call so a mid-charge crash is
-- visible to the reaper.
--
-- Additive: brand-new table, invisible to the running user-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS renewal_attempts.
CREATE TABLE IF NOT EXISTS renewal_attempts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id      UUID   NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    -- the subscription's current_period_end at the time the attempt was enqueued
    period_end           DATE   NOT NULL,
    -- deterministic: 'renew:' || subscription_id || ':' || period_end
    idempotency_key      TEXT   NOT NULL,
    status               TEXT   NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'charging', 'succeeded',
                          'failed_retryable', 'failed_permanent', 'given_up')),
    -- total provider Charge calls made for this attempt (transient retries)
    attempt_count        INT    NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    -- position walked through DUNNING_SCHEDULE once the card permanently declines
    dunning_attempt_count INT   NOT NULL DEFAULT 0 CHECK (dunning_attempt_count >= 0),
    -- Job B claims rows whose next_attempt_at has arrived
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- provider charge id, set on success (also used by daily reconciliation)
    provider_charge_id   TEXT,
    last_error           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- the job can never create a second attempt for the same period
    CONSTRAINT renewal_attempts_unique_period UNIQUE (subscription_id, period_end),
    CONSTRAINT renewal_attempts_unique_idempotency_key UNIQUE (idempotency_key)
);

-- Job B claim: WHERE status IN ('pending','failed_retryable','failed_permanent')
-- AND next_attempt_at <= now() ORDER BY next_attempt_at ... FOR UPDATE SKIP LOCKED.
CREATE INDEX IF NOT EXISTS renewal_attempts_due_idx
    ON renewal_attempts (status, next_attempt_at);

-- Reaper: WHERE status = 'charging' AND updated_at < now() - <timeout>. Partial
-- index -- only the tiny set of in-flight rows.
CREATE INDEX IF NOT EXISTS renewal_attempts_charging_idx
    ON renewal_attempts (status, updated_at)
    WHERE status = 'charging';

-- No standalone index on subscription_id: renewal_attempts_unique_period already
-- provides a btree leading with subscription_id, which serves the FK.
