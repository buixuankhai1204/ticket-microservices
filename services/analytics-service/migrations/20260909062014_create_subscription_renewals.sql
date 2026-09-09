-- Read model: one append-only row per successful subscription renewal, projected
-- from the `SubscriptionRenewed` event user-service publishes on
-- `subscription.events` (docs/sagas/renewal-subscriptions.md section 3, 5a).
-- analytics-service never originates these -- it records what user-service
-- already decided -- and feeds the renewed-MRR / renewal-success-rate reports.
--
-- Written by the Kafka consumer (group analytics-service-SubscriptionRenewed) in
-- the same transaction as the processed_events idempotency marker. event_id is
-- the PK so a redelivered event that slips past processed_events is still a
-- no-op (INSERT ... ON CONFLICT (event_id) DO NOTHING). No FK to any local table
-- -- this is a projection off an at-least-once stream. plan_id is the opaque
-- identifier carried in the event, not a local key.
--
-- Additive: brand-new table, invisible to the running analytics-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS subscription_renewals.
CREATE TABLE IF NOT EXISTS subscription_renewals (
    event_id        UUID   PRIMARY KEY,
    subscription_id UUID   NOT NULL,
    user_id         UUID   NOT NULL,
    plan_id         TEXT   NOT NULL,
    amount_minor    BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency        TEXT   NOT NULL,
    -- the subscription's new current_period_end after this renewal
    new_period_end  DATE   NOT NULL,
    -- provider Charge calls this renewal took (1 on a clean first attempt)
    attempt_count   INT    NOT NULL CHECK (attempt_count >= 0),
    renewed_at      TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Renewed MRR / renewal volume over time buckets renewed_at.
CREATE INDEX IF NOT EXISTS idx_subscription_renewals_renewed_at
    ON subscription_renewals (renewed_at DESC);
