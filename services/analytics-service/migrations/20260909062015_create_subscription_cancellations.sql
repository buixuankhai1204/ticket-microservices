-- Read model: one append-only row per involuntary subscription cancellation,
-- projected from the `SubscriptionCanceled` event user-service publishes on
-- `subscription.events` when dunning is exhausted
-- (docs/sagas/renewal-subscriptions.md section 3, 5b). Feeds the
-- involuntary-churn rate report.
--
-- Written by the Kafka consumer (group analytics-service-SubscriptionCanceled)
-- in the same transaction as the processed_events marker. event_id is the PK so
-- a redelivered event is a no-op (INSERT ... ON CONFLICT (event_id) DO NOTHING).
-- `reason` is left unconstrained TEXT: it is whatever the publisher sends
-- (`dunning_exhausted` today) and a projection should not reject an unknown
-- future value. No FK -- projection off an at-least-once stream.
--
-- Additive: brand-new table, invisible to the running analytics-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS subscription_cancellations.
CREATE TABLE IF NOT EXISTS subscription_cancellations (
    event_id        UUID PRIMARY KEY,
    subscription_id UUID NOT NULL,
    user_id         UUID NOT NULL,
    plan_id         TEXT NOT NULL,
    -- why the subscription ended (`dunning_exhausted`)
    reason          TEXT NOT NULL,
    -- dunning emails/attempts made before giving up
    dunning_attempts INT NOT NULL CHECK (dunning_attempts >= 0),
    canceled_at     TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Involuntary-churn over time buckets canceled_at.
CREATE INDEX IF NOT EXISTS idx_subscription_cancellations_canceled_at
    ON subscription_cancellations (canceled_at DESC);
