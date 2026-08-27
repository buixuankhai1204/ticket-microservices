-- Read model: one row per registered user, projected from the `UserCreated`
-- event that user-service publishes on the `user.created` topic. analytics-service
-- never originates these — it only records what user-service already decided.
-- Rows are written by the Kafka consumer in
-- internal/adapter/messaging/kafka/consumer.go, in the same transaction as the
-- processed_events idempotency marker.
CREATE TABLE IF NOT EXISTS user_registrations (
    user_id       UUID PRIMARY KEY,
    email         TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
