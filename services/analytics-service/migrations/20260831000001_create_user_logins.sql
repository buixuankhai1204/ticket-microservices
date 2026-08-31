-- Read model: one append-only row per successful login, projected from the
-- `UserLoggedIn` event user-service publishes on `user.events` (same aggregate
-- type `user` as `UserCreated`, so it shares the topic). analytics-service never
-- originates these — it only records what user-service already decided.
-- Rows are written by the Kafka consumer in
-- internal/adapter/messaging/kafka/consumer_user_loggedin.go, in the same
-- transaction as the processed_events idempotency marker. event_id is the PK so a
-- redelivered event that slips past processed_events is still a no-op.
CREATE TABLE IF NOT EXISTS user_logins (
    event_id     UUID PRIMARY KEY,
    user_id      UUID NOT NULL,
    email        TEXT NOT NULL,
    logged_in_at TIMESTAMPTZ NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_logins_user_id_logged_in_at
    ON user_logins (user_id, logged_in_at DESC);
