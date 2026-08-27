-- Transactional outbox for user-service. A domain event is written here in the
-- SAME transaction as the state change it describes (see the users insert in
-- adapter/repository/postgres.rs); a background relay
-- (adapter/messaging/kafka/outbox_relay.rs) publishes unpublished rows to Kafka
-- and stamps published_at. This is what makes "commit the state change" and
-- "announce it" atomic without two-phase commit against Kafka.
CREATE TABLE IF NOT EXISTS outbox_events (
    id           UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- The relay only ever scans for unpublished rows, oldest first.
CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
