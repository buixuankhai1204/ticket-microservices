-- Idempotency ledger for Kafka consumers. Kafka delivers at-least-once, so every
-- consumer records the event_id it has applied here, in the SAME transaction as
-- the side effect (the user_registrations write). A redelivered event whose id is
-- already present is skipped. event_ids are globally unique UUIDs minted by the
-- publishing service, so one table serves every consumer in this service.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
