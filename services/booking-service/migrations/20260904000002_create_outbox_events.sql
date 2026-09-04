-- Transactional outbox for booking-service. A domain event is written here in
-- the SAME transaction as the state change it describes (the bookings insert /
-- status update), then DELETEd again in that same transaction -- the INSERT is
-- what Debezium captures from the WAL, so the table itself stays permanently
-- empty.
--
-- Canonical shape from CLAUDE.md: no `published_at` (nothing stamps rows). A
-- nullable `tracecontext TEXT` column is added later by /add-observability so
-- the W3C trace context survives the outbox -> Kafka -> consumer hop.
--
-- The Debezium `booking-service-outbox` connector (added when the first
-- `publish:` saga step is wired -- see docs/sagas/seat-reservation.md §9) routes
-- each insert to `<aggregate_type>.events` via the Outbox Event Router SMT:
-- aggregate_id -> message key, aggregate_type -> topic, event_type/id -> headers,
-- payload -> message value.
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID PRIMARY KEY,
    aggregate_id   UUID  NOT NULL,
    aggregate_type TEXT  NOT NULL,
    event_type     TEXT  NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
