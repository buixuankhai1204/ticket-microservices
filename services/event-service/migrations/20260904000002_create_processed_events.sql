-- Idempotency ledger for event-service's Kafka consumers. Added because the
-- seat-reservation saga (docs/sagas/seat-reservation.md) makes event-service a
-- consumer for the first time: the ReserveSeat / FinalizeSeat / ReleaseSeat
-- steps consume booking.events and must dedupe redelivered messages. Each
-- consumer records the event_id it has applied here in the SAME transaction as
-- the side effect and skips any event whose id is already present. event_ids are
-- globally unique UUIDs minted by the publishing service, so one table serves
-- every consumer group in this service.
--
-- Additive: a brand-new table, invisible to the currently running event-service
-- version. Safe in one step. Down path: DROP TABLE IF EXISTS processed_events.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
