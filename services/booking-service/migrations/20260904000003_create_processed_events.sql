-- Idempotency ledger for booking-service's Kafka consumers (SeatReserved and
-- SeatReservationFailed -- wired later as `consume:` saga steps, see
-- docs/sagas/seat-reservation.md §8). Kafka delivers at-least-once, so each
-- consumer records the event_id it has applied here in the SAME transaction as
-- the side effect and skips any event whose id is already present. event_ids are
-- globally unique UUIDs minted by the publishing service, so one table serves
-- every consumer group in this service.
--
-- Additive: a brand-new table. Down path: DROP TABLE IF EXISTS processed_events;
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
