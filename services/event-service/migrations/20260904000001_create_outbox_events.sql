-- Transactional outbox for event-service. Added because the seat-reservation
-- saga (docs/sagas/seat-reservation.md) makes event-service a publisher for the
-- first time: the ReserveSeat step writes a SeatReserved / SeatReservationFailed
-- row here in the SAME transaction as the seats + seat_reservations state
-- change, then DELETEs it again in that transaction. The INSERT is what the
-- Debezium `event-service-outbox` connector captures from the WAL (routing
-- aggregate_type='seat_reservation' -> the seat_reservation.events topic); the
-- table itself stays permanently empty.
--
-- Canonical shape from CLAUDE.md: no `published_at` (nothing stamps rows). A
-- nullable `tracecontext TEXT` column is added later by /add-observability so
-- the W3C trace context survives the outbox -> Kafka -> consumer hop.
--
-- Additive: a brand-new table, invisible to the currently running event-service
-- version. Safe in one step. Down path: DROP TABLE IF EXISTS outbox_events.
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID  PRIMARY KEY,
    aggregate_id   UUID  NOT NULL,
    aggregate_type TEXT  NOT NULL,
    event_type     TEXT  NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
