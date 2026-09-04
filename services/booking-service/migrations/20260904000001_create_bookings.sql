-- A client's request to hold specific seats for an event. Created in `pending`
-- by POST /api/v1/bookings; the seat-reservation choreography saga
-- (docs/sagas/seat-reservation.md) drives it to `confirmed` or `cancelled`.
--
-- IDs are app-generated UUIDs (CLAUDE.md: never a sequential int) so a booking
-- has an identity before its first insert and equals the saga `aggregate_id`
-- type end to end. `user_id` and `event_id` are bare cross-service UUIDs with
-- no FK -- the owning services (user-service, event-service) are separate
-- databases. Money/price never lives here; that stays in event-service's seats.
--
-- Additive: a brand-new table. Down path: DROP TABLE IF EXISTS bookings;
CREATE TABLE IF NOT EXISTS bookings (
    id             UUID PRIMARY KEY,
    user_id        UUID        NOT NULL,
    event_id       UUID        NOT NULL,
    seat_ids       UUID[]      NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'cancelled')),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The stuck-saga reaper scans pending rows oldest-first (seat-reservation.md §7).
CREATE INDEX IF NOT EXISTS bookings_status_created_at_idx ON bookings (status, created_at);

-- The owner-scoped list endpoint (GET /api/v1/bookings) filters by user_id.
CREATE INDEX IF NOT EXISTS bookings_user_id_idx ON bookings (user_id);
