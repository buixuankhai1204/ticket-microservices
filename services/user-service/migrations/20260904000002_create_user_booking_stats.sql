-- Per-user booking activity read model, one row per user.
--
-- DESIGN GAP: this is a best-guess projection. There is no /design-saga artifact
-- defining which events feed it, and a booking read model arguably belongs in
-- analytics-service (which already owns booking_outcomes / EventBookingStats).
-- Treat the columns below as provisional until a "user-booking-history" saga is
-- designed. Nothing writes here yet -- a consume: step on BookingConfirmed /
-- BookingCancelled (docs/sagas/seat-reservation.md event catalog) would.
--
-- Projected from an at-least-once event stream, so: no FK to users(id) (a
-- replayed / early event must not fail the projection), counters are
-- monotonic non-negative, and the consumer is expected to upsert
-- (INSERT ... ON CONFLICT (user_id) DO UPDATE) guarded by processed_events.
--
-- Additive: brand-new table, invisible to the running user-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS user_booking_stats.
CREATE TABLE IF NOT EXISTS user_booking_stats (
    user_id            UUID PRIMARY KEY,
    -- lifetime counts of terminal booking outcomes for this user
    bookings_confirmed BIGINT      NOT NULL DEFAULT 0 CHECK (bookings_confirmed >= 0),
    bookings_cancelled BIGINT      NOT NULL DEFAULT 0 CHECK (bookings_cancelled >= 0),
    -- occurred_at of the most recent booking outcome seen for this user
    last_booking_at    TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- No secondary index yet: the only access path is by user_id (the PK). Add
-- e.g. an index on (bookings_confirmed DESC) here if a "top bookers" list
-- endpoint is introduced.
