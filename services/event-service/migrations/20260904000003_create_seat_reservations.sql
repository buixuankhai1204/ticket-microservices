-- Per-booking record of which seats event-service is holding for the
-- seat-reservation saga (docs/sagas/seat-reservation.md §2, §9). The ReserveSeat
-- step inserts one row (status='held') alongside the seats available->reserved
-- update; FinalizeSeat moves it held->finalized (seats reserved->booked);
-- ReleaseSeat / the backstop reaper move it held->released (seats
-- reserved->available). The row is what makes the BookingConfirmed / cancelled
-- consumers act idempotently ("no held row for this booking" is a legitimate
-- no-op on the fail-before-reserve path) and what the reaper scans.
--
-- booking_id is the PRIMARY KEY: it is the saga aggregate id, one reservation
-- per booking, and every seat_reservation.events row this service publishes is
-- keyed by it (same value end to end, no translation at the Kafka boundary --
-- see the UUID entity-ID rule in CLAUDE.md).
--
-- event_id / seat_ids are bare UUIDs referencing rows this service owns
-- (events, seats); no FK, so a replayed event can't fail the projection on
-- ordering. seat_ids duplicates the FOR UPDATE lock target so the reaper and the
-- release path don't need to re-derive it.
--
-- Additive: a brand-new table, invisible to the currently running event-service
-- version. Safe in one step. Down path: DROP TABLE IF EXISTS seat_reservations.
CREATE TABLE IF NOT EXISTS seat_reservations (
    -- the booking this hold belongs to; also the saga aggregate_id
    booking_id UUID PRIMARY KEY,
    event_id   UUID   NOT NULL,
    seat_ids   UUID[] NOT NULL,
    status     TEXT   NOT NULL DEFAULT 'held'
        CHECK (status IN ('held', 'finalized', 'released')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The backstop reaper scans held rows oldest-first (seat-reservation.md §7):
--   WHERE status = 'held' AND created_at < now() - $1 ORDER BY created_at
CREATE INDEX IF NOT EXISTS seat_reservations_status_created_at_idx
    ON seat_reservations (status, created_at);
