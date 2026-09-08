-- GET /api/v1/bookings lists a user's bookings as
--   WHERE user_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3
-- The original bookings_user_id_idx (user_id) satisfies only the filter, so
-- every page did a separate top-N sort. This composite serves the filter and
-- the ordering from one index scan; bookings_user_id_idx is then strictly
-- redundant (its leading column is covered here) and is dropped.
--
-- Additive plus a redundant-index drop, safe in one step: the new index is
-- created before the old one is dropped, so there is never a window without a
-- user_id index, and DROP INDEX takes only a brief lock (no table rewrite). The
-- bookings table is empty in every environment (booking-service has not
-- deployed), so the plain (non-CONCURRENTLY) build under the txn-wrapped runner
-- is instant.
--
-- Down path:
--   CREATE INDEX IF NOT EXISTS bookings_user_id_idx ON bookings (user_id);
--   DROP INDEX IF EXISTS bookings_user_id_created_at_id_idx;
CREATE INDEX IF NOT EXISTS bookings_user_id_created_at_id_idx
    ON bookings (user_id, created_at DESC, id DESC);

DROP INDEX IF EXISTS bookings_user_id_idx;
