-- Move the outbox publish side from an in-process polling relay to log-tailing
-- CDC. A Debezium PostgreSQL connector (Kafka Connect) now tails this table's
-- WAL via a logical replication slot and publishes each INSERT through the
-- Outbox Event Router SMT; there is no relay process any more.
--
-- Consequences for the schema:
--   * `published_at` (and its partial index) are gone — nothing stamps rows now.
--   * `aggregate_type` is added and set by the app on every insert. The SMT
--     routes on it: topic = `<aggregate_type>.events` (so UserCreated -> the
--     `user.events` topic). Same value end to end as the saga `aggregate_id`
--     type story: a per-aggregate discriminator, not a per-event one.
--
-- The repository writes each event row and DELETEs it again inside the SAME
-- transaction as the users insert (see adapter/repository/postgres.rs). The
-- INSERT still lands in the WAL, so Debezium captures and publishes it; the
-- table itself stays permanently empty. The connector is configured with
-- `skipped.operations=u,d,t`, so the paired DELETE is ignored.

ALTER TABLE outbox_events ADD COLUMN aggregate_type TEXT NOT NULL DEFAULT 'user';
ALTER TABLE outbox_events ALTER COLUMN aggregate_type DROP DEFAULT;

DROP INDEX IF EXISTS idx_outbox_events_unpublished;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS published_at;
