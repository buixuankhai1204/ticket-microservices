-- Idempotency ledger for a (future) user-service Kafka consumer.
--
-- DESIGN GAP: user-service today only PUBLISHES (UserCreated / UserLoggedIn via
-- the outbox). This table anticipates it also CONSUMING booking events to build
-- the user_booking_stats read model (next migration). That makes user-service a
-- saga participant, which per CLAUDE.md must be designed with /design-saga
-- first -- no such design exists yet. This migration only creates the table;
-- nothing writes to it until a consume: step is wired, and the owning-service
-- decision (user-service vs analytics-service, which already owns
-- booking_outcomes) is still open.
--
-- Canonical shape from CLAUDE.md: a redelivered event whose id is already
-- present is skipped; the insert happens in the SAME transaction as the side
-- effect. event_ids are globally unique UUIDs minted by the publishing service.
--
-- Additive: brand-new table, invisible to the running user-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS processed_events.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
