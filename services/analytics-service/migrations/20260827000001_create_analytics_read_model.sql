-- Read model for analytics-service. Rows are written by Kafka consumers wired
-- later with /add-go-saga-step (analytics-service consumes final saga outcomes
-- only); the HTTP surface stays read-only. IDs are UUID, matching saga event
-- aggregate_ids end to end.
CREATE TABLE IF NOT EXISTS booking_outcomes (
    id          UUID PRIMARY KEY,
    booking_id  UUID NOT NULL UNIQUE,
    event_id    UUID NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_booking_outcomes_event_id
    ON booking_outcomes (event_id);
