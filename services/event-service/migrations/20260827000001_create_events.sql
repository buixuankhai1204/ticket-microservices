-- Events people can browse and book seats for. Large or small: the size of an
-- event is just how many `seats` rows point at it, there is no separate type.
-- IDs are app-generated UUIDs (CLAUDE.md: never a sequential int) so a new
-- event has an identity before its first insert and matches a saga event's
-- aggregate_id type end to end.
CREATE TABLE IF NOT EXISTS events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    venue       TEXT        NOT NULL,
    starts_at   TIMESTAMPTZ NOT NULL,
    ends_at     TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_time_order CHECK (ends_at > starts_at)
);

-- The event list is ordered newest first and can be filtered to upcoming only.
CREATE INDEX IF NOT EXISTS events_created_at_desc_idx ON events (created_at DESC);
CREATE INDEX IF NOT EXISTS events_ends_at_idx ON events (ends_at);
