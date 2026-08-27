-- One bookable position within an event. Status follows the seat lifecycle
-- available -> reserved -> booked (and back to available on a saga
-- compensation). event-service's HTTP surface only reads these; the
-- seat-reservation saga step added later (when booking-service publishes
-- BookingRequested) owns the writes.
CREATE TABLE IF NOT EXISTS seats (
    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID   NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    section  TEXT   NOT NULL DEFAULT '',
    row      TEXT   NOT NULL DEFAULT '',
    number   TEXT   NOT NULL,
    status   TEXT   NOT NULL DEFAULT 'available'
        CHECK (status IN ('available', 'reserved', 'booked')),
    price_minor BIGINT NOT NULL DEFAULT 0 CHECK (price_minor >= 0),
    CONSTRAINT seats_unique_position UNIQUE (event_id, section, row, number)
);

-- Seat maps are always fetched per event, ordered by physical position.
CREATE INDEX IF NOT EXISTS seats_event_position_idx
    ON seats (event_id, section, row, number);
