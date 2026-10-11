-- Additive: a brand-new table, invisible to the running version. Safe in one step.
-- Down path: DROP TABLE IF EXISTS product_order_outcomes.
CREATE TABLE IF NOT EXISTS product_order_outcomes (
    order_id          UUID PRIMARY KEY,
    booking_id        UUID NOT NULL,
    user_id           UUID NOT NULL,
    ticketed_event_id UUID NOT NULL,
    currency          TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    total_minor       BIGINT NOT NULL CHECK (total_minor >= 0),
    item_count        INTEGER NOT NULL CHECK (item_count >= 0),
    status            TEXT NOT NULL CHECK (status IN ('placed', 'cancelled')),
    placed_at         TIMESTAMPTZ,
    cancelled_at      TIMESTAMPTZ,
    cancel_reason     TEXT,
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_product_order_outcomes_ticketed_event_id
    ON product_order_outcomes (ticketed_event_id);
