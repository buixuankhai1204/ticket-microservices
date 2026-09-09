-- Recurring subscription owned by a user-service user. This is the write model
-- for the renewal-subscriptions flow (docs/sagas/renewal-subscriptions.md):
-- Job A sweeps `active` rows whose current_period_end has arrived and enqueues a
-- renewal_attempts row; a successful charge advances current_period_end one
-- billing interval and keeps status `active`; a permanent decline moves it to
-- `past_due` then, if dunning is exhausted, `canceled`. user-service owns every
-- write here -- there is no second writer, so no cross-service compensation.
--
-- Conventions: UUID id (guessable sequential ids make IDOR by enumeration
-- easier), money as integer minor units, status/interval as TEXT + CHECK rather
-- than a Postgres ENUM (a new value must not need a non-transactional DDL).
-- `billing_interval` rather than `interval` because the latter is a Postgres
-- type keyword and awkward as an unquoted column name.
--
-- Additive: brand-new table, invisible to the running user-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS subscriptions.
CREATE TABLE IF NOT EXISTS subscriptions (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- owning user; a billing record cannot outlive its user
    user_id            UUID   NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    -- opaque plan identifier (no separate plans table in this revision)
    plan_id            TEXT   NOT NULL,
    status             TEXT   NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'past_due', 'canceled', 'paused')),
    -- renewal becomes due at/after this date; advanced one interval per success
    current_period_end DATE   NOT NULL,
    billing_interval   TEXT   NOT NULL
        CHECK (billing_interval IN ('month', 'year')),
    price_minor        BIGINT NOT NULL CHECK (price_minor >= 0),
    currency           TEXT   NOT NULL CHECK (char_length(currency) = 3),
    -- payment provider token (not card data)
    payment_method_id  TEXT   NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Job A sweep: WHERE status = 'active' AND current_period_end <= CURRENT_DATE.
CREATE INDEX IF NOT EXISTS subscriptions_due_idx
    ON subscriptions (status, current_period_end);

-- Owner-scoped list endpoint: WHERE user_id = $sub ORDER BY created_at DESC,
-- id DESC. Leading user_id column also serves as the FK index.
CREATE INDEX IF NOT EXISTS subscriptions_user_id_created_at_id_idx
    ON subscriptions (user_id, created_at DESC, id DESC);
