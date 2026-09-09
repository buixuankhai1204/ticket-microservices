-- Dunning-email ledger and dedupe backstop for the renewal-subscriptions flow
-- (docs/sagas/renewal-subscriptions.md section 2 step 4, section 8).
--
-- The dunning email is NOT a Kafka event -- Job B calls EmailGateway.Send
-- directly, best-effort, after the dunning TX2 commits. UNIQUE (event_id) keys
-- the send to the SubscriptionPaymentFailed outbox event id so the same dunning
-- attempt is emailed at most once even if Job B re-runs. A row is inserted only
-- on a successful send, so a failed send is retried by the next dunning pass
-- rather than suppressed.
--
-- Additive: brand-new table, invisible to the running user-service version.
-- Safe in one step. Down path: DROP TABLE IF EXISTS sent_emails.
CREATE TABLE IF NOT EXISTS sent_emails (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- the SubscriptionPaymentFailed event this email corresponds to
    event_id            UUID NOT NULL UNIQUE,
    user_id             UUID NOT NULL,
    -- which template was sent (e.g. 'dunning_card_declined')
    template            TEXT NOT NULL,
    -- id returned by the email provider on a successful send
    provider_message_id TEXT,
    sent_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- No secondary index: the only access path is the UNIQUE (event_id) lookup done
-- before each send.
