use async_trait::async_trait;
use chrono::{Duration, NaiveDate};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{
    DomainEvent, Pagination, RenewalAttempt, Subscription, SubscriptionStatus, User, UserError,
};

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<User, UserError>;
    async fn find_by_email(
        &self,
        conn: &mut PgConnection,
        email: &str,
    ) -> Result<Option<User>, UserError>;
    async fn create(&self, conn: &mut PgConnection, user: &User) -> Result<(), UserError>;
    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), UserError>;
    async fn list(
        &self,
        conn: &mut PgConnection,
        pagination: Pagination,
    ) -> Result<(Vec<User>, i64), UserError>;
}

#[async_trait]
pub trait SubscriptionRepository: Send + Sync {
    async fn create(
        &self,
        conn: &mut PgConnection,
        subscription: &Subscription,
    ) -> Result<(), UserError>;

    /// Owner-scoped fetch: a row whose `user_id` is not `user_id` is reported as
    /// `UserError::NotFound`, indistinguishable from a missing row (no IDOR
    /// existence leak).
    async fn find_by_id_for_user(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Subscription, UserError>;

    /// Owner-scoped page plus the full match count for that owner, both read on
    /// the one connection the use case passes in.
    async fn list_for_user(
        &self,
        conn: &mut PgConnection,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Subscription>, i64), UserError>;

    /// Unscoped fetch by id — for the system-context renewal Job B, which acts
    /// on behalf of no user.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Subscription, UserError>;

    /// Renewal succeeded: advance `current_period_end` and set status `active`.
    async fn renew_period(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        new_period_end: NaiveDate,
    ) -> Result<(), UserError>;

    async fn set_status(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        status: SubscriptionStatus,
    ) -> Result<(), UserError>;

    /// Write a saga event into `outbox_events` (INSERT only, matching the
    /// existing user-service behaviour). Debezium tails the WAL insert.
    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), UserError>;
}

#[async_trait]
pub trait RenewalAttemptRepository: Send + Sync {
    /// The attempt row for a `(subscription, period)`, locked `FOR UPDATE` so a
    /// concurrent Job B tick (`FOR UPDATE SKIP LOCKED`) cannot flip its status
    /// between this read and the caller's write.
    async fn find_for_period_for_update(
        &self,
        conn: &mut PgConnection,
        subscription_id: Uuid,
        period_end: NaiveDate,
    ) -> Result<Option<RenewalAttempt>, UserError>;

    async fn create(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError>;

    /// Persist `status` / `next_attempt_at` / `updated_at` for an existing row.
    async fn update_schedule(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError>;

    // ---- Job B (docs/sagas/renewal-subscriptions.md §2) --------------------

    /// TX1: claim the single oldest due attempt whose subscription is still
    /// `active`, `FOR UPDATE SKIP LOCKED` so parallel Job B workers never grab
    /// the same row. `None` = nothing due.
    async fn claim_one_due(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<RenewalAttempt>, UserError>;

    /// TX1: persist the `charging` transition (`status`, `attempt_count`,
    /// `updated_at`).
    async fn mark_charging(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError>;

    /// TX2: re-read the row `FOR UPDATE` to confirm it is still `charging`
    /// before recording the provider outcome.
    async fn find_by_id_for_update(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Option<RenewalAttempt>, UserError>;

    /// TX2: persist every mutable field of the settled attempt.
    async fn settle(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError>;

    /// Reaper (§7): reset rows wedged in `charging` past `stale_after` back to
    /// `failed_retryable, next_attempt_at=now()` — safe because the provider
    /// idempotency key makes any re-charge a no-op. Returns the count reaped.
    async fn reap_stale_charging(
        &self,
        conn: &mut PgConnection,
        stale_after: Duration,
    ) -> Result<u64, UserError>;

    // ---- dunning-email ledger (step 4) ------------------------------------

    async fn dunning_email_recorded(
        &self,
        conn: &mut PgConnection,
        event_id: Uuid,
    ) -> Result<bool, UserError>;

    async fn record_dunning_email(
        &self,
        conn: &mut PgConnection,
        event_id: Uuid,
        user_id: Uuid,
        template: &str,
    ) -> Result<(), UserError>;
}
