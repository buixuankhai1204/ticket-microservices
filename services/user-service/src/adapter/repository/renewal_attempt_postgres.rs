use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{RenewalAttempt, RenewalAttemptStatus, UserError};
use crate::platform::port::RenewalAttemptRepository;

#[derive(Default)]
pub struct PostgresRenewalAttemptRepository;

impl PostgresRenewalAttemptRepository {
    pub fn new() -> Self {
        Self
    }
}

fn repo_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}

const RENEWAL_COLS: &str = "id, subscription_id, period_end, idempotency_key, status, \
     attempt_count, dunning_attempt_count, next_attempt_at, provider_charge_id, last_error, \
     created_at, updated_at";

/// Statuses Job B's claim considers due (docs/sagas/renewal-subscriptions.md §2).
const CLAIMABLE_STATUSES: &str = "('pending', 'failed_retryable', 'failed_permanent')";

#[derive(sqlx::FromRow)]
struct RenewalAttemptRow {
    id: Uuid,
    subscription_id: Uuid,
    period_end: NaiveDate,
    idempotency_key: String,
    status: String,
    attempt_count: i32,
    dunning_attempt_count: i32,
    next_attempt_at: DateTime<Utc>,
    provider_charge_id: Option<String>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<RenewalAttemptRow> for RenewalAttempt {
    type Error = UserError;

    fn try_from(row: RenewalAttemptRow) -> Result<Self, UserError> {
        Ok(RenewalAttempt {
            id: row.id,
            subscription_id: row.subscription_id,
            period_end: row.period_end,
            idempotency_key: row.idempotency_key,
            status: RenewalAttemptStatus::parse(&row.status)?,
            attempt_count: row.attempt_count,
            dunning_attempt_count: row.dunning_attempt_count,
            next_attempt_at: row.next_attempt_at,
            provider_charge_id: row.provider_charge_id,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl RenewalAttemptRepository for PostgresRenewalAttemptRepository {
    async fn find_for_period_for_update(
        &self,
        conn: &mut PgConnection,
        subscription_id: Uuid,
        period_end: NaiveDate,
    ) -> Result<Option<RenewalAttempt>, UserError> {
        let row = sqlx::query_as::<_, RenewalAttemptRow>(&format!(
            "SELECT {RENEWAL_COLS} FROM renewal_attempts \
             WHERE subscription_id = $1 AND period_end = $2 FOR UPDATE"
        ))
        .bind(subscription_id)
        .bind(period_end)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.map(RenewalAttempt::try_from).transpose()
    }

    async fn create(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO renewal_attempts \
             (id, subscription_id, period_end, idempotency_key, status, attempt_count, \
              dunning_attempt_count, next_attempt_at, provider_charge_id, last_error, \
              created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(attempt.id)
        .bind(attempt.subscription_id)
        .bind(attempt.period_end)
        .bind(&attempt.idempotency_key)
        .bind(attempt.status.as_str())
        .bind(attempt.attempt_count)
        .bind(attempt.dunning_attempt_count)
        .bind(attempt.next_attempt_at)
        .bind(&attempt.provider_charge_id)
        .bind(&attempt.last_error)
        .bind(attempt.created_at)
        .bind(attempt.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn update_schedule(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError> {
        sqlx::query(
            "UPDATE renewal_attempts \
             SET status = $2, next_attempt_at = $3, updated_at = $4 \
             WHERE id = $1",
        )
        .bind(attempt.id)
        .bind(attempt.status.as_str())
        .bind(attempt.next_attempt_at)
        .bind(attempt.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn claim_one_due(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<RenewalAttempt>, UserError> {
        let row = sqlx::query_as::<_, RenewalAttemptRow>(&format!(
            "SELECT {RENEWAL_COLS} FROM renewal_attempts r \
             WHERE r.status IN {CLAIMABLE_STATUSES} \
               AND r.next_attempt_at <= now() \
               AND EXISTS ( \
                 SELECT 1 FROM subscriptions s \
                 WHERE s.id = r.subscription_id AND s.status = 'active' \
               ) \
             ORDER BY r.next_attempt_at \
             FOR UPDATE OF r SKIP LOCKED \
             LIMIT 1"
        ))
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.map(RenewalAttempt::try_from).transpose()
    }

    async fn mark_charging(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError> {
        sqlx::query(
            "UPDATE renewal_attempts \
             SET status = $2, attempt_count = $3, updated_at = $4 \
             WHERE id = $1",
        )
        .bind(attempt.id)
        .bind(attempt.status.as_str())
        .bind(attempt.attempt_count)
        .bind(attempt.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn find_by_id_for_update(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Option<RenewalAttempt>, UserError> {
        let row = sqlx::query_as::<_, RenewalAttemptRow>(&format!(
            "SELECT {RENEWAL_COLS} FROM renewal_attempts WHERE id = $1 FOR UPDATE"
        ))
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.map(RenewalAttempt::try_from).transpose()
    }

    async fn settle(
        &self,
        conn: &mut PgConnection,
        attempt: &RenewalAttempt,
    ) -> Result<(), UserError> {
        sqlx::query(
            "UPDATE renewal_attempts SET \
               status = $2, attempt_count = $3, dunning_attempt_count = $4, \
               next_attempt_at = $5, provider_charge_id = $6, last_error = $7, updated_at = $8 \
             WHERE id = $1",
        )
        .bind(attempt.id)
        .bind(attempt.status.as_str())
        .bind(attempt.attempt_count)
        .bind(attempt.dunning_attempt_count)
        .bind(attempt.next_attempt_at)
        .bind(&attempt.provider_charge_id)
        .bind(&attempt.last_error)
        .bind(attempt.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn reap_stale_charging(
        &self,
        conn: &mut PgConnection,
        stale_after: Duration,
    ) -> Result<u64, UserError> {
        let result = sqlx::query(
            "UPDATE renewal_attempts \
             SET status = 'failed_retryable', next_attempt_at = now(), \
                 last_error = 'charging_timeout_reaped', updated_at = now() \
             WHERE status = 'charging' \
               AND updated_at < now() - make_interval(secs => $1)",
        )
        .bind(stale_after.num_seconds() as f64)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(result.rows_affected())
    }

    async fn dunning_email_recorded(
        &self,
        conn: &mut PgConnection,
        event_id: Uuid,
    ) -> Result<bool, UserError> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sent_emails WHERE event_id = $1)")
                .bind(event_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(repo_err)?;

        Ok(exists)
    }

    async fn record_dunning_email(
        &self,
        conn: &mut PgConnection,
        event_id: Uuid,
        user_id: Uuid,
        template: &str,
    ) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO sent_emails (id, event_id, user_id, template) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(event_id)
        .bind(user_id)
        .bind(template)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn try_advisory_lock(
        &self,
        conn: &mut PgConnection,
        key: &str,
    ) -> Result<bool, UserError> {
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
            .bind(key)
            .fetch_one(&mut *conn)
            .await
            .map_err(repo_err)
    }

    async fn release_advisory_lock(
        &self,
        conn: &mut PgConnection,
        key: &str,
    ) -> Result<(), UserError> {
        sqlx::query("SELECT pg_advisory_unlock(hashtext($1))")
            .bind(key)
            .execute(&mut *conn)
            .await
            .map_err(repo_err)?;

        Ok(())
    }

    async fn enqueue_due(&self, conn: &mut PgConnection) -> Result<u64, UserError> {
        let result = sqlx::query(
            "INSERT INTO renewal_attempts (subscription_id, period_end, idempotency_key) \
             SELECT id, current_period_end, \
                    'renew:' || id::text || ':' || current_period_end::text \
             FROM subscriptions \
             WHERE status = 'active' AND current_period_end <= CURRENT_DATE \
             ON CONFLICT (subscription_id, period_end) DO NOTHING",
        )
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(result.rows_affected())
    }
}
