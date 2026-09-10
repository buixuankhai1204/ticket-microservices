use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
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
        let row = sqlx::query_as::<_, RenewalAttemptRow>(
            "SELECT id, subscription_id, period_end, idempotency_key, status, attempt_count, \
             dunning_attempt_count, next_attempt_at, provider_charge_id, last_error, \
             created_at, updated_at \
             FROM renewal_attempts \
             WHERE subscription_id = $1 AND period_end = $2 \
             FOR UPDATE",
        )
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
}
