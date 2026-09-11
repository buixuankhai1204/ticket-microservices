use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{
    BillingInterval, DomainEvent, Pagination, Subscription, SubscriptionStatus, UserError,
};
use crate::platform::port::SubscriptionRepository;

#[derive(Default)]
pub struct PostgresSubscriptionRepository;

impl PostgresSubscriptionRepository {
    pub fn new() -> Self {
        Self
    }
}

fn repo_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}

const SUBSCRIPTION_COLS: &str = "id, user_id, plan_id, status, current_period_end, \
     billing_interval, price_minor, currency, payment_method_id, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct SubscriptionRow {
    id: Uuid,
    user_id: Uuid,
    plan_id: String,
    status: String,
    current_period_end: NaiveDate,
    billing_interval: String,
    price_minor: i64,
    currency: String,
    payment_method_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<SubscriptionRow> for Subscription {
    type Error = UserError;

    fn try_from(row: SubscriptionRow) -> Result<Self, UserError> {
        Ok(Subscription {
            id: row.id,
            user_id: row.user_id,
            plan_id: row.plan_id,
            status: SubscriptionStatus::parse(&row.status)?,
            current_period_end: row.current_period_end,
            billing_interval: BillingInterval::parse(&row.billing_interval)?,
            price_minor: row.price_minor,
            currency: row.currency,
            payment_method_id: row.payment_method_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl SubscriptionRepository for PostgresSubscriptionRepository {
    async fn create(
        &self,
        conn: &mut PgConnection,
        subscription: &Subscription,
    ) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO subscriptions \
             (id, user_id, plan_id, status, current_period_end, billing_interval, \
              price_minor, currency, payment_method_id, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(subscription.id)
        .bind(subscription.user_id)
        .bind(&subscription.plan_id)
        .bind(subscription.status.as_str())
        .bind(subscription.current_period_end)
        .bind(subscription.billing_interval.as_str())
        .bind(subscription.price_minor)
        .bind(&subscription.currency)
        .bind(&subscription.payment_method_id)
        .bind(subscription.created_at)
        .bind(subscription.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn find_by_id_for_user(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Subscription, UserError> {
        let row = sqlx::query_as::<_, SubscriptionRow>(&format!(
            "SELECT {SUBSCRIPTION_COLS} FROM subscriptions WHERE id = $1 AND user_id = $2"
        ))
        .bind(id)
        .bind(user_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.ok_or(UserError::NotFound)?.try_into()
    }

    async fn list_for_user(
        &self,
        conn: &mut PgConnection,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Subscription>, i64), UserError> {
        let total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM subscriptions WHERE user_id = $1")
                .bind(user_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(repo_err)?;

        let rows = sqlx::query_as::<_, SubscriptionRow>(&format!(
            "SELECT {SUBSCRIPTION_COLS} FROM subscriptions WHERE user_id = $1 \
             ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3"
        ))
        .bind(user_id)
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(&mut *conn)
        .await
        .map_err(repo_err)?;

        let subscriptions = rows
            .into_iter()
            .map(Subscription::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((subscriptions, total))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Subscription, UserError> {
        let row = sqlx::query_as::<_, SubscriptionRow>(&format!(
            "SELECT {SUBSCRIPTION_COLS} FROM subscriptions WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.ok_or(UserError::NotFound)?.try_into()
    }

    async fn renew_period(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        new_period_end: NaiveDate,
    ) -> Result<(), UserError> {
        sqlx::query(
            "UPDATE subscriptions \
             SET current_period_end = $2, status = 'active', updated_at = now() \
             WHERE id = $1",
        )
        .bind(id)
        .bind(new_period_end)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }

    async fn set_status(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        status: SubscriptionStatus,
    ) -> Result<(), UserError> {
        sqlx::query("UPDATE subscriptions SET status = $2, updated_at = now() WHERE id = $1")
            .bind(id)
            .bind(status.as_str())
            .execute(&mut *conn)
            .await
            .map_err(repo_err)?;

        Ok(())
    }

    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(event.event_id())
        .bind(event.aggregate_id())
        .bind(event.aggregate_type())
        .bind(event.event_type())
        .bind(event.payload())
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }
}
