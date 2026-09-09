use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Pagination, Subscription, UserError};
use crate::platform::port::SubscriptionRepository;

pub struct ListSubscriptionsUseCase {
    db_pool: PgPool,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl ListSubscriptionsUseCase {
    pub fn new(db_pool: PgPool, subscription_repository: Arc<dyn SubscriptionRepository>) -> Self {
        Self {
            db_pool,
            subscription_repository,
        }
    }

    /// A page of the caller's own subscriptions plus the total count for that
    /// caller, read on one read-only transaction so page and count agree.
    pub async fn execute(
        &self,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Subscription>, i64), UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let page = self
            .subscription_repository
            .list_for_user(&mut tx, user_id, pagination)
            .await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(page)
    }
}
