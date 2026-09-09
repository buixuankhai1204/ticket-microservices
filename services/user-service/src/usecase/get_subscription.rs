use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Subscription, UserError};
use crate::platform::port::SubscriptionRepository;

pub struct GetSubscriptionUseCase {
    db_pool: PgPool,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl GetSubscriptionUseCase {
    pub fn new(db_pool: PgPool, subscription_repository: Arc<dyn SubscriptionRepository>) -> Self {
        Self {
            db_pool,
            subscription_repository,
        }
    }

    /// Fetch one subscription owned by `user_id`. A subscription that exists but
    /// belongs to another user is reported as `UserError::NotFound`.
    pub async fn execute(&self, id: Uuid, user_id: Uuid) -> Result<Subscription, UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let subscription = self
            .subscription_repository
            .find_by_id_for_user(&mut tx, id, user_id)
            .await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(subscription)
    }
}
