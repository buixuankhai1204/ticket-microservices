use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{Subscription, UserError};
use crate::platform::port::{SubscriptionRepository, Transactor};

pub struct GetSubscriptionUseCase {
    db: Arc<dyn Transactor>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl GetSubscriptionUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
    ) -> Self {
        Self {
            db,
            subscription_repository,
        }
    }

    /// Fetch one subscription owned by `user_id`. A subscription that exists but
    /// belongs to another user is reported as `UserError::NotFound`.
    pub async fn execute(&self, id: Uuid, user_id: Uuid) -> Result<Subscription, UserError> {
        let mut tx = self.db.begin_read_only().await?;
        let subscription = self
            .subscription_repository
            .find_by_id_for_user(&mut tx, id, user_id)
            .await?;
        tx.commit().await?;
        Ok(subscription)
    }
}
