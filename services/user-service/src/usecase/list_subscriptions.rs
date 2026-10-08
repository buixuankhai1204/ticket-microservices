use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{Pagination, Subscription, UserError};
use crate::platform::port::{SubscriptionRepository, Transactor};

pub struct ListSubscriptionsUseCase {
    db: Arc<dyn Transactor>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl ListSubscriptionsUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
    ) -> Self {
        Self {
            db,
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
        let mut tx = self.db.begin_read_only().await?;
        let page = self
            .subscription_repository
            .list_for_user(&mut tx, user_id, pagination)
            .await?;
        tx.commit().await?;
        Ok(page)
    }
}
