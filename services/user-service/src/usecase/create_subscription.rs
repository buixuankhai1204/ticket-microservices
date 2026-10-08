use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{BillingInterval, Subscription, UserError};
use crate::platform::port::{SubscriptionRepository, Transactor};

pub struct CreateSubscriptionInput {
    pub user_id: Uuid,
    pub plan_id: String,
    pub billing_interval: String,
    pub price_minor: i64,
    pub currency: String,
    pub payment_method_id: String,
}

pub struct CreateSubscriptionUseCase {
    db: Arc<dyn Transactor>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl CreateSubscriptionUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
    ) -> Self {
        Self {
            db,
            subscription_repository,
        }
    }

    pub async fn execute(&self, input: CreateSubscriptionInput) -> Result<Subscription, UserError> {
        let billing_interval = BillingInterval::parse(&input.billing_interval)?;
        let subscription = Subscription::new(
            input.user_id,
            input.plan_id,
            billing_interval,
            input.price_minor,
            input.currency,
            input.payment_method_id,
        )?;

        let mut tx = self.db.begin().await?;
        self.subscription_repository
            .create(&mut tx, &subscription)
            .await?;
        tx.commit().await?;

        Ok(subscription)
    }
}
