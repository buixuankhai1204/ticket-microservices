use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{BillingInterval, Subscription, UserError};
use crate::platform::port::SubscriptionRepository;

pub struct CreateSubscriptionInput {
    pub user_id: Uuid,
    pub plan_id: String,
    pub billing_interval: String,
    pub price_minor: i64,
    pub currency: String,
    pub payment_method_id: String,
}

pub struct CreateSubscriptionUseCase {
    db_pool: PgPool,
    subscription_repository: Arc<dyn SubscriptionRepository>,
}

impl CreateSubscriptionUseCase {
    pub fn new(db_pool: PgPool, subscription_repository: Arc<dyn SubscriptionRepository>) -> Self {
        Self {
            db_pool,
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

        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        self.subscription_repository
            .create(&mut tx, &subscription)
            .await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(subscription)
    }
}
