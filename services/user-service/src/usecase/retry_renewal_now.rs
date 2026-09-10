use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{RenewalAttempt, UserError};
use crate::platform::port::{RenewalAttemptRepository, SubscriptionRepository};

pub struct RetryRenewalNowUseCase {
    db_pool: PgPool,
    subscription_repository: Arc<dyn SubscriptionRepository>,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
}

impl RetryRenewalNowUseCase {
    pub fn new(
        db_pool: PgPool,
        subscription_repository: Arc<dyn SubscriptionRepository>,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    ) -> Self {
        Self {
            db_pool,
            subscription_repository,
            renewal_attempt_repository,
        }
    }

    /// Queue the current-period renewal of `subscription_id` (owned by
    /// `user_id`) to run on Job B's next tick. Never charges inline. Idempotent:
    /// calling it twice just leaves one row at `failed_retryable, now()`.
    pub async fn execute(
        &self,
        subscription_id: Uuid,
        user_id: Uuid,
    ) -> Result<RenewalAttempt, UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;

        let subscription = self
            .subscription_repository
            .find_by_id_for_user(&mut tx, subscription_id, user_id)
            .await?;
        subscription.ensure_renewal_retryable()?;

        let period_end = subscription.current_period_end;
        let attempt = match self
            .renewal_attempt_repository
            .find_for_period_for_update(&mut tx, subscription_id, period_end)
            .await?
        {
            Some(mut existing) => {
                existing.requeue_now()?;
                self.renewal_attempt_repository
                    .update_schedule(&mut tx, &existing)
                    .await?;
                existing
            }
            None => {
                let fresh = RenewalAttempt::queued_now(subscription_id, period_end);
                self.renewal_attempt_repository
                    .create(&mut tx, &fresh)
                    .await?;
                fresh
            }
        };

        tx.commit().await.map_err(tx_err)?;
        Ok(attempt)
    }
}
