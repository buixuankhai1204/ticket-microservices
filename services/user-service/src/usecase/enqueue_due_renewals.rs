use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::UserError;
use crate::platform::port::RenewalAttemptRepository;

const ENQUEUE_LOCK_KEY: &str = "renewal-enqueue";

#[derive(Debug)]
pub enum EnqueueOutcome {
    LockNotHeld,
    Enqueued(u64),
}

pub struct EnqueueDueRenewalsUseCase {
    db_pool: PgPool,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
}

impl EnqueueDueRenewalsUseCase {
    pub fn new(
        db_pool: PgPool,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    ) -> Self {
        Self {
            db_pool,
            renewal_attempt_repository,
        }
    }

    pub async fn execute(&self) -> Result<EnqueueOutcome, UserError> {
        let mut conn = self.db_pool.acquire().await.map_err(tx_err)?;

        if !self
            .renewal_attempt_repository
            .try_advisory_lock(&mut conn, ENQUEUE_LOCK_KEY)
            .await?
        {
            return Ok(EnqueueOutcome::LockNotHeld);
        }

        let result = self.renewal_attempt_repository.enqueue_due(&mut conn).await;
        let unlock_result = self
            .renewal_attempt_repository
            .release_advisory_lock(&mut conn, ENQUEUE_LOCK_KEY)
            .await;

        let enqueued = result?;
        unlock_result?;
        Ok(EnqueueOutcome::Enqueued(enqueued))
    }
}
