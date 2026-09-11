use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::{DunningEmail, EmailGateway, UserError};
use crate::platform::port::RenewalAttemptRepository;

/// Step 4 of the renewal flow (docs/sagas/renewal-subscriptions.md §2 / §5.9):
/// after a `SubscriptionPaymentFailed` is committed, send the "update your card"
/// email. Best-effort — a send failure is logged and left for the next dunning
/// pass; the `sent_emails` ledger keeps it to one email per dunning event.
pub struct SendDunningEmailUseCase {
    db_pool: PgPool,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    email_gateway: Arc<dyn EmailGateway>,
}

impl SendDunningEmailUseCase {
    pub fn new(
        db_pool: PgPool,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
        email_gateway: Arc<dyn EmailGateway>,
    ) -> Self {
        Self {
            db_pool,
            renewal_attempt_repository,
            email_gateway,
        }
    }

    pub async fn execute(&self, email: DunningEmail) -> Result<(), UserError> {
        {
            let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(tx_err)?;
            let already = self
                .renewal_attempt_repository
                .dunning_email_recorded(&mut tx, email.idempotency_key)
                .await?;
            tx.commit().await.map_err(tx_err)?;
            if already {
                return Ok(());
            }
        }

        match self.email_gateway.send(email.clone()).await {
            Ok(()) => {
                let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
                self.renewal_attempt_repository
                    .record_dunning_email(
                        &mut tx,
                        email.idempotency_key,
                        email.to_user_id,
                        &email.template,
                    )
                    .await?;
                tx.commit().await.map_err(tx_err)?;
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    subscription_id = %email.subscription_id,
                    "dunning email send failed; the next dunning pass will retry"
                );
                Ok(())
            }
        }
    }
}
