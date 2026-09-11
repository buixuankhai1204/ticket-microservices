use std::sync::Arc;

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{
    ChargeRequest, DomainEvent, DunningEmail, DunningOutcome, PaymentError, PaymentGateway,
    RenewalAttemptStatus, RenewalPolicy, SubscriptionCanceled, SubscriptionPaymentFailed,
    SubscriptionRenewed, SubscriptionStatus, TransientOutcome, UserError,
};
use crate::platform::port::{RenewalAttemptRepository, SubscriptionRepository};

/// What one Job B iteration did (docs/sagas/renewal-subscriptions.md §2).
#[derive(Debug)]
pub enum ProcessOutcome {
    /// No due attempt was claimable this iteration — stop the batch loop.
    NothingDue,
    Renewed {
        subscription_id: Uuid,
    },
    RetryScheduled {
        subscription_id: Uuid,
    },
    GaveUp {
        subscription_id: Uuid,
    },
    /// Dunning advanced; the caller must send `email` (best-effort, no txn).
    Dunning {
        email: DunningEmail,
    },
    Canceled {
        subscription_id: Uuid,
    },
    /// TX2 found the row no longer `charging` (a reaper or manual retry raced) —
    /// nothing to record.
    Skipped,
}

pub struct ProcessRenewalAttemptUseCase {
    db_pool: PgPool,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
    payment_gateway: Arc<dyn PaymentGateway>,
    policy: RenewalPolicy,
}

impl ProcessRenewalAttemptUseCase {
    pub fn new(
        db_pool: PgPool,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
        payment_gateway: Arc<dyn PaymentGateway>,
        policy: RenewalPolicy,
    ) -> Self {
        Self {
            db_pool,
            renewal_attempt_repository,
            subscription_repository,
            payment_gateway,
            policy,
        }
    }

    /// Reset any `charging` rows wedged past `stale_after`. Called once per Job B
    /// tick before the claim loop.
    pub async fn reap_stale_charging(
        &self,
        stale_after: chrono::Duration,
    ) -> Result<u64, UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        let n = self
            .renewal_attempt_repository
            .reap_stale_charging(&mut tx, stale_after)
            .await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(n)
    }

    /// Claim one due attempt, charge it, and record the outcome across the two
    /// transactions the design mandates (state change and payment call cannot
    /// share one txn).
    pub async fn execute(&self) -> Result<ProcessOutcome, UserError> {
        // --- TX1: claim + mark charging -----------------------------------
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        let Some(mut attempt) = self
            .renewal_attempt_repository
            .claim_one_due(&mut tx)
            .await?
        else {
            tx.rollback().await.ok();
            return Ok(ProcessOutcome::NothingDue);
        };
        let subscription = self
            .subscription_repository
            .find_by_id(&mut tx, attempt.subscription_id)
            .await?;
        attempt.begin_charging();
        self.renewal_attempt_repository
            .mark_charging(&mut tx, &attempt)
            .await?;
        tx.commit().await.map_err(tx_err)?;

        // --- provider call (no txn held) --------------------------------
        let charge = self
            .payment_gateway
            .charge(ChargeRequest {
                amount_minor: subscription.price_minor,
                currency: subscription.currency.clone(),
                payment_method_id: subscription.payment_method_id.clone(),
                idempotency_key: attempt.idempotency_key.clone(),
            })
            .await;

        // --- TX2: record the outcome -----------------------------------
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        let Some(mut fresh) = self
            .renewal_attempt_repository
            .find_by_id_for_update(&mut tx, attempt.id)
            .await?
        else {
            tx.rollback().await.ok();
            return Ok(ProcessOutcome::Skipped);
        };
        if fresh.status != RenewalAttemptStatus::Charging {
            tx.rollback().await.ok();
            return Ok(ProcessOutcome::Skipped);
        }

        let now = Utc::now();
        let outcome = match charge {
            Ok(charge) => {
                let new_period_end = subscription
                    .billing_interval
                    .advance(subscription.current_period_end)
                    .ok_or_else(|| {
                        UserError::Repository("renewal period end out of range".to_string())
                    })?;
                fresh.mark_succeeded(charge.provider_charge_id.clone());
                self.renewal_attempt_repository
                    .settle(&mut tx, &fresh)
                    .await?;
                self.subscription_repository
                    .renew_period(&mut tx, subscription.id, new_period_end)
                    .await?;
                let event = DomainEvent::SubscriptionRenewed(SubscriptionRenewed {
                    event_id: Uuid::new_v4(),
                    subscription_id: subscription.id,
                    user_id: subscription.user_id,
                    plan_id: subscription.plan_id.clone(),
                    renewal_attempt_id: fresh.id,
                    period_start: subscription.current_period_end,
                    new_period_end,
                    amount_minor: subscription.price_minor,
                    currency: subscription.currency.clone(),
                    provider_charge_id: charge.provider_charge_id,
                    attempt_count: fresh.attempt_count,
                    renewed_at: now,
                });
                self.subscription_repository
                    .write_outbox(&mut tx, &event)
                    .await?;
                ProcessOutcome::Renewed {
                    subscription_id: subscription.id,
                }
            }
            Err(PaymentError::Transient(message)) => {
                match fresh.mark_transient_failure(message, &self.policy) {
                    TransientOutcome::WillRetry => {
                        self.renewal_attempt_repository
                            .settle(&mut tx, &fresh)
                            .await?;
                        ProcessOutcome::RetryScheduled {
                            subscription_id: subscription.id,
                        }
                    }
                    TransientOutcome::GaveUp => {
                        self.renewal_attempt_repository
                            .settle(&mut tx, &fresh)
                            .await?;
                        self.subscription_repository
                            .set_status(&mut tx, subscription.id, SubscriptionStatus::PastDue)
                            .await?;
                        ProcessOutcome::GaveUp {
                            subscription_id: subscription.id,
                        }
                    }
                }
            }
            Err(PaymentError::Declined { code }) => {
                match fresh.mark_permanent_decline(code.clone(), &self.policy) {
                    DunningOutcome::Continue {
                        dunning_attempt,
                        dunning_max,
                    } => {
                        self.renewal_attempt_repository
                            .settle(&mut tx, &fresh)
                            .await?;
                        self.subscription_repository
                            .set_status(&mut tx, subscription.id, SubscriptionStatus::PastDue)
                            .await?;
                        let event_id = Uuid::new_v4();
                        let event =
                            DomainEvent::SubscriptionPaymentFailed(SubscriptionPaymentFailed {
                                event_id,
                                subscription_id: subscription.id,
                                user_id: subscription.user_id,
                                plan_id: subscription.plan_id.clone(),
                                renewal_attempt_id: fresh.id,
                                period_end: fresh.period_end,
                                amount_minor: subscription.price_minor,
                                currency: subscription.currency.clone(),
                                decline_code: code,
                                dunning_attempt,
                                dunning_max,
                                next_attempt_at: fresh.next_attempt_at,
                                failed_at: now,
                            });
                        self.subscription_repository
                            .write_outbox(&mut tx, &event)
                            .await?;
                        ProcessOutcome::Dunning {
                            email: DunningEmail {
                                to_user_id: subscription.user_id,
                                subscription_id: subscription.id,
                                template: "dunning_card_declined".to_string(),
                                idempotency_key: event_id,
                                period_end: fresh.period_end,
                                amount_minor: subscription.price_minor,
                                currency: subscription.currency.clone(),
                                dunning_attempt,
                                dunning_max,
                            },
                        }
                    }
                    DunningOutcome::Exhausted { dunning_attempts } => {
                        self.renewal_attempt_repository
                            .settle(&mut tx, &fresh)
                            .await?;
                        self.subscription_repository
                            .set_status(&mut tx, subscription.id, SubscriptionStatus::Canceled)
                            .await?;
                        let event = DomainEvent::SubscriptionCanceled(SubscriptionCanceled {
                            event_id: Uuid::new_v4(),
                            subscription_id: subscription.id,
                            user_id: subscription.user_id,
                            plan_id: subscription.plan_id.clone(),
                            renewal_attempt_id: fresh.id,
                            period_end: fresh.period_end,
                            reason: "dunning_exhausted".to_string(),
                            dunning_attempts,
                            canceled_at: now,
                        });
                        self.subscription_repository
                            .write_outbox(&mut tx, &event)
                            .await?;
                        ProcessOutcome::Canceled {
                            subscription_id: subscription.id,
                        }
                    }
                }
            }
        };

        tx.commit().await.map_err(tx_err)?;
        Ok(outcome)
    }
}
