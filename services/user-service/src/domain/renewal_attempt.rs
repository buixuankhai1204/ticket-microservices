use chrono::{DateTime, NaiveDate, Utc};
use uuid::Uuid;

use super::errors::UserError;

/// State of one `(subscription, billing period)` renewal attempt. Mirrors the
/// `renewal_attempts.status` CHECK constraint. The full state machine is driven
/// by the renewal Job B (docs/sagas/renewal-subscriptions.md §2); this module
/// only covers what `RetryRenewalNow` needs — queue an attempt to run now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewalAttemptStatus {
    Pending,
    Charging,
    Succeeded,
    FailedRetryable,
    FailedPermanent,
    GivenUp,
}

impl RenewalAttemptStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RenewalAttemptStatus::Pending => "pending",
            RenewalAttemptStatus::Charging => "charging",
            RenewalAttemptStatus::Succeeded => "succeeded",
            RenewalAttemptStatus::FailedRetryable => "failed_retryable",
            RenewalAttemptStatus::FailedPermanent => "failed_permanent",
            RenewalAttemptStatus::GivenUp => "given_up",
        }
    }

    pub fn parse(value: &str) -> Result<Self, UserError> {
        match value {
            "pending" => Ok(RenewalAttemptStatus::Pending),
            "charging" => Ok(RenewalAttemptStatus::Charging),
            "succeeded" => Ok(RenewalAttemptStatus::Succeeded),
            "failed_retryable" => Ok(RenewalAttemptStatus::FailedRetryable),
            "failed_permanent" => Ok(RenewalAttemptStatus::FailedPermanent),
            "given_up" => Ok(RenewalAttemptStatus::GivenUp),
            other => Err(UserError::Repository(format!(
                "unknown renewal_attempts.status in database: '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RenewalAttempt {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub period_end: NaiveDate,
    pub idempotency_key: String,
    pub status: RenewalAttemptStatus,
    pub attempt_count: i32,
    pub dunning_attempt_count: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub provider_charge_id: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl RenewalAttempt {
    /// Deterministic per `(subscription, period)`. Also the key passed to the
    /// payment provider's `Idempotency-Key` header by Job B.
    pub fn idempotency_key_for(subscription_id: Uuid, period_end: NaiveDate) -> String {
        format!("renew:{subscription_id}:{period_end}")
    }

    /// A fresh attempt queued to run immediately — used when a client asks to
    /// retry a period that Job A has not enqueued yet. Per
    /// docs/sagas/renewal-subscriptions.md §7 the status is `failed_retryable`
    /// so Job B's claim picks it up on its next tick.
    pub fn queued_now(subscription_id: Uuid, period_end: NaiveDate) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            subscription_id,
            period_end,
            idempotency_key: Self::idempotency_key_for(subscription_id, period_end),
            status: RenewalAttemptStatus::FailedRetryable,
            attempt_count: 0,
            dunning_attempt_count: 0,
            next_attempt_at: now,
            provider_charge_id: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Move an existing attempt to the front of the run queue. Rejected if the
    /// renewal already succeeded (nothing to retry) or a charge is in flight
    /// (`charging` — Job B is mid-attempt; let it finish).
    pub fn requeue_now(&mut self) -> Result<(), UserError> {
        match self.status {
            RenewalAttemptStatus::Succeeded => Err(UserError::RenewalNotRetryable(
                "renewal for the current period already succeeded".to_string(),
            )),
            RenewalAttemptStatus::Charging => Err(UserError::RenewalNotRetryable(
                "a renewal attempt is already in progress".to_string(),
            )),
            _ => {
                let now = Utc::now();
                self.status = RenewalAttemptStatus::FailedRetryable;
                self.next_attempt_at = now;
                self.updated_at = now;
                Ok(())
            }
        }
    }
}
