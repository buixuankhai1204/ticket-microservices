use chrono::{DateTime, Duration, NaiveDate, Utc};
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

    // ---- Job B state machine (docs/sagas/renewal-subscriptions.md §2) --------

    /// TX1: mark the claimed row `charging` and count the attempt, so a crash
    /// before TX2 leaves a row the reaper can see (§5.3 / §5.4).
    pub fn begin_charging(&mut self) {
        self.status = RenewalAttemptStatus::Charging;
        self.attempt_count += 1;
        self.updated_at = Utc::now();
    }

    /// TX2 arm 3a: the charge went through.
    pub fn mark_succeeded(&mut self, provider_charge_id: String) {
        self.status = RenewalAttemptStatus::Succeeded;
        self.provider_charge_id = Some(provider_charge_id);
        self.last_error = None;
        self.updated_at = Utc::now();
    }

    /// TX2 arm 3b: a transient provider failure. Reschedules with capped
    /// exponential backoff while attempts remain, otherwise gives up.
    pub fn mark_transient_failure(
        &mut self,
        error: String,
        policy: &RenewalPolicy,
    ) -> TransientOutcome {
        self.last_error = Some(error);
        self.updated_at = Utc::now();
        if self.attempt_count < policy.max_transient_attempts {
            self.status = RenewalAttemptStatus::FailedRetryable;
            self.next_attempt_at = Utc::now() + policy.backoff(self.attempt_count);
            TransientOutcome::WillRetry
        } else {
            self.status = RenewalAttemptStatus::GivenUp;
            TransientOutcome::GaveUp
        }
    }

    /// TX2 arm 3c: a permanent card decline. Advances the dunning schedule while
    /// slots remain, otherwise the schedule is exhausted and the caller cancels.
    pub fn mark_permanent_decline(
        &mut self,
        decline_code: String,
        policy: &RenewalPolicy,
    ) -> DunningOutcome {
        self.status = RenewalAttemptStatus::FailedPermanent;
        self.last_error = Some(decline_code);
        self.updated_at = Utc::now();
        let slots = policy.dunning_schedule_days.len() as i32;
        if self.dunning_attempt_count < slots {
            self.dunning_attempt_count += 1;
            let days = policy.dunning_schedule_days[(self.dunning_attempt_count - 1) as usize];
            self.next_attempt_at = self
                .period_end
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid")
                .and_utc()
                + Duration::days(days);
            DunningOutcome::Continue {
                dunning_attempt: self.dunning_attempt_count,
                dunning_max: slots,
            }
        } else {
            DunningOutcome::Exhausted {
                dunning_attempts: self.dunning_attempt_count,
            }
        }
    }
}

/// Result of [`RenewalAttempt::mark_transient_failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransientOutcome {
    WillRetry,
    GaveUp,
}

/// Result of [`RenewalAttempt::mark_permanent_decline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DunningOutcome {
    Continue {
        dunning_attempt: i32,
        dunning_max: i32,
    },
    Exhausted {
        dunning_attempts: i32,
    },
}

/// Job B tuning, parsed from env at the composition root.
#[derive(Debug, Clone)]
pub struct RenewalPolicy {
    /// Total provider attempts before a transient failure becomes `given_up`.
    pub max_transient_attempts: i32,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    /// Days after `period_end` for each dunning retry, e.g. `[1, 3, 5, 7]`.
    pub dunning_schedule_days: Vec<i64>,
}

impl RenewalPolicy {
    /// `min(base * 2^(attempt-1), cap)` plus sub-second jitter derived from the
    /// wall clock (no RNG dependency) to spread a thundering herd.
    pub fn backoff(&self, attempt_count: i32) -> Duration {
        let exp = attempt_count.saturating_sub(1).clamp(0, 20) as u32;
        let scaled = self
            .backoff_base
            .checked_mul(2_i32.saturating_pow(exp))
            .unwrap_or(self.backoff_cap);
        let capped = scaled.min(self.backoff_cap);
        let jitter = Duration::milliseconds(i64::from(Utc::now().timestamp_subsec_millis()));
        capped + jitter
    }
}

impl Default for RenewalPolicy {
    fn default() -> Self {
        Self {
            max_transient_attempts: 3,
            backoff_base: Duration::hours(6),
            backoff_cap: Duration::hours(48),
            dunning_schedule_days: vec![1, 3, 5, 7],
        }
    }
}
