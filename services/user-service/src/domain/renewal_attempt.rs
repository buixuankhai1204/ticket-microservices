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
        now: DateTime<Utc>,
    ) -> TransientOutcome {
        self.last_error = Some(error);
        self.updated_at = now;
        if self.attempt_count < policy.max_transient_attempts {
            self.status = RenewalAttemptStatus::FailedRetryable;
            let jitter = Duration::milliseconds(i64::from(now.timestamp_subsec_millis()));
            self.next_attempt_at = now + policy.backoff(self.attempt_count) + jitter;
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
    /// `min(base * 2^(attempt-1), cap)`.
    pub fn backoff(&self, attempt_count: i32) -> Duration {
        let exp = attempt_count.saturating_sub(1).clamp(0, 20) as u32;
        let scaled = self
            .backoff_base
            .checked_mul(2_i32.saturating_pow(exp))
            .unwrap_or(self.backoff_cap);
        scaled.min(self.backoff_cap)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    fn attempt(attempt_count: i32, period_end: NaiveDate) -> RenewalAttempt {
        RenewalAttempt {
            id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            period_end,
            idempotency_key: String::new(),
            status: RenewalAttemptStatus::Charging,
            attempt_count,
            dunning_attempt_count: 0,
            next_attempt_at: at(2031, 1, 1),
            provider_charge_id: None,
            last_error: None,
            created_at: at(2031, 1, 1),
            updated_at: at(2031, 1, 1),
        }
    }

    #[test]
    fn backoff_doubles_per_attempt_then_caps_without_overflowing() {
        let policy = RenewalPolicy::default();

        assert_eq!(policy.backoff(1), Duration::hours(6));
        assert_eq!(policy.backoff(2), Duration::hours(12));
        assert_eq!(policy.backoff(3), Duration::hours(24));
        assert_eq!(policy.backoff(4), Duration::hours(48));
        assert_eq!(policy.backoff(5), Duration::hours(48));
        assert_eq!(policy.backoff(i32::MAX), Duration::hours(48));
    }

    #[test]
    fn transient_failure_below_the_max_reschedules_with_backoff_and_jitter() {
        let policy = RenewalPolicy::default();
        let now = Utc.with_ymd_and_hms(2031, 3, 4, 5, 6, 7).unwrap() + Duration::milliseconds(250);
        let mut a = attempt(1, NaiveDate::from_ymd_opt(2031, 3, 31).unwrap());

        let outcome = a.mark_transient_failure("timeout".to_string(), &policy, now);

        assert_eq!(outcome, TransientOutcome::WillRetry);
        assert_eq!(a.status, RenewalAttemptStatus::FailedRetryable);
        assert_eq!(a.last_error.as_deref(), Some("timeout"));
        assert_eq!(a.updated_at, now);
        assert_eq!(
            a.next_attempt_at,
            now + Duration::hours(6) + Duration::milliseconds(250)
        );
    }

    #[test]
    fn transient_failure_at_the_max_gives_up_and_keeps_the_schedule() {
        let policy = RenewalPolicy::default();
        let mut a = attempt(
            policy.max_transient_attempts,
            NaiveDate::from_ymd_opt(2031, 3, 31).unwrap(),
        );
        let scheduled = a.next_attempt_at;

        let outcome = a.mark_transient_failure("timeout".to_string(), &policy, at(2031, 3, 4));

        assert_eq!(outcome, TransientOutcome::GaveUp);
        assert_eq!(a.status, RenewalAttemptStatus::GivenUp);
        assert_eq!(a.next_attempt_at, scheduled);
    }

    #[test]
    fn permanent_decline_walks_the_dunning_schedule_then_exhausts() {
        let policy = RenewalPolicy {
            dunning_schedule_days: vec![1, 3],
            ..RenewalPolicy::default()
        };
        let mut a = attempt(1, NaiveDate::from_ymd_opt(2031, 3, 31).unwrap());

        let first = a.mark_permanent_decline("card_declined".to_string(), &policy);
        assert_eq!(
            first,
            DunningOutcome::Continue {
                dunning_attempt: 1,
                dunning_max: 2
            }
        );
        assert_eq!(a.status, RenewalAttemptStatus::FailedPermanent);
        assert_eq!(a.next_attempt_at, at(2031, 4, 1));

        let second = a.mark_permanent_decline("card_declined".to_string(), &policy);
        assert_eq!(
            second,
            DunningOutcome::Continue {
                dunning_attempt: 2,
                dunning_max: 2
            }
        );
        assert_eq!(a.next_attempt_at, at(2031, 4, 3));

        let third = a.mark_permanent_decline("card_declined".to_string(), &policy);
        assert_eq!(
            third,
            DunningOutcome::Exhausted {
                dunning_attempts: 2
            }
        );
        assert_eq!(a.next_attempt_at, at(2031, 4, 3));
    }

    #[test]
    fn requeue_now_refuses_a_succeeded_or_in_flight_attempt_and_requeues_the_rest() {
        let period_end = NaiveDate::from_ymd_opt(2031, 3, 31).unwrap();
        for status in [
            RenewalAttemptStatus::Succeeded,
            RenewalAttemptStatus::Charging,
        ] {
            let mut a = attempt(1, period_end);
            a.status = status;
            assert!(matches!(
                a.requeue_now().unwrap_err(),
                UserError::RenewalNotRetryable(_)
            ));
            assert_eq!(a.status, status);
        }

        let mut a = attempt(1, period_end);
        a.status = RenewalAttemptStatus::GivenUp;
        a.requeue_now().unwrap();
        assert_eq!(a.status, RenewalAttemptStatus::FailedRetryable);
    }
}
