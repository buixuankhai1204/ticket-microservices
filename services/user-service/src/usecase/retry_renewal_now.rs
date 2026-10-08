use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{RenewalAttempt, UserError};
use crate::platform::port::{RenewalAttemptRepository, SubscriptionRepository, Transactor};

pub struct RetryRenewalNowUseCase {
    db: Arc<dyn Transactor>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
}

impl RetryRenewalNowUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    ) -> Self {
        Self {
            db,
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
        let mut tx = self.db.begin().await?;

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

        tx.commit().await?;
        Ok(attempt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{RenewalAttemptStatus, Subscription, SubscriptionStatus};
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::{MockRenewalAttemptRepository, MockSubscriptionRepository};
    use crate::usecase::fixtures;

    struct Outcome {
        result: Result<RenewalAttempt, UserError>,
        log: CallLog,
        created: Captured<RenewalAttempt>,
        updated: Captured<RenewalAttempt>,
    }

    async fn retry(
        subscription: Option<Subscription>,
        existing: Option<RenewalAttempt>,
    ) -> Outcome {
        let db = FakeDb::new();
        let log = db.log();
        let (created, updated) = (Captured::default(), Captured::default());
        let id = subscription
            .as_ref()
            .map(|s| s.id)
            .unwrap_or_else(Uuid::new_v4);
        let owner = subscription
            .as_ref()
            .map(|s| s.user_id)
            .unwrap_or_else(Uuid::new_v4);

        let mut subscriptions = MockSubscriptionRepository::new();
        let find_log = log.clone();
        subscriptions
            .expect_find_by_id_for_user()
            .returning(move |_, _, _| {
                find_log.note("find_by_id_for_user");
                subscription.clone().ok_or(UserError::NotFound)
            });

        let mut attempts = MockRenewalAttemptRepository::new();
        let lookup_log = log.clone();
        attempts
            .expect_find_for_period_for_update()
            .returning(move |_, _, _| {
                lookup_log.note("find_for_period_for_update");
                Ok(existing.clone())
            });
        let (create_log, create_capture) = (log.clone(), created.clone());
        attempts.expect_create().returning(move |_, attempt| {
            create_log.note("create");
            create_capture.push(attempt.clone());
            Ok(())
        });
        let (update_log, update_capture) = (log.clone(), updated.clone());
        attempts
            .expect_update_schedule()
            .returning(move |_, attempt| {
                update_log.note("update_schedule");
                update_capture.push(attempt.clone());
                Ok(())
            });

        let use_case = RetryRenewalNowUseCase::new(
            db.transactor(),
            Arc::new(subscriptions),
            Arc::new(attempts),
        );
        let result = use_case.execute(id, owner).await;
        Outcome {
            result,
            log,
            created,
            updated,
        }
    }

    #[tokio::test]
    async fn queues_a_new_attempt_for_the_current_period_when_none_exists() {
        let subscription =
            fixtures::subscription(SubscriptionStatus::PastDue, fixtures::date(2031, 3, 31));

        let outcome = retry(Some(subscription.clone()), None).await;

        let queued = outcome.result.unwrap();
        assert_eq!(
            outcome.log.calls(),
            [
                "begin",
                "find_by_id_for_user",
                "find_for_period_for_update",
                "create",
                "commit"
            ]
        );
        let stored = outcome.created.only();
        assert_eq!(stored.id, queued.id);
        assert_eq!(stored.subscription_id, subscription.id);
        assert_eq!(stored.period_end, fixtures::date(2031, 3, 31));
        assert_eq!(stored.status, RenewalAttemptStatus::FailedRetryable);
        assert_eq!(stored.attempt_count, 0);
        assert_eq!(
            stored.idempotency_key,
            format!("renew:{}:2031-03-31", subscription.id)
        );
    }

    #[tokio::test]
    async fn requeues_an_existing_attempt_instead_of_creating_a_second_row() {
        let subscription =
            fixtures::subscription(SubscriptionStatus::PastDue, fixtures::date(2031, 3, 31));
        let given_up = fixtures::attempt(&subscription, RenewalAttemptStatus::GivenUp, 3, 0);

        let outcome = retry(Some(subscription), Some(given_up.clone())).await;

        assert_eq!(outcome.result.unwrap().id, given_up.id);
        assert!(outcome.created.all().is_empty());
        assert_eq!(
            outcome.updated.only().status,
            RenewalAttemptStatus::FailedRetryable
        );
        assert!(outcome.log.committed());

        let stranger = retry(None, None).await;
        assert!(matches!(stranger.result, Err(UserError::NotFound)));
        assert_eq!(stranger.log.calls(), ["begin", "find_by_id_for_user"]);
    }
}
