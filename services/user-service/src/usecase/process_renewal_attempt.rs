use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use crate::domain::{
    ChargeRequest, DomainEvent, DunningEmail, DunningOutcome, PaymentError, PaymentGateway,
    RenewalAttemptStatus, RenewalPolicy, SubscriptionCanceled, SubscriptionPaymentFailed,
    SubscriptionRenewed, SubscriptionStatus, TransientOutcome, UserError,
};
use crate::platform::port::{RenewalAttemptRepository, SubscriptionRepository, Transactor};

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
    db: Arc<dyn Transactor>,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    subscription_repository: Arc<dyn SubscriptionRepository>,
    payment_gateway: Arc<dyn PaymentGateway>,
    policy: RenewalPolicy,
}

impl ProcessRenewalAttemptUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
        subscription_repository: Arc<dyn SubscriptionRepository>,
        payment_gateway: Arc<dyn PaymentGateway>,
        policy: RenewalPolicy,
    ) -> Self {
        Self {
            db,
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
        let mut tx = self.db.begin().await?;
        let n = self
            .renewal_attempt_repository
            .reap_stale_charging(&mut tx, stale_after)
            .await?;
        tx.commit().await?;
        Ok(n)
    }

    /// Claim one due attempt, charge it, and record the outcome across the two
    /// transactions the design mandates (state change and payment call cannot
    /// share one txn).
    pub async fn execute(&self) -> Result<ProcessOutcome, UserError> {
        // --- TX1: claim + mark charging -----------------------------------
        let mut tx = self.db.begin().await?;
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
        tx.commit().await?;

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
        let mut tx = self.db.begin().await?;
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
                match fresh.mark_transient_failure(message, &self.policy, now) {
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

        tx.commit().await?;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ports::MockPaymentGateway;
    use crate::domain::{ChargeOutcome, ChargeRequest, RenewalAttempt, Subscription};
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::{MockRenewalAttemptRepository, MockSubscriptionRepository};
    use crate::usecase::fixtures;
    use chrono::{Duration, NaiveDate};

    #[derive(Clone, Copy)]
    enum Charge {
        Approved(&'static str),
        Declined(&'static str),
        Transient(&'static str),
    }

    #[derive(Clone, Copy)]
    enum Reread {
        Charging,
        Missing,
        Status(RenewalAttemptStatus),
    }

    struct Scenario {
        subscription: Subscription,
        claimed: Option<RenewalAttempt>,
        charge: Charge,
        reread: Reread,
        policy: RenewalPolicy,
        fail_mark_charging: bool,
        fail_write_outbox: bool,
    }

    impl Scenario {
        fn new(charge: Charge, attempt_count: i32, dunning_attempt_count: i32) -> Self {
            let subscription =
                fixtures::subscription(SubscriptionStatus::Active, fixtures::date(2031, 1, 31));
            let claimed = fixtures::attempt(
                &subscription,
                RenewalAttemptStatus::Pending,
                attempt_count,
                dunning_attempt_count,
            );
            Self {
                subscription,
                claimed: Some(claimed),
                charge,
                reread: Reread::Charging,
                policy: RenewalPolicy::default(),
                fail_mark_charging: false,
                fail_write_outbox: false,
            }
        }
    }

    struct Outcome {
        result: Result<ProcessOutcome, UserError>,
        log: CallLog,
        marked: Captured<RenewalAttempt>,
        charges: Captured<ChargeRequest>,
        settled: Captured<RenewalAttempt>,
        renewed: Captured<(Uuid, NaiveDate)>,
        statuses: Captured<(Uuid, SubscriptionStatus)>,
        events: Captured<DomainEvent>,
    }

    async fn run(scenario: Scenario) -> Outcome {
        let db = FakeDb::new();
        let log = db.log();
        let marked = Captured::default();
        let charges = Captured::default();
        let settled = Captured::default();
        let renewed = Captured::default();
        let statuses = Captured::default();
        let events = Captured::default();

        let mut attempts = MockRenewalAttemptRepository::new();
        let claim_log = log.clone();
        let claimed = scenario.claimed.clone();
        attempts.expect_claim_one_due().returning(move |_| {
            claim_log.note("claim_one_due");
            Ok(claimed.clone())
        });
        let (mark_log, mark_capture, mark_fails) =
            (log.clone(), marked.clone(), scenario.fail_mark_charging);
        attempts
            .expect_mark_charging()
            .returning(move |_, attempt| {
                mark_log.note("mark_charging");
                mark_capture.push(attempt.clone());
                if mark_fails {
                    Err(UserError::Repository("lock timeout".to_string()))
                } else {
                    Ok(())
                }
            });
        let (reread_log, reread_marked, reread) = (log.clone(), marked.clone(), scenario.reread);
        attempts
            .expect_find_by_id_for_update()
            .returning(move |_, _| {
                reread_log.note("find_by_id_for_update");
                let mut row = reread_marked.only();
                match reread {
                    Reread::Charging => Ok(Some(row)),
                    Reread::Missing => Ok(None),
                    Reread::Status(status) => {
                        row.status = status;
                        Ok(Some(row))
                    }
                }
            });
        let (settle_log, settle_capture) = (log.clone(), settled.clone());
        attempts.expect_settle().returning(move |_, attempt| {
            settle_log.note("settle");
            settle_capture.push(attempt.clone());
            Ok(())
        });
        let mut subscriptions = MockSubscriptionRepository::new();
        let (find_log, subscription) = (log.clone(), scenario.subscription.clone());
        subscriptions.expect_find_by_id().returning(move |_, _| {
            find_log.note("find_subscription");
            Ok(subscription.clone())
        });
        let (renew_log, renew_capture) = (log.clone(), renewed.clone());
        subscriptions
            .expect_renew_period()
            .returning(move |_, id, new_period_end| {
                renew_log.note("renew_period");
                renew_capture.push((id, new_period_end));
                Ok(())
            });
        let (status_log, status_capture) = (log.clone(), statuses.clone());
        subscriptions
            .expect_set_status()
            .returning(move |_, id, status| {
                status_log.note("set_status");
                status_capture.push((id, status));
                Ok(())
            });
        let (outbox_log, outbox_capture, outbox_fails) =
            (log.clone(), events.clone(), scenario.fail_write_outbox);
        subscriptions
            .expect_write_outbox()
            .returning(move |_, event| {
                outbox_log.note("write_outbox");
                outbox_capture.push(event.clone());
                if outbox_fails {
                    Err(UserError::Repository("serialization failure".to_string()))
                } else {
                    Ok(())
                }
            });

        let mut gateway = MockPaymentGateway::new();
        let (charge_log, charge_capture, charge) = (log.clone(), charges.clone(), scenario.charge);
        gateway.expect_charge().returning(move |request| {
            charge_log.note("charge");
            charge_capture.push(request);
            match charge {
                Charge::Approved(id) => Ok(ChargeOutcome {
                    provider_charge_id: id.to_string(),
                }),
                Charge::Declined(code) => Err(PaymentError::Declined {
                    code: code.to_string(),
                }),
                Charge::Transient(message) => Err(PaymentError::Transient(message.to_string())),
            }
        });

        let use_case = ProcessRenewalAttemptUseCase::new(
            db.transactor(),
            Arc::new(attempts),
            Arc::new(subscriptions),
            Arc::new(gateway),
            scenario.policy,
        );
        let result = use_case.execute().await;
        Outcome {
            result,
            log,
            marked,
            charges,
            settled,
            renewed,
            statuses,
            events,
        }
    }

    #[tokio::test]
    async fn an_approved_charge_renews_one_period_and_publishes_subscription_renewed() {
        let scenario = Scenario::new(Charge::Approved("ch_1"), 0, 0);
        let subscription = scenario.subscription.clone();
        let attempt = scenario.claimed.clone().unwrap();

        let outcome = run(scenario).await;

        assert!(matches!(
            outcome.result,
            Ok(ProcessOutcome::Renewed { subscription_id }) if subscription_id == subscription.id
        ));
        assert_eq!(
            outcome.log.calls(),
            [
                "begin",
                "claim_one_due",
                "find_subscription",
                "mark_charging",
                "commit",
                "charge",
                "begin",
                "find_by_id_for_update",
                "settle",
                "renew_period",
                "write_outbox",
                "commit"
            ]
        );
        let marked = outcome.marked.only();
        assert_eq!(marked.status, RenewalAttemptStatus::Charging);
        assert_eq!(marked.attempt_count, 1);
        let request = outcome.charges.only();
        assert_eq!(request.amount_minor, 1999);
        assert_eq!(request.currency, "USD");
        assert_eq!(request.payment_method_id, "pm_1");
        assert_eq!(request.idempotency_key, attempt.idempotency_key);
        assert_eq!(
            request.idempotency_key,
            format!("renew:{}:2031-01-31", subscription.id)
        );
        let settled = outcome.settled.only();
        assert_eq!(settled.status, RenewalAttemptStatus::Succeeded);
        assert_eq!(settled.provider_charge_id.as_deref(), Some("ch_1"));
        assert_eq!(settled.last_error, None);
        assert_eq!(
            outcome.renewed.only(),
            (subscription.id, fixtures::date(2031, 2, 28))
        );
        match outcome.events.only() {
            DomainEvent::SubscriptionRenewed(event) => {
                assert_eq!(event.subscription_id, subscription.id);
                assert_eq!(event.user_id, subscription.user_id);
                assert_eq!(event.plan_id, "pro");
                assert_eq!(event.renewal_attempt_id, attempt.id);
                assert_eq!(event.period_start, fixtures::date(2031, 1, 31));
                assert_eq!(event.new_period_end, fixtures::date(2031, 2, 28));
                assert_eq!(event.amount_minor, 1999);
                assert_eq!(event.currency, "USD");
                assert_eq!(event.provider_charge_id, "ch_1");
                assert_eq!(event.attempt_count, 1);
            }
            other => panic!("expected SubscriptionRenewed, got {other:?}"),
        }
        assert!(outcome.statuses.all().is_empty());
    }

    #[tokio::test]
    async fn the_first_decline_starts_dunning_and_hands_back_the_email_keyed_by_the_event() {
        let scenario = Scenario::new(Charge::Declined("insufficient_funds"), 0, 0);
        let subscription = scenario.subscription.clone();
        let attempt = scenario.claimed.clone().unwrap();

        let outcome = run(scenario).await;

        let settled = outcome.settled.only();
        assert_eq!(settled.status, RenewalAttemptStatus::FailedPermanent);
        assert_eq!(settled.dunning_attempt_count, 1);
        assert_eq!(
            settled.next_attempt_at,
            fixtures::date(2031, 1, 31)
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                + Duration::days(1)
        );
        assert_eq!(
            outcome.statuses.only(),
            (subscription.id, SubscriptionStatus::PastDue)
        );
        assert!(outcome.renewed.all().is_empty());
        let event = match outcome.events.only() {
            DomainEvent::SubscriptionPaymentFailed(event) => event,
            other => panic!("expected SubscriptionPaymentFailed, got {other:?}"),
        };
        assert_eq!(event.decline_code, "insufficient_funds");
        assert_eq!((event.dunning_attempt, event.dunning_max), (1, 4));
        assert_eq!(event.renewal_attempt_id, attempt.id);
        assert_eq!(event.period_end, fixtures::date(2031, 1, 31));
        assert_eq!(event.next_attempt_at, settled.next_attempt_at);
        match outcome.result {
            Ok(ProcessOutcome::Dunning { email }) => {
                assert_eq!(email.idempotency_key, event.event_id);
                assert_eq!(email.to_user_id, subscription.user_id);
                assert_eq!(email.subscription_id, subscription.id);
                assert_eq!(email.template, "dunning_card_declined");
                assert_eq!((email.dunning_attempt, email.dunning_max), (1, 4));
                assert_eq!((email.amount_minor, email.currency.as_str()), (1999, "USD"));
            }
            other => panic!("expected Dunning, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_decline_after_the_last_dunning_slot_cancels_the_subscription() {
        let scenario = Scenario::new(Charge::Declined("card_declined"), 1, 4);
        let subscription_id = scenario.subscription.id;

        let outcome = run(scenario).await;

        assert!(matches!(
            outcome.result,
            Ok(ProcessOutcome::Canceled { subscription_id: id }) if id == subscription_id
        ));
        let settled = outcome.settled.only();
        assert_eq!(settled.status, RenewalAttemptStatus::FailedPermanent);
        assert_eq!(settled.dunning_attempt_count, 4);
        assert_eq!(
            outcome.statuses.only(),
            (subscription_id, SubscriptionStatus::Canceled)
        );
        match outcome.events.only() {
            DomainEvent::SubscriptionCanceled(event) => {
                assert_eq!(event.subscription_id, subscription_id);
                assert_eq!(event.reason, "dunning_exhausted");
                assert_eq!(event.dunning_attempts, 4);
            }
            other => panic!("expected SubscriptionCanceled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transient_failures_back_off_then_give_up_without_publishing() {
        let before = Utc::now();
        let retry = run(Scenario::new(
            Charge::Transient("provider returned 503"),
            0,
            0,
        ))
        .await;
        let after = Utc::now();

        assert!(matches!(
            retry.result,
            Ok(ProcessOutcome::RetryScheduled { .. })
        ));
        let settled = retry.settled.only();
        assert_eq!(settled.status, RenewalAttemptStatus::FailedRetryable);
        assert_eq!(settled.last_error.as_deref(), Some("provider returned 503"));
        assert!(settled.next_attempt_at >= before + Duration::hours(6));
        assert!(settled.next_attempt_at <= after + Duration::hours(6) + Duration::seconds(1));
        assert!(retry.statuses.all().is_empty());
        assert!(retry.events.all().is_empty());

        let scenario = Scenario::new(Charge::Transient("timeout"), 2, 0);
        let subscription_id = scenario.subscription.id;
        let gave_up = run(scenario).await;

        assert!(matches!(
            gave_up.result,
            Ok(ProcessOutcome::GaveUp { subscription_id: id }) if id == subscription_id
        ));
        assert_eq!(gave_up.settled.only().status, RenewalAttemptStatus::GivenUp);
        assert_eq!(
            gave_up.statuses.only(),
            (subscription_id, SubscriptionStatus::PastDue)
        );
        assert!(gave_up.events.all().is_empty());
    }

    #[tokio::test]
    async fn nothing_due_and_a_raced_row_record_nothing_and_a_failed_write_never_commits() {
        let mut idle = Scenario::new(Charge::Approved("ch_1"), 0, 0);
        idle.claimed = None;
        let idle = run(idle).await;
        assert!(matches!(idle.result, Ok(ProcessOutcome::NothingDue)));
        assert_eq!(idle.log.calls(), ["begin", "claim_one_due", "rollback"]);

        for reread in [
            Reread::Missing,
            Reread::Status(RenewalAttemptStatus::FailedRetryable),
        ] {
            let mut raced = Scenario::new(Charge::Approved("ch_1"), 0, 0);
            raced.reread = reread;
            let raced = run(raced).await;

            assert!(matches!(raced.result, Ok(ProcessOutcome::Skipped)));
            assert_eq!(raced.charges.all().len(), 1);
            assert!(raced.settled.all().is_empty());
            assert!(raced.events.all().is_empty());
            assert_eq!(
                raced.log.calls().last().map(String::as_str),
                Some("rollback")
            );
        }

        let mut failing = Scenario::new(Charge::Approved("ch_1"), 0, 0);
        failing.fail_write_outbox = true;
        let failing = run(failing).await;
        assert!(
            matches!(failing.result, Err(UserError::Repository(m)) if m == "serialization failure")
        );
        let commits = failing
            .log
            .calls()
            .iter()
            .filter(|c| *c == "commit")
            .count();
        assert_eq!(commits, 1, "only the claim transaction may have committed");
    }
}
