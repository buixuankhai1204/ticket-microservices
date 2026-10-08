use std::sync::Arc;

use crate::domain::{DunningEmail, EmailGateway, UserError};
use crate::platform::port::{RenewalAttemptRepository, Transactor};

/// Step 4 of the renewal flow (docs/sagas/renewal-subscriptions.md §2 / §5.9):
/// after a `SubscriptionPaymentFailed` is committed, send the "update your card"
/// email. Best-effort — a send failure is logged and left for the next dunning
/// pass; the `sent_emails` ledger keeps it to one email per dunning event.
pub struct SendDunningEmailUseCase {
    db: Arc<dyn Transactor>,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    email_gateway: Arc<dyn EmailGateway>,
}

impl SendDunningEmailUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
        email_gateway: Arc<dyn EmailGateway>,
    ) -> Self {
        Self {
            db,
            renewal_attempt_repository,
            email_gateway,
        }
    }

    pub async fn execute(&self, email: DunningEmail) -> Result<(), UserError> {
        {
            let mut tx = self.db.begin_read_only().await?;
            let already = self
                .renewal_attempt_repository
                .dunning_email_recorded(&mut tx, email.idempotency_key)
                .await?;
            tx.commit().await?;
            if already {
                return Ok(());
            }
        }

        match self.email_gateway.send(email.clone()).await {
            Ok(()) => {
                let mut tx = self.db.begin().await?;
                self.renewal_attempt_repository
                    .record_dunning_email(
                        &mut tx,
                        email.idempotency_key,
                        email.to_user_id,
                        &email.template,
                    )
                    .await?;
                tx.commit().await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ports::MockEmailGateway;
    use crate::domain::EmailError;
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::MockRenewalAttemptRepository;
    use chrono::NaiveDate;
    use uuid::Uuid;

    fn dunning_email() -> DunningEmail {
        DunningEmail {
            to_user_id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            template: "dunning_card_declined".to_string(),
            idempotency_key: Uuid::new_v4(),
            period_end: NaiveDate::from_ymd_opt(2031, 3, 31).unwrap(),
            amount_minor: 1999,
            currency: "USD".to_string(),
            dunning_attempt: 1,
            dunning_max: 4,
        }
    }

    struct Scripted {
        already_sent: bool,
        send_fails: bool,
        record_fails: bool,
    }

    async fn send(
        script: Scripted,
        email: &DunningEmail,
    ) -> (
        Result<(), UserError>,
        CallLog,
        Captured<(Uuid, Uuid, String)>,
    ) {
        let db = FakeDb::new();
        let log = db.log();
        let recorded = Captured::default();
        let mut attempts = MockRenewalAttemptRepository::new();
        let check_log = log.clone();
        attempts
            .expect_dunning_email_recorded()
            .returning(move |_, _| {
                check_log.note("dunning_email_recorded");
                Ok(script.already_sent)
            });
        let (record_log, record_capture) = (log.clone(), recorded.clone());
        attempts
            .expect_record_dunning_email()
            .returning(move |_, event_id, user_id, template| {
                record_log.note("record_dunning_email");
                record_capture.push((event_id, user_id, template.to_string()));
                if script.record_fails {
                    Err(UserError::Repository("deadlock detected".to_string()))
                } else {
                    Ok(())
                }
            });
        let mut gateway = MockEmailGateway::new();
        let send_log = log.clone();
        gateway.expect_send().returning(move |_| {
            send_log.note("send");
            if script.send_fails {
                Err(EmailError("provider returned 500".to_string()))
            } else {
                Ok(())
            }
        });
        let use_case =
            SendDunningEmailUseCase::new(db.transactor(), Arc::new(attempts), Arc::new(gateway));
        let result = use_case.execute(email.clone()).await;
        (result, log, recorded)
    }

    #[tokio::test]
    async fn checks_the_ledger_then_sends_outside_any_transaction_then_records_the_send() {
        let email = dunning_email();

        let (result, log, recorded) = send(
            Scripted {
                already_sent: false,
                send_fails: false,
                record_fails: false,
            },
            &email,
        )
        .await;

        result.unwrap();
        assert_eq!(
            log.calls(),
            [
                "begin_read_only",
                "dunning_email_recorded",
                "commit",
                "send",
                "begin",
                "record_dunning_email",
                "commit"
            ]
        );
        assert_eq!(
            recorded.only(),
            (
                email.idempotency_key,
                email.to_user_id,
                "dunning_card_declined".to_string()
            )
        );
    }

    #[tokio::test]
    async fn a_recorded_email_is_not_sent_again_and_a_failed_send_leaves_no_ledger_row() {
        let (result, log, _) = send(
            Scripted {
                already_sent: true,
                send_fails: false,
                record_fails: false,
            },
            &dunning_email(),
        )
        .await;
        result.unwrap();
        assert_eq!(
            log.calls(),
            ["begin_read_only", "dunning_email_recorded", "commit"]
        );

        let (result, log, recorded) = send(
            Scripted {
                already_sent: false,
                send_fails: true,
                record_fails: false,
            },
            &dunning_email(),
        )
        .await;
        result.unwrap();
        assert!(recorded.all().is_empty());
        assert_eq!(log.calls().last().map(String::as_str), Some("send"));
    }
}
