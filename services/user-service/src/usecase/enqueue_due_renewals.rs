use std::sync::Arc;

use crate::domain::UserError;
use crate::platform::port::{RenewalAttemptRepository, Transactor};

const ENQUEUE_LOCK_KEY: &str = "renewal-enqueue";

#[derive(Debug)]
pub enum EnqueueOutcome {
    LockNotHeld,
    Enqueued(u64),
}

pub struct EnqueueDueRenewalsUseCase {
    db: Arc<dyn Transactor>,
    renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
}

impl EnqueueDueRenewalsUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        renewal_attempt_repository: Arc<dyn RenewalAttemptRepository>,
    ) -> Self {
        Self {
            db,
            renewal_attempt_repository,
        }
    }

    pub async fn execute(&self) -> Result<EnqueueOutcome, UserError> {
        let mut conn = self.db.acquire().await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::MockRenewalAttemptRepository;

    struct Scripted {
        lock_free: bool,
        enqueue: Result<u64, &'static str>,
        release: Result<(), &'static str>,
    }

    fn attempts(
        log: &CallLog,
        keys: &Captured<String>,
        script: Scripted,
    ) -> MockRenewalAttemptRepository {
        let mut attempts = MockRenewalAttemptRepository::new();
        let (lock_log, lock_keys) = (log.clone(), keys.clone());
        attempts
            .expect_try_advisory_lock()
            .returning(move |_, key| {
                lock_log.note("try_advisory_lock");
                lock_keys.push(key.to_string());
                Ok(script.lock_free)
            });
        let enqueue_log = log.clone();
        attempts.expect_enqueue_due().returning(move |_| {
            enqueue_log.note("enqueue_due");
            script
                .enqueue
                .map_err(|m| UserError::Repository(m.to_string()))
        });
        let release_log = log.clone();
        attempts
            .expect_release_advisory_lock()
            .returning(move |_, _| {
                release_log.note("release_advisory_lock");
                script
                    .release
                    .map_err(|m| UserError::Repository(m.to_string()))
            });
        attempts
    }

    async fn run(
        script: Scripted,
    ) -> (Result<EnqueueOutcome, UserError>, Vec<String>, Vec<String>) {
        let db = FakeDb::new();
        let log = db.log();
        let keys = Captured::default();
        let use_case = EnqueueDueRenewalsUseCase::new(
            db.transactor(),
            Arc::new(attempts(&log, &keys, script)),
        );
        let result = use_case.execute().await;
        (result, log.calls(), keys.all())
    }

    #[tokio::test]
    async fn sweeps_under_the_advisory_lock_and_always_releases_it() {
        let (result, calls, keys) = run(Scripted {
            lock_free: true,
            enqueue: Ok(3),
            release: Ok(()),
        })
        .await;
        assert!(matches!(result, Ok(EnqueueOutcome::Enqueued(3))));
        assert_eq!(
            calls,
            [
                "acquire",
                "try_advisory_lock",
                "enqueue_due",
                "release_advisory_lock"
            ]
        );
        assert_eq!(keys, ["renewal-enqueue"]);

        let (result, calls, _) = run(Scripted {
            lock_free: false,
            enqueue: Ok(3),
            release: Ok(()),
        })
        .await;
        assert!(matches!(result, Ok(EnqueueOutcome::LockNotHeld)));
        assert_eq!(calls, ["acquire", "try_advisory_lock"]);

        let (result, calls, _) = run(Scripted {
            lock_free: true,
            enqueue: Err("deadlock detected"),
            release: Ok(()),
        })
        .await;
        assert!(matches!(result, Err(UserError::Repository(m)) if m == "deadlock detected"));
        assert_eq!(
            calls.last().map(String::as_str),
            Some("release_advisory_lock")
        );
    }
}
