use std::sync::Arc;

use chrono::Utc;

use crate::domain::{DomainEvent, PasswordHasher, TokenIssuer, UserError, UserLoggedIn};
use crate::platform::port::{Transactor, UserRepository};

pub struct LoginUserUseCase {
    db: Arc<dyn Transactor>,
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
    token_issuer: Arc<dyn TokenIssuer>,
}

impl LoginUserUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
        token_issuer: Arc<dyn TokenIssuer>,
    ) -> Self {
        Self {
            db,
            user_repository,
            password_hasher,
            token_issuer,
        }
    }

    pub async fn execute(&self, email: String, password: String) -> Result<String, UserError> {
        let user = {
            let mut tx = self.db.begin_read_only().await?;
            let found = self.user_repository.find_by_email(&mut tx, &email).await?;
            tx.commit().await?;
            found
        }
        .ok_or(UserError::InvalidCredentials)?;

        if !self
            .password_hasher
            .verify(&password, &user.password_hash)?
        {
            return Err(UserError::InvalidCredentials);
        }

        let token = self.token_issuer.issue(user.id, &user.email)?;

        let event =
            DomainEvent::UserLoggedIn(UserLoggedIn::new(user.id, user.email.clone(), Utc::now()));
        let mut tx = self.db.begin().await?;
        self.user_repository.write_outbox(&mut tx, &event).await?;
        tx.commit().await?;

        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ports::{MockPasswordHasher, MockTokenIssuer};
    use crate::domain::User;
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::MockUserRepository;
    use crate::usecase::fixtures;

    struct Harness {
        db: FakeDb,
        log: CallLog,
        users: MockUserRepository,
        hasher: MockPasswordHasher,
        issuer: MockTokenIssuer,
        outbox: Captured<DomainEvent>,
    }

    impl Harness {
        fn new(stored: Option<User>, password_matches: bool) -> Self {
            let db = FakeDb::new();
            let log = db.log();
            let mut users = MockUserRepository::new();
            let find_log = log.clone();
            users.expect_find_by_email().returning(move |_, _| {
                find_log.note("find_by_email");
                Ok(stored.clone())
            });
            let mut hasher = MockPasswordHasher::new();
            let verify_log = log.clone();
            hasher.expect_verify().returning(move |_, _| {
                verify_log.note("verify");
                Ok(password_matches)
            });
            let mut issuer = MockTokenIssuer::new();
            let issue_log = log.clone();
            issuer.expect_issue().returning(move |_, _| {
                issue_log.note("issue");
                Ok("signed.jwt.token".to_string())
            });
            Self {
                db,
                log,
                users,
                hasher,
                issuer,
                outbox: Captured::default(),
            }
        }

        fn recording_outbox(&mut self) {
            let (log, outbox) = (self.log.clone(), self.outbox.clone());
            self.users.expect_write_outbox().returning(move |_, event| {
                log.note("write_outbox");
                outbox.push(event.clone());
                Ok(())
            });
        }

        async fn login(self) -> (Result<String, UserError>, CallLog, Captured<DomainEvent>) {
            let use_case = LoginUserUseCase::new(
                self.db.transactor(),
                Arc::new(self.users),
                Arc::new(self.hasher),
                Arc::new(self.issuer),
            );
            let result = use_case
                .execute("ada@example.com".to_string(), "s3cret".to_string())
                .await;
            (result, self.log, self.outbox)
        }
    }

    #[tokio::test]
    async fn a_valid_login_reads_then_issues_a_token_then_publishes_user_logged_in() {
        let user = fixtures::user("ada@example.com");
        let mut harness = Harness::new(Some(user.clone()), true);
        harness.recording_outbox();

        let (token, log, outbox) = harness.login().await;

        assert_eq!(token.unwrap(), "signed.jwt.token");
        assert_eq!(
            log.calls(),
            [
                "begin_read_only",
                "find_by_email",
                "commit",
                "verify",
                "issue",
                "begin",
                "write_outbox",
                "commit"
            ]
        );
        match outbox.only() {
            DomainEvent::UserLoggedIn(event) => {
                assert_eq!(event.user_id, user.id);
                assert_eq!(event.email, "ada@example.com");
            }
            other => panic!("expected UserLoggedIn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unknown_email_and_a_wrong_password_are_the_same_error_and_publish_nothing() {
        let unknown = Harness::new(None, true);
        let (result, log, outbox) = unknown.login().await;
        assert!(matches!(result, Err(UserError::InvalidCredentials)));
        assert_eq!(log.calls(), ["begin_read_only", "find_by_email", "commit"]);
        assert!(outbox.all().is_empty());

        let wrong_password = Harness::new(Some(fixtures::user("ada@example.com")), false);
        let (result, log, outbox) = wrong_password.login().await;
        assert!(matches!(result, Err(UserError::InvalidCredentials)));
        assert_eq!(
            log.calls(),
            ["begin_read_only", "find_by_email", "commit", "verify"]
        );
        assert!(outbox.all().is_empty());
    }
}
