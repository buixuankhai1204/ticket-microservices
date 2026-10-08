use std::sync::Arc;

use crate::domain::{DomainEvent, PasswordHasher, User, UserCreated, UserError};
use crate::platform::port::{Transactor, UserRepository};

pub struct RegisterUserUseCase {
    db: Arc<dyn Transactor>,
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
}

impl RegisterUserUseCase {
    pub fn new(
        db: Arc<dyn Transactor>,
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
    ) -> Self {
        Self {
            db,
            user_repository,
            password_hasher,
        }
    }

    pub async fn execute(&self, email: String, password: String) -> Result<User, UserError> {
        let password_hash = self.password_hasher.hash(&password)?;
        let mut user = User::new(email, password_hash)?;

        user.record_event(DomainEvent::UserCreated(UserCreated::new(
            user.id,
            user.email.clone(),
            user.created_at,
        )));

        let mut tx = self.db.begin().await?;
        if self
            .user_repository
            .find_by_email(&mut tx, &user.email)
            .await?
            .is_some()
        {
            return Err(UserError::EmailAlreadyExists);
        }
        self.user_repository.create(&mut tx, &user).await?;
        tx.commit().await?;

        Ok(user)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ports::MockPasswordHasher;
    use crate::platform::port::testing::{CallLog, Captured, FakeDb};
    use crate::platform::port::MockUserRepository;
    use crate::usecase::fixtures;

    fn hasher(log: &CallLog) -> MockPasswordHasher {
        let log = log.clone();
        let mut hasher = MockPasswordHasher::new();
        hasher.expect_hash().returning(move |password| {
            log.note("hash");
            Ok(format!("hashed:{password}"))
        });
        hasher
    }

    fn users(log: &CallLog, existing: Option<User>) -> MockUserRepository {
        let mut users = MockUserRepository::new();
        let find_log = log.clone();
        users.expect_find_by_email().returning(move |_, _| {
            find_log.note("find_by_email");
            Ok(existing.clone())
        });
        users
    }

    fn use_case(
        db: &FakeDb,
        users: MockUserRepository,
        hasher: MockPasswordHasher,
    ) -> RegisterUserUseCase {
        RegisterUserUseCase::new(db.transactor(), Arc::new(users), Arc::new(hasher))
    }

    #[tokio::test]
    async fn registers_a_user_hashing_before_the_transaction_and_creating_inside_it() {
        let db = FakeDb::new();
        let log = db.log();
        let mut users = users(&log, None);
        let created = Captured::default();
        let (create_log, create_capture) = (log.clone(), created.clone());
        users.expect_create().returning(move |_, user| {
            create_log.note("create");
            create_capture.push(user.clone());
            Ok(())
        });

        let user = use_case(&db, users, hasher(&log))
            .execute("ada@example.com".to_string(), "s3cret".to_string())
            .await
            .unwrap();

        assert_eq!(
            log.calls(),
            ["hash", "begin", "find_by_email", "create", "commit"]
        );
        assert_eq!(user.email, "ada@example.com");
        assert_eq!(user.password_hash, "hashed:s3cret");
        let stored = created.only();
        assert_eq!(stored.id, user.id);
        match stored.pending_events() {
            [DomainEvent::UserCreated(event)] => {
                assert_eq!(event.user_id, user.id);
                assert_eq!(event.email, "ada@example.com");
                assert_eq!(event.created_at, user.created_at);
            }
            other => panic!("expected exactly one UserCreated event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejections_never_commit_and_return_the_error_unchanged() {
        let db = FakeDb::new();
        let log = db.log();
        let invalid = use_case(&db, MockUserRepository::new(), hasher(&log))
            .execute("not-an-email".to_string(), "s3cret".to_string())
            .await;
        assert!(matches!(invalid, Err(UserError::InvalidEmail)));
        assert!(!log.calls().iter().any(|c| c == "begin"));

        let mut taken = users(&log, Some(fixtures::user("ada@example.com")));
        taken.expect_create().never();
        let duplicate = use_case(&db, taken, hasher(&log))
            .execute("ada@example.com".to_string(), "s3cret".to_string())
            .await;
        assert!(matches!(duplicate, Err(UserError::EmailAlreadyExists)));

        let mut broken = users(&log, None);
        broken
            .expect_create()
            .returning(|_, _| Err(UserError::Repository("deadlock detected".to_string())));
        let failed = use_case(&db, broken, hasher(&log))
            .execute("grace@example.com".to_string(), "s3cret".to_string())
            .await;
        assert!(matches!(failed, Err(UserError::Repository(m)) if m == "deadlock detected"));
        assert!(!log.committed());
    }
}
