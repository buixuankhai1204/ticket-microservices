use std::sync::Arc;

use crate::domain::{DomainEvent, PasswordHasher, User, UserCreated, UserError, UserRepository};

pub struct RegisterUserUseCase {
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
}

impl RegisterUserUseCase {
    pub fn new(
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
    ) -> Self {
        Self {
            user_repository,
            password_hasher,
        }
    }

    pub async fn execute(&self, email: String, password: String) -> Result<User, UserError> {
        if self.user_repository.find_by_email(&email).await?.is_some() {
            return Err(UserError::EmailAlreadyExists);
        }

        let password_hash = self.password_hasher.hash(&password)?;
        let mut user = User::new(email, password_hash)?;

        // This use case announces its own state change (choreography saga): the
        // `UserCreated` event rides the transactional outbox, written by
        // `create` in the same transaction as the user row.
        user.record_event(DomainEvent::UserCreated(UserCreated::new(
            user.id,
            user.email.clone(),
            user.created_at,
        )));

        self.user_repository.create(&user).await?;
        Ok(user)
    }
}
