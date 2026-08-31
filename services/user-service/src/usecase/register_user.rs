use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::{DomainEvent, PasswordHasher, User, UserCreated, UserError};
use crate::platform::port::UserRepository;

pub struct RegisterUserUseCase {
    db_pool: PgPool,
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
}

impl RegisterUserUseCase {
    pub fn new(
        db_pool: PgPool,
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
    ) -> Self {
        Self {
            db_pool,
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

        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        if self
            .user_repository
            .find_by_email(&mut tx, &user.email)
            .await?
            .is_some()
        {
            return Err(UserError::EmailAlreadyExists);
        }
        self.user_repository.create(&mut tx, &user).await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(user)
    }
}
