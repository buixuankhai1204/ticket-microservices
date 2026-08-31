use std::sync::Arc;

use chrono::Utc;
use sqlx::PgPool;

use super::tx_err;
use crate::domain::{DomainEvent, PasswordHasher, TokenIssuer, UserError, UserLoggedIn};
use crate::platform::port::UserRepository;

pub struct LoginUserUseCase {
    db_pool: PgPool,
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
    token_issuer: Arc<dyn TokenIssuer>,
}

impl LoginUserUseCase {
    pub fn new(
        db_pool: PgPool,
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
        token_issuer: Arc<dyn TokenIssuer>,
    ) -> Self {
        Self {
            db_pool,
            user_repository,
            password_hasher,
            token_issuer,
        }
    }

    pub async fn execute(&self, email: String, password: String) -> Result<String, UserError> {
        let user = {
            let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(tx_err)?;
            let found = self.user_repository.find_by_email(&mut tx, &email).await?;
            tx.commit().await.map_err(tx_err)?;
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
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        self.user_repository.write_outbox(&mut tx, &event).await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(token)
    }
}
