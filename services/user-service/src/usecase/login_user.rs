use std::sync::Arc;

use crate::domain::{PasswordHasher, TokenIssuer, UserError, UserRepository};

pub struct LoginUserUseCase {
    user_repository: Arc<dyn UserRepository>,
    password_hasher: Arc<dyn PasswordHasher>,
    token_issuer: Arc<dyn TokenIssuer>,
}

impl LoginUserUseCase {
    pub fn new(
        user_repository: Arc<dyn UserRepository>,
        password_hasher: Arc<dyn PasswordHasher>,
        token_issuer: Arc<dyn TokenIssuer>,
    ) -> Self {
        Self {
            user_repository,
            password_hasher,
            token_issuer,
        }
    }

    pub async fn execute(&self, email: String, password: String) -> Result<String, UserError> {
        let user = self
            .user_repository
            .find_by_email(&email)
            .await?
            .ok_or(UserError::InvalidCredentials)?;

        if !self
            .password_hasher
            .verify(&password, &user.password_hash)?
        {
            return Err(UserError::InvalidCredentials);
        }

        self.token_issuer.issue(user.id, &user.email)
    }
}
