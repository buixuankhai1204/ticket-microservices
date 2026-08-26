use std::sync::Arc;

use crate::domain::{PasswordHasher, User, UserError, UserRepository};

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
        let user = User::new(email, password_hash)?;
        self.user_repository.create(&user).await?;
        Ok(user)
    }
}
