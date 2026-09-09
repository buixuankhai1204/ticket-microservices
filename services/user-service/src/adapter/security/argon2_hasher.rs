use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher as _, PasswordVerifier, SaltString};
use argon2::Argon2;

use crate::domain::{PasswordHasher, UserError};

#[derive(Default)]
pub struct Argon2PasswordHasher {
    argon2: Argon2<'static>,
}

impl Argon2PasswordHasher {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PasswordHasher for Argon2PasswordHasher {
    fn hash(&self, password: &str) -> Result<String, UserError> {
        let salt = SaltString::generate(&mut OsRng);
        self.argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|e| UserError::Hashing(e.to_string()))
    }

    fn verify(&self, password: &str, hash: &str) -> Result<bool, UserError> {
        let parsed_hash = PasswordHash::new(hash).map_err(|e| UserError::Hashing(e.to_string()))?;
        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }
}
