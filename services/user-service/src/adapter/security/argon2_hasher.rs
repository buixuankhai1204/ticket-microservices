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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_verify() {
        let hasher = Argon2PasswordHasher::new();
        let password = "my_secure_password";
        let hash = hasher.hash(password).expect("Hashing failed");
        assert!(hasher.verify(password, &hash).expect("Verification failed"));
        assert!(!hasher
            .verify("wrong_password", &hash)
            .expect("Verification failed"));
    }

    #[test]
    fn test_hash_uniqueness() {
        let hasher = Argon2PasswordHasher::new();
        let password = "my_secure_password";
        let hash1 = hasher.hash(password).expect("Hashing failed");
        let hash2 = hasher.hash(password).expect("Hashing failed");
        assert_ne!(
            hash1, hash2,
            "Hashes should be unique due to different salts"
        );
    }
}
