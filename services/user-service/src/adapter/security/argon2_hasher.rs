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
    fn hash_produces_a_phc_string_that_verifies_for_the_same_password() {
        let hasher = Argon2PasswordHasher::new();

        let hash = hasher.hash("correct horse battery staple").unwrap();

        assert!(hash.starts_with("$argon2"));
        assert!(PasswordHash::new(&hash).is_ok());
        assert!(hasher
            .verify("correct horse battery staple", &hash)
            .unwrap());
    }

    #[test]
    fn verify_is_false_for_a_wrong_password() {
        let hasher = Argon2PasswordHasher::new();
        let hash = hasher.hash("correct horse battery staple").unwrap();

        assert!(!hasher.verify("Tr0ub4dor&3", &hash).unwrap());
    }

    #[test]
    fn verify_errors_on_a_malformed_hash() {
        let hasher = Argon2PasswordHasher::new();

        let result = hasher.verify("whatever", "not-a-phc-string");

        assert!(matches!(result, Err(UserError::Hashing(_))));
    }

    #[test]
    fn hash_uses_a_fresh_salt_per_call_so_digests_differ() {
        let hasher = Argon2PasswordHasher::new();

        let first = hasher.hash("same password").unwrap();
        let second = hasher.hash("same password").unwrap();

        assert_ne!(first, second);
    }
}
