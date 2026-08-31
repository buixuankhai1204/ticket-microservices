use uuid::Uuid;

use super::errors::UserError;

pub trait PasswordHasher: Send + Sync {
    fn hash(&self, password: &str) -> Result<String, UserError>;
    fn verify(&self, password: &str, hash: &str) -> Result<bool, UserError>;
}

pub trait TokenIssuer: Send + Sync {
    fn issue(&self, user_id: Uuid, email: &str) -> Result<String, UserError>;
}
