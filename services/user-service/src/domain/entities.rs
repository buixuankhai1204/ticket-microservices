use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::errors::UserError;

#[derive(Debug, Clone)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}

impl User {
    /// Constructs a new user, enforcing the email invariant at the point of creation
    /// rather than leaving callers to remember to validate it.
    pub fn new(email: String, password_hash: String) -> Result<Self, UserError> {
        if !email.contains('@') || email.is_empty() {
            return Err(UserError::InvalidEmail);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            email,
            password_hash,
            created_at: Utc::now(),
        })
    }
}
