use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::errors::UserError;
use super::events::DomainEvent;

#[derive(Debug, Clone)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
    pending_events: Vec<DomainEvent>,
}

impl User {
    pub fn new(email: String, password_hash: String) -> Result<Self, UserError> {
        if !email.contains('@') || email.is_empty() {
            return Err(UserError::InvalidEmail);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            email,
            password_hash,
            created_at: Utc::now(),
            pending_events: Vec::new(),
        })
    }

    pub fn from_persisted(
        id: Uuid,
        email: String,
        password_hash: String,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            email,
            password_hash,
            created_at,
            pending_events: Vec::new(),
        }
    }

    pub fn record_event(&mut self, event: DomainEvent) {
        self.pending_events.push(event);
    }

    pub fn pending_events(&self) -> &[DomainEvent] {
        &self.pending_events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let email = String::from("test@example.com");
        let password_hash = String::from("hashed_password");

        let user = User::new(email, password_hash).unwrap();
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.password_hash, "hashed_password");
    }

    #[test]
    fn test_invalid_email() {
        let email = String::from("invalid_email");
        let password_hash = String::from("hashed_password");

        let result = User::new(email, password_hash);
        assert!(result.is_err());
    }
}
