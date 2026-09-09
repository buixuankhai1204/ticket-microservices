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
    use crate::domain::events::UserCreated;
    use chrono::TimeZone;

    #[test]
    fn new_accepts_a_valid_email_and_mints_a_v4_id() {
        let user = User::new("alice@example.com".to_string(), "phc-hash".to_string()).unwrap();

        assert!(!user.id.is_nil());
        assert_eq!(user.id.get_version_num(), 4);
        assert_eq!(user.email, "alice@example.com");
        assert_eq!(user.password_hash, "phc-hash");
        assert!(user.pending_events().is_empty());
    }

    #[test]
    fn new_rejects_an_email_without_an_at_sign() {
        let err = User::new("not-an-email".to_string(), "phc-hash".to_string()).unwrap_err();
        assert!(matches!(err, UserError::InvalidEmail));
    }

    #[test]
    fn from_persisted_passes_every_field_through_unchanged() {
        let id = Uuid::new_v4();
        let created_at = Utc.with_ymd_and_hms(2031, 3, 4, 5, 6, 7).unwrap();

        let user = User::from_persisted(
            id,
            "bob@example.com".to_string(),
            "stored-hash".to_string(),
            created_at,
        );

        assert_eq!(user.id, id);
        assert_eq!(user.email, "bob@example.com");
        assert_eq!(user.password_hash, "stored-hash");
        assert_eq!(user.created_at, created_at);
        assert!(user.pending_events().is_empty());
    }

    #[test]
    fn record_event_appends_and_pending_events_returns_them_in_order() {
        let mut user = User::new("carol@example.com".to_string(), "phc-hash".to_string()).unwrap();
        let first = DomainEvent::UserCreated(UserCreated::new(
            user.id,
            user.email.clone(),
            user.created_at,
        ));
        let second = DomainEvent::UserCreated(UserCreated::new(
            user.id,
            user.email.clone(),
            user.created_at,
        ));

        user.record_event(first.clone());
        user.record_event(second.clone());

        let recorded = user.pending_events();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0], first);
        assert_eq!(recorded[1], second);
    }
}
