use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCreated {
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

impl UserCreated {
    pub fn new(user_id: Uuid, email: String, created_at: DateTime<Utc>) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            user_id,
            email,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserLoggedIn {
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub logged_in_at: DateTime<Utc>,
}

impl UserLoggedIn {
    pub fn new(user_id: Uuid, email: String, logged_in_at: DateTime<Utc>) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            user_id,
            email,
            logged_in_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    UserCreated(UserCreated),
    UserLoggedIn(UserLoggedIn),
}

impl DomainEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.event_id,
            DomainEvent::UserLoggedIn(e) => e.event_id,
        }
    }

    pub fn aggregate_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.user_id,
            DomainEvent::UserLoggedIn(e) => e.user_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            DomainEvent::UserCreated(_) => "UserCreated",
            DomainEvent::UserLoggedIn(_) => "UserLoggedIn",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            DomainEvent::UserCreated(_) => "user",
            DomainEvent::UserLoggedIn(_) => "user",
        }
    }

    pub fn payload(&self) -> serde_json::Value {
        match self {
            DomainEvent::UserCreated(e) => {
                serde_json::to_value(e).expect("UserCreated is serializable")
            }
            DomainEvent::UserLoggedIn(e) => {
                serde_json::to_value(e).expect("UserLoggedIn is serializable")
            }
        }
    }
}
