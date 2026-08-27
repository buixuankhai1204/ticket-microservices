use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `UserCreated` is announced by the register-user use case once a user has been
/// durably persisted. It is a plain serializable value — no Kafka types here; the
/// messaging adapter turns it into a record. `event_id` is a fresh per-event UUID
/// used by consumers as the idempotency key; `user_id` is the aggregate id and
/// the Kafka partition key, so all events about one user stay ordered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCreated {
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

impl UserCreated {
    /// Builds the event for a just-registered user, minting its own `event_id`.
    /// Kept in `domain` so the event's shape and id generation never leak into
    /// `usecase`.
    pub fn new(user_id: Uuid, email: String, created_at: DateTime<Utc>) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            user_id,
            email,
            created_at,
        }
    }
}

/// Every event a use case can produce, so an aggregate can carry a list of
/// "pending events" for the repository to write to the outbox in the same
/// transaction as the state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    UserCreated(UserCreated),
}

impl DomainEvent {
    /// Unique id of this event instance — the consumer's idempotency key.
    pub fn event_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.event_id,
        }
    }

    /// The aggregate this event is about; used as the Kafka message key so a
    /// partition preserves per-aggregate order.
    pub fn aggregate_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.user_id,
        }
    }

    /// Stable discriminator stored in `outbox_events.event_type`.
    pub fn event_type(&self) -> &'static str {
        match self {
            DomainEvent::UserCreated(_) => "UserCreated",
        }
    }

    /// JSON body stored in `outbox_events.payload` and published verbatim.
    pub fn payload(&self) -> serde_json::Value {
        match self {
            DomainEvent::UserCreated(e) => {
                serde_json::to_value(e).expect("UserCreated is serializable")
            }
        }
    }
}
