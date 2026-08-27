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
    /// Events produced by a successful operation on this aggregate, not yet
    /// persisted. A use case appends them via `record_event`; the repository
    /// drains them into `outbox_events` in the same transaction as the state
    /// write. A `User` loaded from the DB carries none.
    pending_events: Vec<DomainEvent>,
}

impl User {
    /// Constructs a new user, enforcing the email invariant at the point of
    /// creation rather than leaving callers to remember to validate it. The id
    /// and `created_at` are minted here (in the domain constructor, per the repo
    /// convention), not in `usecase`.
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

    /// Rehydrates a `User` already persisted in the store. Skips the email
    /// invariant (it held when the row was first written) and carries no pending
    /// events — no operation ran. The repository adapter is the only caller.
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

    /// Records a domain event this aggregate produced during a use case, to be
    /// written to the transactional outbox alongside the state change.
    pub fn record_event(&mut self, event: DomainEvent) {
        self.pending_events.push(event);
    }

    /// The events awaiting outbox persistence. The repository is the only caller.
    pub fn pending_events(&self) -> &[DomainEvent] {
        &self.pending_events
    }
}
