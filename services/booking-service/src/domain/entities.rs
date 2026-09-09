use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::errors::BookingError;

pub const MAX_SEATS_PER_BOOKING: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookingStatus {
    Pending,
    Confirmed,
    Cancelled,
}

impl BookingStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BookingStatus::Pending => "pending",
            BookingStatus::Confirmed => "confirmed",
            BookingStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, BookingError> {
        match raw {
            "pending" => Ok(BookingStatus::Pending),
            "confirmed" => Ok(BookingStatus::Confirmed),
            "cancelled" => Ok(BookingStatus::Cancelled),
            other => Err(BookingError::InvalidStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Booking {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub status: BookingStatus,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Booking {
    pub fn request(
        user_id: Uuid,
        event_id: Uuid,
        seat_ids: Vec<Uuid>,
    ) -> Result<Self, BookingError> {
        if seat_ids.is_empty() {
            return Err(BookingError::NoSeats);
        }
        if seat_ids.len() > MAX_SEATS_PER_BOOKING {
            return Err(BookingError::TooManySeats(MAX_SEATS_PER_BOOKING));
        }
        let mut deduped = seat_ids.clone();
        deduped.sort();
        deduped.dedup();
        if deduped.len() != seat_ids.len() {
            return Err(BookingError::DuplicateSeats);
        }

        let now = Utc::now();
        Ok(Self {
            id: Uuid::new_v4(),
            user_id,
            event_id,
            seat_ids,
            status: BookingStatus::Pending,
            failure_reason: None,
            created_at: now,
            updated_at: now,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_persisted(
        id: Uuid,
        user_id: Uuid,
        event_id: Uuid,
        seat_ids: Vec<Uuid>,
        status: BookingStatus,
        failure_reason: Option<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            user_id,
            event_id,
            seat_ids,
            status,
            failure_reason,
            created_at,
            updated_at,
        }
    }

    pub fn confirm(&mut self) -> Result<(), BookingError> {
        match self.status {
            BookingStatus::Pending => {
                self.status = BookingStatus::Confirmed;
                self.updated_at = Utc::now();
                Ok(())
            }
            BookingStatus::Confirmed => Ok(()),
            BookingStatus::Cancelled => Err(BookingError::AlreadyTerminal),
        }
    }

    pub fn cancel(&mut self, reason: impl Into<String>) -> Result<(), BookingError> {
        match self.status {
            BookingStatus::Pending => {
                self.status = BookingStatus::Cancelled;
                self.failure_reason = Some(reason.into());
                self.updated_at = Utc::now();
                Ok(())
            }
            BookingStatus::Cancelled => Ok(()),
            BookingStatus::Confirmed => Err(BookingError::AlreadyTerminal),
        }
    }
}
