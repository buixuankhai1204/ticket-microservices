use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::errors::BookingError;

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
    /// Rehydrate a booking from its persisted row.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_str() {
        for s in [
            BookingStatus::Pending,
            BookingStatus::Confirmed,
            BookingStatus::Cancelled,
        ] {
            assert_eq!(BookingStatus::parse(s.as_str()).unwrap(), s);
        }
    }

    #[test]
    fn parse_rejects_unknown_status() {
        let err = BookingStatus::parse("in_progress").unwrap_err();
        assert!(matches!(err, BookingError::InvalidStatus(v) if v == "in_progress"));
    }
}
