use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::errors::BookingError;
use super::events::DomainEvent;

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
    pending_events: Vec<DomainEvent>,
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
            pending_events: Vec::new(),
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
            pending_events: Vec::new(),
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

    fn seats(n: usize) -> Vec<Uuid> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

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

    #[test]
    fn request_starts_pending_with_no_pending_events() {
        let b = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(2)).unwrap();
        assert_eq!(b.status, BookingStatus::Pending);
        assert!(b.failure_reason.is_none());
        assert!(b.pending_events().is_empty());
    }

    #[test]
    fn request_rejects_no_seats() {
        let err = Booking::request(Uuid::new_v4(), Uuid::new_v4(), vec![]).unwrap_err();
        assert!(matches!(err, BookingError::NoSeats));
    }

    #[test]
    fn request_rejects_duplicate_seats() {
        let s = Uuid::new_v4();
        let err = Booking::request(Uuid::new_v4(), Uuid::new_v4(), vec![s, s]).unwrap_err();
        assert!(matches!(err, BookingError::DuplicateSeats));
    }

    #[test]
    fn request_rejects_more_than_the_cap() {
        let err = Booking::request(
            Uuid::new_v4(),
            Uuid::new_v4(),
            seats(MAX_SEATS_PER_BOOKING + 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BookingError::TooManySeats(MAX_SEATS_PER_BOOKING)
        ));
    }

    #[test]
    fn request_accepts_exactly_the_cap() {
        let b =
            Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(MAX_SEATS_PER_BOOKING)).unwrap();
        assert_eq!(b.seat_ids.len(), MAX_SEATS_PER_BOOKING);
    }

    #[test]
    fn record_event_queues_it_for_the_outbox() {
        use super::super::events::BookingRequested;

        let mut b = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        b.record_event(DomainEvent::BookingRequested(BookingRequested::new(
            b.id,
            b.user_id,
            b.event_id,
            b.seat_ids.clone(),
            b.created_at,
        )));
        assert_eq!(b.pending_events().len(), 1);
    }

    #[test]
    fn confirm_moves_pending_to_confirmed() {
        let mut b = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        let before = b.updated_at;
        b.confirm().unwrap();
        assert_eq!(b.status, BookingStatus::Confirmed);
        assert!(b.updated_at >= before);
    }

    #[test]
    fn confirm_is_idempotent_when_already_confirmed() {
        let mut b = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        b.confirm().unwrap();
        b.confirm().unwrap();
        assert_eq!(b.status, BookingStatus::Confirmed);
    }

    #[test]
    fn confirm_rejects_a_cancelled_booking() {
        let mut b = Booking::from_persisted(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            seats(1),
            BookingStatus::Cancelled,
            Some("seat_unavailable".to_string()),
            Utc::now(),
            Utc::now(),
        );
        assert!(matches!(
            b.confirm().unwrap_err(),
            BookingError::AlreadyTerminal
        ));
    }
}
