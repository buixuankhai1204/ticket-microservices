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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn seats(n: usize) -> Vec<Uuid> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

    #[test]
    fn request_happy_path_mints_v4_id_and_starts_pending() {
        let user_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let seat_ids = seats(3);

        let booking = Booking::request(user_id, event_id, seat_ids.clone()).unwrap();

        assert!(!booking.id.is_nil());
        assert_eq!(booking.id.get_version_num(), 4);
        assert_eq!(booking.user_id, user_id);
        assert_eq!(booking.event_id, event_id);
        assert_eq!(booking.seat_ids, seat_ids);
        assert_eq!(booking.status, BookingStatus::Pending);
        assert!(booking.failure_reason.is_none());
        assert_eq!(booking.created_at, booking.updated_at);
    }

    #[test]
    fn request_rejects_empty_seat_list() {
        let err = Booking::request(Uuid::new_v4(), Uuid::new_v4(), vec![]).unwrap_err();
        assert!(matches!(err, BookingError::NoSeats));
    }

    #[test]
    fn request_rejects_more_seats_than_the_cap() {
        let err = Booking::request(
            Uuid::new_v4(),
            Uuid::new_v4(),
            seats(MAX_SEATS_PER_BOOKING + 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BookingError::TooManySeats(n) if n == MAX_SEATS_PER_BOOKING
        ));
    }

    #[test]
    fn request_rejects_duplicate_seat_ids() {
        let dup = Uuid::new_v4();
        let err = Booking::request(
            Uuid::new_v4(),
            Uuid::new_v4(),
            vec![dup, Uuid::new_v4(), dup],
        )
        .unwrap_err();
        assert!(matches!(err, BookingError::DuplicateSeats));
    }

    #[test]
    fn request_accepts_exactly_the_cap() {
        let booking =
            Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(MAX_SEATS_PER_BOOKING)).unwrap();
        assert_eq!(booking.seat_ids.len(), MAX_SEATS_PER_BOOKING);
    }

    #[test]
    fn from_persisted_passes_every_field_through_unchanged() {
        let id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let seat_ids = seats(2);
        let created_at = Utc.with_ymd_and_hms(2030, 1, 2, 3, 4, 5).unwrap();
        let updated_at = Utc.with_ymd_and_hms(2030, 6, 7, 8, 9, 10).unwrap();

        let booking = Booking::from_persisted(
            id,
            user_id,
            event_id,
            seat_ids.clone(),
            BookingStatus::Cancelled,
            Some("seat_unavailable".to_string()),
            created_at,
            updated_at,
        );

        assert_eq!(booking.id, id);
        assert_eq!(booking.user_id, user_id);
        assert_eq!(booking.event_id, event_id);
        assert_eq!(booking.seat_ids, seat_ids);
        assert_eq!(booking.status, BookingStatus::Cancelled);
        assert_eq!(booking.failure_reason.as_deref(), Some("seat_unavailable"));
        assert_eq!(booking.created_at, created_at);
        assert_eq!(booking.updated_at, updated_at);
    }

    #[test]
    fn confirm_moves_pending_to_confirmed() {
        let mut booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        let before = booking.updated_at;

        booking.confirm().unwrap();

        assert_eq!(booking.status, BookingStatus::Confirmed);
        assert!(booking.updated_at >= before);
    }

    #[test]
    fn confirm_is_idempotent_when_already_confirmed() {
        let mut booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        booking.confirm().unwrap();

        booking.confirm().unwrap();

        assert_eq!(booking.status, BookingStatus::Confirmed);
    }

    #[test]
    fn confirm_rejects_a_cancelled_booking_as_already_terminal() {
        let mut booking = Booking::from_persisted(
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
            booking.confirm().unwrap_err(),
            BookingError::AlreadyTerminal
        ));
    }

    #[test]
    fn cancel_moves_pending_to_cancelled_and_records_the_reason() {
        let mut booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();

        booking.cancel("seat_unavailable").unwrap();

        assert_eq!(booking.status, BookingStatus::Cancelled);
        assert_eq!(booking.failure_reason.as_deref(), Some("seat_unavailable"));
    }

    #[test]
    fn cancel_is_idempotent_and_keeps_the_first_reason() {
        let mut booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        booking.cancel("seat_unavailable").unwrap();

        booking.cancel("reservation_timeout").unwrap();

        assert_eq!(booking.status, BookingStatus::Cancelled);
        assert_eq!(booking.failure_reason.as_deref(), Some("seat_unavailable"));
    }

    #[test]
    fn cancel_rejects_a_confirmed_booking_as_already_terminal() {
        let mut booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(1)).unwrap();
        booking.confirm().unwrap();

        assert!(matches!(
            booking.cancel("too_late").unwrap_err(),
            BookingError::AlreadyTerminal
        ));
    }

    #[test]
    fn status_as_str_uses_the_wire_literals() {
        assert_eq!(BookingStatus::Pending.as_str(), "pending");
        assert_eq!(BookingStatus::Confirmed.as_str(), "confirmed");
        assert_eq!(BookingStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn status_parse_round_trips_every_variant() {
        for status in [
            BookingStatus::Pending,
            BookingStatus::Confirmed,
            BookingStatus::Cancelled,
        ] {
            assert_eq!(BookingStatus::parse(status.as_str()).unwrap(), status);
        }
    }

    #[test]
    fn status_parse_rejects_an_unknown_literal() {
        let err = BookingStatus::parse("reserved").unwrap_err();
        assert!(matches!(err, BookingError::InvalidStatus(v) if v == "reserved"));
    }
}
