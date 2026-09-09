use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const REASON_SEAT_UNAVAILABLE: &str = "seat_unavailable";
pub const REASON_RESERVATION_TIMEOUT: &str = "reservation_timeout";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookingRequested {
    pub event_id: Uuid,
    pub booking_id: Uuid,
    pub user_id: Uuid,
    pub ticketed_event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub requested_at: DateTime<Utc>,
}

impl BookingRequested {
    pub fn new(
        booking_id: Uuid,
        user_id: Uuid,
        ticketed_event_id: Uuid,
        seat_ids: Vec<Uuid>,
        requested_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids,
            requested_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatReserved {
    pub event_id: Uuid,
    pub booking_id: Uuid,
    pub ticketed_event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub reserved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookingConfirmed {
    pub event_id: Uuid,
    pub booking_id: Uuid,
    pub user_id: Uuid,
    pub ticketed_event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub occurred_at: DateTime<Utc>,
}

impl BookingConfirmed {
    pub fn new(
        booking_id: Uuid,
        user_id: Uuid,
        ticketed_event_id: Uuid,
        seat_ids: Vec<Uuid>,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids,
            occurred_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatReservationFailed {
    pub event_id: Uuid,
    pub booking_id: Uuid,
    pub ticketed_event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub reason: String,
    pub failed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookingCancelled {
    pub event_id: Uuid,
    pub booking_id: Uuid,
    pub user_id: Uuid,
    pub ticketed_event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
}

impl BookingCancelled {
    pub fn new(
        booking_id: Uuid,
        user_id: Uuid,
        ticketed_event_id: Uuid,
        seat_ids: Vec<Uuid>,
        reason: String,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids,
            reason,
            occurred_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum DomainEvent {
    BookingRequested(BookingRequested),
    BookingConfirmed(BookingConfirmed),
    BookingCancelled(BookingCancelled),
}

impl DomainEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            DomainEvent::BookingRequested(e) => e.event_id,
            DomainEvent::BookingConfirmed(e) => e.event_id,
            DomainEvent::BookingCancelled(e) => e.event_id,
        }
    }

    pub fn aggregate_id(&self) -> Uuid {
        match self {
            DomainEvent::BookingRequested(e) => e.booking_id,
            DomainEvent::BookingConfirmed(e) => e.booking_id,
            DomainEvent::BookingCancelled(e) => e.booking_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            DomainEvent::BookingRequested(_) => "BookingRequested",
            DomainEvent::BookingConfirmed(_) => "BookingConfirmed",
            DomainEvent::BookingCancelled(_) => "BookingCancelled",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            DomainEvent::BookingRequested(_) => "booking",
            DomainEvent::BookingConfirmed(_) => "booking",
            DomainEvent::BookingCancelled(_) => "booking",
        }
    }

    pub fn payload(&self) -> serde_json::Value {
        match self {
            DomainEvent::BookingRequested(e) => {
                serde_json::to_value(e).expect("BookingRequested is serializable")
            }
            DomainEvent::BookingConfirmed(e) => {
                serde_json::to_value(e).expect("BookingConfirmed is serializable")
            }
            DomainEvent::BookingCancelled(e) => {
                serde_json::to_value(e).expect("BookingCancelled is serializable")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::BTreeSet;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2030, 5, 1, 12, 0, 0).unwrap()
    }

    fn uuids(n: usize) -> Vec<Uuid> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

    fn payload_keys(value: &serde_json::Value) -> BTreeSet<String> {
        value
            .as_object()
            .expect("payload is a JSON object")
            .keys()
            .cloned()
            .collect()
    }

    fn key_set(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn booking_requested_new_mints_a_distinct_v4_event_id_and_copies_fields() {
        let booking_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let ticketed_event_id = Uuid::new_v4();
        let seat_ids = uuids(3);
        let requested_at = ts();

        let event = BookingRequested::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            requested_at,
        );
        let again = BookingRequested::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            requested_at,
        );

        assert!(!event.event_id.is_nil());
        assert_eq!(event.event_id.get_version_num(), 4);
        assert_ne!(event.event_id, again.event_id);
        assert_eq!(event.booking_id, booking_id);
        assert_eq!(event.user_id, user_id);
        assert_eq!(event.ticketed_event_id, ticketed_event_id);
        assert_eq!(event.seat_ids, seat_ids);
        assert_eq!(event.requested_at, requested_at);
    }

    #[test]
    fn booking_confirmed_new_mints_a_distinct_v4_event_id_and_copies_fields() {
        let booking_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let ticketed_event_id = Uuid::new_v4();
        let seat_ids = uuids(2);
        let occurred_at = ts();

        let event = BookingConfirmed::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            occurred_at,
        );
        let again = BookingConfirmed::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            occurred_at,
        );

        assert_eq!(event.event_id.get_version_num(), 4);
        assert_ne!(event.event_id, again.event_id);
        assert_eq!(event.booking_id, booking_id);
        assert_eq!(event.user_id, user_id);
        assert_eq!(event.ticketed_event_id, ticketed_event_id);
        assert_eq!(event.seat_ids, seat_ids);
        assert_eq!(event.occurred_at, occurred_at);
    }

    #[test]
    fn booking_cancelled_new_mints_a_distinct_v4_event_id_and_copies_fields() {
        let booking_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let ticketed_event_id = Uuid::new_v4();
        let seat_ids = uuids(1);
        let occurred_at = ts();

        let event = BookingCancelled::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            REASON_SEAT_UNAVAILABLE.to_string(),
            occurred_at,
        );
        let again = BookingCancelled::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seat_ids.clone(),
            REASON_SEAT_UNAVAILABLE.to_string(),
            occurred_at,
        );

        assert_eq!(event.event_id.get_version_num(), 4);
        assert_ne!(event.event_id, again.event_id);
        assert_eq!(event.booking_id, booking_id);
        assert_eq!(event.user_id, user_id);
        assert_eq!(event.ticketed_event_id, ticketed_event_id);
        assert_eq!(event.seat_ids, seat_ids);
        assert_eq!(event.reason, REASON_SEAT_UNAVAILABLE);
        assert_eq!(event.occurred_at, occurred_at);
    }

    #[test]
    fn domain_event_booking_requested_exposes_metadata_and_round_trip_payload() {
        let inner = BookingRequested::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            ts(),
        );
        let event = DomainEvent::BookingRequested(inner.clone());

        assert_eq!(event.event_id(), inner.event_id);
        assert_eq!(event.aggregate_id(), inner.booking_id);
        assert_eq!(event.event_type(), "BookingRequested");
        assert_eq!(event.aggregate_type(), "booking");

        let payload = event.payload();
        assert_eq!(
            payload_keys(&payload),
            key_set(&[
                "event_id",
                "booking_id",
                "user_id",
                "ticketed_event_id",
                "seat_ids",
                "requested_at",
            ])
        );
        let round_tripped: BookingRequested = serde_json::from_value(payload).unwrap();
        assert_eq!(round_tripped, inner);
    }

    #[test]
    fn domain_event_booking_confirmed_exposes_metadata_and_round_trip_payload() {
        let inner = BookingConfirmed::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            ts(),
        );
        let event = DomainEvent::BookingConfirmed(inner.clone());

        assert_eq!(event.event_id(), inner.event_id);
        assert_eq!(event.aggregate_id(), inner.booking_id);
        assert_eq!(event.event_type(), "BookingConfirmed");
        assert_eq!(event.aggregate_type(), "booking");

        let payload = event.payload();
        assert_eq!(
            payload_keys(&payload),
            key_set(&[
                "event_id",
                "booking_id",
                "user_id",
                "ticketed_event_id",
                "seat_ids",
                "occurred_at",
            ])
        );
        let round_tripped: BookingConfirmed = serde_json::from_value(payload).unwrap();
        assert_eq!(round_tripped, inner);
    }

    #[test]
    fn domain_event_booking_cancelled_exposes_metadata_and_round_trip_payload() {
        let inner = BookingCancelled::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            REASON_RESERVATION_TIMEOUT.to_string(),
            ts(),
        );
        let event = DomainEvent::BookingCancelled(inner.clone());

        assert_eq!(event.event_id(), inner.event_id);
        assert_eq!(event.aggregate_id(), inner.booking_id);
        assert_eq!(event.event_type(), "BookingCancelled");
        assert_eq!(event.aggregate_type(), "booking");

        let payload = event.payload();
        assert_eq!(
            payload_keys(&payload),
            key_set(&[
                "event_id",
                "booking_id",
                "user_id",
                "ticketed_event_id",
                "seat_ids",
                "reason",
                "occurred_at",
            ])
        );
        let round_tripped: BookingCancelled = serde_json::from_value(payload).unwrap();
        assert_eq!(round_tripped, inner);
    }

    #[test]
    fn reason_constants_hold_their_wire_values() {
        assert_eq!(REASON_SEAT_UNAVAILABLE, "seat_unavailable");
        assert_eq!(REASON_RESERVATION_TIMEOUT, "reservation_timeout");
    }
}
