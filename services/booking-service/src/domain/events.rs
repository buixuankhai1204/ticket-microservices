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

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2030, 5, 1, 12, 0, 0).unwrap()
    }

    fn uuids(n: usize) -> Vec<Uuid> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

    #[test]
    fn every_event_routes_by_booking_id_with_its_own_type_and_payload() {
        let booking_id = Uuid::new_v4();
        let requested = DomainEvent::BookingRequested(BookingRequested::new(
            booking_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            ts(),
        ));
        let confirmed = DomainEvent::BookingConfirmed(BookingConfirmed::new(
            booking_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            ts(),
        ));
        let cancelled = DomainEvent::BookingCancelled(BookingCancelled::new(
            booking_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            uuids(2),
            REASON_SEAT_UNAVAILABLE.to_string(),
            ts(),
        ));

        for (event, event_type) in [
            (&requested, "BookingRequested"),
            (&confirmed, "BookingConfirmed"),
            (&cancelled, "BookingCancelled"),
        ] {
            assert_eq!(event.aggregate_id(), booking_id);
            assert_eq!(event.aggregate_type(), "booking");
            assert_eq!(event.event_type(), event_type);
            assert_eq!(event.payload()["booking_id"], booking_id.to_string());
        }
    }

    #[test]
    fn published_payloads_keep_the_snake_case_field_names_other_services_consume() {
        let (booking_id, user_id, ticketed_event_id) =
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let seats = uuids(2);
        let confirmed =
            BookingConfirmed::new(booking_id, user_id, ticketed_event_id, seats.clone(), ts());
        let cancelled = BookingCancelled::new(
            booking_id,
            user_id,
            ticketed_event_id,
            seats.clone(),
            REASON_RESERVATION_TIMEOUT.to_string(),
            ts(),
        );
        let requested = BookingRequested::new(booking_id, user_id, ticketed_event_id, seats, ts());

        let keys = |event: DomainEvent| {
            let mut keys: Vec<String> = event
                .payload()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect();
            keys.sort();
            keys
        };

        assert_eq!(
            keys(DomainEvent::BookingConfirmed(confirmed.clone())),
            [
                "booking_id",
                "event_id",
                "occurred_at",
                "seat_ids",
                "ticketed_event_id",
                "user_id"
            ]
        );
        assert_eq!(
            keys(DomainEvent::BookingCancelled(cancelled)),
            [
                "booking_id",
                "event_id",
                "occurred_at",
                "reason",
                "seat_ids",
                "ticketed_event_id",
                "user_id"
            ]
        );
        assert_eq!(
            keys(DomainEvent::BookingRequested(requested)),
            [
                "booking_id",
                "event_id",
                "requested_at",
                "seat_ids",
                "ticketed_event_id",
                "user_id"
            ]
        );
        let again = BookingConfirmed::new(booking_id, user_id, ticketed_event_id, uuids(2), ts());
        assert_ne!(
            confirmed.event_id, again.event_id,
            "every event needs its own id: it is the consumers' dedupe key"
        );
    }
}
