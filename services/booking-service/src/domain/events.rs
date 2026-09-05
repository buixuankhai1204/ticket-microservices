use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    BookingRequested(BookingRequested),
    BookingConfirmed(BookingConfirmed),
}

impl DomainEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            DomainEvent::BookingRequested(e) => e.event_id,
            DomainEvent::BookingConfirmed(e) => e.event_id,
        }
    }

    pub fn aggregate_id(&self) -> Uuid {
        match self {
            DomainEvent::BookingRequested(e) => e.booking_id,
            DomainEvent::BookingConfirmed(e) => e.booking_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            DomainEvent::BookingRequested(_) => "BookingRequested",
            DomainEvent::BookingConfirmed(_) => "BookingConfirmed",
        }
    }

    pub fn aggregate_type(&self) -> &'static str {
        match self {
            DomainEvent::BookingRequested(_) => "booking",
            DomainEvent::BookingConfirmed(_) => "booking",
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
        }
    }
}
