use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::Booking;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateBookingRequest {
    pub event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BookingResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
    pub status: String,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&Booking> for BookingResponse {
    fn from(booking: &Booking) -> Self {
        Self {
            id: booking.id,
            user_id: booking.user_id,
            event_id: booking.event_id,
            seat_ids: booking.seat_ids.clone(),
            status: booking.status.as_str().to_string(),
            failure_reason: booking.failure_reason.clone(),
            created_at: booking.created_at,
            updated_at: booking.updated_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}
