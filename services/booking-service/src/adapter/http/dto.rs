use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::{Booking, Pagination};

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
pub struct PaginationMeta {
    pub limit: i64,
    pub offset: i64,
    pub total: i64,
    pub has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedBookingsResponse {
    pub data: Vec<BookingResponse>,
    pub pagination: PaginationMeta,
}

impl PaginatedBookingsResponse {
    pub fn new(bookings: &[Booking], pagination: &Pagination, total: i64) -> Self {
        let data: Vec<BookingResponse> = bookings.iter().map(BookingResponse::from).collect();
        let has_more = pagination.has_more(data.len(), total);
        Self {
            data,
            pagination: PaginationMeta {
                limit: pagination.limit,
                offset: pagination.offset,
                total,
                has_more,
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}
