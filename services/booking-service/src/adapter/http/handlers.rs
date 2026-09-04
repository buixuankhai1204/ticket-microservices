use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use uuid::Uuid;

use crate::domain::BookingError;
use crate::platform::db::DbPool;
use crate::usecase::GetBookingUseCase;

use super::dto::{BookingResponse, ErrorResponse};

pub struct AppState {
    pub get_booking: GetBookingUseCase,
    pub db_pool: DbPool,
}

fn map_error(err: BookingError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match err {
        BookingError::NotFound => StatusCode::NOT_FOUND,
        BookingError::InvalidStatus(_) | BookingError::Repository(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (
        status,
        Json(ErrorResponse {
            error: err.to_string(),
        }),
    )
}

#[utoipa::path(
    get,
    path = "/api/v1/bookings/{id}",
    params(
        ("id" = Uuid, Path, description = "Booking ID"),
    ),
    responses(
        (status = 200, description = "Booking found", body = BookingResponse),
        (status = 404, description = "Booking not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_booking(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<BookingResponse>, (StatusCode, Json<ErrorResponse>)> {
    let booking = state.get_booking.execute(id).await.map_err(map_error)?;
    Ok(Json(BookingResponse::from(&booking)))
}

pub async fn healthz() -> StatusCode {
    StatusCode::OK
}

pub async fn readyz(State(state): State<Arc<AppState>>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&state.db_pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
