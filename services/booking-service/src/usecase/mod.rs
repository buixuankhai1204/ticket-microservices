pub mod create_booking;
pub mod get_booking;

pub use create_booking::{CreateBookingInput, CreateBookingUseCase};
pub use get_booking::GetBookingUseCase;

use crate::domain::BookingError;

pub(crate) fn tx_err(e: sqlx::Error) -> BookingError {
    BookingError::Repository(e.to_string())
}
