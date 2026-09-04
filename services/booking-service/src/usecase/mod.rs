pub mod get_booking;

pub use get_booking::GetBookingUseCase;

use crate::domain::BookingError;

pub(crate) fn tx_err(e: sqlx::Error) -> BookingError {
    BookingError::Repository(e.to_string())
}
