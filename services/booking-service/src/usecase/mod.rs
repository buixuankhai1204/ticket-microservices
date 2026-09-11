pub mod cancel_booking;
pub mod confirm_booking;
pub mod create_booking;
pub mod get_booking;
pub mod list_bookings;
pub mod reap_pending_bookings;

pub use cancel_booking::CancelBookingUseCase;
pub use confirm_booking::ConfirmBookingUseCase;
pub use create_booking::{CreateBookingInput, CreateBookingUseCase};
pub use get_booking::GetBookingUseCase;
pub use list_bookings::ListBookingsUseCase;
pub use reap_pending_bookings::ReapPendingBookingsUseCase;

use crate::domain::BookingError;

pub(crate) fn tx_err(e: sqlx::Error) -> BookingError {
    let sqlstate = e
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.into_owned());
    BookingError::Repository {
        message: e.to_string(),
        sqlstate,
    }
}
