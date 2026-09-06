pub mod entities;
pub mod errors;
pub mod events;

pub use entities::{Booking, BookingStatus};
pub use errors::BookingError;
pub use events::{
    BookingCancelled, BookingConfirmed, BookingRequested, DomainEvent, SeatReservationFailed,
    SeatReserved, REASON_SEAT_UNAVAILABLE,
};
