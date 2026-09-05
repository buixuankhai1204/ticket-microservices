use thiserror::Error;

#[derive(Debug, Error)]
pub enum BookingError {
    #[error("booking not found")]
    NotFound,
    #[error("a booking must reference at least one seat")]
    NoSeats,
    #[error("the same seat appears more than once in the request")]
    DuplicateSeats,
    #[error("a booking may hold at most {0} seats")]
    TooManySeats(usize),
    #[error("booking is already in a terminal state")]
    AlreadyTerminal,
    #[error("unknown booking status {0:?}")]
    InvalidStatus(String),
    #[error("repository error: {0}")]
    Repository(String),
}
