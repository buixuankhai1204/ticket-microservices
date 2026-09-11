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
    #[error("invalid pagination parameters")]
    InvalidPagination,
    #[error("unknown booking status {0:?}")]
    InvalidStatus(String),
    /// `sqlstate` is the Postgres error code when the failure came from the
    /// database (`None` for a non-database repository failure, e.g. a
    /// pool-acquire timeout). Kept alongside the message so the Kafka consumer
    /// can classify retryable vs. permanent without re-parsing the message
    /// string — see `adapter/messaging/kafka/consumer.rs::is_retryable_sqlstate`.
    #[error("repository error: {message}")]
    Repository {
        message: String,
        sqlstate: Option<String>,
    },
}
