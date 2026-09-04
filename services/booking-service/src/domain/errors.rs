use thiserror::Error;

/// Domain-level failures for booking-service. Transport (`adapter/http`) maps
/// each variant to a status code; the repository wraps driver errors in
/// `Repository` and the use case wraps transaction errors the same way.
#[derive(Debug, Error)]
pub enum BookingError {
    #[error("booking not found")]
    NotFound,
    #[error("unknown booking status {0:?}")]
    InvalidStatus(String),
    #[error("repository error: {0}")]
    Repository(String),
}
