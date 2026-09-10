use thiserror::Error;

#[derive(Debug, Error)]
pub enum UserError {
    #[error("user not found")]
    NotFound,
    #[error("email already registered")]
    EmailAlreadyExists,
    #[error("invalid email")]
    InvalidEmail,
    #[error("invalid email or password")]
    InvalidCredentials,
    #[error("invalid pagination parameters")]
    InvalidPagination,
    #[error("invalid subscription: {0}")]
    InvalidSubscription(String),
    #[error("renewal cannot be retried: {0}")]
    RenewalNotRetryable(String),
    #[error("repository error: {0}")]
    Repository(String),
    #[error("password hashing error: {0}")]
    Hashing(String),
    #[error("token error: {0}")]
    Token(String),
}
