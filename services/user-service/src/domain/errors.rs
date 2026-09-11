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

/// Outcome of an outbound charge against the payment provider
/// (docs/sagas/renewal-subscriptions.md §5.1 / §5.2). `Declined` is a permanent
/// card problem → the dunning arm (3c); `Transient` is a provider blip →
/// backoff/give-up (3b). The renewal period is never advanced on either.
#[derive(Debug, Error)]
pub enum PaymentError {
    #[error("card declined: {code}")]
    Declined { code: String },
    #[error("payment provider transient failure: {0}")]
    Transient(String),
}

/// Failure of a best-effort dunning email send
/// (docs/sagas/renewal-subscriptions.md §5.9). Always logged and swallowed by
/// the caller — a lost dunning notice is re-sent by the next dunning pass.
#[derive(Debug, Error)]
#[error("email send failed: {0}")]
pub struct EmailError(pub String);
