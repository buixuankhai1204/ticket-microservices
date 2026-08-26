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
    #[error("repository error: {0}")]
    Repository(String),
    #[error("password hashing error: {0}")]
    Hashing(String),
    #[error("token error: {0}")]
    Token(String),
}
