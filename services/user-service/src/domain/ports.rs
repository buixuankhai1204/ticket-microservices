use async_trait::async_trait;
use uuid::Uuid;

use super::entities::User;
use super::errors::UserError;

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_id(&self, id: Uuid) -> Result<User, UserError>;
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, UserError>;
    async fn create(&self, user: &User) -> Result<(), UserError>;
}

/// Outbound gateway: turns a plaintext password into a stored hash and back-checks
/// a login attempt against one, without `usecase`/`domain` knowing which algorithm.
pub trait PasswordHasher: Send + Sync {
    fn hash(&self, password: &str) -> Result<String, UserError>;
    fn verify(&self, password: &str, hash: &str) -> Result<bool, UserError>;
}

/// Outbound gateway: issues the access token returned to a client on successful login.
pub trait TokenIssuer: Send + Sync {
    fn issue(&self, user_id: Uuid, email: &str) -> Result<String, UserError>;
}
