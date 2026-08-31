use async_trait::async_trait;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Pagination, User, UserError};

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<User, UserError>;
    async fn find_by_email(
        &self,
        conn: &mut PgConnection,
        email: &str,
    ) -> Result<Option<User>, UserError>;
    async fn create(&self, conn: &mut PgConnection, user: &User) -> Result<(), UserError>;
    async fn list(
        &self,
        conn: &mut PgConnection,
        pagination: Pagination,
    ) -> Result<(Vec<User>, i64), UserError>;
}
