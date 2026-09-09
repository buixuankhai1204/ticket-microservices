use async_trait::async_trait;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{DomainEvent, Pagination, Subscription, User, UserError};

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<User, UserError>;
    async fn find_by_email(
        &self,
        conn: &mut PgConnection,
        email: &str,
    ) -> Result<Option<User>, UserError>;
    async fn create(&self, conn: &mut PgConnection, user: &User) -> Result<(), UserError>;
    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), UserError>;
    async fn list(
        &self,
        conn: &mut PgConnection,
        pagination: Pagination,
    ) -> Result<(Vec<User>, i64), UserError>;
}

#[async_trait]
pub trait SubscriptionRepository: Send + Sync {
    async fn create(
        &self,
        conn: &mut PgConnection,
        subscription: &Subscription,
    ) -> Result<(), UserError>;

    /// Owner-scoped fetch: a row whose `user_id` is not `user_id` is reported as
    /// `UserError::NotFound`, indistinguishable from a missing row (no IDOR
    /// existence leak).
    async fn find_by_id_for_user(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Subscription, UserError>;

    /// Owner-scoped page plus the full match count for that owner, both read on
    /// the one connection the use case passes in.
    async fn list_for_user(
        &self,
        conn: &mut PgConnection,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Subscription>, i64), UserError>;
}
