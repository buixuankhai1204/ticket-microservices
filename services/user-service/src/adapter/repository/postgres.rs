use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{DomainEvent, Pagination, User, UserError};
use crate::platform::port::UserRepository;

#[derive(Default)]
pub struct PostgresUserRepository;

impl PostgresUserRepository {
    pub fn new() -> Self {
        Self
    }
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: Uuid,
    email: String,
    password_hash: String,
    created_at: DateTime<Utc>,
}

impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        User::from_persisted(row.id, row.email, row.password_hash, row.created_at)
    }
}

fn repo_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}

#[async_trait]
impl UserRepository for PostgresUserRepository {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<User, UserError> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        row.map(User::from).ok_or(UserError::NotFound)
    }

    async fn find_by_email(
        &self,
        conn: &mut PgConnection,
        email: &str,
    ) -> Result<Option<User>, UserError> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(row.map(User::from))
    }

    async fn list(
        &self,
        conn: &mut PgConnection,
        pagination: Pagination,
    ) -> Result<(Vec<User>, i64), UserError> {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&mut *conn)
            .await
            .map_err(repo_err)?;

        let rows = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users \
             ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2",
        )
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok((rows.into_iter().map(User::from).collect(), total))
    }

    async fn create(&self, conn: &mut PgConnection, user: &User) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(user.id)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(user.created_at)
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        for event in user.pending_events() {
            self.write_outbox(&mut *conn, event).await?;
        }

        Ok(())
    }

    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), UserError> {
        sqlx::query(
            "INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(event.event_id())
        .bind(event.aggregate_id())
        .bind(event.aggregate_type())
        .bind(event.event_type())
        .bind(event.payload())
        .execute(&mut *conn)
        .await
        .map_err(repo_err)?;

        Ok(())
    }
}
