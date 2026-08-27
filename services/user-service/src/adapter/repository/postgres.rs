use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::{User, UserError, UserRepository};

pub struct PostgresUserRepository {
    pool: PgPool,
}

impl PostgresUserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// DB row shape, kept separate from `domain::User` so the `sqlx::FromRow` derive
/// (and any column-naming detail) never leaks into the domain layer.
#[derive(sqlx::FromRow)]
struct UserRow {
    id: Uuid,
    email: String,
    password_hash: String,
    created_at: DateTime<Utc>,
}

impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        User {
            id: row.id,
            email: row.email,
            password_hash: row.password_hash,
            created_at: row.created_at,
        }
    }
}

fn repo_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}

#[async_trait]
impl UserRepository for PostgresUserRepository {
    async fn find_by_id(&self, id: Uuid) -> Result<User, UserError> {
        // Every endpoint's DB access runs inside one transaction (see CLAUDE.md).
        // A read uses a read-only transaction: a consistent snapshot across its
        // queries, and it can never accidentally write.
        let mut tx = self.pool.begin().await.map_err(repo_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(repo_err)?;

        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repo_err)?;

        tx.commit().await.map_err(repo_err)?;

        row.map(User::from).ok_or(UserError::NotFound)
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, UserError> {
        let mut tx = self.pool.begin().await.map_err(repo_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(repo_err)?;

        let row = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repo_err)?;

        tx.commit().await.map_err(repo_err)?;

        Ok(row.map(User::from))
    }

    async fn create(&self, user: &User) -> Result<(), UserError> {
        // A write uses a normal read-write transaction — required anyway once
        // /add-rust-saga-step adds an outbox insert alongside this state write.
        let mut tx = self.pool.begin().await.map_err(repo_err)?;

        sqlx::query(
            "INSERT INTO users (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(user.id)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(user.created_at)
        .execute(&mut *tx)
        .await
        .map_err(repo_err)?;

        tx.commit().await.map_err(repo_err)?;

        Ok(())
    }
}
