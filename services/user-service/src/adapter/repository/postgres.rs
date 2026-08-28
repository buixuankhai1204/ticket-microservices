use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::{Pagination, User, UserError, UserRepository};

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
        // Rehydrate through the domain constructor rather than a struct literal,
        // so the adapter never depends on `User`'s private layout.
        User::from_persisted(row.id, row.email, row.password_hash, row.created_at)
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

    async fn list(&self, pagination: Pagination) -> Result<(Vec<User>, i64), UserError> {
        // Read-only transaction: the COUNT and the page SELECT see one
        // consistent snapshot, so a concurrent insert between them can't make
        // `total` disagree with the returned rows.
        let mut tx = self.pool.begin().await.map_err(repo_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(repo_err)?;

        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&mut *tx)
            .await
            .map_err(repo_err)?;

        let rows = sqlx::query_as::<_, UserRow>(
            "SELECT id, email, password_hash, created_at FROM users \
             ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2",
        )
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(&mut *tx)
        .await
        .map_err(repo_err)?;

        tx.commit().await.map_err(repo_err)?;

        Ok((rows.into_iter().map(User::from).collect(), total))
    }

    async fn create(&self, user: &User) -> Result<(), UserError> {
        // A write uses a normal read-write transaction: the users row and the
        // user's pending domain events (the outbox rows) commit together or not
        // at all — the transactional outbox pattern, avoiding the dual-write
        // problem without two-phase commit against Kafka.
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

        for event in user.pending_events() {
            // Insert the event, then delete it again in this same transaction.
            // The INSERT is still written to the WAL, so the Debezium connector
            // tailing it captures and publishes the event (routed by
            // aggregate_type to `<aggregate_type>.events`); the table stays
            // empty. The connector skips deletes, so the paired DELETE is inert
            // on the wire — it's only here to keep `outbox_events` from growing.
            sqlx::query(
                "INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(event.event_id())
            .bind(event.aggregate_id())
            .bind(event.aggregate_type())
            .bind(event.event_type())
            .bind(event.payload())
            .execute(&mut *tx)
            .await
            .map_err(repo_err)?;

            sqlx::query("DELETE FROM outbox_events WHERE id = $1")
                .bind(event.event_id())
                .execute(&mut *tx)
                .await
                .map_err(repo_err)?;
        }

        tx.commit().await.map_err(repo_err)?;

        Ok(())
    }
}
