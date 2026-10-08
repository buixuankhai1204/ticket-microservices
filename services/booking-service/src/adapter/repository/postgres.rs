use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Booking, BookingError, BookingStatus, DomainEvent, Pagination};
use crate::platform::port::{database_error, BookingRepository, Tx};

#[derive(Default)]
pub struct PostgresBookingRepository;

impl PostgresBookingRepository {
    pub fn new() -> Self {
        Self
    }
}

#[derive(sqlx::FromRow)]
struct BookingRow {
    id: Uuid,
    user_id: Uuid,
    event_id: Uuid,
    seat_ids: Vec<Uuid>,
    status: String,
    failure_reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<BookingRow> for Booking {
    type Error = BookingError;

    fn try_from(row: BookingRow) -> Result<Self, Self::Error> {
        Ok(Booking::from_persisted(
            row.id,
            row.user_id,
            row.event_id,
            row.seat_ids,
            BookingStatus::parse(&row.status)?,
            row.failure_reason,
            row.created_at,
            row.updated_at,
        ))
    }
}

impl PostgresBookingRepository {
    async fn fetch_booking(
        &self,
        conn: &mut PgConnection,
        sql: &str,
        id: Uuid,
    ) -> Result<Booking, BookingError> {
        let row = sqlx::query_as::<_, BookingRow>(sql)
            .bind(id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(database_error)?;

        row.map(Booking::try_from)
            .transpose()?
            .ok_or(BookingError::NotFound)
    }
}

#[async_trait]
impl BookingRepository for PostgresBookingRepository {
    async fn find_by_id_for_user(
        &self,
        tx: &mut Tx,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Booking, BookingError> {
        let conn = tx.conn();
        let row = sqlx::query_as::<_, BookingRow>(
            "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
             FROM bookings WHERE id = $1 AND user_id = $2",
        )
        .bind(id)
        .bind(user_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(database_error)?;

        row.map(Booking::try_from)
            .transpose()?
            .ok_or(BookingError::NotFound)
    }

    async fn find_for_update(&self, tx: &mut Tx, id: Uuid) -> Result<Booking, BookingError> {
        let conn = tx.conn();
        self.fetch_booking(
            conn,
            "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
             FROM bookings WHERE id = $1 FOR UPDATE",
            id,
        )
        .await
    }

    async fn claim_oldest_stale_pending(
        &self,
        tx: &mut Tx,
        older_than_secs: i64,
    ) -> Result<Option<Booking>, BookingError> {
        let conn = tx.conn();
        let row = sqlx::query_as::<_, BookingRow>(
            "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
             FROM bookings \
             WHERE status = 'pending' AND created_at < now() - make_interval(secs => $1) \
             ORDER BY created_at \
             FOR UPDATE SKIP LOCKED \
             LIMIT 1",
        )
        .bind(older_than_secs as f64)
        .fetch_optional(&mut *conn)
        .await
        .map_err(database_error)?;

        row.map(Booking::try_from).transpose()
    }

    async fn count_oversold_seats(&self, tx: &mut Tx) -> Result<i64, BookingError> {
        let conn = tx.conn();
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ( \
                 SELECT 1 FROM bookings, unnest(seat_ids) AS seat_id \
                 WHERE status = 'confirmed' \
                 GROUP BY event_id, seat_id \
                 HAVING count(*) > 1 \
             ) oversold",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(database_error)
    }

    async fn count_stuck_pending(
        &self,
        tx: &mut Tx,
        older_than_secs: i64,
    ) -> Result<i64, BookingError> {
        let conn = tx.conn();
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM bookings \
             WHERE status = 'pending' AND created_at < now() - make_interval(secs => $1)",
        )
        .bind(older_than_secs as f64)
        .fetch_one(&mut *conn)
        .await
        .map_err(database_error)
    }

    async fn list_for_user(
        &self,
        tx: &mut Tx,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Booking>, i64), BookingError> {
        let conn = tx.conn();
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bookings WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(database_error)?;

        let rows = sqlx::query_as::<_, BookingRow>(
            "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
             FROM bookings WHERE user_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3",
        )
        .bind(user_id)
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(&mut *conn)
        .await
        .map_err(database_error)?;

        let bookings = rows
            .into_iter()
            .map(Booking::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((bookings, total))
    }

    async fn create(&self, tx: &mut Tx, booking: &Booking) -> Result<(), BookingError> {
        let conn = tx.conn();
        sqlx::query(
            "INSERT INTO bookings (id, user_id, event_id, seat_ids, status, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(booking.id)
        .bind(booking.user_id)
        .bind(booking.event_id)
        .bind(&booking.seat_ids)
        .bind(booking.status.as_str())
        .bind(booking.created_at)
        .bind(booking.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(database_error)?;

        Ok(())
    }

    async fn update_status(&self, tx: &mut Tx, booking: &Booking) -> Result<(), BookingError> {
        let conn = tx.conn();
        sqlx::query(
            "UPDATE bookings SET status = $1, failure_reason = $2, updated_at = $3 WHERE id = $4",
        )
        .bind(booking.status.as_str())
        .bind(&booking.failure_reason)
        .bind(booking.updated_at)
        .bind(booking.id)
        .execute(&mut *conn)
        .await
        .map_err(database_error)?;

        Ok(())
    }

    async fn mark_processed(&self, tx: &mut Tx, event_id: Uuid) -> Result<bool, BookingError> {
        let conn = tx.conn();
        let result = sqlx::query(
            "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        .bind(event_id)
        .execute(&mut *conn)
        .await
        .map_err(database_error)?;

        Ok(result.rows_affected() == 0)
    }

    async fn write_outbox(&self, tx: &mut Tx, event: &DomainEvent) -> Result<(), BookingError> {
        let conn = tx.conn();
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
        .map_err(database_error)?;

        sqlx::query("DELETE FROM outbox_events WHERE id = $1")
            .bind(event.event_id())
            .execute(&mut *conn)
            .await
            .map_err(database_error)?;

        Ok(())
    }
}
