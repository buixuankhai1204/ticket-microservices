use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Booking, BookingError, BookingStatus};
use crate::platform::port::BookingRepository;

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

fn repo_err(e: sqlx::Error) -> BookingError {
    BookingError::Repository(e.to_string())
}

#[async_trait]
impl BookingRepository for PostgresBookingRepository {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<Booking, BookingError> {
        let row = sqlx::query_as::<_, BookingRow>(
            "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
             FROM bookings WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(repo_err)?;

        match row {
            Some(row) => Booking::try_from(row),
            None => Err(BookingError::NotFound),
        }
    }
}
