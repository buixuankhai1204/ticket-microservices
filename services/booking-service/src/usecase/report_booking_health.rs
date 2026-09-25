use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::BookingError;
use crate::platform::port::BookingRepository;

pub struct BookingHealth {
    pub oversold_seats: i64,
    pub stuck_pending: i64,
}

pub struct ReportBookingHealthUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
    stuck_after_secs: i64,
}

impl ReportBookingHealthUseCase {
    pub fn new(
        db_pool: PgPool,
        booking_repository: Arc<dyn BookingRepository>,
        stuck_after_secs: i64,
    ) -> Self {
        Self {
            db_pool,
            booking_repository,
            stuck_after_secs,
        }
    }

    pub async fn execute(&self) -> Result<BookingHealth, BookingError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let oversold_seats = self
            .booking_repository
            .count_oversold_seats(&mut tx)
            .await?;
        let stuck_pending = self
            .booking_repository
            .count_stuck_pending(&mut tx, self.stuck_after_secs)
            .await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(BookingHealth {
            oversold_seats,
            stuck_pending,
        })
    }
}
