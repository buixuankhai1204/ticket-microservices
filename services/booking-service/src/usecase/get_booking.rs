use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Booking, BookingError};
use crate::platform::port::BookingRepository;

/// Read one booking by id. Opens a single read-only transaction (a consistent
/// snapshot), threads the handle through the repository, and commits — the use
/// case owns the transaction boundary (CLAUDE.md).
pub struct GetBookingUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
}

impl GetBookingUseCase {
    pub fn new(db_pool: PgPool, booking_repository: Arc<dyn BookingRepository>) -> Self {
        Self {
            db_pool,
            booking_repository,
        }
    }

    pub async fn execute(&self, id: Uuid) -> Result<Booking, BookingError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let booking = self.booking_repository.find_by_id(&mut tx, id).await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(booking)
    }
}
