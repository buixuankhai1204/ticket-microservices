use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Booking, BookingError};
use crate::platform::port::BookingRepository;

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

    pub async fn execute(
        &self,
        requesting_user_id: Uuid,
        id: Uuid,
    ) -> Result<Booking, BookingError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let booking = self
            .booking_repository
            .find_by_id_for_user(&mut tx, id, requesting_user_id)
            .await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(booking)
    }
}
