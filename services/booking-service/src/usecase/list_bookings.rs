use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Booking, BookingError, Pagination};
use crate::platform::port::BookingRepository;

pub struct ListBookingsUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
}

impl ListBookingsUseCase {
    pub fn new(db_pool: PgPool, booking_repository: Arc<dyn BookingRepository>) -> Self {
        Self {
            db_pool,
            booking_repository,
        }
    }

    pub async fn execute(
        &self,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Booking>, i64), BookingError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let page = self
            .booking_repository
            .list_for_user(&mut tx, user_id, pagination)
            .await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(page)
    }
}
