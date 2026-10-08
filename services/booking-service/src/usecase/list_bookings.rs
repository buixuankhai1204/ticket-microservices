use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{Booking, BookingError, Pagination};
use crate::platform::port::{BookingRepository, Transactor};

pub struct ListBookingsUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
}

impl ListBookingsUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
        }
    }

    pub async fn execute(
        &self,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Booking>, i64), BookingError> {
        let mut tx = self.transactor.begin_read_only().await?;
        let page = self
            .booking_repository
            .list_for_user(&mut tx, user_id, pagination)
            .await?;
        tx.commit().await?;
        Ok(page)
    }
}
