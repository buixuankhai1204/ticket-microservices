use std::sync::Arc;

use crate::domain::BookingError;
use crate::platform::port::{BookingRepository, Transactor};

pub struct BookingHealth {
    pub oversold_seats: i64,
    pub stuck_pending: i64,
}

pub struct ReportBookingHealthUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
    stuck_after_secs: i64,
}

impl ReportBookingHealthUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
        stuck_after_secs: i64,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
            stuck_after_secs,
        }
    }

    pub async fn execute(&self) -> Result<BookingHealth, BookingError> {
        let mut tx = self.transactor.begin_read_only().await?;
        let oversold_seats = self
            .booking_repository
            .count_oversold_seats(&mut tx)
            .await?;
        let stuck_pending = self
            .booking_repository
            .count_stuck_pending(&mut tx, self.stuck_after_secs)
            .await?;
        tx.commit().await?;

        Ok(BookingHealth {
            oversold_seats,
            stuck_pending,
        })
    }
}
