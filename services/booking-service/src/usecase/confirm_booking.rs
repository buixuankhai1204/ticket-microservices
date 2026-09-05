use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::{BookingConfirmed, BookingError, BookingStatus, DomainEvent, SeatReserved};
use crate::platform::port::BookingRepository;

pub struct ConfirmBookingUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
}

impl ConfirmBookingUseCase {
    pub fn new(db_pool: PgPool, booking_repository: Arc<dyn BookingRepository>) -> Self {
        Self {
            db_pool,
            booking_repository,
        }
    }

    pub async fn execute(&self, ev: &SeatReserved) -> Result<bool, BookingError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;

        if self
            .booking_repository
            .mark_processed(&mut tx, ev.event_id)
            .await?
        {
            tx.commit().await.map_err(tx_err)?;
            return Ok(true);
        }

        let mut booking = self
            .booking_repository
            .find_for_update(&mut tx, ev.booking_id)
            .await?;

        if booking.status == BookingStatus::Pending {
            booking.confirm()?;
            self.booking_repository
                .update_status(&mut tx, &booking)
                .await?;
            let confirmed = DomainEvent::BookingConfirmed(BookingConfirmed::new(
                booking.id,
                booking.user_id,
                booking.event_id,
                booking.seat_ids.clone(),
                booking.updated_at,
            ));
            self.booking_repository
                .write_outbox(&mut tx, &confirmed)
                .await?;
        }

        tx.commit().await.map_err(tx_err)?;
        Ok(false)
    }
}
