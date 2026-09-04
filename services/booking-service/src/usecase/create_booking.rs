use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{Booking, BookingError, BookingRequested, DomainEvent};
use crate::platform::port::BookingRepository;

pub struct CreateBookingInput {
    pub user_id: Uuid,
    pub event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
}

pub struct CreateBookingUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
}

impl CreateBookingUseCase {
    pub fn new(db_pool: PgPool, booking_repository: Arc<dyn BookingRepository>) -> Self {
        Self {
            db_pool,
            booking_repository,
        }
    }

    pub async fn execute(&self, input: CreateBookingInput) -> Result<Booking, BookingError> {
        let mut booking = Booking::request(input.user_id, input.event_id, input.seat_ids)?;
        booking.record_event(DomainEvent::BookingRequested(BookingRequested::new(
            booking.id,
            booking.user_id,
            booking.event_id,
            booking.seat_ids.clone(),
            booking.created_at,
        )));

        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        self.booking_repository.create(&mut tx, &booking).await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(booking)
    }
}
