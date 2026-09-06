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
        let booking = Booking::request(input.user_id, input.event_id, input.seat_ids)?;
        let requested = DomainEvent::BookingRequested(BookingRequested::new(
            booking.id,
            booking.user_id,
            booking.event_id,
            booking.seat_ids.clone(),
            booking.created_at,
        ));

        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        self.booking_repository.create(&mut tx, &booking).await?;
        self.booking_repository
            .write_outbox(&mut tx, &requested)
            .await?;
        tx.commit().await.map_err(tx_err)?;

        Ok(booking)
    }
}
