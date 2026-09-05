use async_trait::async_trait;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Booking, BookingError, DomainEvent};

#[async_trait]
pub trait BookingRepository: Send + Sync {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<Booking, BookingError>;
    async fn find_for_update(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Booking, BookingError>;
    async fn create(&self, conn: &mut PgConnection, booking: &Booking) -> Result<(), BookingError>;
    async fn update_status(
        &self,
        conn: &mut PgConnection,
        booking: &Booking,
    ) -> Result<(), BookingError>;
    async fn mark_processed(
        &self,
        conn: &mut PgConnection,
        event_id: Uuid,
    ) -> Result<bool, BookingError>;
    async fn write_outbox(
        &self,
        conn: &mut PgConnection,
        event: &DomainEvent,
    ) -> Result<(), BookingError>;
}
