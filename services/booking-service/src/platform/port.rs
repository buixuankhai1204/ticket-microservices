use async_trait::async_trait;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Booking, BookingError, DomainEvent, Pagination};

#[async_trait]
pub trait BookingRepository: Send + Sync {
    async fn find_by_id_for_user(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Booking, BookingError>;
    async fn find_for_update(
        &self,
        conn: &mut PgConnection,
        id: Uuid,
    ) -> Result<Booking, BookingError>;
    async fn list_for_user(
        &self,
        conn: &mut PgConnection,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Booking>, i64), BookingError>;
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
