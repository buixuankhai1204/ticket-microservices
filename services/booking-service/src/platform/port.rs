use async_trait::async_trait;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{Booking, BookingError};

/// Persistence port for booking-service. Every method runs on the connection
/// handle the use case passes in (`&mut PgConnection`) — the use case owns the
/// transaction boundary, so the repository never calls `begin` / `commit`.
///
/// New methods (`create`, `update_status`, `write_outbox`, `mark_processed`,
/// `list_for_user`) are added alongside the endpoints that need them via
/// `/new-rust-api-endpoint` (see `docs/sagas/seat-reservation.md` §11).
#[async_trait]
pub trait BookingRepository: Send + Sync {
    async fn find_by_id(&self, conn: &mut PgConnection, id: Uuid) -> Result<Booking, BookingError>;
}
