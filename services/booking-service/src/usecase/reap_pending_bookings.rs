use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::tx_err;
use crate::domain::{BookingCancelled, BookingError, DomainEvent, REASON_RESERVATION_TIMEOUT};
use crate::platform::port::BookingRepository;

pub struct ReapPendingBookingsUseCase {
    db_pool: PgPool,
    booking_repository: Arc<dyn BookingRepository>,
    pending_timeout_secs: i64,
    max_per_tick: usize,
}

impl ReapPendingBookingsUseCase {
    pub fn new(
        db_pool: PgPool,
        booking_repository: Arc<dyn BookingRepository>,
        pending_timeout_secs: i64,
        max_per_tick: usize,
    ) -> Self {
        Self {
            db_pool,
            booking_repository,
            pending_timeout_secs,
            max_per_tick,
        }
    }

    pub async fn reap_tick(&self) -> Result<usize, BookingError> {
        let mut reaped = 0;

        while reaped < self.max_per_tick {
            let mut tx = self.db_pool.begin().await.map_err(tx_err)?;

            let claimed = self
                .booking_repository
                .claim_oldest_stale_pending(&mut tx, self.pending_timeout_secs)
                .await?;

            let Some(mut booking) = claimed else {
                tx.commit().await.map_err(tx_err)?;
                break;
            };

            booking.cancel(REASON_RESERVATION_TIMEOUT)?;
            self.booking_repository
                .update_status(&mut tx, &booking)
                .await?;

            let cancelled = DomainEvent::BookingCancelled(BookingCancelled::new(
                booking.id,
                booking.user_id,
                booking.event_id,
                booking.seat_ids.clone(),
                REASON_RESERVATION_TIMEOUT.to_string(),
                booking.updated_at,
            ));
            self.booking_repository
                .write_outbox(&mut tx, &cancelled)
                .await?;

            tx.commit().await.map_err(tx_err)?;
            reaped += 1;
        }

        Ok(reaped)
    }

    pub async fn run(self, shutdown: CancellationToken, interval: Duration) {
        tracing::info!(
            interval_secs = interval.as_secs(),
            pending_timeout_secs = self.pending_timeout_secs,
            "booking reaper started"
        );

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("booking reaper stopping");
                    return;
                }
                _ = ticker.tick() => match self.reap_tick().await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(reaped = n, "cancelled stale pending bookings"),
                    Err(e) => tracing::error!(err = %e, "booking reaper tick failed"),
                },
            }
        }
    }
}
