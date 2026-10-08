use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::domain::{BookingCancelled, BookingError, DomainEvent, REASON_RESERVATION_TIMEOUT};
use crate::platform::port::{BookingRepository, Transactor};

pub struct ReapPendingBookingsUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
    pending_timeout_secs: i64,
    max_per_tick: usize,
}

impl ReapPendingBookingsUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
        pending_timeout_secs: i64,
        max_per_tick: usize,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
            pending_timeout_secs,
            max_per_tick,
        }
    }

    pub async fn reap_tick(&self) -> Result<usize, BookingError> {
        let mut reaped = 0;

        while reaped < self.max_per_tick {
            let mut tx = self.transactor.begin().await?;

            let claimed = self
                .booking_repository
                .claim_oldest_stale_pending(&mut tx, self.pending_timeout_secs)
                .await?;

            let Some(mut booking) = claimed else {
                tx.commit().await?;
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

            tx.commit().await?;
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::{Booking, BookingStatus};
    use crate::platform::port::testing::{transactor, Journal};
    use crate::platform::port::MockBookingRepository;

    fn stale_booking() -> Booking {
        Booking::request(Uuid::new_v4(), Uuid::new_v4(), vec![Uuid::new_v4()]).unwrap()
    }

    fn repository(
        journal: &Journal,
        stale: Vec<Booking>,
        fail_outbox_for: Option<Uuid>,
    ) -> MockBookingRepository {
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        let mut queue = stale.into_iter();
        repo.expect_claim_oldest_stale_pending()
            .withf(|_, secs| *secs == 120)
            .returning(move |_, _| {
                j.note("claim");
                Ok(queue.next())
            });
        let j = journal.clone();
        repo.expect_update_status()
            .withf(|_, b| {
                b.status == BookingStatus::Cancelled
                    && b.failure_reason.as_deref() == Some(REASON_RESERVATION_TIMEOUT)
            })
            .returning(move |_, _| {
                j.note("update_status");
                Ok(())
            });
        let j = journal.clone();
        repo.expect_write_outbox().returning(move |_, e| {
            j.note("write_outbox");
            let reasoned = matches!(e, DomainEvent::BookingCancelled(c) if c.reason == REASON_RESERVATION_TIMEOUT);
            assert!(reasoned, "the reaper must publish reservation_timeout");
            if Some(e.aggregate_id()) == fail_outbox_for {
                Err(BookingError::Repository {
                    message: "connection reset".to_string(),
                    sqlstate: None,
                })
            } else {
                Ok(())
            }
        });
        repo
    }

    fn reaper(
        journal: &Journal,
        repo: MockBookingRepository,
        cap: usize,
    ) -> ReapPendingBookingsUseCase {
        ReapPendingBookingsUseCase::new(Arc::new(transactor(journal)), Arc::new(repo), 120, cap)
    }

    #[tokio::test]
    async fn every_stale_booking_is_cancelled_in_its_own_transaction_until_none_are_left() {
        let journal = Journal::default();
        let repo = repository(&journal, vec![stale_booking(), stale_booking()], None);

        let reaped = reaper(&journal, repo, 100).reap_tick().await.unwrap();

        assert_eq!(reaped, 2);
        assert_eq!(
            journal.steps(),
            [
                "begin",
                "claim",
                "update_status",
                "write_outbox",
                "commit",
                "begin",
                "claim",
                "update_status",
                "write_outbox",
                "commit",
                "begin",
                "claim",
                "commit",
            ]
        );
    }

    #[tokio::test]
    async fn a_tick_never_claims_more_than_its_cap() {
        let journal = Journal::default();
        let stale = vec![stale_booking(), stale_booking(), stale_booking()];
        let repo = repository(&journal, stale, None);

        let reaped = reaper(&journal, repo, 2).reap_tick().await.unwrap();

        let claims = journal.steps().iter().filter(|s| *s == "claim").count();
        assert_eq!((reaped, claims), (2, 2));
    }
}
