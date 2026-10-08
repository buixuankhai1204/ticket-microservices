use std::sync::Arc;

use crate::domain::{BookingConfirmed, BookingError, BookingStatus, DomainEvent, SeatReserved};
use crate::platform::port::{BookingRepository, Transactor};

pub struct ConfirmBookingUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
}

impl ConfirmBookingUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
        }
    }

    pub async fn execute(&self, ev: &SeatReserved) -> Result<bool, BookingError> {
        let mut tx = self.transactor.begin().await?;

        if self
            .booking_repository
            .mark_processed(&mut tx, ev.event_id)
            .await?
        {
            tx.commit().await?;
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

        tx.commit().await?;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::domain::Booking;
    use crate::platform::port::testing::{transactor, Journal};
    use crate::platform::port::MockBookingRepository;

    fn pending_booking() -> Booking {
        Booking::request(
            Uuid::new_v4(),
            Uuid::new_v4(),
            vec![Uuid::new_v4(), Uuid::new_v4()],
        )
        .unwrap()
    }

    fn seat_reserved(booking: &Booking) -> SeatReserved {
        SeatReserved {
            event_id: Uuid::new_v4(),
            booking_id: booking.id,
            ticketed_event_id: booking.event_id,
            seat_ids: booking.seat_ids.clone(),
            reserved_at: Utc::now(),
        }
    }

    fn deadlock() -> BookingError {
        BookingError::Repository {
            message: "deadlock detected".to_string(),
            sqlstate: Some("40P01".to_string()),
        }
    }

    fn outcome<T>(fail_at: &str, step: &str, ok: T) -> Result<T, BookingError> {
        if fail_at == step {
            Err(deadlock())
        } else {
            Ok(ok)
        }
    }

    fn repository_walking(
        journal: &Journal,
        stored: &Booking,
        fail_at: &'static str,
    ) -> MockBookingRepository {
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        repo.expect_mark_processed().returning(move |_, _| {
            j.note("mark_processed");
            outcome(fail_at, "mark_processed", false)
        });
        let j = journal.clone();
        let found = stored.clone();
        repo.expect_find_for_update().returning(move |_, _| {
            j.note("find_for_update");
            outcome(fail_at, "find_for_update", found.clone())
        });
        let j = journal.clone();
        repo.expect_update_status().returning(move |_, _| {
            j.note("update_status");
            outcome(fail_at, "update_status", ())
        });
        let j = journal.clone();
        repo.expect_write_outbox().returning(move |_, _| {
            j.note("write_outbox");
            outcome(fail_at, "write_outbox", ())
        });
        repo
    }

    #[tokio::test]
    async fn seat_reserved_confirms_a_pending_booking_and_publishes_booking_confirmed_in_one_transaction(
    ) {
        let journal = Journal::default();
        let booking = pending_booking();
        let ev = seat_reserved(&booking);
        let (booking_id, user_id, event_id, seats, ev_id) = (
            booking.id,
            booking.user_id,
            booking.event_id,
            booking.seat_ids.clone(),
            ev.event_id,
        );
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        repo.expect_mark_processed()
            .withf(move |_, id| *id == ev_id)
            .times(1)
            .returning(move |_, _| {
                j.note("mark_processed");
                Ok(false)
            });
        let j = journal.clone();
        let found = booking.clone();
        repo.expect_find_for_update()
            .withf(move |_, id| *id == booking_id)
            .times(1)
            .returning(move |_, _| {
                j.note("find_for_update");
                Ok(found.clone())
            });
        let j = journal.clone();
        repo.expect_update_status()
            .withf(move |_, b| b.id == booking_id && b.status == BookingStatus::Confirmed)
            .times(1)
            .returning(move |_, _| {
                j.note("update_status");
                Ok(())
            });
        let j = journal.clone();
        repo.expect_write_outbox()
            .withf(move |_, e| {
                matches!(e, DomainEvent::BookingConfirmed(c)
                    if c.booking_id == booking_id
                        && c.user_id == user_id
                        && c.ticketed_event_id == event_id
                        && c.seat_ids == seats)
            })
            .times(1)
            .returning(move |_, _| {
                j.note("write_outbox");
                Ok(())
            });
        let use_case = ConfirmBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let already = use_case.execute(&ev).await.unwrap();

        assert!(!already);
        assert_eq!(
            journal.steps(),
            [
                "begin",
                "mark_processed",
                "find_for_update",
                "update_status",
                "write_outbox",
                "commit"
            ]
        );
    }

    #[tokio::test]
    async fn a_duplicate_event_is_committed_and_reported_without_touching_the_booking() {
        let journal = Journal::default();
        let booking = pending_booking();
        let mut repo = MockBookingRepository::new();
        repo.expect_mark_processed()
            .times(1)
            .returning(|_, _| Ok(true));
        let use_case = ConfirmBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let already = use_case.execute(&seat_reserved(&booking)).await.unwrap();

        assert!(already);
        assert_eq!(journal.steps(), ["begin", "commit"]);
    }

    #[tokio::test]
    async fn a_failure_at_any_step_rolls_back_and_reaches_the_consumer_unchanged() {
        for fail_at in [
            "mark_processed",
            "find_for_update",
            "update_status",
            "write_outbox",
        ] {
            let journal = Journal::default();
            let booking = pending_booking();
            let repo = repository_walking(&journal, &booking, fail_at);
            let use_case =
                ConfirmBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

            let err = use_case
                .execute(&seat_reserved(&booking))
                .await
                .unwrap_err();

            assert!(
                matches!(&err, BookingError::Repository { sqlstate: Some(code), .. } if code == "40P01"),
                "{fail_at}: {err:?}"
            );
            assert!(
                !journal.committed(),
                "{fail_at} must not commit: {:?}",
                journal.steps()
            );
        }
    }
}
