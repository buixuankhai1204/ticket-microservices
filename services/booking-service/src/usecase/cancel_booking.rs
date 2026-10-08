use std::sync::Arc;

use crate::domain::{
    BookingCancelled, BookingError, BookingStatus, DomainEvent, SeatReservationFailed,
    REASON_SEAT_UNAVAILABLE,
};
use crate::platform::port::{BookingRepository, Transactor};

pub struct CancelBookingUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
}

impl CancelBookingUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
        }
    }

    pub async fn execute(&self, ev: &SeatReservationFailed) -> Result<bool, BookingError> {
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
            booking.cancel(ev.reason.clone())?;
            self.booking_repository
                .update_status(&mut tx, &booking)
                .await?;
            let cancelled = DomainEvent::BookingCancelled(BookingCancelled::new(
                booking.id,
                booking.user_id,
                booking.event_id,
                booking.seat_ids.clone(),
                REASON_SEAT_UNAVAILABLE.to_string(),
                booking.updated_at,
            ));
            self.booking_repository
                .write_outbox(&mut tx, &cancelled)
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

    fn seat_reservation_failed(booking: &Booking) -> SeatReservationFailed {
        SeatReservationFailed {
            event_id: Uuid::new_v4(),
            booking_id: booking.id,
            ticketed_event_id: booking.event_id,
            seat_ids: booking.seat_ids.clone(),
            reason: REASON_SEAT_UNAVAILABLE.to_string(),
            failed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn seat_reservation_failed_cancels_a_pending_booking_and_publishes_booking_cancelled_in_one_transaction(
    ) {
        let journal = Journal::default();
        let booking = pending_booking();
        let ev = seat_reservation_failed(&booking);
        let (booking_id, user_id, event_id, seats) = (
            booking.id,
            booking.user_id,
            booking.event_id,
            booking.seat_ids.clone(),
        );
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        repo.expect_mark_processed()
            .times(1)
            .returning(move |_, _| {
                j.note("mark_processed");
                Ok(false)
            });
        let j = journal.clone();
        let found = booking.clone();
        repo.expect_find_for_update()
            .times(1)
            .returning(move |_, _| {
                j.note("find_for_update");
                Ok(found.clone())
            });
        let j = journal.clone();
        repo.expect_update_status()
            .withf(move |_, b| {
                b.id == booking_id
                    && b.status == BookingStatus::Cancelled
                    && b.failure_reason.as_deref() == Some(REASON_SEAT_UNAVAILABLE)
            })
            .times(1)
            .returning(move |_, _| {
                j.note("update_status");
                Ok(())
            });
        let j = journal.clone();
        repo.expect_write_outbox()
            .withf(move |_, e| {
                matches!(e, DomainEvent::BookingCancelled(c)
                    if c.booking_id == booking_id
                        && c.user_id == user_id
                        && c.ticketed_event_id == event_id
                        && c.seat_ids == seats
                        && c.reason == REASON_SEAT_UNAVAILABLE)
            })
            .times(1)
            .returning(move |_, _| {
                j.note("write_outbox");
                Ok(())
            });
        let use_case = CancelBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

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
        let use_case = CancelBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let already = use_case
            .execute(&seat_reservation_failed(&booking))
            .await
            .unwrap();

        assert!(already);
        assert_eq!(journal.steps(), ["begin", "commit"]);
    }
}
