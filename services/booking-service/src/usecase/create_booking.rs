use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{Booking, BookingError, BookingRequested, DomainEvent};
use crate::platform::port::{BookingRepository, Transactor};

pub struct CreateBookingInput {
    pub user_id: Uuid,
    pub event_id: Uuid,
    pub seat_ids: Vec<Uuid>,
}

pub struct CreateBookingUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
}

impl CreateBookingUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
    ) -> Self {
        Self {
            transactor,
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

        let mut tx = self.transactor.begin().await?;
        self.booking_repository.create(&mut tx, &booking).await?;
        self.booking_repository
            .write_outbox(&mut tx, &requested)
            .await?;
        tx.commit().await?;

        Ok(booking)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::MAX_SEATS_PER_BOOKING;
    use crate::domain::BookingStatus;
    use crate::platform::port::testing::{transactor, Journal};
    use crate::platform::port::{MockBookingRepository, MockTransactor};

    fn input(seats: usize) -> CreateBookingInput {
        CreateBookingInput {
            user_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            seat_ids: (0..seats).map(|_| Uuid::new_v4()).collect(),
        }
    }

    fn repository_noting(journal: &Journal, fail_outbox: bool) -> MockBookingRepository {
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        repo.expect_create().returning(move |_, _| {
            j.note("create");
            Ok(())
        });
        let j = journal.clone();
        repo.expect_write_outbox().returning(move |_, _| {
            j.note("write_outbox");
            if fail_outbox {
                Err(BookingError::Repository {
                    message: "serialization failure".to_string(),
                    sqlstate: Some("40001".to_string()),
                })
            } else {
                Ok(())
            }
        });
        repo
    }

    #[tokio::test]
    async fn a_request_the_domain_rejects_never_opens_a_transaction() {
        let rejected = [
            (input(0), "no seats"),
            (
                CreateBookingInput {
                    seat_ids: vec![Uuid::nil(), Uuid::nil()],
                    ..input(0)
                },
                "duplicate seats",
            ),
            (input(MAX_SEATS_PER_BOOKING + 1), "too many seats"),
        ];
        for (request, why) in rejected {
            let use_case = CreateBookingUseCase::new(
                Arc::new(MockTransactor::new()),
                Arc::new(MockBookingRepository::new()),
            );

            let err = use_case.execute(request).await.unwrap_err();

            assert!(
                matches!(
                    err,
                    BookingError::NoSeats
                        | BookingError::DuplicateSeats
                        | BookingError::TooManySeats(_)
                ),
                "{why}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn creating_a_booking_stores_it_pending_and_publishes_booking_requested_in_one_transaction(
    ) {
        let journal = Journal::default();
        let request = input(3);
        let (user_id, event_id, seats) =
            (request.user_id, request.event_id, request.seat_ids.clone());
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        let (u, e, s) = (user_id, event_id, seats.clone());
        repo.expect_create()
            .withf(move |_, b| {
                b.user_id == u
                    && b.event_id == e
                    && b.seat_ids == s
                    && b.status == BookingStatus::Pending
            })
            .times(1)
            .returning(move |_, _| {
                j.note("create");
                Ok(())
            });
        let j = journal.clone();
        repo.expect_write_outbox()
            .withf(move |_, ev| {
                matches!(ev, DomainEvent::BookingRequested(r)
                    if r.user_id == user_id
                        && r.ticketed_event_id == event_id
                        && r.seat_ids == seats)
            })
            .times(1)
            .returning(move |_, _| {
                j.note("write_outbox");
                Ok(())
            });
        let use_case = CreateBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let booking = use_case.execute(request).await.unwrap();

        assert_eq!(booking.status, BookingStatus::Pending);
        assert_eq!(
            journal.steps(),
            ["begin", "create", "write_outbox", "commit"]
        );
    }

    #[tokio::test]
    async fn a_failed_outbox_write_rolls_the_booking_back_and_surfaces_the_error() {
        let journal = Journal::default();
        let repo = repository_noting(&journal, true);
        let use_case = CreateBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let err = use_case.execute(input(1)).await.unwrap_err();

        assert!(
            matches!(&err, BookingError::Repository { sqlstate: Some(code), .. } if code == "40001")
        );
        assert_eq!(journal.steps(), ["begin", "create", "write_outbox"]);
    }
}
