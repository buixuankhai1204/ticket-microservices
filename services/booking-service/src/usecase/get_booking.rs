use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{Booking, BookingError};
use crate::platform::port::{BookingRepository, Transactor};

pub struct GetBookingUseCase {
    transactor: Arc<dyn Transactor>,
    booking_repository: Arc<dyn BookingRepository>,
}

impl GetBookingUseCase {
    pub fn new(
        transactor: Arc<dyn Transactor>,
        booking_repository: Arc<dyn BookingRepository>,
    ) -> Self {
        Self {
            transactor,
            booking_repository,
        }
    }

    pub async fn execute(
        &self,
        requesting_user_id: Uuid,
        id: Uuid,
    ) -> Result<Booking, BookingError> {
        let mut tx = self.transactor.begin_read_only().await?;
        let booking = self
            .booking_repository
            .find_by_id_for_user(&mut tx, id, requesting_user_id)
            .await?;
        tx.commit().await?;

        Ok(booking)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::port::testing::{transactor, Journal};
    use crate::platform::port::MockBookingRepository;

    #[tokio::test]
    async fn a_booking_is_looked_up_for_the_requesting_user_only_in_a_read_only_transaction() {
        let journal = Journal::default();
        let (user_id, id) = (Uuid::new_v4(), Uuid::new_v4());
        let stored = Booking::request(user_id, Uuid::new_v4(), vec![Uuid::new_v4()]).unwrap();
        let mut repo = MockBookingRepository::new();
        let j = journal.clone();
        repo.expect_find_by_id_for_user()
            .withf(move |_, booking_id, owner| *booking_id == id && *owner == user_id)
            .times(1)
            .returning(move |_, _, _| {
                j.note("find_by_id_for_user");
                Ok(stored.clone())
            });
        let use_case = GetBookingUseCase::new(Arc::new(transactor(&journal)), Arc::new(repo));

        let booking = use_case.execute(user_id, id).await.unwrap();

        assert_eq!(booking.user_id, user_id);
        assert_eq!(
            journal.steps(),
            ["begin_read_only", "find_by_id_for_user", "commit"]
        );
    }
}
