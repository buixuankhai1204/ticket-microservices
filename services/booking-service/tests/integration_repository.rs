mod common;

use std::sync::Arc;

use booking_service::adapter::repository::postgres::PostgresBookingRepository;
use booking_service::domain::{
    Booking, BookingError, BookingRequested, BookingStatus, DomainEvent,
};
use booking_service::platform::port::{BookingRepository, PgTransactor, Transactor};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use common::{install_outbox_tap, published, TestDb};
use uuid::Uuid;

struct Fixture {
    db: TestDb,
    transactor: Arc<PgTransactor>,
    repo: Arc<PostgresBookingRepository>,
}

async fn fixture() -> Fixture {
    let db = TestDb::migrated().await;
    install_outbox_tap(&db.pool).await;
    Fixture {
        transactor: Arc::new(PgTransactor::new(db.pool.clone())),
        repo: Arc::new(PostgresBookingRepository::new()),
        db,
    }
}

fn seats(count: usize) -> Vec<Uuid> {
    (0..count).map(|_| Uuid::new_v4()).collect()
}

fn booking_at(status: BookingStatus, created_at: DateTime<Utc>) -> Booking {
    Booking::from_persisted(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        seats(1),
        status,
        None,
        created_at,
        created_at,
    )
}

fn ago(minutes: i64) -> DateTime<Utc> {
    Utc::now() - ChronoDuration::minutes(minutes)
}

impl Fixture {
    async fn store(&self, booking: &Booking) {
        let mut tx = self.transactor.begin().await.unwrap();
        self.repo.create(&mut tx, booking).await.unwrap();
        tx.commit().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_booking_round_trips_stays_private_to_its_owner_and_keeps_a_status_change() {
    let fx = fixture().await;
    let owner = Uuid::new_v4();
    let mut booking = Booking::request(owner, Uuid::new_v4(), seats(3)).unwrap();
    fx.store(&booking).await;

    let mut tx = fx.transactor.begin().await.unwrap();
    let loaded = fx
        .repo
        .find_by_id_for_user(&mut tx, booking.id, owner)
        .await
        .unwrap();
    let stranger = fx
        .repo
        .find_by_id_for_user(&mut tx, booking.id, Uuid::new_v4())
        .await;
    booking.cancel("seat_unavailable").unwrap();
    fx.repo.update_status(&mut tx, &booking).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = fx.transactor.begin_read_only().await.unwrap();
    let cancelled = fx
        .repo
        .find_by_id_for_user(&mut tx, booking.id, owner)
        .await
        .unwrap();

    assert_eq!(loaded.seat_ids, booking.seat_ids);
    assert_eq!(loaded.status, BookingStatus::Pending);
    assert_eq!(
        loaded.created_at.timestamp_micros(),
        booking.created_at.timestamp_micros()
    );
    assert!(matches!(stranger, Err(BookingError::NotFound)));
    assert_eq!(cancelled.status, BookingStatus::Cancelled);
    assert_eq!(
        cancelled.failure_reason.as_deref(),
        Some("seat_unavailable")
    );
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn an_event_is_processed_once_and_a_rolled_back_attempt_is_forgotten() {
    let fx = fixture().await;
    let event_id = Uuid::new_v4();

    let mut abandoned = fx.transactor.begin().await.unwrap();
    assert!(!fx
        .repo
        .mark_processed(&mut abandoned, event_id)
        .await
        .unwrap());
    drop(abandoned);

    let mut first = fx.transactor.begin().await.unwrap();
    let first_time = fx.repo.mark_processed(&mut first, event_id).await.unwrap();
    first.commit().await.unwrap();
    let mut second = fx.transactor.begin().await.unwrap();
    let repeat = fx.repo.mark_processed(&mut second, event_id).await.unwrap();
    second.commit().await.unwrap();

    assert!(!first_time, "the rolled back attempt must not count");
    assert!(repeat, "a committed event id is a duplicate");
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn an_outbox_event_leaves_the_table_empty_and_only_exists_if_its_transaction_commits() {
    let fx = fixture().await;
    let booking = Booking::request(Uuid::new_v4(), Uuid::new_v4(), seats(2)).unwrap();
    let requested = BookingRequested::new(
        booking.id,
        booking.user_id,
        booking.event_id,
        booking.seat_ids.clone(),
        booking.created_at,
    );
    let event = DomainEvent::BookingRequested(requested.clone());

    let mut rolled_back = fx.transactor.begin().await.unwrap();
    fx.repo.create(&mut rolled_back, &booking).await.unwrap();
    fx.repo
        .write_outbox(&mut rolled_back, &event)
        .await
        .unwrap();
    drop(rolled_back);
    assert!(published(&fx.db.pool).await.is_empty());

    let mut tx = fx.transactor.begin().await.unwrap();
    fx.repo.create(&mut tx, &booking).await.unwrap();
    fx.repo.write_outbox(&mut tx, &event).await.unwrap();
    tx.commit().await.unwrap();

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox_events")
        .fetch_one(&fx.db.pool)
        .await
        .unwrap();
    let events = published(&fx.db.pool).await;
    assert_eq!(remaining, 0, "the row is deleted in the same transaction");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, requested.event_id);
    assert_eq!(events[0].aggregate_id, booking.id);
    assert_eq!(events[0].aggregate_type, "booking");
    assert_eq!(events[0].event_type, "BookingRequested");
    assert_eq!(events[0].payload["booking_id"], booking.id.to_string());
    assert_eq!(events[0].payload["seat_ids"].as_array().unwrap().len(), 2);
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_reaper_claims_only_stale_pending_bookings_oldest_first_and_skips_locked_ones() {
    let fx = fixture().await;
    let oldest = booking_at(BookingStatus::Pending, ago(180));
    let older = booking_at(BookingStatus::Pending, ago(120));
    let fresh = booking_at(BookingStatus::Pending, ago(0));
    let confirmed = booking_at(BookingStatus::Confirmed, ago(240));
    for booking in [&oldest, &older, &fresh, &confirmed] {
        fx.store(booking).await;
    }

    let mut first = fx.transactor.begin().await.unwrap();
    let mut second = fx.transactor.begin().await.unwrap();
    let mut third = fx.transactor.begin().await.unwrap();
    let claimed_first = fx.repo.claim_oldest_stale_pending(&mut first, 3600).await;
    let claimed_second = fx.repo.claim_oldest_stale_pending(&mut second, 3600).await;
    let claimed_third = fx.repo.claim_oldest_stale_pending(&mut third, 3600).await;

    assert_eq!(claimed_first.unwrap().unwrap().id, oldest.id);
    assert_eq!(claimed_second.unwrap().unwrap().id, older.id);
    assert!(claimed_third.unwrap().is_none());
}
