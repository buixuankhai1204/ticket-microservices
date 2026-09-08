#![allow(dead_code, unused_imports)]

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use tokio::sync::OnceCell;

#[path = "../src/domain/mod.rs"]
mod domain;

#[path = "../src/platform/mod.rs"]
mod platform;

#[path = "../src/usecase/mod.rs"]
mod usecase;

#[path = "../src/adapter/repository/postgres.rs"]
mod postgres_repo;

#[path = "../src/adapter/http/dto.rs"]
mod dto;

use domain::{BookingError, BookingStatus, Pagination, SeatReservationFailed, SeatReserved};
use dto::PaginatedBookingsResponse;
use platform::port::BookingRepository;
use postgres_repo::PostgresBookingRepository;
use usecase::{
    CancelBookingUseCase, ConfirmBookingUseCase, CreateBookingInput, CreateBookingUseCase,
    GetBookingUseCase, ListBookingsUseCase,
};

struct SharedPg {
    _container: ContainerAsync<PostgresImage>,
    base_url: String,
}

static SHARED_PG: OnceCell<SharedPg> = OnceCell::const_new();

async fn shared_pg() -> &'static SharedPg {
    SHARED_PG
        .get_or_init(|| async {
            let container = PostgresImage::default().start().await.expect(
                "failed to start the Postgres testcontainer - is Docker running? \
                 These are integration tests and will not run without it.",
            );
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("failed to map the Postgres container port");
            let base_url = format!("postgres://postgres:postgres@127.0.0.1:{port}");
            SharedPg {
                _container: container,
                base_url,
            }
        })
        .await
}

async fn fresh_db() -> PgPool {
    use sqlx::Connection;

    let shared = shared_pg().await;

    let mut admin = sqlx::PgConnection::connect(&format!("{}/postgres", shared.base_url))
        .await
        .expect("failed to open an admin connection");

    let name = format!("test_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(&mut admin)
        .await
        .expect("failed to create the isolated test database");
    let _ = admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect(&format!("{}/{}", shared.base_url, name))
        .await
        .expect("failed to connect the test database pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations on the test database");

    pool
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .expect("count query")
}

fn repo() -> Arc<dyn BookingRepository> {
    Arc::new(PostgresBookingRepository::new())
}

#[derive(sqlx::FromRow)]
struct PersistedBookingRow {
    id: Uuid,
    user_id: Uuid,
    event_id: Uuid,
    seat_ids: Vec<Uuid>,
    status: String,
    failure_reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[tokio::test]
async fn create_booking_persists_pending_booking_with_empty_outbox() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let user_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let seat_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

    let booking = create
        .execute(CreateBookingInput {
            user_id,
            event_id,
            seat_ids: seat_ids.clone(),
        })
        .await
        .expect("create should succeed");

    assert_eq!(booking.status, BookingStatus::Pending);

    let row: PersistedBookingRow = sqlx::query_as(
        "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
         FROM bookings WHERE id = $1",
    )
    .bind(booking.id)
    .fetch_one(&pool)
    .await
    .expect("a bookings row must exist");

    assert_eq!(row.id, booking.id);
    assert_eq!(row.user_id, user_id);
    assert_eq!(row.event_id, event_id);
    assert_eq!(row.seat_ids, seat_ids);
    assert_eq!(row.status, "pending");
    assert!(row.failure_reason.is_none());
    assert_eq!(row.created_at, row.updated_at);

    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}

#[tokio::test]
async fn create_booking_rejects_no_seats_and_persists_nothing() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let err = create
        .execute(CreateBookingInput {
            user_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            seat_ids: vec![],
        })
        .await
        .expect_err("empty seat_ids must be rejected");

    assert!(matches!(err, BookingError::NoSeats), "got {err:?}");
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 0);
}

#[tokio::test]
async fn create_booking_rejects_duplicate_seats_and_persists_nothing() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let seat = Uuid::new_v4();
    let err = create
        .execute(CreateBookingInput {
            user_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            seat_ids: vec![seat, seat],
        })
        .await
        .expect_err("duplicate seats must be rejected");

    assert!(matches!(err, BookingError::DuplicateSeats), "got {err:?}");
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 0);
}

#[tokio::test]
async fn create_booking_rejects_too_many_seats_and_persists_nothing() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let seat_ids: Vec<Uuid> = (0..21).map(|_| Uuid::new_v4()).collect();
    let err = create
        .execute(CreateBookingInput {
            user_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            seat_ids,
        })
        .await
        .expect_err("more than the cap must be rejected");

    assert!(matches!(err, BookingError::TooManySeats(20)), "got {err:?}");
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 0);
}

#[tokio::test]
async fn create_booking_rolls_back_everything_when_outbox_insert_fails() {
    let pool = fresh_db().await;
    sqlx::query("DROP TABLE outbox_events")
        .execute(&pool)
        .await
        .expect("drop outbox_events for the failure scenario");

    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let err = create
        .execute(CreateBookingInput {
            user_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            seat_ids: vec![Uuid::new_v4()],
        })
        .await
        .expect_err("the outbox insert must fail without the table");

    assert!(matches!(err, BookingError::Repository(_)), "got {err:?}");
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 0);
}

#[tokio::test]
async fn get_booking_returns_matching_persisted_fields() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let get = GetBookingUseCase::new(pool.clone(), repo());

    let user_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let seat_ids = vec![Uuid::new_v4(), Uuid::new_v4()];

    let created = create
        .execute(CreateBookingInput {
            user_id,
            event_id,
            seat_ids: seat_ids.clone(),
        })
        .await
        .expect("create should succeed");

    let fetched = get
        .execute(user_id, created.id)
        .await
        .expect("get should succeed");

    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.user_id, user_id);
    assert_eq!(fetched.event_id, event_id);
    assert_eq!(fetched.seat_ids, seat_ids);
    assert_eq!(fetched.status, BookingStatus::Pending);
    assert!(fetched.failure_reason.is_none());

    let created_diff = (fetched.created_at - created.created_at)
        .num_microseconds()
        .unwrap()
        .abs();
    let updated_diff = (fetched.updated_at - created.updated_at)
        .num_microseconds()
        .unwrap()
        .abs();
    assert!(
        created_diff < 1_000,
        "created_at drifted by {created_diff}us"
    );
    assert!(
        updated_diff < 1_000,
        "updated_at drifted by {updated_diff}us"
    );
}

#[tokio::test]
async fn get_booking_not_found_returns_not_found() {
    let pool = fresh_db().await;
    let get = GetBookingUseCase::new(pool.clone(), repo());

    let err = get
        .execute(Uuid::new_v4(), Uuid::new_v4())
        .await
        .expect_err("a random id must not resolve to a booking");

    assert!(matches!(err, BookingError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn get_booking_another_users_booking_is_not_found() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let get = GetBookingUseCase::new(pool.clone(), repo());

    let owner_id = Uuid::new_v4();
    let created = create
        .execute(CreateBookingInput {
            user_id: owner_id,
            event_id: Uuid::new_v4(),
            seat_ids: vec![Uuid::new_v4()],
        })
        .await
        .expect("create should succeed");

    let err = get
        .execute(Uuid::new_v4(), created.id)
        .await
        .expect_err("another user must not read this booking");

    assert!(matches!(err, BookingError::NotFound), "got {err:?}");

    let fetched = get
        .execute(owner_id, created.id)
        .await
        .expect("the owner still reads it");
    assert_eq!(fetched.id, created.id);
}

async fn seed_pending_booking(
    create: &CreateBookingUseCase,
    user_id: Uuid,
    seat_count: usize,
) -> domain::Booking {
    create
        .execute(CreateBookingInput {
            user_id,
            event_id: Uuid::new_v4(),
            seat_ids: (0..seat_count).map(|_| Uuid::new_v4()).collect(),
        })
        .await
        .expect("seed pending booking")
}

async fn booking_row(pool: &PgPool, id: Uuid) -> PersistedBookingRow {
    sqlx::query_as(
        "SELECT id, user_id, event_id, seat_ids, status, failure_reason, created_at, updated_at \
         FROM bookings WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("a bookings row must exist")
}

#[tokio::test]
async fn list_bookings_returns_only_the_callers_page_newest_first() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let list = ListBookingsUseCase::new(pool.clone(), repo());

    let user_a = Uuid::new_v4();
    let user_b = Uuid::new_v4();

    let mut a_oldest_first: Vec<Uuid> = Vec::new();
    for _ in 0..3 {
        let a = seed_pending_booking(&create, user_a, 1).await;
        a_oldest_first.push(a.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let _b = seed_pending_booking(&create, user_b, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let a_newest_first: Vec<Uuid> = a_oldest_first.iter().rev().copied().collect();

    let page_one = Pagination::new(2, 0).expect("valid pagination");
    let (rows_one, total_one) = list
        .execute(user_a, page_one)
        .await
        .expect("first page for A");

    assert_eq!(total_one, 3);
    assert_eq!(rows_one.len(), 2);
    assert!(rows_one.iter().all(|b| b.user_id == user_a));
    assert_eq!(
        rows_one.iter().map(|b| b.id).collect::<Vec<_>>(),
        a_newest_first[..2].to_vec()
    );

    let envelope_one = PaginatedBookingsResponse::new(&rows_one, &page_one, total_one);
    assert_eq!(envelope_one.pagination.limit, 2);
    assert_eq!(envelope_one.pagination.offset, 0);
    assert_eq!(envelope_one.pagination.total, 3);
    assert!(envelope_one.pagination.has_more);
    assert_eq!(envelope_one.data.len(), 2);
    assert_eq!(envelope_one.data[0].id, a_newest_first[0]);
    assert_eq!(envelope_one.data[0].user_id, user_a);
    assert_eq!(envelope_one.data[0].status, "pending");

    let page_two = Pagination::new(2, 2).expect("valid pagination");
    let (rows_two, total_two) = list
        .execute(user_a, page_two)
        .await
        .expect("second page for A");

    assert_eq!(total_two, 3);
    assert_eq!(rows_two.len(), 1);
    assert_eq!(rows_two[0].id, a_newest_first[2]);
    assert!(rows_two.iter().all(|b| b.user_id == user_a));

    let envelope_two = PaginatedBookingsResponse::new(&rows_two, &page_two, total_two);
    assert_eq!(envelope_two.pagination.offset, 2);
    assert!(!envelope_two.pagination.has_more);

    let seen: HashSet<Uuid> = rows_one
        .iter()
        .chain(rows_two.iter())
        .map(|b| b.id)
        .collect();
    assert_eq!(seen, a_oldest_first.iter().copied().collect::<HashSet<_>>());
}

#[tokio::test]
async fn confirm_booking_pending_to_confirmed_and_emits_outbox() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let confirm = ConfirmBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 2).await;

    let event = SeatReserved {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reserved_at: Utc::now(),
    };

    let already = confirm
        .execute(&event)
        .await
        .expect("confirm should succeed");
    assert!(!already);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "confirmed");
    assert!(row.failure_reason.is_none());
    assert!(row.updated_at > created.updated_at);

    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}

#[tokio::test]
async fn confirm_booking_replaying_the_same_event_is_a_noop() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let confirm = ConfirmBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 1).await;

    let event = SeatReserved {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reserved_at: Utc::now(),
    };

    assert!(!confirm.execute(&event).await.expect("first confirm"));
    let after_first = booking_row(&pool, created.id).await.updated_at;

    let replayed = confirm.execute(&event).await.expect("replayed confirm");
    assert!(replayed);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "confirmed");
    assert_eq!(row.updated_at, after_first);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}

#[tokio::test]
async fn cancel_booking_pending_to_cancelled_with_reason_and_emits_outbox() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let cancel = CancelBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 2).await;

    let event = SeatReservationFailed {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reason: "seats 4A,4B already held by another booking".to_string(),
        failed_at: Utc::now(),
    };

    let already = cancel.execute(&event).await.expect("cancel should succeed");
    assert!(!already);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "cancelled");
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("seats 4A,4B already held by another booking")
    );
    assert!(row.updated_at > created.updated_at);

    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}

#[tokio::test]
async fn cancel_booking_replaying_the_same_failure_event_stays_cancelled() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let cancel = CancelBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 1).await;

    let event = SeatReservationFailed {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reason: "seat_unavailable".to_string(),
        failed_at: Utc::now(),
    };

    assert!(!cancel
        .execute(&event)
        .await
        .expect("first cancel reaches the terminal state"));
    let after_first = booking_row(&pool, created.id).await.updated_at;

    let replayed = cancel.execute(&event).await.expect("replayed cancel");
    assert!(replayed);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "cancelled");
    assert_eq!(row.failure_reason.as_deref(), Some("seat_unavailable"));
    assert_eq!(row.updated_at, after_first);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}

#[tokio::test]
async fn create_booking_concurrent_requests_do_not_corrupt_rows() {
    let pool = fresh_db().await;
    let create = Arc::new(CreateBookingUseCase::new(pool.clone(), repo()));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let create = create.clone();
        handles.push(tokio::spawn(async move {
            create
                .execute(CreateBookingInput {
                    user_id: Uuid::new_v4(),
                    event_id: Uuid::new_v4(),
                    seat_ids: vec![Uuid::new_v4(), Uuid::new_v4()],
                })
                .await
        }));
    }

    let mut ids = HashSet::new();
    for handle in handles {
        let booking = handle
            .await
            .expect("task must not panic")
            .expect("create should succeed");
        assert!(ids.insert(booking.id), "booking ids must be distinct");
    }

    assert_eq!(ids.len(), 10);
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 10);
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
}
