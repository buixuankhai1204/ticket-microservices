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

use domain::{BookingError, BookingStatus};
use platform::port::BookingRepository;
use postgres_repo::PostgresBookingRepository;
use usecase::{CreateBookingInput, CreateBookingUseCase, GetBookingUseCase};

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

    let fetched = get.execute(created.id).await.expect("get should succeed");

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
        .execute(Uuid::new_v4())
        .await
        .expect_err("a random id must not resolve to a booking");

    assert!(matches!(err, BookingError::NotFound), "got {err:?}");
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
