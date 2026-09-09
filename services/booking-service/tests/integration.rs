#![allow(dead_code, unused_imports)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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

use domain::{
    Booking, BookingError, BookingStatus, Pagination, SeatReservationFailed, SeatReserved,
    REASON_RESERVATION_TIMEOUT,
};
use dto::{BookingResponse, PaginatedBookingsResponse};
use platform::port::BookingRepository;
use postgres_repo::PostgresBookingRepository;
use usecase::{
    CancelBookingUseCase, ConfirmBookingUseCase, CreateBookingInput, CreateBookingUseCase,
    GetBookingUseCase, ListBookingsUseCase, ReapPendingBookingsUseCase,
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

    install_outbox_audit(&pool).await;

    pool
}

async fn install_outbox_audit(pool: &PgPool) {
    sqlx::query(
        r#"CREATE TABLE outbox_audit (
            id             UUID NOT NULL,
            aggregate_id   UUID NOT NULL,
            aggregate_type TEXT NOT NULL,
            event_type     TEXT NOT NULL,
            payload        JSONB NOT NULL,
            captured_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .expect("create the outbox_audit table");

    sqlx::query(
        r#"CREATE FUNCTION capture_outbox_insert() RETURNS trigger AS $$
        BEGIN
            INSERT INTO outbox_audit (id, aggregate_id, aggregate_type, event_type, payload)
            VALUES (NEW.id, NEW.aggregate_id, NEW.aggregate_type, NEW.event_type, NEW.payload);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql"#,
    )
    .execute(pool)
    .await
    .expect("create the capture_outbox_insert function");

    sqlx::query(
        r#"CREATE TRIGGER outbox_audit_capture
        AFTER INSERT ON outbox_events
        FOR EACH ROW EXECUTE PROCEDURE capture_outbox_insert()"#,
    )
    .execute(pool)
    .await
    .expect("create the outbox_audit trigger");
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .expect("count query")
}

async fn audit_count(pool: &PgPool, event_type: &str, aggregate_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM outbox_audit WHERE event_type = $1 AND aggregate_id = $2",
    )
    .bind(event_type)
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("outbox audit count query")
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

async fn seed_pending_booking(
    create: &CreateBookingUseCase,
    user_id: Uuid,
    seat_count: usize,
) -> Booking {
    create
        .execute(CreateBookingInput {
            user_id,
            event_id: Uuid::new_v4(),
            seat_ids: (0..seat_count).map(|_| Uuid::new_v4()).collect(),
        })
        .await
        .expect("seed a pending booking")
}

async fn insert_pending_booking_row(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
    created_at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO bookings (id, user_id, event_id, seat_ids, status, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 'pending', $5, $5)",
    )
    .bind(id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(vec![Uuid::new_v4()])
    .bind(created_at)
    .execute(pool)
    .await
    .expect("insert a bookings row directly");
}

async fn age_booking(pool: &PgPool, id: Uuid, seconds: i64) {
    sqlx::query("UPDATE bookings SET created_at = now() - make_interval(secs => $1) WHERE id = $2")
        .bind(seconds as f64)
        .bind(id)
        .execute(pool)
        .await
        .expect("backdate a bookings row");
}

#[tokio::test]
async fn create_booking_persists_pending_and_writes_then_deletes_the_requested_outbox() {
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

    let row = booking_row(&pool, booking.id).await;
    assert_eq!(row.id, booking.id);
    assert_eq!(row.user_id, user_id);
    assert_eq!(row.event_id, event_id);
    assert_eq!(row.seat_ids, seat_ids);
    assert_eq!(row.status, "pending");
    assert!(row.failure_reason.is_none());
    assert_eq!(row.created_at, row.updated_at);

    let response = BookingResponse::from(&booking);
    assert_eq!(response.status, "pending");
    assert_eq!(response.seat_ids, seat_ids);
    assert_eq!(response.user_id, user_id);

    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingRequested", booking.id).await, 1);
}

#[tokio::test]
async fn get_booking_owner_reads_it_and_another_user_gets_not_found() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let get = GetBookingUseCase::new(pool.clone(), repo());

    let owner_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let seat_ids = vec![Uuid::new_v4(), Uuid::new_v4()];

    let created = create
        .execute(CreateBookingInput {
            user_id: owner_id,
            event_id,
            seat_ids: seat_ids.clone(),
        })
        .await
        .expect("create should succeed");

    let fetched = get
        .execute(owner_id, created.id)
        .await
        .expect("the owner reads their booking");
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.user_id, owner_id);
    assert_eq!(fetched.event_id, event_id);
    assert_eq!(fetched.seat_ids, seat_ids);
    assert_eq!(fetched.status, BookingStatus::Pending);

    let err = get
        .execute(Uuid::new_v4(), created.id)
        .await
        .expect_err("a different user must not read this booking");
    assert!(matches!(err, BookingError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn list_bookings_pages_only_the_callers_rows_newest_first_with_envelope() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let list = ListBookingsUseCase::new(pool.clone(), repo());

    let user_a = Uuid::new_v4();
    let user_b = Uuid::new_v4();

    let mut a_in_order: Vec<Uuid> = Vec::new();
    for _ in 0..3 {
        let a = seed_pending_booking(&create, user_a, 1).await;
        a_in_order.push(a.id);
        tokio::time::sleep(Duration::from_millis(5)).await;
        seed_pending_booking(&create, user_b, 1).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let a_newest_first: Vec<Uuid> = a_in_order.iter().rev().copied().collect();

    let page_one = Pagination::new(2, 0).expect("valid pagination");
    let (rows_one, total_one) = list
        .execute(user_a, page_one)
        .await
        .expect("first page for user A");

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
        .expect("second page for user A");

    assert_eq!(total_two, 3);
    assert_eq!(rows_two.len(), 1);
    assert_eq!(rows_two[0].id, a_newest_first[2]);

    let envelope_two = PaginatedBookingsResponse::new(&rows_two, &page_two, total_two);
    assert_eq!(envelope_two.pagination.offset, 2);
    assert!(!envelope_two.pagination.has_more);

    let seen: HashSet<Uuid> = rows_one
        .iter()
        .chain(rows_two.iter())
        .map(|b| b.id)
        .collect();
    assert_eq!(seen, a_in_order.iter().copied().collect::<HashSet<_>>());

    let user_c = Uuid::new_v4();
    let tie_ts = Utc::now();
    let lower_id = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    let higher_id = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
    insert_pending_booking_row(&pool, lower_id, user_c, tie_ts).await;
    insert_pending_booking_row(&pool, higher_id, user_c, tie_ts).await;

    let (tie_rows, tie_total) = list
        .execute(user_c, Pagination::new(10, 0).expect("valid pagination"))
        .await
        .expect("tie-break page for user C");
    assert_eq!(tie_total, 2);
    assert_eq!(
        tie_rows.iter().map(|b| b.id).collect::<Vec<_>>(),
        vec![higher_id, lower_id]
    );
}

#[tokio::test]
async fn confirm_booking_moves_pending_to_confirmed_and_emits_confirmed_outbox() {
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

    let replayed = confirm
        .execute(&event)
        .await
        .expect("confirm should succeed");
    assert!(!replayed);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "confirmed");
    assert!(row.failure_reason.is_none());
    assert!(row.updated_at > created.updated_at);

    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingConfirmed", created.id).await, 1);
}

#[tokio::test]
async fn confirm_booking_replaying_the_same_event_id_is_a_noop() {
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
    let updated_after_first = booking_row(&pool, created.id).await.updated_at;

    assert!(confirm.execute(&event).await.expect("replayed confirm"));

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "confirmed");
    assert_eq!(row.updated_at, updated_after_first);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM bookings").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingConfirmed", created.id).await, 1);
}

#[tokio::test]
async fn cancel_booking_moves_pending_to_cancelled_with_event_reason_and_emits_cancelled_outbox() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let cancel = CancelBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 2).await;

    let event = SeatReservationFailed {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reason: "seats 12C,12D already held by another booking".to_string(),
        failed_at: Utc::now(),
    };

    let replayed = cancel.execute(&event).await.expect("cancel should succeed");
    assert!(!replayed);

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "cancelled");
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("seats 12C,12D already held by another booking")
    );
    assert!(row.updated_at > created.updated_at);

    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingCancelled", created.id).await, 1);
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
    let updated_after_first = booking_row(&pool, created.id).await.updated_at;

    assert!(cancel.execute(&event).await.expect("replayed cancel"));

    let row = booking_row(&pool, created.id).await;
    assert_eq!(row.status, "cancelled");
    assert_eq!(row.failure_reason.as_deref(), Some("seat_unavailable"));
    assert_eq!(row.updated_at, updated_after_first);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM processed_events").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingCancelled", created.id).await, 1);
}

#[tokio::test]
async fn reap_tick_cancels_only_the_stale_pending_booking() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());
    let confirm = ConfirmBookingUseCase::new(pool.clone(), repo());
    let reaper = ReapPendingBookingsUseCase::new(pool.clone(), repo(), 60, 100);

    let stale = seed_pending_booking(&create, Uuid::new_v4(), 1).await;
    age_booking(&pool, stale.id, 3600).await;

    let fresh = seed_pending_booking(&create, Uuid::new_v4(), 1).await;

    let confirmed = seed_pending_booking(&create, Uuid::new_v4(), 1).await;
    confirm
        .execute(&SeatReserved {
            event_id: Uuid::new_v4(),
            booking_id: confirmed.id,
            ticketed_event_id: confirmed.event_id,
            seat_ids: confirmed.seat_ids.clone(),
            reserved_at: Utc::now(),
        })
        .await
        .expect("confirm the third booking");
    age_booking(&pool, confirmed.id, 3600).await;

    let reaped = reaper.reap_tick().await.expect("reap tick");
    assert_eq!(reaped, 1);

    let stale_row = booking_row(&pool, stale.id).await;
    assert_eq!(stale_row.status, "cancelled");
    assert_eq!(
        stale_row.failure_reason.as_deref(),
        Some(REASON_RESERVATION_TIMEOUT)
    );
    assert!(stale_row.updated_at > stale.updated_at);

    assert_eq!(booking_row(&pool, fresh.id).await.status, "pending");
    assert_eq!(booking_row(&pool, confirmed.id).await.status, "confirmed");

    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(audit_count(&pool, "BookingCancelled", stale.id).await, 1);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM outbox_audit WHERE event_type = 'BookingCancelled'"
        )
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reap_tick_racing_confirm_on_the_same_booking_yields_exactly_one_terminal_outcome() {
    let pool = fresh_db().await;
    let create = CreateBookingUseCase::new(pool.clone(), repo());

    let created = seed_pending_booking(&create, Uuid::new_v4(), 2).await;
    age_booking(&pool, created.id, 3600).await;

    let confirm = Arc::new(ConfirmBookingUseCase::new(pool.clone(), repo()));
    let reaper = Arc::new(ReapPendingBookingsUseCase::new(
        pool.clone(),
        repo(),
        60,
        100,
    ));

    let event = SeatReserved {
        event_id: Uuid::new_v4(),
        booking_id: created.id,
        ticketed_event_id: created.event_id,
        seat_ids: created.seat_ids.clone(),
        reserved_at: Utc::now(),
    };

    let confirm_task = {
        let confirm = Arc::clone(&confirm);
        tokio::spawn(async move { confirm.execute(&event).await })
    };
    let reap_task = {
        let reaper = Arc::clone(&reaper);
        tokio::spawn(async move { reaper.reap_tick().await })
    };

    let confirm_replayed = confirm_task
        .await
        .expect("confirm task must not panic")
        .expect("confirm must not error under contention");
    let reaped = reap_task
        .await
        .expect("reap task must not panic")
        .expect("reap must not error under contention");

    let row = booking_row(&pool, created.id).await;

    let cancelled_events = count(
        &pool,
        "SELECT count(*) FROM outbox_audit WHERE event_type = 'BookingCancelled'",
    )
    .await;
    let confirmed_events = count(
        &pool,
        "SELECT count(*) FROM outbox_audit WHERE event_type = 'BookingConfirmed'",
    )
    .await;

    assert_eq!(count(&pool, "SELECT count(*) FROM outbox_events").await, 0);
    assert_eq!(
        cancelled_events + confirmed_events,
        1,
        "exactly one terminal outbox event must exist (status={}, reaped={reaped}, confirm_replayed={confirm_replayed})",
        row.status
    );

    match row.status.as_str() {
        "cancelled" => {
            assert_eq!(
                row.failure_reason.as_deref(),
                Some(REASON_RESERVATION_TIMEOUT)
            );
            assert_eq!(cancelled_events, 1);
            assert_eq!(confirmed_events, 0);
            assert_eq!(reaped, 1);
        }
        "confirmed" => {
            assert!(row.failure_reason.is_none());
            assert_eq!(confirmed_events, 1);
            assert_eq!(cancelled_events, 0);
            assert_eq!(reaped, 0);
        }
        other => panic!("booking ended in an unexpected state: {other}"),
    }
}
