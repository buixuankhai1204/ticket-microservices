mod common;

use std::sync::Arc;

use booking_service::adapter::http::build_router;
use booking_service::adapter::messaging::kafka::consumer::{
    CancelBookingHandler, ConfirmBookingHandler, HandlerError, SagaHandler,
};
use booking_service::adapter::repository::postgres::PostgresBookingRepository;
use booking_service::app::{App, AppConfig};
use booking_service::platform::port::{BookingRepository, PgTransactor, Transactor};
use booking_service::usecase::{CancelBookingUseCase, ConfirmBookingUseCase};
use common::{
    install_outbox_tap, published, published_types, seat_reservation_failed, seat_reserved,
    token_for, token_with, Api, TestDb, JWT_ISSUER, JWT_SECRET,
};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
enum Outcome {
    Applied,
    Duplicate,
    Ignored,
    Unparseable,
    Transient(String),
    Permanent(String),
}

struct Service {
    api: Api,
    db: TestDb,
    app: App,
    confirm: ConfirmBookingHandler,
    cancel: CancelBookingHandler,
    server: JoinHandle<()>,
}

impl Drop for Service {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn service() -> Service {
    let db = TestDb::migrated().await;
    install_outbox_tap(&db.pool).await;
    let app = App::new(
        db.pool.clone(),
        AppConfig {
            jwt_secret: JWT_SECRET.to_string(),
            jwt_issuer: JWT_ISSUER.to_string(),
            kafka_brokers: "unused:9092".to_string(),
            seat_reservation_topic: "unused".to_string(),
            consumer_group_suffix: String::new(),
            consumer_max_attempts: 3,
            pending_timeout_secs: 120,
            reaper_interval_secs: 30,
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = build_router(app.state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let transactor: Arc<dyn Transactor> = Arc::new(PgTransactor::new(db.pool.clone()));
    let repository: Arc<dyn BookingRepository> = Arc::new(PostgresBookingRepository::new());
    let confirm = ConfirmBookingHandler::new(
        Arc::new(ConfirmBookingUseCase::new(
            transactor.clone(),
            repository.clone(),
        )),
        "",
    );
    let cancel = CancelBookingHandler::new(
        Arc::new(CancelBookingUseCase::new(transactor, repository)),
        "",
    );
    Service {
        api: Api::new(base),
        db,
        app,
        confirm,
        cancel,
        server,
    }
}

async fn run<H: SagaHandler>(handler: &H, payload: &[u8]) -> Outcome {
    let Ok(event) = serde_json::from_slice::<H::Event>(payload) else {
        return Outcome::Unparseable;
    };
    match handler.handle(&event).await {
        Ok(false) => Outcome::Applied,
        Ok(true) => Outcome::Duplicate,
        Err(HandlerError::Transient(message)) => Outcome::Transient(message),
        Err(HandlerError::Permanent(message)) => Outcome::Permanent(message),
    }
}

impl Service {
    async fn deliver(&self, event_type: &str, payload: &[u8]) -> Outcome {
        match event_type {
            "SeatReserved" => run(&self.confirm, payload).await,
            "SeatReservationFailed" => run(&self.cancel, payload).await,
            _ => Outcome::Ignored,
        }
    }

    async fn deliver_json(&self, event_type: &str, event: &Value) -> Outcome {
        self.deliver(event_type, &serde_json::to_vec(event).unwrap())
            .await
    }

    async fn status_of(&self, token: &str, booking: &Value) -> Value {
        self.api
            .booking(token, booking["id"].as_str().unwrap())
            .await
    }

    async fn backdate(&self, booking: &Value) {
        let id: Uuid = booking["id"].as_str().unwrap().parse().unwrap();
        sqlx::query("UPDATE bookings SET created_at = now() - interval '1 hour' WHERE id = $1")
            .bind(id)
            .execute(&self.db.pool)
            .await
            .unwrap();
    }
}

fn id_of(booking: &Value) -> &str {
    booking["id"].as_str().unwrap()
}

fn event_of(booking: &Value) -> Uuid {
    booking["event_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_booking_is_created_pending_readable_only_by_its_owner_and_announced_as_booking_requested(
) {
    let svc = service().await;
    let (alice_id, bob_id) = (Uuid::new_v4(), Uuid::new_v4());
    let (alice, bob) = (token_for(alice_id), token_for(bob_id));
    let (event_id, seats) = (Uuid::new_v4(), vec![Uuid::new_v4(), Uuid::new_v4()]);

    let (status, created) = svc.api.create_booking(&alice, event_id, &seats).await;

    assert_eq!(status, 202);
    assert_eq!(created["status"], "pending");
    assert_eq!(created["user_id"], alice_id.to_string());
    assert_eq!(created["event_id"], event_id.to_string());
    assert_eq!(created["failure_reason"], Value::Null);
    let path = format!("/api/v1/bookings/{}", id_of(&created));
    assert_eq!(svc.api.get(&path, Some(&alice)).await.0, 200);
    assert_eq!(svc.api.get(&path, Some(&bob)).await.0, 404);
    assert_eq!(svc.api.get(&path, None).await.0, 401);
    let events = published(&svc.db.pool).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "BookingRequested");
    assert_eq!(events[0].aggregate_type, "booking");
    assert_eq!(events[0].aggregate_id.to_string(), id_of(&created));
    assert_eq!(events[0].payload["booking_id"], created["id"]);
    assert_eq!(events[0].payload["user_id"], alice_id.to_string());
    assert_eq!(events[0].payload["ticketed_event_id"], event_id.to_string());
    assert_eq!(events[0].payload["seat_ids"], json!(seats));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn only_a_token_signed_by_the_user_service_for_a_real_user_is_accepted() {
    let svc = service().await;
    let user = Uuid::new_v4().to_string();
    let rejected = [
        ("no header", None),
        (
            "not a bearer scheme",
            Some("Basic YWxpY2U6c2VjcmV0".to_string()),
        ),
        ("garbage token", Some("Bearer not-a-jwt".to_string())),
        (
            "expired token",
            Some(format!(
                "Bearer {}",
                token_with(&user, JWT_SECRET, JWT_ISSUER, -3600)
            )),
        ),
        (
            "token signed with another secret",
            Some(format!(
                "Bearer {}",
                token_with(&user, "another-secret", JWT_ISSUER, 3600)
            )),
        ),
        (
            "token from another issuer",
            Some(format!(
                "Bearer {}",
                token_with(&user, JWT_SECRET, "someone-else", 3600)
            )),
        ),
        (
            "subject that is not a uuid",
            Some(format!(
                "Bearer {}",
                token_with("alice", JWT_SECRET, JWT_ISSUER, 3600)
            )),
        ),
    ];
    let http = reqwest::Client::new();

    for (why, header) in rejected {
        let mut request = http.get(format!("{}/api/v1/bookings", svc.api.base));
        if let Some(header) = header {
            request = request.header("Authorization", header);
        }
        let status = request.send().await.unwrap().status().as_u16();
        assert_eq!(status, 401, "{why}");
    }
    assert_eq!(
        svc.api
            .get("/api/v1/bookings", Some(&token_for(Uuid::new_v4())))
            .await
            .0,
        200
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn seat_reserved_confirms_the_booking_once_and_a_redelivery_changes_nothing() {
    let svc = service().await;
    let user = Uuid::new_v4();
    let token = token_for(user);
    let booking = svc.api.create_booking_with_seats(&token, 2).await;
    let reserved = seat_reserved(id_of(&booking), event_of(&booking));

    let first = svc.deliver_json("SeatReserved", &reserved).await;
    let confirmed = svc.status_of(&token, &booking).await;
    let after_first = published_types(&svc.db.pool).await;
    let again = svc.deliver_json("SeatReserved", &reserved).await;

    assert_eq!(first, Outcome::Applied);
    assert_eq!(confirmed["status"], "confirmed");
    assert_eq!(after_first, ["BookingRequested", "BookingConfirmed"]);
    let events = published(&svc.db.pool).await;
    assert_eq!(events[1].aggregate_id.to_string(), id_of(&booking));
    assert_eq!(events[1].payload["booking_id"], booking["id"]);
    assert_eq!(events[1].payload["user_id"], user.to_string());
    assert_eq!(events[1].payload["seat_ids"], booking["seat_ids"]);
    assert_eq!(again, Outcome::Duplicate);
    assert_eq!(published_types(&svc.db.pool).await, after_first);
    assert_eq!(svc.status_of(&token, &booking).await["status"], "confirmed");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn seat_reservation_failed_cancels_the_booking_with_its_reason_and_announces_booking_cancelled(
) {
    let svc = service().await;
    let token = token_for(Uuid::new_v4());
    let booking = svc.api.create_booking_with_seats(&token, 1).await;
    let failed = seat_reservation_failed(id_of(&booking), event_of(&booking), "seat_unavailable");

    let outcome = svc.deliver_json("SeatReservationFailed", &failed).await;

    assert_eq!(outcome, Outcome::Applied);
    let cancelled = svc.status_of(&token, &booking).await;
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["failure_reason"], "seat_unavailable");
    let events = published(&svc.db.pool).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, "BookingCancelled");
    assert_eq!(events[1].payload["booking_id"], booking["id"]);
    assert_eq!(events[1].payload["reason"], "seat_unavailable");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_late_outcome_never_overturns_a_booking_that_already_reached_a_terminal_state() {
    let svc = service().await;
    let token = token_for(Uuid::new_v4());
    let cancelled = svc.api.create_booking_with_seats(&token, 1).await;
    let confirmed = svc.api.create_booking_with_seats(&token, 1).await;
    svc.deliver_json(
        "SeatReservationFailed",
        &seat_reservation_failed(id_of(&cancelled), event_of(&cancelled), "seat_unavailable"),
    )
    .await;
    svc.deliver_json(
        "SeatReserved",
        &seat_reserved(id_of(&confirmed), event_of(&confirmed)),
    )
    .await;
    let before = published_types(&svc.db.pool).await;

    let late_reserved = svc
        .deliver_json(
            "SeatReserved",
            &seat_reserved(id_of(&cancelled), event_of(&cancelled)),
        )
        .await;
    let late_failed = svc
        .deliver_json(
            "SeatReservationFailed",
            &seat_reservation_failed(id_of(&confirmed), event_of(&confirmed), "seat_unavailable"),
        )
        .await;

    assert_eq!(late_reserved, Outcome::Applied);
    assert_eq!(late_failed, Outcome::Applied);
    assert_eq!(
        svc.status_of(&token, &cancelled).await["status"],
        "cancelled"
    );
    assert_eq!(
        svc.status_of(&token, &confirmed).await["status"],
        "confirmed"
    );
    assert_eq!(published_types(&svc.db.pool).await, before);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn messages_the_service_cannot_use_never_touch_a_booking() {
    let svc = service().await;
    let token = token_for(Uuid::new_v4());
    let booking = svc.api.create_booking_with_seats(&token, 1).await;
    let unknown_booking = seat_reserved(&Uuid::new_v4().to_string(), event_of(&booking));
    let mut without_booking_id = seat_reserved(id_of(&booking), event_of(&booking));
    without_booking_id
        .as_object_mut()
        .unwrap()
        .remove("booking_id");

    let ignored = svc
        .deliver_json(
            "BookingRequested",
            &seat_reserved(id_of(&booking), event_of(&booking)),
        )
        .await;
    let not_json = svc.deliver("SeatReserved", b"{{ nope").await;
    let empty = svc.deliver("SeatReserved", b"").await;
    let missing_field = svc.deliver_json("SeatReserved", &without_booking_id).await;
    let unknown = svc.deliver_json("SeatReserved", &unknown_booking).await;

    assert_eq!(ignored, Outcome::Ignored);
    assert_eq!(not_json, Outcome::Unparseable);
    assert_eq!(empty, Outcome::Unparseable);
    assert_eq!(missing_field, Outcome::Unparseable);
    assert_eq!(unknown, Outcome::Permanent("booking not found".to_string()));
    assert_eq!(svc.status_of(&token, &booking).await["status"], "pending");
    assert_eq!(published_types(&svc.db.pool).await, ["BookingRequested"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn when_the_database_is_gone_events_fail_as_transient_and_readiness_reports_it() {
    let svc = service().await;
    let token = token_for(Uuid::new_v4());
    let booking = svc.api.create_booking_with_seats(&token, 1).await;
    assert_eq!(svc.api.get("/healthz", None).await.0, 200);
    assert_eq!(svc.api.get("/readyz", None).await.0, 200);

    svc.db.pool.close().await;

    let outcome = svc
        .deliver_json(
            "SeatReserved",
            &seat_reserved(id_of(&booking), event_of(&booking)),
        )
        .await;
    assert!(matches!(outcome, Outcome::Transient(_)), "{outcome:?}");
    assert_eq!(svc.api.get("/readyz", None).await.0, 503);
    assert_eq!(svc.api.get("/healthz", None).await.0, 200);
    assert_eq!(svc.api.get("/api/v1/bookings", Some(&token)).await.0, 500);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_reaper_cancels_only_stale_pending_bookings_once_and_announces_the_timeout() {
    let svc = service().await;
    let token = token_for(Uuid::new_v4());
    let stale = svc.api.create_booking_with_seats(&token, 1).await;
    let fresh = svc.api.create_booking_with_seats(&token, 1).await;
    let confirmed = svc.api.create_booking_with_seats(&token, 1).await;
    svc.deliver_json(
        "SeatReserved",
        &seat_reserved(id_of(&confirmed), event_of(&confirmed)),
    )
    .await;
    svc.backdate(&stale).await;
    svc.backdate(&confirmed).await;

    let first_tick = svc.app.reaper.reap_tick().await.unwrap();
    let second_tick = svc.app.reaper.reap_tick().await.unwrap();

    assert_eq!((first_tick, second_tick), (1, 0));
    let reaped = svc.status_of(&token, &stale).await;
    assert_eq!(reaped["status"], "cancelled");
    assert_eq!(reaped["failure_reason"], "reservation_timeout");
    assert_eq!(svc.status_of(&token, &fresh).await["status"], "pending");
    assert_eq!(
        svc.status_of(&token, &confirmed).await["status"],
        "confirmed"
    );
    let cancellations: Vec<_> = published(&svc.db.pool)
        .await
        .into_iter()
        .filter(|event| event.event_type == "BookingCancelled")
        .collect();
    assert_eq!(cancellations.len(), 1);
    assert_eq!(cancellations[0].payload["booking_id"], stale["id"]);
    assert_eq!(cancellations[0].payload["reason"], "reservation_timeout");
    let late = svc
        .deliver_json(
            "SeatReserved",
            &seat_reserved(id_of(&stale), event_of(&stale)),
        )
        .await;
    assert_eq!(late, Outcome::Applied);
    assert_eq!(svc.status_of(&token, &stale).await["status"], "cancelled");
}
