mod support;

use std::time::{Duration, Instant};

use booking_service::adapter::http::build_router;
use booking_service::app::{App, AppConfig};
use jsonwebtoken::{EncodingKey, Header};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::db::{new_database, TestDb};
use support::kafka::{create_topic, produce};
use support::server::{serve, Server};

const SECRET: &str = "component-test-secret";
const ISSUER: &str = "component-test";

struct Fixture {
    db: TestDb,
    server: Server,
    app: App,
    http: reqwest::Client,
    topic: String,
    shutdown: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

async fn fixture(with_kafka: bool) -> Fixture {
    let db = new_database().await;
    let topic = format!("comp-{}", &Uuid::new_v4().simple().to_string()[..8]);
    if with_kafka {
        create_topic(&topic).await;
        create_topic(&format!("{topic}.dlq")).await;
    }
    let app = App::new(
        db.pool.clone(),
        AppConfig {
            jwt_secret: SECRET.to_string(),
            jwt_issuer: ISSUER.to_string(),
            kafka_brokers: support::kafka::brokers(),
            seat_reservation_topic: topic.clone(),
            consumer_group_suffix: format!("-{topic}"),
            consumer_max_attempts: 3,
            pending_timeout_secs: 1,
            reaper_interval_secs: 1,
        },
    );
    let server = serve(build_router(app.state.clone())).await;

    let shutdown = CancellationToken::new();
    let mut tasks = Vec::new();
    if with_kafka {
        let confirm = app.confirm_consumer().expect("confirm consumer");
        let cancel = app.cancel_consumer().expect("cancel consumer");
        tasks.push(tokio::spawn(confirm.run(shutdown.clone())));
        tasks.push(tokio::spawn(cancel.run(shutdown.clone())));
    }
    Fixture {
        db,
        server,
        app,
        http: reqwest::Client::new(),
        topic,
        shutdown,
        tasks,
    }
}

impl Fixture {
    async fn finish(self) {
        self.shutdown.cancel();
        for t in self.tasks {
            let _ = t.await;
        }
        self.server.stop();
        self.db.drop_database().await;
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = self
            .http
            .request(method, format!("{}{}", self.server.base, path));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let res = req.send().await.expect("request");
        let status = res.status();
        (status, res.json().await.unwrap_or(Value::Null))
    }

    async fn create(&self, token: &str, seats: usize) -> (StatusCode, Value) {
        let seat_ids: Vec<Uuid> = (0..seats).map(|_| Uuid::new_v4()).collect();
        self.send(
            reqwest::Method::POST,
            "/api/v1/bookings",
            Some(token),
            Some(json!({ "event_id": Uuid::new_v4(), "seat_ids": seat_ids })),
        )
        .await
    }

    async fn status_of(&self, token: &str, id: &str) -> Value {
        self.send(
            reqwest::Method::GET,
            &format!("/api/v1/bookings/{id}"),
            Some(token),
            None,
        )
        .await
        .1
    }

    async fn wait_status(&self, token: &str, id: &str, want: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.status_of(token, id).await["status"] == want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "booking {id} never reached {want}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn publish(&self, event_type: &str, booking: &str, extra: Value) {
        let mut payload = json!({
            "event_id": Uuid::new_v4(),
            "booking_id": booking,
            "ticketed_event_id": Uuid::new_v4(),
            "seat_ids": [Uuid::new_v4()],
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        produce(&self.topic, booking, Some(&payload.to_string()), event_type).await;
    }
}

fn token_for(user: Uuid) -> String {
    #[derive(Serialize)]
    struct Claims {
        sub: String,
        iss: String,
        exp: usize,
    }
    jsonwebtoken::encode(
        &Header::default(),
        &Claims {
            sub: user.to_string(),
            iss: ISSUER.to_string(),
            exp: (chrono::Utc::now().timestamp() + 3600) as usize,
        },
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and kafka: scripts/run-component-tests.sh booking"]
async fn only_the_owner_can_see_a_booking_and_anonymous_calls_are_rejected() {
    let fx = fixture(false).await;
    let (alice, bob) = (token_for(Uuid::new_v4()), token_for(Uuid::new_v4()));

    let (anon, _) = fx
        .send(reqwest::Method::GET, "/api/v1/bookings", None, None)
        .await;
    let (forged, _) = fx
        .send(
            reqwest::Method::GET,
            "/api/v1/bookings",
            Some("not-a-jwt"),
            None,
        )
        .await;
    assert_eq!(
        (anon, forged),
        (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED)
    );

    let (status, created) = fx.create(&alice, 2).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(created["status"], "pending");
    let id = created["id"].as_str().unwrap().to_string();

    let (own, _) = fx
        .send(
            reqwest::Method::GET,
            &format!("/api/v1/bookings/{id}"),
            Some(&alice),
            None,
        )
        .await;
    let (other, _) = fx
        .send(
            reqwest::Method::GET,
            &format!("/api/v1/bookings/{id}"),
            Some(&bob),
            None,
        )
        .await;
    assert_eq!((own, other), (StatusCode::OK, StatusCode::NOT_FOUND));

    let (_, bobs) = fx
        .send(reqwest::Method::GET, "/api/v1/bookings", Some(&bob), None)
        .await;
    assert_eq!(bobs["pagination"]["total"], 0);

    let (empty, _) = fx.create(&alice, 0).await;
    assert_eq!(empty, StatusCode::BAD_REQUEST);

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and kafka: scripts/run-component-tests.sh booking"]
async fn listing_is_paginated_clamped_and_scoped_to_the_caller() {
    let fx = fixture(false).await;
    let (alice, bob) = (token_for(Uuid::new_v4()), token_for(Uuid::new_v4()));
    for _ in 0..3 {
        fx.create(&alice, 1).await;
    }
    fx.create(&bob, 1).await;

    let (_, page) = fx
        .send(
            reqwest::Method::GET,
            "/api/v1/bookings?limit=2",
            Some(&alice),
            None,
        )
        .await;
    assert_eq!(page["data"].as_array().unwrap().len(), 2);
    assert_eq!(page["pagination"]["total"], 3);
    assert_eq!(page["pagination"]["has_more"], true);

    let (_, clamped) = fx
        .send(
            reqwest::Method::GET,
            "/api/v1/bookings?limit=1000",
            Some(&alice),
            None,
        )
        .await;
    assert_eq!(clamped["pagination"]["limit"], 100);
    assert_eq!(clamped["pagination"]["has_more"], false);

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and kafka: scripts/run-component-tests.sh booking"]
async fn seat_outcomes_confirm_or_cancel_a_booking_and_a_late_one_cannot_undo_a_cancellation() {
    let fx = fixture(true).await;
    let alice = token_for(Uuid::new_v4());
    let confirmed = fx.create(&alice, 1).await.1["id"]
        .as_str()
        .unwrap()
        .to_string();
    let cancelled = fx.create(&alice, 1).await.1["id"]
        .as_str()
        .unwrap()
        .to_string();
    let sentinel = fx.create(&alice, 1).await.1["id"]
        .as_str()
        .unwrap()
        .to_string();

    fx.publish(
        "SeatReserved",
        &confirmed,
        json!({ "reserved_at": chrono::Utc::now() }),
    )
    .await;
    fx.wait_status(&alice, &confirmed, "confirmed").await;

    fx.publish(
        "SeatReservationFailed",
        &cancelled,
        json!({ "reason": "seat_unavailable", "failed_at": chrono::Utc::now() }),
    )
    .await;
    fx.wait_status(&alice, &cancelled, "cancelled").await;
    let after_cancel = fx.status_of(&alice, &cancelled).await;
    assert_eq!(after_cancel["failure_reason"], "seat_unavailable");

    fx.publish(
        "SeatReserved",
        &cancelled,
        json!({ "reserved_at": chrono::Utc::now() }),
    )
    .await;
    fx.publish(
        "SeatReserved",
        &sentinel,
        json!({ "reserved_at": chrono::Utc::now() }),
    )
    .await;
    fx.wait_status(&alice, &sentinel, "confirmed").await;

    assert_eq!(
        fx.status_of(&alice, &cancelled).await["status"],
        "cancelled",
        "a late SeatReserved must not resurrect a cancelled booking"
    );

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and kafka: scripts/run-component-tests.sh booking"]
async fn the_reaper_cancels_a_booking_that_stayed_pending_too_long() {
    let fx = fixture(false).await;
    let alice = token_for(Uuid::new_v4());
    let id = fx.create(&alice, 1).await.1["id"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let reaped = fx.app.reaper.reap_tick().await.expect("reap");

    assert_eq!(reaped, 1);
    let booking = fx.status_of(&alice, &id).await;
    assert_eq!(booking["status"], "cancelled");
    assert_eq!(booking["failure_reason"], "reservation_timeout");

    fx.finish().await;
}
