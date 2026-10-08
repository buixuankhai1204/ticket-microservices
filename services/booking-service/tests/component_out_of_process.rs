mod common;

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::{
    create_topics, free_port, install_outbox_tap, kafka_brokers, produce, published,
    seat_reservation_failed, seat_reserved, token_for, unique, Api, TestDb, JWT_ISSUER, JWT_SECRET,
};
use serde_json::Value;
use uuid::Uuid;

struct Running {
    child: Child,
    api: Api,
    db: TestDb,
    brokers: String,
    topic: String,
}

fn service_env(port: u16, db_url: &str, brokers: &str, topic: &str) -> Vec<(String, String)> {
    [
        ("PORT", port.to_string()),
        ("DATABASE_URL", db_url.to_string()),
        ("DB_MAX_CONNECTIONS", "4".to_string()),
        ("JWT_SECRET", JWT_SECRET.to_string()),
        ("JWT_ISSUER", JWT_ISSUER.to_string()),
        ("KAFKA_BROKERS", brokers.to_string()),
        ("KAFKA_SEAT_RESERVATION_EVENTS_TOPIC", topic.to_string()),
        (
            "KAFKA_GROUP_SUFFIX",
            format!("-{}", &Uuid::new_v4().simple().to_string()[..8]),
        ),
        ("KAFKA_CONSUMER_MAX_ATTEMPTS", "3".to_string()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}

fn spawn_service(env: &[(String, String)], piped: bool) -> Child {
    let output = || if piped { Stdio::piped() } else { Stdio::null() };
    Command::new(env!("CARGO_BIN_EXE_booking-service"))
        .env_clear()
        .envs(
            env.iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .stdout(output())
        .stderr(output())
        .spawn()
        .expect("start booking-service")
}

impl Running {
    async fn start(extra_env: &[(&str, &str)]) -> Running {
        let brokers = kafka_brokers().await;
        let topic = unique("seat_reservation.events");
        create_topics(&brokers, &[&topic, &format!("{topic}.dlq")]).await;
        let db = TestDb::empty().await;
        let port = free_port();
        let mut env = service_env(port, &db.url, &brokers, &topic);
        env.extend(
            extra_env
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let mut running = Running {
            child: spawn_service(&env, false),
            api: Api::new(format!("http://127.0.0.1:{port}")),
            db,
            brokers,
            topic,
        };
        running.wait_until_ready().await;
        install_outbox_tap(&running.db.pool).await;
        running
    }

    async fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("booking-service exited early with {status}");
            }
            if self.api.try_status("/readyz").await == Some(200) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "booking-service did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn publish(&self, event_type: &str, booking_id: &str, event: &Value) {
        produce(
            &self.brokers,
            &self.topic,
            booking_id,
            Some(&serde_json::to_vec(event).unwrap()),
            Some(event_type),
        )
        .await;
    }

    async fn terminate(&mut self) -> std::process::ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        assert!(sent.success(), "could not send SIGTERM");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "booking-service did not stop within the shutdown grace"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn event_of(booking: &Value) -> Uuid {
    booking["event_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL and TEST_KAFKA_BROKERS)"]
async fn the_binary_migrates_its_database_and_seat_outcomes_over_kafka_settle_bookings_made_over_http(
) {
    let service = Running::start(&[]).await;
    for table in ["bookings", "outbox_events", "processed_events"] {
        let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(table)
            .fetch_one(&service.db.pool)
            .await
            .unwrap();
        assert_eq!(found.as_deref(), Some(table), "{table} after startup");
    }
    let token = token_for(Uuid::new_v4());
    let to_confirm = service.api.create_booking_with_seats(&token, 2).await;
    let to_cancel = service.api.create_booking_with_seats(&token, 1).await;
    let requested: Vec<_> = published(&service.db.pool)
        .await
        .into_iter()
        .map(|event| (event.event_type, event.aggregate_id.to_string()))
        .collect();
    assert_eq!(requested.len(), 2);
    assert!(requested.iter().all(|(kind, _)| kind == "BookingRequested"));

    let confirm_id = to_confirm["id"].as_str().unwrap();
    let cancel_id = to_cancel["id"].as_str().unwrap();
    service
        .publish(
            "SeatReserved",
            confirm_id,
            &seat_reserved(confirm_id, event_of(&to_confirm)),
        )
        .await;
    service
        .publish(
            "SeatReservationFailed",
            cancel_id,
            &seat_reservation_failed(cancel_id, event_of(&to_cancel), "seat_unavailable"),
        )
        .await;

    service
        .api
        .wait_for_status(&token, confirm_id, "confirmed")
        .await;
    let cancelled = service
        .api
        .wait_for_status(&token, cancel_id, "cancelled")
        .await;
    assert_eq!(cancelled["failure_reason"], "seat_unavailable");
    let outcomes: Vec<_> = published(&service.db.pool)
        .await
        .into_iter()
        .filter(|event| event.event_type != "BookingRequested")
        .map(|event| (event.event_type, event.aggregate_id.to_string()))
        .collect();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.contains(&("BookingConfirmed".to_string(), confirm_id.to_string())));
    assert!(outcomes.contains(&("BookingCancelled".to_string(), cancel_id.to_string())));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL and TEST_KAFKA_BROKERS)"]
async fn the_binary_refuses_to_start_without_its_required_configuration() {
    let brokers = kafka_brokers().await;
    let db = TestDb::empty().await;

    for missing in ["DATABASE_URL", "JWT_SECRET", "KAFKA_BROKERS"] {
        let env: Vec<_> = service_env(free_port(), &db.url, &brokers, "unused")
            .into_iter()
            .filter(|(key, _)| key != missing)
            .collect();
        let mut child = spawn_service(&env, true);
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("booking-service kept running without {missing}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let mut output = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();

        assert!(!status.success(), "{missing}: exit status {status}");
        assert!(
            output.contains(missing),
            "{missing}: the failure must name the variable, got: {output}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL and TEST_KAFKA_BROKERS)"]
async fn the_reaper_started_by_main_cancels_a_booking_nobody_answered_and_sigterm_stops_the_binary_cleanly(
) {
    let mut service = Running::start(&[
        ("BOOKING_PENDING_TIMEOUT", "1"),
        ("BOOKING_REAPER_INTERVAL", "1"),
    ])
    .await;
    let token = token_for(Uuid::new_v4());
    let booking = service.api.create_booking_with_seats(&token, 1).await;
    let id = booking["id"].as_str().unwrap();

    let cancelled = service.api.wait_for_status(&token, id, "cancelled").await;

    assert_eq!(cancelled["failure_reason"], "reservation_timeout");
    let events = published(&service.db.pool).await;
    let cancellation = events
        .iter()
        .find(|event| event.event_type == "BookingCancelled")
        .expect("the reaper publishes BookingCancelled");
    assert_eq!(cancellation.payload["booking_id"], booking["id"]);
    assert_eq!(cancellation.payload["reason"], "reservation_timeout");

    let status = service.terminate().await;

    assert!(status.success(), "exit status {status}");
}
