use std::future::Future;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;

fn gateway() -> String {
    std::env::var("E2E_BASE_URL")
        .expect(
            "set E2E_BASE_URL to the Kong gateway of a running stack, e.g. http://localhost:8000",
        )
        .trim_end_matches('/')
        .to_string()
}

async fn eventually<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

struct Stack {
    base: String,
    http: reqwest::Client,
}

struct Customer {
    token: String,
}

struct Concert {
    event_id: Uuid,
    seat_ids: Vec<Uuid>,
}

impl Stack {
    fn new() -> Stack {
        Stack {
            base: gateway(),
            http: reqwest::Client::new(),
        }
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("request through the gateway");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn sign_up_and_log_in(&self) -> Customer {
        let credentials = json!({
            "email": format!("e2e-{}@example.com", &Uuid::new_v4().simple().to_string()[..12]),
            "password": "correct-horse-battery-staple",
        });
        let (status, registered) = self
            .call(
                reqwest::Method::POST,
                "/api/v1/auth/register",
                None,
                Some(credentials.clone()),
            )
            .await;
        assert_eq!(status, 201, "register: {registered}");
        let (status, session) = self
            .call(
                reqwest::Method::POST,
                "/api/v1/auth/login",
                None,
                Some(credentials),
            )
            .await;
        assert_eq!(status, 200, "login: {session}");
        Customer {
            token: session["token"].as_str().expect("a token").to_string(),
        }
    }

    async fn publish_concert(&self, seats: u32) -> Concert {
        let starts_at = chrono::Utc::now() + chrono::Duration::days(30);
        let (status, created) = self
            .call(
                reqwest::Method::POST,
                "/api/v1/events",
                None,
                Some(json!({
                    "name": format!("e2e concert {}", Uuid::new_v4()),
                    "description": "booking-service end-to-end journey",
                    "venue": "Test Arena",
                    "starts_at": starts_at.to_rfc3339(),
                    "ends_at": (starts_at + chrono::Duration::hours(3)).to_rfc3339(),
                    "layout": {
                        "sections": [
                            { "name": "A", "rows": 1, "seats_per_row": seats, "price_minor": 5000 }
                        ]
                    }
                })),
            )
            .await;
        assert_eq!(status, 201, "create event: {created}");
        let event_id: Uuid = created["event"]["id"].as_str().unwrap().parse().unwrap();
        let seats = self.seats(event_id).await;
        assert_eq!(
            seats.len(),
            created["seat_count"].as_u64().unwrap() as usize
        );
        Concert {
            event_id,
            seat_ids: seats
                .iter()
                .map(|seat| seat["id"].as_str().unwrap().parse().unwrap())
                .collect(),
        }
    }

    async fn seats(&self, event_id: Uuid) -> Vec<Value> {
        let (status, page) = self
            .call(
                reqwest::Method::GET,
                &format!("/api/v1/events/{event_id}/seats?limit=100"),
                None,
                None,
            )
            .await;
        assert_eq!(status, 200, "list seats: {page}");
        page["data"].as_array().unwrap().clone()
    }

    async fn book(&self, customer: &Customer, concert: &Concert, seat: usize) -> Value {
        let (status, booking) = self
            .call(
                reqwest::Method::POST,
                "/api/v1/bookings",
                Some(&customer.token),
                Some(json!({
                    "event_id": concert.event_id,
                    "seat_ids": [concert.seat_ids[seat]],
                })),
            )
            .await;
        assert_eq!(status, 202, "create booking: {booking}");
        assert_eq!(booking["status"], "pending");
        booking
    }

    async fn wait_for_booking(&self, customer: &Customer, booking: &Value, wanted: &str) -> Value {
        let path = format!("/api/v1/bookings/{}", booking["id"].as_str().unwrap());
        eventually(&format!("the booking to become {wanted}"), || async {
            let (status, current) = self
                .call(reqwest::Method::GET, &path, Some(&customer.token), None)
                .await;
            (status == 200 && current["status"] == wanted).then_some(current)
        })
        .await
    }

    async fn wait_for_seat(&self, concert: &Concert, seat: usize, wanted: &str) {
        let id = concert.seat_ids[seat].to_string();
        eventually(&format!("the seat to become {wanted}"), || async {
            let seats = self.seats(concert.event_id).await;
            seats
                .iter()
                .any(|s| s["id"] == id.as_str() && s["status"] == wanted)
                .then_some(())
        })
        .await;
    }
}

#[tokio::test]
#[ignore = "needs a running stack: set E2E_BASE_URL to the Kong gateway"]
async fn a_signed_in_user_books_a_free_seat_and_the_booking_ends_confirmed() {
    let stack = Stack::new();
    let customer = stack.sign_up_and_log_in().await;
    let concert = stack.publish_concert(4).await;
    let (anonymous, _) = stack
        .call(
            reqwest::Method::POST,
            "/api/v1/bookings",
            None,
            Some(json!({ "event_id": concert.event_id, "seat_ids": [concert.seat_ids[0]] })),
        )
        .await;
    assert_eq!(anonymous, 401, "the gateway must demand a token");

    let booking = stack.book(&customer, &concert, 0).await;

    let confirmed = stack
        .wait_for_booking(&customer, &booking, "confirmed")
        .await;
    assert_eq!(confirmed["failure_reason"], Value::Null);
    stack.wait_for_seat(&concert, 0, "booked").await;
    let (status, mine) = stack
        .call(
            reqwest::Method::GET,
            "/api/v1/bookings?limit=10",
            Some(&customer.token),
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(mine["pagination"]["total"], 1);
    assert_eq!(mine["data"][0]["id"], booking["id"]);
}

#[tokio::test]
#[ignore = "needs a running stack: set E2E_BASE_URL to the Kong gateway"]
async fn a_conflicting_booking_for_a_seat_that_is_already_taken_ends_cancelled() {
    let stack = Stack::new();
    let (first, second) = (
        stack.sign_up_and_log_in().await,
        stack.sign_up_and_log_in().await,
    );
    let concert = stack.publish_concert(4).await;
    let winner = stack.book(&first, &concert, 1).await;
    stack.wait_for_booking(&first, &winner, "confirmed").await;
    stack.wait_for_seat(&concert, 1, "booked").await;

    let loser = stack.book(&second, &concert, 1).await;

    let cancelled = stack.wait_for_booking(&second, &loser, "cancelled").await;
    assert_eq!(cancelled["failure_reason"], "seat_unavailable");
    stack.wait_for_seat(&concert, 1, "booked").await;
    assert_eq!(
        stack.wait_for_booking(&first, &winner, "confirmed").await["status"],
        "confirmed"
    );
}
