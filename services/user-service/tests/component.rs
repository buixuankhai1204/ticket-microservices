mod support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{Months, NaiveDate};
use reqwest::StatusCode;
use serde_json::{json, Value};
use user_service::adapter::email::{HttpEmailGateway, StubEmailGateway};
use user_service::adapter::http::build_router;
use user_service::adapter::payment::{HttpPaymentGateway, StubOutcome, StubPaymentGateway};
use user_service::app::{run_renewal_batch, App, AppConfig, Gateways};
use user_service::domain::RenewalPolicy;
use uuid::Uuid;

use support::db::{new_database, TestDb};
use support::server::{serve, Server};
use support::wiremock::{json_response, raw_response, Stub};

struct Fixture {
    db: TestDb,
    server: Server,
    app: App,
    http: reqwest::Client,
}

async fn fixture(provider: Option<&Stub>) -> Fixture {
    let db = new_database().await;
    let gateways = match provider {
        Some(stub) => Gateways {
            payment: Arc::new(
                HttpPaymentGateway::new(&stub.base_url(), "k".into(), Duration::from_secs(5))
                    .unwrap(),
            ),
            email: Arc::new(
                HttpEmailGateway::new(&stub.base_url(), "k".into(), Duration::from_secs(5))
                    .unwrap(),
            ),
        },
        None => Gateways {
            payment: Arc::new(StubPaymentGateway::new(StubOutcome::Succeed)),
            email: Arc::new(StubEmailGateway::new(false)),
        },
    };
    let app = App::new(
        db.pool.clone(),
        AppConfig {
            jwt_secret: "component-test-secret".to_string(),
            jwt_issuer: "component-test".to_string(),
            renewal_policy: RenewalPolicy::default(),
        },
        gateways,
    );
    let server = serve(build_router(app.state.clone())).await;
    Fixture {
        db,
        server,
        app,
        http: reqwest::Client::new(),
    }
}

impl Fixture {
    async fn finish(self) {
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

    async fn signup(&self) -> (Uuid, String) {
        let email = format!("{}@example.com", Uuid::new_v4().simple());
        let (status, user) = self
            .send(
                reqwest::Method::POST,
                "/api/v1/auth/register",
                None,
                Some(json!({ "email": email, "password": "correct-horse-battery" })),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, login) = self
            .send(
                reqwest::Method::POST,
                "/api/v1/auth/login",
                None,
                Some(json!({ "email": email, "password": "correct-horse-battery" })),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        (
            user["id"].as_str().unwrap().parse().unwrap(),
            login["token"].as_str().unwrap().to_string(),
        )
    }

    async fn subscribe(&self, token: &str) -> Value {
        let (status, sub) = self
            .send(
                reqwest::Method::POST,
                "/api/v1/subscriptions",
                Some(token),
                Some(json!({
                    "plan_id": "pro", "billing_interval": "month", "price_minor": 1999,
                    "currency": "USD", "payment_method_id": "pm_1",
                })),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        sub
    }

    async fn subscription(&self, token: &str, id: &str) -> Value {
        self.send(
            reqwest::Method::GET,
            &format!("/api/v1/subscriptions/{id}"),
            Some(token),
            None,
        )
        .await
        .1
    }

    async fn queue_renewal(&self, token: &str, id: &str) -> StatusCode {
        self.send(
            reqwest::Method::POST,
            &format!("/api/v1/subscriptions/{id}/retry-renewal"),
            Some(token),
            None,
        )
        .await
        .0
    }

    async fn run_job_b(&self) {
        run_renewal_batch(
            &self.app.process_renewal,
            &self.app.send_dunning_email,
            10,
            chrono::Duration::seconds(900),
        )
        .await;
    }
}

fn date(v: &Value) -> NaiveDate {
    v.as_str().unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres: scripts/run-component-tests.sh user"]
async fn a_user_registers_logs_in_and_reads_their_profile_with_the_issued_token() {
    let fx = fixture(None).await;
    let (id, token) = fx.signup().await;

    let (status, profile) = fx
        .send(
            reqwest::Method::GET,
            &format!("/api/v1/users/{id}"),
            Some(&token),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["id"], id.to_string());

    let email = profile["email"].as_str().unwrap();
    let (duplicate, _) = fx
        .send(
            reqwest::Method::POST,
            "/api/v1/auth/register",
            None,
            Some(json!({ "email": email, "password": "another-password-1" })),
        )
        .await;
    let (wrong_password, _) = fx
        .send(
            reqwest::Method::POST,
            "/api/v1/auth/login",
            None,
            Some(json!({ "email": email, "password": "not-the-password" })),
        )
        .await;
    assert_eq!(
        (duplicate, wrong_password),
        (StatusCode::BAD_REQUEST, StatusCode::UNAUTHORIZED)
    );

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres: scripts/run-component-tests.sh user"]
async fn subscriptions_are_validated_and_only_visible_to_their_owner() {
    let fx = fixture(None).await;
    let (_, alice) = fx.signup().await;
    let (_, bob) = fx.signup().await;

    let created = fx.subscribe(&alice).await;
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["status"], "active");

    let (own, _) = fx
        .send(
            reqwest::Method::GET,
            &format!("/api/v1/subscriptions/{id}"),
            Some(&alice),
            None,
        )
        .await;
    let (other, _) = fx
        .send(
            reqwest::Method::GET,
            &format!("/api/v1/subscriptions/{id}"),
            Some(&bob),
            None,
        )
        .await;
    assert_eq!((own, other), (StatusCode::OK, StatusCode::NOT_FOUND));

    let (_, alices) = fx
        .send(
            reqwest::Method::GET,
            "/api/v1/subscriptions",
            Some(&alice),
            None,
        )
        .await;
    let (_, bobs) = fx
        .send(
            reqwest::Method::GET,
            "/api/v1/subscriptions",
            Some(&bob),
            None,
        )
        .await;
    assert_eq!(
        (&alices["pagination"]["total"], &bobs["pagination"]["total"]),
        (&json!(1), &json!(0))
    );

    let (negative, _) = fx
        .send(
            reqwest::Method::POST,
            "/api/v1/subscriptions",
            Some(&alice),
            Some(json!({
                "plan_id": "pro", "billing_interval": "month", "price_minor": -1,
                "currency": "USD", "payment_method_id": "pm_1",
            })),
        )
        .await;
    let (bad_currency, _) = fx
        .send(
            reqwest::Method::POST,
            "/api/v1/subscriptions",
            Some(&alice),
            Some(json!({
                "plan_id": "pro", "billing_interval": "month", "price_minor": 100,
                "currency": "US", "payment_method_id": "pm_1",
            })),
        )
        .await;
    assert_eq!(
        (negative, bad_currency),
        (StatusCode::BAD_REQUEST, StatusCode::BAD_REQUEST)
    );

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres: scripts/run-component-tests.sh user"]
async fn a_renewal_retry_is_queued_once_and_refused_for_a_canceled_subscription() {
    let fx = fixture(None).await;
    let (_, token) = fx.signup().await;
    let sub = fx.subscribe(&token).await;
    let id = sub["id"].as_str().unwrap();

    let first = fx
        .send(
            reqwest::Method::POST,
            &format!("/api/v1/subscriptions/{id}/retry-renewal"),
            Some(&token),
            None,
        )
        .await;
    let second = fx.queue_renewal(&token, id).await;
    assert_eq!(
        (first.0, second),
        (StatusCode::ACCEPTED, StatusCode::ACCEPTED)
    );
    assert_eq!(first.1["status"], "failed_retryable");

    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM renewal_attempts")
        .fetch_one(&fx.db.pool)
        .await
        .unwrap();
    assert_eq!(queued, 1, "calling retry twice must not queue two attempts");

    sqlx::query("UPDATE subscriptions SET status = 'canceled'")
        .execute(&fx.db.pool)
        .await
        .unwrap();
    assert_eq!(fx.queue_renewal(&token, id).await, StatusCode::CONFLICT);

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and wiremock: scripts/run-component-tests.sh user"]
async fn a_successful_charge_renews_the_subscription_by_one_period_with_the_idempotency_key() {
    let provider = Stub::new();
    provider
        .respond("/v1/charges", json_response(201, json!({ "id": "ch_1" })))
        .await;
    let fx = fixture(Some(&provider)).await;
    let (_, token) = fx.signup().await;
    let sub = fx.subscribe(&token).await;
    let id = sub["id"].as_str().unwrap();
    let period_end = date(&sub["current_period_end"]);
    fx.queue_renewal(&token, id).await;

    fx.run_job_b().await;

    let renewed = fx.subscription(&token, id).await;
    assert_eq!(renewed["status"], "active");
    assert_eq!(
        date(&renewed["current_period_end"]),
        period_end.checked_add_months(Months::new(1)).unwrap()
    );
    let charges = provider.wait_for_requests("/v1/charges", 1).await;
    assert_eq!(charges.len(), 1);
    assert_eq!(
        support::wiremock::header(&charges[0], "Idempotency-Key"),
        format!("renew:{id}:{period_end}")
    );

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and wiremock: scripts/run-component-tests.sh user"]
async fn a_declined_card_makes_the_subscription_past_due_and_emails_the_customer_once() {
    let provider = Stub::new();
    provider
        .respond(
            "/v1/charges",
            json_response(402, json!({ "error": { "code": "card_declined" } })),
        )
        .await;
    provider.respond("/v1/emails", raw_response(202, "")).await;
    let fx = fixture(Some(&provider)).await;
    let (_, token) = fx.signup().await;
    let sub = fx.subscribe(&token).await;
    let id = sub["id"].as_str().unwrap();
    fx.queue_renewal(&token, id).await;

    fx.run_job_b().await;

    let after = fx.subscription(&token, id).await;
    assert_eq!(after["status"], "past_due");
    assert_eq!(after["current_period_end"], sub["current_period_end"]);
    assert_eq!(provider.wait_for_requests("/v1/emails", 1).await.len(), 1);

    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs postgres and wiremock: scripts/run-component-tests.sh user"]
async fn a_provider_outage_never_cancels_or_emails_and_the_charge_is_not_hammered() {
    let provider = Stub::new();
    provider.respond("/v1/charges", raw_response(503, "")).await;
    let fx = fixture(Some(&provider)).await;
    let (_, token) = fx.signup().await;
    let sub = fx.subscribe(&token).await;
    let id = sub["id"].as_str().unwrap();
    fx.queue_renewal(&token, id).await;

    fx.run_job_b().await;
    fx.run_job_b().await;

    let after = fx.subscription(&token, id).await;
    assert_eq!(after["status"], "active");
    assert_eq!(after["current_period_end"], sub["current_period_end"]);
    assert_eq!(provider.wait_for_requests("/v1/charges", 1).await.len(), 1);
    assert!(provider.requests("/v1/emails").await.is_empty());

    fx.finish().await;
}
