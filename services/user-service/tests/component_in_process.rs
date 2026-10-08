mod common;

use std::sync::Arc;

use chrono::{Months, NaiveDate, Utc};
use common::{
    new_database, published_events, published_of, Account, Api, Charge, ScriptedEmail,
    ScriptedPayment, TestDb, PASSWORD,
};
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use user_service::adapter::http::build_router;
use user_service::app::{run_renewal_batch, App, AppConfig, Gateways};
use user_service::domain::RenewalPolicy;
use user_service::usecase::EnqueueOutcome;
use uuid::Uuid;

const JWT_SECRET: &str = "component-test-secret";
const JWT_ISSUER: &str = "component-test";

struct TestApp {
    api: Api,
    app: App,
    payment: Arc<ScriptedPayment>,
    email: Arc<ScriptedEmail>,
    server: JoinHandle<()>,
    db: TestDb,
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn spawn_app() -> TestApp {
    let db = new_database().await;
    let payment = ScriptedPayment::approving();
    let email = ScriptedEmail::new();
    let app = App::new(
        db.pool.clone(),
        AppConfig {
            jwt_secret: JWT_SECRET.to_string(),
            jwt_issuer: JWT_ISSUER.to_string(),
            renewal_policy: RenewalPolicy::default(),
        },
        Gateways {
            payment: payment.clone(),
            email: email.clone(),
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = build_router(app.state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    TestApp {
        api: Api::new(base),
        app,
        payment,
        email,
        server,
        db,
    }
}

fn date(value: &Value) -> NaiveDate {
    value.as_str().unwrap().parse().unwrap()
}

#[derive(Serialize)]
struct Claims {
    iss: String,
    sub: String,
    exp: i64,
}

fn forged_token(user_id: Uuid, secret: &str, issuer: &str, expires_in_secs: i64) -> String {
    let claims = Claims {
        iss: issuer.to_string(),
        sub: user_id.to_string(),
        exp: Utc::now().timestamp() + expires_in_secs,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

impl TestApp {
    async fn due_subscription(&self, account: &Account) -> Value {
        let subscription = self.api.subscribe(&account.token).await;
        let id: Uuid = subscription["id"].as_str().unwrap().parse().unwrap();
        sqlx::query("UPDATE subscriptions SET current_period_end = CURRENT_DATE - 1 WHERE id = $1")
            .bind(id)
            .execute(&self.db.pool)
            .await
            .unwrap();
        self.subscription(account, &subscription["id"]).await
    }

    async fn subscription(&self, account: &Account, id: &Value) -> Value {
        let (status, subscription) = self
            .api
            .get(
                &format!("/api/v1/subscriptions/{}", id.as_str().unwrap()),
                Some(&account.token),
            )
            .await;
        assert_eq!(status, 200, "{subscription}");
        subscription
    }

    async fn job_a(&self) -> EnqueueOutcome {
        self.app.enqueue_due_renewals.execute().await.unwrap()
    }

    async fn job_b(&self) {
        run_renewal_batch(
            &self.app.process_renewal,
            &self.app.send_dunning_email,
            10,
            chrono::Duration::seconds(900),
        )
        .await;
    }

    async fn attempt(&self, subscription_id: &Value) -> (String, i32, i32, Option<String>) {
        let id: Uuid = subscription_id.as_str().unwrap().parse().unwrap();
        sqlx::query_as(
            "SELECT status, attempt_count, dunning_attempt_count, provider_charge_id \
             FROM renewal_attempts WHERE subscription_id = $1",
        )
        .bind(id)
        .fetch_one(&self.db.pool)
        .await
        .unwrap()
    }

    async fn make_attempt_due(&self, subscription_id: &Value) {
        let id: Uuid = subscription_id.as_str().unwrap().parse().unwrap();
        sqlx::query(
            "UPDATE renewal_attempts SET next_attempt_at = now() WHERE subscription_id = $1",
        )
        .bind(id)
        .execute(&self.db.pool)
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_user_registers_logs_in_and_reads_their_profile_and_the_events_are_published() {
    let app = spawn_app().await;
    let email = common::unique_email();

    let (status, registered) = app.api.register(&email, PASSWORD).await;
    assert_eq!(status, 201);
    assert_eq!(registered["email"], email.as_str());
    assert!(registered.get("password_hash").is_none());
    let id = registered["id"].as_str().unwrap().to_string();

    let (status, session) = app.api.login(&email, PASSWORD).await;
    assert_eq!(status, 200);
    let token = session["token"].as_str().unwrap();

    let (status, profile) = app
        .api
        .get(&format!("/api/v1/users/{id}"), Some(token))
        .await;
    assert_eq!((status, profile["email"].clone()), (200, json!(email)));
    let stored: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE email = $1")
        .bind(&email)
        .fetch_one(&app.db.pool)
        .await
        .unwrap();
    assert!(stored.starts_with("$argon2") && !stored.contains(PASSWORD));

    let created = published_of(&app.db.pool, "UserCreated").await;
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].aggregate_type, "user");
    assert_eq!(created[0].aggregate_id.to_string(), id);
    assert_eq!(created[0].payload["email"], email.as_str());
    assert_eq!(created[0].payload["user_id"], id.as_str());
    let logged_in = published_of(&app.db.pool, "UserLoggedIn").await;
    assert_eq!(logged_in.len(), 1);
    assert_eq!(logged_in[0].aggregate_id.to_string(), id);
    assert_eq!(logged_in[0].payload["email"], email.as_str());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn bad_credentials_and_bad_input_are_rejected_without_publishing_anything_extra() {
    let app = spawn_app().await;
    let account = app.api.signup().await;
    let before = published_events(&app.db.pool).await.len();

    let (duplicate, _) = app.api.register(&account.email, "another-password-1").await;
    let (invalid_email, _) = app.api.register("not-an-email", PASSWORD).await;
    let (wrong_password, wrong_body) = app.api.login(&account.email, "not-the-password").await;
    let (unknown_user, unknown_body) = app.api.login("nobody@example.com", PASSWORD).await;
    let (missing_user, _) = app
        .api
        .get(&format!("/api/v1/users/{}", Uuid::new_v4()), None)
        .await;
    let (malformed, _) = app
        .api
        .http
        .post(format!("{}/api/v1/auth/register", app.api.base))
        .header("content-type", "application/json")
        .body("{not json")
        .send()
        .await
        .map(|r| (r.status().as_u16(), ()))
        .unwrap();

    assert_eq!((duplicate, invalid_email), (400, 400));
    assert_eq!((wrong_password, unknown_user), (401, 401));
    assert_eq!(wrong_body, unknown_body);
    assert_eq!(missing_user, 404);
    assert_eq!(malformed, 400);
    assert_eq!(published_events(&app.db.pool).await.len(), before);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn subscriptions_need_a_valid_token_are_validated_and_stay_private_to_their_owner() {
    let app = spawn_app().await;
    let alice = app.api.signup().await;
    let bob = app.api.signup().await;
    let path = "/api/v1/subscriptions";

    let forged = [
        forged_token(alice.id, "some-other-secret", JWT_ISSUER, 3600),
        forged_token(alice.id, JWT_SECRET, "someone-else", 3600),
        forged_token(alice.id, JWT_SECRET, JWT_ISSUER, -3600),
        "not.a.jwt".to_string(),
    ];
    let (anonymous, _) = app.api.get(path, None).await;
    assert_eq!(anonymous, 401);
    for token in &forged {
        let (status, _) = app.api.get(path, Some(token)).await;
        assert_eq!(status, 401, "{token}");
    }

    let created = app.api.subscribe(&alice.token).await;
    assert_eq!(created["status"], "active");
    assert_eq!(created["user_id"], alice.id.to_string());
    assert_eq!(created["billing_interval"], "month");
    let expected_end = Utc::now()
        .date_naive()
        .checked_add_months(Months::new(1))
        .unwrap();
    assert!(
        (date(&created["current_period_end"]) - expected_end)
            .num_days()
            .abs()
            <= 1
    );
    let id = created["id"].as_str().unwrap();

    let (own, _) = app
        .api
        .get(&format!("{path}/{id}"), Some(&alice.token))
        .await;
    let (stranger, _) = app.api.get(&format!("{path}/{id}"), Some(&bob.token)).await;
    assert_eq!((own, stranger), (200, 404));
    let (_, alices) = app.api.get(path, Some(&alice.token)).await;
    let (_, bobs) = app.api.get(path, Some(&bob.token)).await;
    assert_eq!(alices["pagination"]["total"], 1);
    assert_eq!(bobs["pagination"]["total"], 0);

    let mut invalid = Vec::new();
    for (field, value) in [
        ("price_minor", json!(-1)),
        ("currency", json!("US")),
        ("billing_interval", json!("weekly")),
        ("plan_id", json!("  ")),
    ] {
        let mut body = Api::subscription_request();
        body[field] = value;
        invalid.push(app.api.post(path, Some(&alice.token), body).await.0);
    }
    assert_eq!(invalid, [400, 400, 400, 400]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_renewal_retry_is_queued_once_and_refused_for_strangers_and_canceled_subscriptions() {
    let app = spawn_app().await;
    let owner = app.api.signup().await;
    let stranger = app.api.signup().await;
    let subscription = app.api.subscribe(&owner.token).await;
    let id = subscription["id"].as_str().unwrap();
    let retry = format!("/api/v1/subscriptions/{id}/retry-renewal");

    let (first, queued) = app
        .api
        .send(reqwest::Method::POST, &retry, Some(&owner.token), None)
        .await;
    let (second, _) = app
        .api
        .send(reqwest::Method::POST, &retry, Some(&owner.token), None)
        .await;
    let (foreign, _) = app
        .api
        .send(reqwest::Method::POST, &retry, Some(&stranger.token), None)
        .await;
    assert_eq!((first, second, foreign), (202, 202, 404));
    assert_eq!(queued["status"], "failed_retryable");
    assert_eq!(queued["period_end"], subscription["current_period_end"]);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM renewal_attempts")
        .fetch_one(&app.db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    sqlx::query("UPDATE subscriptions SET status = 'canceled'")
        .execute(&app.db.pool)
        .await
        .unwrap();
    let (canceled, _) = app
        .api
        .send(reqwest::Method::POST, &retry, Some(&owner.token), None)
        .await;
    assert_eq!(canceled, 409);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn an_approved_charge_renews_one_period_once_and_publishes_subscription_renewed() {
    let app = spawn_app().await;
    let owner = app.api.signup().await;
    let due = app.due_subscription(&owner).await;
    let period_end = date(&due["current_period_end"]);

    assert!(matches!(app.job_a().await, EnqueueOutcome::Enqueued(1)));
    assert!(matches!(app.job_a().await, EnqueueOutcome::Enqueued(0)));
    app.job_b().await;
    app.job_b().await;

    let renewed = app.subscription(&owner, &due["id"]).await;
    assert_eq!(renewed["status"], "active");
    assert_eq!(
        date(&renewed["current_period_end"]),
        period_end.checked_add_months(Months::new(1)).unwrap()
    );
    let charges = app.payment.requests();
    assert_eq!(
        charges.len(),
        1,
        "a renewed period must never be charged twice"
    );
    assert_eq!(
        charges[0].idempotency_key,
        format!("renew:{}:{period_end}", due["id"].as_str().unwrap())
    );
    assert_eq!(
        (
            charges[0].amount_minor,
            charges[0].currency.as_str(),
            charges[0].payment_method_id.as_str()
        ),
        (1999, "USD", "pm_card_visa")
    );
    let (status, attempts, _, charge_id) = app.attempt(&due["id"]).await;
    assert_eq!(
        (status.as_str(), charge_id.as_deref(), attempts),
        ("succeeded", Some("ch_scripted"), 1)
    );
    assert!(matches!(app.job_a().await, EnqueueOutcome::Enqueued(0)));

    let events = published_of(&app.db.pool, "SubscriptionRenewed").await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_type, "subscription");
    assert_eq!(
        events[0].aggregate_id.to_string(),
        due["id"].as_str().unwrap()
    );
    let payload = &events[0].payload;
    assert_eq!(payload["user_id"], owner.id.to_string());
    assert_eq!(payload["plan_id"], "pro");
    assert_eq!(payload["period_start"], period_end.to_string());
    assert_eq!(payload["new_period_end"], renewed["current_period_end"]);
    assert_eq!(payload["amount_minor"], 1999);
    assert_eq!(payload["currency"], "USD");
    assert_eq!(payload["provider_charge_id"], "ch_scripted");
    assert_eq!(payload["attempt_count"], 1);
    assert!(app.email.sent().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_declined_card_starts_dunning_with_one_event_and_one_email_per_decline() {
    let app = spawn_app().await;
    app.payment
        .always(Charge::Declined("insufficient_funds".to_string()));
    let owner = app.api.signup().await;
    let due = app.due_subscription(&owner).await;

    app.job_a().await;
    app.job_b().await;
    app.job_b().await;

    let after = app.subscription(&owner, &due["id"]).await;
    assert_eq!(after["status"], "past_due");
    assert_eq!(after["current_period_end"], due["current_period_end"]);
    assert_eq!(app.payment.requests().len(), 1);
    let row = app.attempt(&due["id"]).await;
    assert_eq!((row.0.as_str(), row.2), ("failed_permanent", 1));

    let failed = published_of(&app.db.pool, "SubscriptionPaymentFailed").await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].payload["decline_code"], "insufficient_funds");
    assert_eq!(failed[0].payload["dunning_attempt"], 1);
    assert_eq!(failed[0].payload["dunning_max"], 4);
    assert_eq!(failed[0].payload["period_end"], due["current_period_end"]);
    assert_eq!(failed[0].payload["user_id"], owner.id.to_string());
    let next_attempt: chrono::DateTime<Utc> = failed[0].payload["next_attempt_at"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        next_attempt,
        date(&due["current_period_end"])
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            + chrono::Duration::days(1)
    );
    assert!(published_of(&app.db.pool, "SubscriptionRenewed")
        .await
        .is_empty());
    assert!(published_of(&app.db.pool, "SubscriptionCanceled")
        .await
        .is_empty());

    let sent = app.email.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].idempotency_key, failed[0].id);
    assert_eq!(sent[0].to_user_id, owner.id);
    assert_eq!(sent[0].template, "dunning_card_declined");
    assert_eq!((sent[0].dunning_attempt, sent[0].dunning_max), (1, 4));
    app.app
        .send_dunning_email
        .execute(sent[0].clone())
        .await
        .unwrap();
    assert_eq!(
        app.email.sent().len(),
        1,
        "the ledger must make a re-send a no-op"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_persistent_provider_outage_backs_off_then_gives_up_without_events_or_emails() {
    let app = spawn_app().await;
    app.payment
        .always(Charge::Transient("provider returned 503".to_string()));
    let owner = app.api.signup().await;
    let due = app.due_subscription(&owner).await;
    app.job_a().await;

    app.job_b().await;
    app.job_b().await;

    assert_eq!(
        app.payment.requests().len(),
        1,
        "the backoff must stop the charge being hammered"
    );
    let (status, attempts, _, _) = app.attempt(&due["id"]).await;
    assert_eq!((status.as_str(), attempts), ("failed_retryable", 1));
    let wait: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM next_attempt_at - now())::float8 FROM renewal_attempts",
    )
    .fetch_one(&app.db.pool)
    .await
    .unwrap();
    assert!(
        wait > 5.0 * 3600.0 && wait < 7.0 * 3600.0,
        "first backoff is about 6h, got {wait}s"
    );
    assert_eq!(
        app.subscription(&owner, &due["id"]).await["status"],
        "active"
    );

    for _ in 0..2 {
        app.make_attempt_due(&due["id"]).await;
        app.job_b().await;
    }

    let (status, attempts, dunning, _) = app.attempt(&due["id"]).await;
    assert_eq!((status.as_str(), attempts, dunning), ("given_up", 3, 0));
    assert_eq!(
        app.subscription(&owner, &due["id"]).await["status"],
        "past_due"
    );
    assert_eq!(app.payment.requests().len(), 3);
    assert!(published_events(&app.db.pool)
        .await
        .iter()
        .all(|e| e.aggregate_type == "user"));
    assert!(app.email.sent().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_charge_wedged_in_charging_is_reaped_and_recharged_under_the_same_idempotency_key() {
    let app = spawn_app().await;
    let owner = app.api.signup().await;
    let due = app.due_subscription(&owner).await;
    app.job_a().await;
    sqlx::query(
        "UPDATE renewal_attempts SET status = 'charging', attempt_count = 1, \
         updated_at = now() - interval '20 minutes'",
    )
    .execute(&app.db.pool)
    .await
    .unwrap();
    let stored_key: String = sqlx::query_scalar("SELECT idempotency_key FROM renewal_attempts")
        .fetch_one(&app.db.pool)
        .await
        .unwrap();

    app.job_b().await;

    let requests = app.payment.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].idempotency_key, stored_key);
    let (status, attempts, _, charge_id) = app.attempt(&due["id"]).await;
    assert_eq!((status.as_str(), attempts), ("succeeded", 2));
    assert!(charge_id.is_some());
    assert_eq!(
        published_of(&app.db.pool, "SubscriptionRenewed")
            .await
            .len(),
        1
    );
}
