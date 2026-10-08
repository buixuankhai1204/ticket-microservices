#![allow(dead_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use user_service::domain::{
    ChargeOutcome, ChargeRequest, DunningEmail, EmailError, EmailGateway, PaymentError,
    PaymentGateway,
};
use uuid::Uuid;

static ADMIN_URL: OnceLock<String> = OnceLock::new();

pub fn admin_url() -> &'static str {
    ADMIN_URL.get_or_init(|| {
        if let Ok(url) = std::env::var("TEST_DATABASE_URL") {
            return url;
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime for the Postgres container");
            runtime.block_on(async move {
                let node = Postgres::default()
                    .with_tag("16-alpine")
                    .with_cmd(["postgres", "-c", "max_connections=400"])
                    .start()
                    .await
                    .expect("failed to start the Postgres container (is Docker running?)");
                let port = node.get_host_port_ipv4(5432).await.unwrap();
                sender
                    .send(format!(
                        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
                    ))
                    .unwrap();
                std::future::pending::<()>().await;
            });
        });
        receiver
            .recv()
            .expect("the Postgres container did not start")
    })
}

fn url_for(database: &str) -> String {
    let admin = admin_url();
    let base = admin.rsplit_once('/').map(|(base, _)| base).unwrap();
    format!("{base}/{database}")
}

pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
    name: String,
}

pub async fn new_empty_database() -> TestDb {
    let mut admin = PgConnection::connect(admin_url())
        .await
        .expect("connect to Postgres");
    let name = format!("t_{}", Uuid::new_v4().simple());
    let mut attempt = 0;
    loop {
        match sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
        {
            Ok(_) => break,
            Err(e) if attempt < 5 => {
                attempt += 1;
                eprintln!("CREATE DATABASE failed ({e}), retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("create database: {e}"),
        }
    }
    let url = url_for(&name);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to the throwaway database");
    TestDb { pool, url, name }
}

pub async fn new_database() -> TestDb {
    let db = new_empty_database().await;
    user_service::app::migrate(&db.pool)
        .await
        .expect("run migrations");
    install_outbox_tap(&db.pool).await;
    db
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                if let Ok(mut admin) = PgConnection::connect(admin_url()).await {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                        .execute(&mut admin)
                        .await;
                }
            });
        })
        .join();
    }
}

const OUTBOX_TAP: &str = "
CREATE TABLE outbox_tap (
    seq            BIGSERIAL PRIMARY KEY,
    id             UUID,
    aggregate_id   UUID,
    aggregate_type TEXT,
    event_type     TEXT,
    payload        JSONB
);
CREATE FUNCTION outbox_tap_copy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox_tap (id, aggregate_id, aggregate_type, event_type, payload)
    VALUES (NEW.id, NEW.aggregate_id, NEW.aggregate_type, NEW.event_type, NEW.payload);
    RETURN NEW;
END
$$;
CREATE TRIGGER outbox_tap_after_insert
    AFTER INSERT ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_tap_copy();
";

pub async fn install_outbox_tap(pool: &PgPool) {
    pool.execute(OUTBOX_TAP)
        .await
        .expect("install the outbox tap");
}

#[derive(Debug, Clone)]
pub struct PublishedEvent {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub aggregate_type: String,
    pub event_type: String,
    pub payload: Value,
}

pub async fn published_events(pool: &PgPool) -> Vec<PublishedEvent> {
    sqlx::query(
        "SELECT id, aggregate_id, aggregate_type, event_type, payload FROM outbox_tap ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .expect("read the outbox tap")
    .into_iter()
    .map(|row| PublishedEvent {
        id: row.get("id"),
        aggregate_id: row.get("aggregate_id"),
        aggregate_type: row.get("aggregate_type"),
        event_type: row.get("event_type"),
        payload: row.get("payload"),
    })
    .collect()
}

pub async fn published_of(pool: &PgPool, event_type: &str) -> Vec<PublishedEvent> {
    published_events(pool)
        .await
        .into_iter()
        .filter(|e| e.event_type == event_type)
        .collect()
}

pub async fn eventually<T, F, Fut>(timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn unique_email() -> String {
    format!("{}@example.com", Uuid::new_v4().simple())
}

pub const PASSWORD: &str = "correct-horse-battery";

pub struct Account {
    pub id: Uuid,
    pub email: String,
    pub token: String,
}

pub struct Api {
    pub base: String,
    pub http: reqwest::Client,
}

impl Api {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn send(
        &self,
        method: Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = self.http.request(method, format!("{}{}", self.base, path));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("request to the service");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    pub async fn get(&self, path: &str, token: Option<&str>) -> (u16, Value) {
        self.send(Method::GET, path, token, None).await
    }

    pub async fn post(&self, path: &str, token: Option<&str>, body: Value) -> (u16, Value) {
        self.send(Method::POST, path, token, Some(body)).await
    }

    pub async fn register(&self, email: &str, password: &str) -> (u16, Value) {
        self.post(
            "/api/v1/auth/register",
            None,
            json!({ "email": email, "password": password }),
        )
        .await
    }

    pub async fn login(&self, email: &str, password: &str) -> (u16, Value) {
        self.post(
            "/api/v1/auth/login",
            None,
            json!({ "email": email, "password": password }),
        )
        .await
    }

    pub async fn signup(&self) -> Account {
        let email = unique_email();
        let (status, user) = self.register(&email, PASSWORD).await;
        assert_eq!(status, 201, "register failed: {user}");
        let (status, session) = self.login(&email, PASSWORD).await;
        assert_eq!(status, 200, "login failed: {session}");
        Account {
            id: user["id"].as_str().unwrap().parse().unwrap(),
            email,
            token: session["token"].as_str().unwrap().to_string(),
        }
    }

    pub fn subscription_request() -> Value {
        json!({
            "plan_id": "pro",
            "billing_interval": "month",
            "price_minor": 1999,
            "currency": "USD",
            "payment_method_id": "pm_card_visa"
        })
    }

    pub async fn subscribe(&self, token: &str) -> Value {
        let (status, subscription) = self
            .post(
                "/api/v1/subscriptions",
                Some(token),
                Self::subscription_request(),
            )
            .await;
        assert_eq!(status, 201, "create subscription failed: {subscription}");
        subscription
    }
}

#[derive(Clone, Debug)]
pub enum Charge {
    Approved(String),
    Declined(String),
    Transient(String),
}

pub struct ScriptedPayment {
    script: Mutex<VecDeque<Charge>>,
    fallback: Mutex<Charge>,
    requests: Mutex<Vec<ChargeRequest>>,
}

impl ScriptedPayment {
    pub fn new(fallback: Charge) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(VecDeque::new()),
            fallback: Mutex::new(fallback),
            requests: Mutex::new(Vec::new()),
        })
    }

    pub fn approving() -> Arc<Self> {
        Self::new(Charge::Approved("ch_scripted".to_string()))
    }

    pub fn then(&self, next: Charge) {
        self.script.lock().unwrap().push_back(next);
    }

    pub fn always(&self, outcome: Charge) {
        *self.fallback.lock().unwrap() = outcome;
    }

    pub fn requests(&self) -> Vec<ChargeRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl PaymentGateway for ScriptedPayment {
    async fn charge(&self, request: ChargeRequest) -> Result<ChargeOutcome, PaymentError> {
        self.requests.lock().unwrap().push(request);
        let next = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.fallback.lock().unwrap().clone());
        match next {
            Charge::Approved(id) => Ok(ChargeOutcome {
                provider_charge_id: id,
            }),
            Charge::Declined(code) => Err(PaymentError::Declined { code }),
            Charge::Transient(message) => Err(PaymentError::Transient(message)),
        }
    }
}

pub struct ScriptedEmail {
    sent: Mutex<Vec<DunningEmail>>,
}

impl ScriptedEmail {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sent: Mutex::new(Vec::new()),
        })
    }

    pub fn sent(&self) -> Vec<DunningEmail> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl EmailGateway for ScriptedEmail {
    async fn send(&self, email: DunningEmail) -> Result<(), EmailError> {
        self.sent.lock().unwrap().push(email);
        Ok(())
    }
}
