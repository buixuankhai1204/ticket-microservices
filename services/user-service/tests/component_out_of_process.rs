mod common;

use std::net::TcpListener;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use common::{eventually, new_database, new_empty_database, published_of, Api, TestDb};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

type Env = Vec<(&'static str, String)>;

struct Service {
    child: Child,
    port: u16,
    api: Api,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn base_env(database_url: &str, port: u16) -> Env {
    vec![
        ("DATABASE_URL", database_url.to_string()),
        ("JWT_SECRET", "out-of-process-secret".to_string()),
        ("JWT_ISSUER", "out-of-process".to_string()),
        ("PORT", port.to_string()),
        ("DB_MAX_CONNECTIONS", "5".to_string()),
    ]
}

fn command(env: &Env) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_user-service"));
    command
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())));
    command
}

impl Service {
    async fn start(database_url: &str, extra: &[(&'static str, String)]) -> Self {
        let port = free_port();
        let mut env = base_env(database_url, port);
        env.extend(extra.iter().cloned());
        let child = command(&env)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start user-service");
        let mut service = Service {
            child,
            port,
            api: Api::new(format!("http://127.0.0.1:{port}")),
        };
        service.wait_until_healthy().await;
        service
    }

    async fn wait_until_healthy(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("user-service exited early with {status}");
            }
            if let Ok(response) = self
                .api
                .http
                .get(format!("{}/healthz", self.api.base))
                .send()
                .await
            {
                if response.status().is_success() {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "user-service did not become healthy in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn terminate(&self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(status.success());
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "user-service did not exit in time"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fails_to_start(env: &Env) -> (ExitStatus, String) {
    let mut child = command(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start user-service");
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("user-service kept running although its config is invalid");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let output = child.wait_with_output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (status, text)
}

async fn provider(endpoint: &str, response: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(endpoint))
        .respond_with(response)
        .mount(&server)
        .await;
    server
}

async fn make_due(db: &TestDb, subscription: &Value) -> String {
    sqlx::query_scalar::<_, chrono::NaiveDate>(
        "UPDATE subscriptions SET current_period_end = CURRENT_DATE - 1 WHERE id = $1 \
         RETURNING current_period_end",
    )
    .bind(
        subscription["id"]
            .as_str()
            .unwrap()
            .parse::<uuid::Uuid>()
            .unwrap(),
    )
    .fetch_one(&db.pool)
    .await
    .unwrap()
    .to_string()
}

fn fast_jobs() -> Vec<(&'static str, String)> {
    vec![
        ("RENEWAL_ENQUEUE_INTERVAL_SECS", "1".to_string()),
        ("RENEWAL_PROCESS_INTERVAL_SECS", "1".to_string()),
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_compiled_binary_migrates_serves_the_api_and_exits_cleanly_on_sigterm() {
    let db = new_empty_database().await;
    let mut service = Service::start(&db.url, &[]).await;
    let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(migrations, 8);

    let account = service.api.signup().await;
    let (status, profile) = service
        .api
        .get(
            &format!("/api/v1/users/{}", account.id),
            Some(&account.token),
        )
        .await;
    assert_eq!(
        (status, profile["email"].clone()),
        (200, json!(account.email))
    );
    let (readyz, _) = service.api.get("/readyz", None).await;
    assert_eq!(readyz, 200);

    let metrics = service
        .api
        .http
        .get(format!("{}/metrics", service.api.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("http_requests_total"));
    assert!(metrics.contains("path=\"/api/v1/auth/register\""));
    let (status, spec) = service.api.get("/api-docs/openapi.json", None).await;
    assert_eq!(status, 200);
    assert!(spec["paths"]["/api/v1/subscriptions"].is_object());

    let port = service.port();
    service.terminate();
    let exit = service.wait_for_exit(Duration::from_secs(15));
    assert_eq!(exit.code(), Some(0), "{exit}");
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_binary_refuses_to_start_when_required_config_is_missing_or_unsafe() {
    let db = new_database().await;
    let with = |set: &[(&'static str, &str)], unset: &[&str]| -> Env {
        let mut env = base_env(&db.url, free_port());
        env.retain(|(key, _)| !unset.contains(key));
        env.extend(set.iter().map(|(k, v)| (*k, v.to_string())));
        env
    };
    let missing_database = {
        let mut env = base_env(&db.url, free_port());
        env.retain(|(key, _)| *key != "DATABASE_URL");
        env
    };
    let unknown_database = base_env(
        &format!(
            "{}/does_not_exist",
            common::admin_url().rsplit_once('/').unwrap().0
        ),
        free_port(),
    );

    let cases: Vec<(&str, Env, &str)> = vec![
        (
            "no DATABASE_URL",
            missing_database,
            "DATABASE_URL must be set",
        ),
        (
            "a database that does not exist",
            unknown_database,
            "failed to connect to postgres",
        ),
        (
            "no JWT_SECRET",
            with(&[], &["JWT_SECRET"]),
            "JWT_SECRET must be set",
        ),
        (
            "a payment provider without an API key",
            with(&[("PAYMENT_PROVIDER_BASE_URL", "http://127.0.0.1:9")], &[]),
            "PAYMENT_PROVIDER_API_KEY must be set",
        ),
        (
            "an email provider without an API key",
            with(&[("EMAIL_PROVIDER_BASE_URL", "http://127.0.0.1:9")], &[]),
            "EMAIL_PROVIDER_API_KEY must be set",
        ),
        (
            "a payment timeout that outlives the charging reaper",
            with(
                &[
                    ("PAYMENT_PROVIDER_BASE_URL", "http://127.0.0.1:9"),
                    ("PAYMENT_PROVIDER_API_KEY", "k"),
                    ("PAYMENT_TIMEOUT_SECS", "900"),
                    ("RENEWAL_CHARGING_TIMEOUT_SECS", "900"),
                ],
                &[],
            ),
            "PAYMENT_TIMEOUT_SECS must be shorter than RENEWAL_CHARGING_TIMEOUT_SECS",
        ),
    ];
    for (label, env, message) in cases {
        let (status, output) = fails_to_start(&env);
        assert!(!status.success(), "{label}: exited with {status}");
        assert!(output.contains(message), "{label}: {output}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_background_jobs_started_by_main_renew_a_due_subscription_on_their_own() {
    let db = new_database().await;
    let payment = provider(
        "/v1/charges",
        ResponseTemplate::new(201).set_body_json(json!({ "id": "ch_oop" })),
    )
    .await;
    let mut env = fast_jobs();
    env.push(("PAYMENT_PROVIDER_BASE_URL", payment.uri()));
    env.push(("PAYMENT_PROVIDER_API_KEY", "payment-key".to_string()));
    let service = Service::start(&db.url, &env).await;
    let account = service.api.signup().await;
    let subscription = service.api.subscribe(&account.token).await;
    let id = subscription["id"].as_str().unwrap().to_string();
    let due_end: chrono::NaiveDate = make_due(&db, &subscription).await.parse().unwrap();

    let renewed = eventually(Duration::from_secs(40), || async {
        let (_, current) = service
            .api
            .get(&format!("/api/v1/subscriptions/{id}"), Some(&account.token))
            .await;
        let advanced = current["current_period_end"]
            .as_str()
            .and_then(|end| end.parse::<chrono::NaiveDate>().ok())
            .is_some_and(|end| end > chrono::Utc::now().date_naive());
        advanced.then_some(current)
    })
    .await;

    assert_eq!(renewed["status"], "active");
    assert_eq!(
        renewed["current_period_end"],
        json!(due_end
            .checked_add_months(chrono::Months::new(1))
            .unwrap()
            .to_string())
    );
    let charges = payment.received_requests().await.unwrap();
    assert_eq!(charges.len(), 1);
    assert_eq!(
        charges[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer payment-key"
    );
    assert_eq!(
        charges[0]
            .headers
            .get("idempotency-key")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("renew:{id}:{due_end}")
    );
    let events = published_of(&db.pool, "SubscriptionRenewed").await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["provider_charge_id"], "ch_oop");
}
