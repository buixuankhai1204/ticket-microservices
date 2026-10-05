use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;

pub fn wiremock_url() -> String {
    std::env::var("WIREMOCK_URL").unwrap_or_else(|_| "http://localhost:8089".to_string())
}

pub struct Stub {
    pub prefix: String,
    http: reqwest::Client,
}

impl Stub {
    pub fn new() -> Self {
        Self {
            prefix: format!("/t/{}", Uuid::new_v4()),
            http: reqwest::Client::new(),
        }
    }

    pub fn base_url(&self) -> String {
        format!("{}{}", wiremock_url(), self.prefix)
    }

    pub async fn respond(&self, path: &str, response: Value) {
        let status = self
            .http
            .post(format!("{}/__admin/mappings", wiremock_url()))
            .json(&json!({
                "request": { "method": "POST", "urlPath": format!("{}{}", self.prefix, path) },
                "response": response,
            }))
            .send()
            .await
            .expect("wiremock admin api is unreachable")
            .status();
        assert!(
            status.is_success(),
            "registering the stub failed with {status}"
        );
    }

    pub async fn requests(&self, path: &str) -> Vec<Value> {
        let body: Value = self
            .http
            .post(format!("{}/__admin/requests/find", wiremock_url()))
            .json(&json!({ "method": "POST", "urlPath": format!("{}{}", self.prefix, path) }))
            .send()
            .await
            .expect("wiremock admin api is unreachable")
            .json()
            .await
            .expect("journal response");
        body["requests"].as_array().cloned().unwrap_or_default()
    }

    pub async fn wait_for_requests(&self, path: &str, count: usize) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let found = self.requests(path).await;
            if found.len() >= count || Instant::now() > deadline {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

pub fn header(request: &Value, name: &str) -> String {
    request["headers"]
        .as_object()
        .and_then(|h| h.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)))
        .map(|(_, v)| match v {
            Value::Array(a) => a
                .first()
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            other => other.as_str().unwrap_or_default().to_string(),
        })
        .unwrap_or_default()
}

pub fn json_response(status: u16, body: Value) -> Value {
    json!({ "status": status, "jsonBody": body, "headers": { "Content-Type": "application/json" } })
}

pub fn raw_response(status: u16, body: &str) -> Value {
    json!({ "status": status, "body": body, "headers": { "Content-Type": "application/json" } })
}
