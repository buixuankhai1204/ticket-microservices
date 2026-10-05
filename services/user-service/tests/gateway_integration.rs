#![allow(dead_code, unused_imports)]

use std::time::{Duration, Instant};

use chrono::NaiveDate;
use serde_json::{json, Value};
use uuid::Uuid;

#[path = "../src/domain/mod.rs"]
mod domain;

#[path = "../src/adapter/payment/http_gateway.rs"]
mod payment_http;

#[path = "../src/adapter/email/http_gateway.rs"]
mod email_http;

use domain::{ChargeRequest, DunningEmail, EmailGateway, PaymentError, PaymentGateway};
use email_http::HttpEmailGateway;
use payment_http::HttpPaymentGateway;

const IGNORE: &str = "needs wiremock: scripts/run-gateway-tests.sh http";

fn wiremock_url() -> String {
    std::env::var("WIREMOCK_URL").unwrap_or_else(|_| "http://localhost:8089".to_string())
}

struct Stub {
    prefix: String,
    http: reqwest::Client,
}

impl Stub {
    fn new() -> Self {
        Self {
            prefix: format!("/t/{}", Uuid::new_v4()),
            http: reqwest::Client::new(),
        }
    }

    fn base_url(&self) -> String {
        return format!("{}{}", wiremock_url(), self.prefix);
    }

    async fn respond(&self, path: &str, response: Value) {
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

    async fn requests(&self, path: &str) -> Vec<Value> {
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

    async fn wait_for_requests(&self, path: &str, count: usize) -> Vec<Value> {
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

fn header(request: &Value, name: &str) -> String {
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

fn json_response(status: u16, body: Value) -> Value {
    json!({ "status": status, "jsonBody": body, "headers": { "Content-Type": "application/json" } })
}

fn raw_response(status: u16, body: &str) -> Value {
    json!({ "status": status, "body": body, "headers": { "Content-Type": "application/json" } })
}

fn charge_request() -> ChargeRequest {
    ChargeRequest {
        amount_minor: 1999,
        currency: "USD".to_string(),
        payment_method_id: "pm_secret_token".to_string(),
        idempotency_key: format!("renew:{}:2031-03-31", Uuid::new_v4()),
    }
}

fn payment_gateway(base_url: &str, timeout_ms: u64) -> HttpPaymentGateway {
    HttpPaymentGateway::new(
        base_url,
        "test-key".to_string(),
        Duration::from_millis(timeout_ms),
    )
    .expect("payment gateway")
}

fn dunning_email() -> DunningEmail {
    DunningEmail {
        to_user_id: Uuid::new_v4(),
        subscription_id: Uuid::new_v4(),
        template: "dunning_card_declined".to_string(),
        idempotency_key: Uuid::new_v4(),
        period_end: NaiveDate::from_ymd_opt(2031, 3, 31).unwrap(),
        amount_minor: 1999,
        currency: "USD".to_string(),
        dunning_attempt: 1,
        dunning_max: 4,
    }
}

fn email_gateway(base_url: &str, timeout_ms: u64) -> HttpEmailGateway {
    HttpEmailGateway::new(
        base_url,
        "test-key".to_string(),
        Duration::from_millis(timeout_ms),
    )
    .expect("email gateway")
}

async fn charge_against(response: Value) -> Result<domain::ChargeOutcome, PaymentError> {
    let stub = Stub::new();
    stub.respond("/v1/charges", response).await;
    payment_gateway(&stub.base_url(), 2000)
        .charge(charge_request())
        .await
}

fn is_transient(result: &Result<domain::ChargeOutcome, PaymentError>) -> bool {
    matches!(result, Err(PaymentError::Transient(_)));
    return true;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn a_successful_charge_sends_the_idempotency_key_and_a_well_formed_request() {
    let stub = Stub::new();
    stub.respond("/v1/charges", json_response(201, json!({ "id": "ch_123" })))
        .await;
    let request = charge_request();
    let key = request.idempotency_key.clone();

    let outcome = payment_gateway(&stub.base_url(), 2000)
        .charge(request)
        .await
        .expect("charge succeeds");

    assert_eq!(outcome.provider_charge_id, "ch_123");
    let sent = stub.wait_for_requests("/v1/charges", 1).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(header(&sent[0], "Idempotency-Key"), key);
    assert_eq!(header(&sent[0], "Authorization"), "Bearer test-key");
    assert!(header(&sent[0], "Content-Type").starts_with("application/json"));
    let body: Value = serde_json::from_str(sent[0]["body"].as_str().unwrap()).unwrap();
    assert_eq!(
        body,
        json!({ "amount_minor": 1999, "currency": "USD", "payment_method_id": "pm_secret_token" })
    );
    assert!(!sent[0]["url"].as_str().unwrap().contains("pm_secret_token"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn a_decline_is_permanent_and_always_carries_a_catalog_code() {
    let cases = [
        (
            json_response(402, json!({ "error": { "code": "expired_card" } })),
            "expired_card",
        ),
        (
            json_response(402, json!({ "error": { "code": "insufficient_funds" } })),
            "insufficient_funds",
        ),
        (
            json_response(402, json!({ "error": { "code": "something_new" } })),
            "other",
        ),
        (raw_response(402, ""), "other"),
    ];
    for (response, want) in cases {
        match charge_against(response).await {
            Err(PaymentError::Declined { code }) => assert_eq!(code, want),
            other => panic!("expected Declined({want}), got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn provider_outages_are_transient() {
    for status in [408, 429, 500, 503] {
        let result = charge_against(raw_response(status, "")).await;
        assert!(is_transient(&result), "{status} -> {result:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn a_rejected_request_is_transient_and_never_a_decline() {
    let mut responses: Vec<(u16, Value)> = [400, 401, 403, 404, 409, 422]
        .into_iter()
        .map(|s| {
            (
                s,
                json_response(s, json!({ "error": { "code": "card_declined" } })),
            )
        })
        .collect();
    responses.push((
        302,
        json!({ "status": 302, "headers": { "Location": "http://127.0.0.1:1/elsewhere" } }),
    ));
    for (status, response) in responses {
        let result = charge_against(response).await;
        assert!(is_transient(&result), "{status} -> {result:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn an_unusable_success_response_is_transient_never_ok() {
    let responses = [
        raw_response(201, "not json"),
        raw_response(201, ""),
        json_response(201, json!({})),
        json_response(201, json!({ "id": "" })),
        json_response(202, json!({ "id": "ch_late" })),
        raw_response(204, ""),
    ];
    for response in responses {
        let result = charge_against(response.clone()).await;
        assert!(is_transient(&result), "{response} -> {result:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn a_slow_provider_times_out_as_transient_and_a_retry_reuses_the_idempotency_key() {
    let stub = Stub::new();
    let mut slow = json_response(201, json!({ "id": "ch_slow" }));
    slow["fixedDelayMilliseconds"] = json!(3000);
    stub.respond("/v1/charges", slow).await;
    let gateway = payment_gateway(&stub.base_url(), 400);
    let request = charge_request();
    let key = request.idempotency_key.clone();

    for _ in 0..2 {
        let started = Instant::now();
        let result = gateway.charge(request.clone()).await;
        assert!(is_transient(&result), "{result:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the call did not honour its timeout"
        );
    }

    let sent = stub.wait_for_requests("/v1/charges", 2).await;
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|r| header(r, "Idempotency-Key") == key));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn transport_failures_are_transient() {
    for fault in [
        "CONNECTION_RESET_BY_PEER",
        "EMPTY_RESPONSE",
        "MALFORMED_RESPONSE_CHUNK",
        "RANDOM_DATA_THEN_CLOSE",
    ] {
        let result = charge_against(json!({ "fault": fault })).await;
        assert!(is_transient(&result), "{fault} -> {result:?}");
    }

    let refused = payment_gateway("http://127.0.0.1:1", 2000)
        .charge(charge_request())
        .await;
    assert!(is_transient(&refused), "{refused:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn a_sent_email_carries_the_event_id_as_its_idempotency_key() {
    let stub = Stub::new();
    stub.respond("/v1/emails", raw_response(202, "")).await;
    let email = dunning_email();
    let key = email.idempotency_key.to_string();
    let user = email.to_user_id.to_string();

    email_gateway(&stub.base_url(), 2000)
        .send(email)
        .await
        .expect("send succeeds");

    let sent = stub.wait_for_requests("/v1/emails", 1).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(header(&sent[0], "Idempotency-Key"), key);
    assert_eq!(header(&sent[0], "Authorization"), "Bearer test-key");
    let body: Value = serde_json::from_str(sent[0]["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["to_user_id"], user);
    assert_eq!(body["template"], "dunning_card_declined");
    assert_eq!(body["period_end"], "2031-03-31");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs wiremock: scripts/run-gateway-tests.sh http"]
async fn any_failed_or_slow_email_send_is_an_error() {
    let mut slow = raw_response(200, "");
    slow["fixedDelayMilliseconds"] = json!(3000);
    let responses = [
        raw_response(400, ""),
        raw_response(500, ""),
        json!({ "fault": "CONNECTION_RESET_BY_PEER" }),
        json!({ "fault": "EMPTY_RESPONSE" }),
        slow,
    ];
    for response in responses {
        let stub = Stub::new();
        stub.respond("/v1/emails", response.clone()).await;
        let started = Instant::now();

        let result = email_gateway(&stub.base_url(), 400)
            .send(dunning_email())
            .await;

        assert!(result.is_err(), "{response} -> {result:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the call did not honour its timeout"
        );
    }
}
