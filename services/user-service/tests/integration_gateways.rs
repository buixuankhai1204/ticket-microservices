use std::time::{Duration, Instant};

use chrono::NaiveDate;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use user_service::adapter::email::HttpEmailGateway;
use user_service::adapter::payment::HttpPaymentGateway;
use user_service::domain::{
    ChargeOutcome, ChargeRequest, DunningEmail, EmailGateway, PaymentError, PaymentGateway,
};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn charge_request() -> ChargeRequest {
    ChargeRequest {
        amount_minor: 1999,
        currency: "USD".to_string(),
        payment_method_id: "pm_secret_token".to_string(),
        idempotency_key: format!("renew:{}:2031-03-31", Uuid::new_v4()),
    }
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

fn payment_gateway(base_url: &str, timeout: Duration) -> HttpPaymentGateway {
    HttpPaymentGateway::new(base_url, "test-key".to_string(), timeout).unwrap()
}

fn email_gateway(base_url: &str, timeout: Duration) -> HttpEmailGateway {
    HttpEmailGateway::new(base_url, "test-key".to_string(), timeout).unwrap()
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

async fn charge_against(response: ResponseTemplate) -> Result<ChargeOutcome, PaymentError> {
    let server = provider("/v1/charges", response).await;
    payment_gateway(&server.uri(), Duration::from_secs(2))
        .charge(charge_request())
        .await
}

fn is_transient(result: &Result<ChargeOutcome, PaymentError>) -> bool {
    matches!(result, Err(PaymentError::Transient(_)))
}

async fn misbehaving_server(reply: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let _ = socket.write_all(reply).await;
        }
    });
    format!("http://{address}")
}

fn header(request: &wiremock::Request, name: &str) -> String {
    request
        .headers
        .get(name)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn a_successful_charge_sends_the_idempotency_key_and_a_well_formed_request() {
    let server = provider(
        "/v1/charges",
        ResponseTemplate::new(201).set_body_json(json!({ "id": "ch_123" })),
    )
    .await;
    let request = charge_request();
    let key = request.idempotency_key.clone();

    let outcome = payment_gateway(&server.uri(), Duration::from_secs(2))
        .charge(request)
        .await
        .unwrap();

    assert_eq!(outcome.provider_charge_id, "ch_123");
    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    let sent = &received[0];
    assert_eq!(header(sent, "idempotency-key"), key);
    assert_eq!(header(sent, "authorization"), "Bearer test-key");
    assert!(header(sent, "content-type").starts_with("application/json"));
    let body: Value = serde_json::from_slice(&sent.body).unwrap();
    assert_eq!(
        body,
        json!({ "amount_minor": 1999, "currency": "USD", "payment_method_id": "pm_secret_token" })
    );
    assert!(!sent.url.as_str().contains("pm_secret_token"));
}

#[tokio::test]
async fn a_decline_is_permanent_but_a_rejected_request_is_never_mistaken_for_one() {
    let declines = [
        ("card_declined", "card_declined"),
        ("expired_card", "expired_card"),
        ("insufficient_funds", "insufficient_funds"),
        ("something_new", "other"),
    ];
    for (code, want) in declines {
        let response =
            ResponseTemplate::new(402).set_body_json(json!({ "error": { "code": code } }));
        match charge_against(response).await {
            Err(PaymentError::Declined { code }) => assert_eq!(code, want),
            other => panic!("expected Declined({want}), got {other:?}"),
        }
    }
    match charge_against(ResponseTemplate::new(402)).await {
        Err(PaymentError::Declined { code }) => assert_eq!(code, "other"),
        other => panic!("expected Declined(other), got {other:?}"),
    }

    for status in [400, 401, 403, 404, 409, 422] {
        let response = ResponseTemplate::new(status)
            .set_body_json(json!({ "error": { "code": "card_declined" } }));
        let result = charge_against(response).await;
        assert!(is_transient(&result), "{status} -> {result:?}");
    }
    let redirect =
        ResponseTemplate::new(302).insert_header("Location", "http://127.0.0.1:1/elsewhere");
    assert!(is_transient(&charge_against(redirect).await));
}

#[tokio::test]
async fn outages_unusable_successes_timeouts_and_broken_connections_are_all_transient() {
    for status in [408, 429, 500, 503] {
        let result = charge_against(ResponseTemplate::new(status)).await;
        assert!(is_transient(&result), "{status} -> {result:?}");
    }
    let unusable = [
        ResponseTemplate::new(201).set_body_string("not json"),
        ResponseTemplate::new(201).set_body_json(json!({})),
        ResponseTemplate::new(201).set_body_json(json!({ "id": "  " })),
        ResponseTemplate::new(202).set_body_json(json!({ "id": "ch_late" })),
        ResponseTemplate::new(204),
    ];
    for response in unusable {
        let result = charge_against(response).await;
        assert!(is_transient(&result), "{result:?}");
    }

    let slow = provider(
        "/v1/charges",
        ResponseTemplate::new(201)
            .set_body_json(json!({ "id": "ch_slow" }))
            .set_delay(Duration::from_secs(3)),
    )
    .await;
    let gateway = payment_gateway(&slow.uri(), Duration::from_millis(300));
    let request = charge_request();
    for _ in 0..2 {
        let started = Instant::now();
        let result = gateway.charge(request.clone()).await;
        assert!(is_transient(&result), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    let received = slow.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);
    assert!(received
        .iter()
        .all(|r| header(r, "idempotency-key") == request.idempotency_key));

    for reply in [
        &b""[..],
        &b"\x00\x01garbage\xff"[..],
        &b"HTTP/1.1 200 OK\r\nContent-Le"[..],
    ] {
        let base = misbehaving_server(reply).await;
        let result = payment_gateway(&base, Duration::from_secs(2))
            .charge(charge_request())
            .await;
        assert!(is_transient(&result), "{reply:?} -> {result:?}");
    }
    let refused = payment_gateway("http://127.0.0.1:1", Duration::from_secs(2))
        .charge(charge_request())
        .await;
    assert!(is_transient(&refused));
}

#[tokio::test]
async fn an_email_carries_the_event_id_as_its_key_and_any_failure_or_timeout_is_an_error() {
    let server = provider("/v1/emails", ResponseTemplate::new(202)).await;
    let email = dunning_email();
    let key = email.idempotency_key.to_string();
    let user = email.to_user_id.to_string();

    email_gateway(&server.uri(), Duration::from_secs(2))
        .send(email)
        .await
        .unwrap();

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(header(&received[0], "idempotency-key"), key);
    assert_eq!(header(&received[0], "authorization"), "Bearer test-key");
    let body: Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["to_user_id"], user);
    assert_eq!(body["template"], "dunning_card_declined");
    assert_eq!(body["period_end"], "2031-03-31");

    for response in [
        ResponseTemplate::new(400),
        ResponseTemplate::new(500),
        ResponseTemplate::new(200).set_delay(Duration::from_secs(3)),
    ] {
        let failing = provider("/v1/emails", response).await;
        let started = Instant::now();
        let result = email_gateway(&failing.uri(), Duration::from_millis(300))
            .send(dunning_email())
            .await;
        assert!(result.is_err(), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
