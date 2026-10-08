use std::time::Duration;

use chrono::NaiveDate;
use pact_consumer::prelude::*;
use user_service::adapter::email::HttpEmailGateway;
use user_service::adapter::payment::HttpPaymentGateway;
use user_service::domain::{
    ChargeRequest, DunningEmail, EmailGateway, PaymentError, PaymentGateway,
};
use uuid::Uuid;

const PACT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/pacts");
const API_KEY: &str = "test-key";
const IDEMPOTENCY_KEY_PATTERN: &str = "^renew:[0-9a-f-]{36}:[0-9]{4}-[0-9]{2}-[0-9]{2}$";
const EXAMPLE_IDEMPOTENCY_KEY: &str = "renew:3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90:2031-03-31";

fn charge(payment_method_id: &str) -> ChargeRequest {
    ChargeRequest {
        amount_minor: 1999,
        currency: "USD".to_string(),
        payment_method_id: payment_method_id.to_string(),
        idempotency_key: EXAMPLE_IDEMPOTENCY_KEY.to_string(),
    }
}

fn charge_interaction(
    pact: &mut PactBuilder,
    description: &str,
    state: &str,
    payment_method_id: &str,
    status: u16,
    response: JsonPattern,
) {
    pact.interaction(description, "", |mut i| {
        i.given(state);
        i.request
            .post()
            .path("/v1/charges")
            .header("Authorization", format!("Bearer {API_KEY}"))
            .header(
                "Idempotency-Key",
                term!(IDEMPOTENCY_KEY_PATTERN, EXAMPLE_IDEMPOTENCY_KEY),
            )
            .json_body(json_pattern!({
                "amount_minor": 1999,
                "currency": "USD",
                "payment_method_id": payment_method_id
            }));
        i.response.status(status).json_body(response);
        i
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_payment_provider_charge_api_means_what_the_gateway_assumes() {
    let mut pact = PactBuilder::new_v4("user-service", "payment-provider");
    pact.with_output_dir(PACT_DIR);
    charge_interaction(
        &mut pact,
        "a charge for a chargeable card",
        "the payment method pm_card_visa can be charged",
        "pm_card_visa",
        201,
        json_pattern!({ "id": like!("ch_3PqXk2") }),
    );
    for (method, code) in [
        ("pm_card_chargeDeclined", "card_declined"),
        ("pm_card_expired", "expired_card"),
        ("pm_card_insufficientFunds", "insufficient_funds"),
    ] {
        charge_interaction(
            &mut pact,
            &format!("a charge for a card that is declined with {code}"),
            &format!("the payment method {method} is declined with {code}"),
            method,
            402,
            json_pattern!({ "error": { "code": code } }),
        );
    }
    charge_interaction(
        &mut pact,
        "a charge while the provider is rate limiting",
        "the provider is rate limiting",
        "pm_card_rateLimited",
        429,
        json_pattern!({ "error": { "code": "rate_limited" } }),
    );
    charge_interaction(
        &mut pact,
        "a charge while the provider is down",
        "the provider is unavailable",
        "pm_card_providerDown",
        503,
        json_pattern!({ "error": { "code": "unavailable" } }),
    );
    charge_interaction(
        &mut pact,
        "a charge with an API key the provider rejects",
        "the API key is not accepted",
        "pm_card_badKey",
        401,
        json_pattern!({ "error": { "code": "card_declined" } }),
    );
    charge_interaction(
        &mut pact,
        "a charge the provider cannot process",
        "the provider cannot process the request",
        "pm_card_unprocessable",
        422,
        json_pattern!({ "error": { "code": "card_declined" } }),
    );
    let provider = pact.start_mock_server(None, None);
    let gateway = HttpPaymentGateway::new(
        provider.url().as_str(),
        API_KEY.to_string(),
        Duration::from_secs(5),
    )
    .unwrap();

    let approved = gateway.charge(charge("pm_card_visa")).await.unwrap();
    assert_eq!(approved.provider_charge_id, "ch_3PqXk2");

    for (method, want) in [
        ("pm_card_chargeDeclined", "card_declined"),
        ("pm_card_expired", "expired_card"),
        ("pm_card_insufficientFunds", "insufficient_funds"),
    ] {
        match gateway.charge(charge(method)).await {
            Err(PaymentError::Declined { code }) => assert_eq!(code, want),
            other => panic!("{method}: expected Declined({want}), got {other:?}"),
        }
    }

    for method in [
        "pm_card_rateLimited",
        "pm_card_providerDown",
        "pm_card_badKey",
        "pm_card_unprocessable",
    ] {
        let result = gateway.charge(charge(method)).await;
        assert!(
            matches!(result, Err(PaymentError::Transient(_))),
            "{method}: expected Transient, got {result:?}"
        );
    }
}

fn dunning_email(template: &str) -> DunningEmail {
    DunningEmail {
        to_user_id: Uuid::parse_str("3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90").unwrap(),
        subscription_id: Uuid::parse_str("9c1d0a64-7e1b-4f55-8f0a-2b6f3d4e5a61").unwrap(),
        template: template.to_string(),
        idempotency_key: Uuid::parse_str("0b9e7c52-3a1d-4c8e-b2f4-6d5a1e9c7f30").unwrap(),
        period_end: NaiveDate::from_ymd_opt(2031, 3, 31).unwrap(),
        amount_minor: 1999,
        currency: "USD".to_string(),
        dunning_attempt: 2,
        dunning_max: 4,
    }
}

fn email_interaction(
    pact: &mut PactBuilder,
    description: &str,
    state: &str,
    template: &str,
    status: u16,
) {
    pact.interaction(description, "", |mut i| {
        i.given(state);
        i.request
            .post()
            .path("/v1/emails")
            .header("Authorization", format!("Bearer {API_KEY}"))
            .header(
                "Idempotency-Key",
                term!(
                    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                    "0b9e7c52-3a1d-4c8e-b2f4-6d5a1e9c7f30"
                ),
            )
            .json_body(json_pattern!({
                "to_user_id": like!("3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90"),
                "subscription_id": like!("9c1d0a64-7e1b-4f55-8f0a-2b6f3d4e5a61"),
                "template": template,
                "period_end": like!("2031-03-31"),
                "amount_minor": like!(1999),
                "currency": like!("USD"),
                "dunning_attempt": like!(2),
                "dunning_max": like!(4)
            }));
        i.response.status(status);
        i
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_email_provider_api_means_what_the_gateway_assumes() {
    let mut pact = PactBuilder::new_v4("user-service", "email-provider");
    pact.with_output_dir(PACT_DIR);
    email_interaction(
        &mut pact,
        "a dunning email the provider accepts",
        "the template dunning_card_declined exists",
        "dunning_card_declined",
        202,
    );
    email_interaction(
        &mut pact,
        "a dunning email with a template the provider does not know",
        "the template dunning_unknown_template does not exist",
        "dunning_unknown_template",
        422,
    );
    email_interaction(
        &mut pact,
        "a dunning email while the provider is down",
        "the email provider is unavailable",
        "dunning_provider_down",
        503,
    );
    let provider = pact.start_mock_server(None, None);
    let gateway = HttpEmailGateway::new(
        provider.url().as_str(),
        API_KEY.to_string(),
        Duration::from_secs(5),
    )
    .unwrap();

    gateway
        .send(dunning_email("dunning_card_declined"))
        .await
        .unwrap();
    for template in ["dunning_unknown_template", "dunning_provider_down"] {
        let result = gateway.send(dunning_email(template)).await;
        assert!(
            result.is_err(),
            "{template}: expected an error, got {result:?}"
        );
    }
}
