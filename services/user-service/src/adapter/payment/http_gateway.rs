use std::time::Duration;

use async_trait::async_trait;
use reqwest::redirect::Policy;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::domain::{ChargeOutcome, ChargeRequest, PaymentError, PaymentGateway};

const DECLINE_CODES: [&str; 3] = ["card_declined", "expired_card", "insufficient_funds"];
const OTHER_DECLINE: &str = "other";

#[derive(Serialize)]
struct ChargeBody<'a> {
    amount_minor: i64,
    currency: &'a str,
    payment_method_id: &'a str,
}

#[derive(Deserialize)]
struct ChargeResponse {
    id: String,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: Option<ErrorDetail>,
}

#[derive(Deserialize)]
struct ErrorDetail {
    code: Option<String>,
}

pub struct HttpPaymentGateway {
    client: reqwest::Client,
    charges_url: String,
    api_key: String,
}

impl HttpPaymentGateway {
    pub fn new(base_url: &str, api_key: String, timeout: Duration) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(Policy::none())
            .build()?;
        Ok(Self {
            client,
            charges_url: format!("{}/v1/charges", base_url.trim_end_matches('/')),
            api_key,
        })
    }

    async fn declined(response: reqwest::Response) -> PaymentError {
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .ok()
            .and_then(|e| e.error)
            .and_then(|e| e.code)
            .filter(|c| DECLINE_CODES.contains(&c.as_str()))
            .unwrap_or_else(|| OTHER_DECLINE.to_string());
        PaymentError::Declined { code }
    }
}

#[async_trait]
impl PaymentGateway for HttpPaymentGateway {
    async fn charge(&self, request: ChargeRequest) -> Result<ChargeOutcome, PaymentError> {
        let response = self
            .client
            .post(&self.charges_url)
            .bearer_auth(&self.api_key)
            .header("Idempotency-Key", &request.idempotency_key)
            .json(&ChargeBody {
                amount_minor: request.amount_minor,
                currency: &request.currency,
                payment_method_id: &request.payment_method_id,
            })
            .send()
            .await
            .map_err(|e| {
                PaymentError::Transient(format!(
                    "payment provider unreachable: {}",
                    e.without_url()
                ))
            })?;

        let status = response.status();
        if status == StatusCode::OK || status == StatusCode::CREATED {
            return match response.json::<ChargeResponse>().await {
                Ok(body) if !body.id.trim().is_empty() => Ok(ChargeOutcome {
                    provider_charge_id: body.id,
                }),
                Ok(_) => Err(PaymentError::Transient(
                    "payment provider returned an empty charge id".to_string(),
                )),
                Err(e) => Err(PaymentError::Transient(format!(
                    "payment provider returned an unreadable success response: {}",
                    e.without_url()
                ))),
            };
        }
        if status == StatusCode::PAYMENT_REQUIRED {
            return Err(Self::declined(response).await);
        }
        if status == StatusCode::REQUEST_TIMEOUT
            || status == StatusCode::TOO_MANY_REQUESTS
            || status.is_server_error()
        {
            return Err(PaymentError::Transient(format!(
                "payment provider returned {status}"
            )));
        }
        tracing::error!(
            status = %status,
            "payment provider rejected the charge request; check PAYMENT_PROVIDER_API_KEY and the request contract"
        );
        Err(PaymentError::Transient(format!(
            "payment provider rejected the request with {status}"
        )))
    }
}
