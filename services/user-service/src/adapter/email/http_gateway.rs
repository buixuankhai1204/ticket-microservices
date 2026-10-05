use std::time::Duration;

use async_trait::async_trait;
use reqwest::redirect::Policy;
use serde::Serialize;
use uuid::Uuid;

use crate::domain::{DunningEmail, EmailError, EmailGateway};

#[derive(Serialize)]
struct EmailBody<'a> {
    to_user_id: Uuid,
    subscription_id: Uuid,
    template: &'a str,
    period_end: String,
    amount_minor: i64,
    currency: &'a str,
    dunning_attempt: i32,
    dunning_max: i32,
}

pub struct HttpEmailGateway {
    client: reqwest::Client,
    emails_url: String,
    api_key: String,
}

impl HttpEmailGateway {
    pub fn new(base_url: &str, api_key: String, timeout: Duration) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(Policy::none())
            .build()?;
        Ok(Self {
            client,
            emails_url: format!("{}/v1/emails", base_url.trim_end_matches('/')),
            api_key,
        })
    }
}

#[async_trait]
impl EmailGateway for HttpEmailGateway {
    async fn send(&self, email: DunningEmail) -> Result<(), EmailError> {
        let response = self
            .client
            .post(&self.emails_url)
            .bearer_auth(&self.api_key)
            .header("Idempotency-Key", email.idempotency_key.to_string())
            .json(&EmailBody {
                to_user_id: email.to_user_id,
                subscription_id: email.subscription_id,
                template: &email.template,
                period_end: email.period_end.to_string(),
                amount_minor: email.amount_minor,
                currency: &email.currency,
                dunning_attempt: email.dunning_attempt,
                dunning_max: email.dunning_max,
            })
            .send()
            .await
            .map_err(|e| EmailError(format!("email provider unreachable: {}", e.without_url())))?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(EmailError(format!("email provider returned {status}")))
        }
    }
}
