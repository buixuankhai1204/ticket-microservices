use async_trait::async_trait;
use chrono::NaiveDate;
use uuid::Uuid;

use super::errors::{EmailError, PaymentError, UserError};

pub trait PasswordHasher: Send + Sync {
    fn hash(&self, password: &str) -> Result<String, UserError>;
    fn verify(&self, password: &str, hash: &str) -> Result<bool, UserError>;
}

pub trait TokenIssuer: Send + Sync {
    fn issue(&self, user_id: Uuid, email: &str) -> Result<String, UserError>;
}

/// One renewal charge to attempt against the payment provider.
#[derive(Debug, Clone)]
pub struct ChargeRequest {
    pub amount_minor: i64,
    pub currency: String,
    pub payment_method_id: String,
    /// Deterministic per `(subscription, period)` —
    /// `renew:{subscription_id}:{period_end}`. The provider must dedupe on it so
    /// a retried call after a crash returns the original charge, never a second
    /// one (docs/sagas/renewal-subscriptions.md §5.4).
    pub idempotency_key: String,
}

#[derive(Debug, Clone)]
pub struct ChargeOutcome {
    pub provider_charge_id: String,
}

/// Outbound port: charge a saved payment method. Pure — names no infra type.
#[async_trait]
pub trait PaymentGateway: Send + Sync {
    async fn charge(&self, request: ChargeRequest) -> Result<ChargeOutcome, PaymentError>;
}

/// One dunning email to send.
#[derive(Debug, Clone)]
pub struct DunningEmail {
    pub to_user_id: Uuid,
    pub subscription_id: Uuid,
    pub template: String,
    /// The `SubscriptionPaymentFailed` `event_id` — one email per dunning event.
    pub idempotency_key: Uuid,
    pub period_end: NaiveDate,
    pub amount_minor: i64,
    pub currency: String,
    pub dunning_attempt: i32,
    pub dunning_max: i32,
}

/// Outbound port: send a transactional email. Pure — names no infra type.
#[async_trait]
pub trait EmailGateway: Send + Sync {
    async fn send(&self, email: DunningEmail) -> Result<(), EmailError>;
}
