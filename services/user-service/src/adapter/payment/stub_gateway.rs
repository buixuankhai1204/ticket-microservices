use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{ChargeOutcome, ChargeRequest, PaymentError, PaymentGateway};

/// What the stub does on every `charge` call — selected by `PAYMENT_STUB_OUTCOME`
/// so `e2e-saga-tester` can drive the happy path *and* the dunning / give-up
/// paths without a real provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StubOutcome {
    Succeed,
    Decline,
    Transient,
}

impl StubOutcome {
    pub fn from_env(var: &str) -> Self {
        match std::env::var(var)
            .unwrap_or_default()
            .trim()
            .to_lowercase()
            .as_str()
        {
            "decline" => StubOutcome::Decline,
            "transient" => StubOutcome::Transient,
            _ => StubOutcome::Succeed,
        }
    }
}

/// Placeholder for a real payment-provider client. It does not talk to any
/// network; swap it for an HTTP adapter wrapped with `/add-resilience` when a
/// provider is chosen. The `Idempotency-Key` contract on [`ChargeRequest`] still
/// holds — the stub echoes it into the fake charge id so retries are visibly
/// stable.
pub struct StubPaymentGateway {
    outcome: StubOutcome,
}

impl StubPaymentGateway {
    pub fn new(outcome: StubOutcome) -> Self {
        Self { outcome }
    }
}

#[async_trait]
impl PaymentGateway for StubPaymentGateway {
    async fn charge(&self, request: ChargeRequest) -> Result<ChargeOutcome, PaymentError> {
        tracing::info!(
            idempotency_key = %request.idempotency_key,
            payment_method_id = %request.payment_method_id,
            amount_minor = request.amount_minor,
            currency = %request.currency,
            outcome = ?self.outcome,
            "stub payment gateway: charge"
        );
        match self.outcome {
            StubOutcome::Succeed => Ok(ChargeOutcome {
                provider_charge_id: format!("stub_{}", Uuid::new_v4()),
            }),
            StubOutcome::Decline => Err(PaymentError::Declined {
                code: "card_declined".to_string(),
            }),
            StubOutcome::Transient => Err(PaymentError::Transient(
                "stub provider unavailable".to_string(),
            )),
        }
    }
}
