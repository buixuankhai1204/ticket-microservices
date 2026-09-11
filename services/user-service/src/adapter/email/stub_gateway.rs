use async_trait::async_trait;

use crate::domain::{DunningEmail, EmailError, EmailGateway};

/// Placeholder for a real transactional-email client. It logs the dunning notice
/// instead of sending it. `EMAIL_STUB_FAIL=1` makes every send fail so the
/// best-effort fallback (docs/sagas/renewal-subscriptions.md §5.9) can be
/// exercised.
pub struct StubEmailGateway {
    fail: bool,
}

impl StubEmailGateway {
    pub fn new(fail: bool) -> Self {
        Self { fail }
    }

    pub fn from_env(var: &str) -> Self {
        let fail = matches!(
            std::env::var(var).unwrap_or_default().trim(),
            "1" | "true" | "yes"
        );
        Self::new(fail)
    }
}

#[async_trait]
impl EmailGateway for StubEmailGateway {
    async fn send(&self, email: DunningEmail) -> Result<(), EmailError> {
        if self.fail {
            return Err(EmailError("stub email gateway: forced failure".to_string()));
        }
        tracing::info!(
            to_user_id = %email.to_user_id,
            subscription_id = %email.subscription_id,
            template = %email.template,
            period_end = %email.period_end,
            dunning_attempt = email.dunning_attempt,
            dunning_max = email.dunning_max,
            amount_minor = email.amount_minor,
            currency = %email.currency,
            "stub email gateway: dunning notice"
        );
        Ok(())
    }
}
