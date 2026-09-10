use chrono::{DateTime, Months, NaiveDate, Utc};
use uuid::Uuid;

use super::errors::UserError;

/// The renewal cadence of a subscription. Stored as the TEXT values the
/// `subscriptions.billing_interval` CHECK constraint allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BillingInterval {
    Month,
    Year,
}

impl BillingInterval {
    pub fn as_str(&self) -> &'static str {
        match self {
            BillingInterval::Month => "month",
            BillingInterval::Year => "year",
        }
    }

    pub fn parse(value: &str) -> Result<Self, UserError> {
        match value.trim() {
            "month" => Ok(BillingInterval::Month),
            "year" => Ok(BillingInterval::Year),
            other => Err(UserError::InvalidSubscription(format!(
                "billing_interval must be 'month' or 'year', got '{other}'"
            ))),
        }
    }

    fn months(&self) -> u32 {
        match self {
            BillingInterval::Month => 1,
            BillingInterval::Year => 12,
        }
    }
}

/// Lifecycle state of a subscription. Only `Active` is set here; `PastDue`,
/// `Canceled` and `Paused` are driven by the renewal Job B and the dunning
/// sequence (docs/sagas/renewal-subscriptions.md §2), wired separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionStatus {
    Active,
    PastDue,
    Canceled,
    Paused,
}

impl SubscriptionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubscriptionStatus::Active => "active",
            SubscriptionStatus::PastDue => "past_due",
            SubscriptionStatus::Canceled => "canceled",
            SubscriptionStatus::Paused => "paused",
        }
    }

    /// Parse a value read back from the `subscriptions.status` column. An
    /// unrecognised value means the DB drifted from this enum — a repository
    /// error, not a client error.
    pub fn parse(value: &str) -> Result<Self, UserError> {
        match value {
            "active" => Ok(SubscriptionStatus::Active),
            "past_due" => Ok(SubscriptionStatus::PastDue),
            "canceled" => Ok(SubscriptionStatus::Canceled),
            "paused" => Ok(SubscriptionStatus::Paused),
            other => Err(UserError::Repository(format!(
                "unknown subscription status in database: '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: Uuid,
    pub user_id: Uuid,
    pub plan_id: String,
    pub status: SubscriptionStatus,
    pub current_period_end: NaiveDate,
    pub billing_interval: BillingInterval,
    pub price_minor: i64,
    pub currency: String,
    pub payment_method_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Subscription {
    /// A brand-new `Active` subscription owned by `user_id`. The first renewal
    /// falls due one billing interval from today (`current_period_end`).
    pub fn new(
        user_id: Uuid,
        plan_id: String,
        billing_interval: BillingInterval,
        price_minor: i64,
        currency: String,
        payment_method_id: String,
    ) -> Result<Self, UserError> {
        let plan_id = plan_id.trim().to_string();
        if plan_id.is_empty() {
            return Err(UserError::InvalidSubscription(
                "plan_id must not be empty".to_string(),
            ));
        }
        if price_minor < 0 {
            return Err(UserError::InvalidSubscription(
                "price_minor must not be negative".to_string(),
            ));
        }
        let currency = currency.trim().to_uppercase();
        if currency.len() != 3 || !currency.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(UserError::InvalidSubscription(
                "currency must be a 3-letter ISO 4217 code".to_string(),
            ));
        }
        let payment_method_id = payment_method_id.trim().to_string();
        if payment_method_id.is_empty() {
            return Err(UserError::InvalidSubscription(
                "payment_method_id must not be empty".to_string(),
            ));
        }

        let now = Utc::now();
        let current_period_end = now
            .date_naive()
            .checked_add_months(Months::new(billing_interval.months()))
            .ok_or_else(|| {
                UserError::InvalidSubscription("period end is out of range".to_string())
            })?;

        Ok(Self {
            id: Uuid::new_v4(),
            user_id,
            plan_id,
            status: SubscriptionStatus::Active,
            current_period_end,
            billing_interval,
            price_minor,
            currency,
            payment_method_id,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether a renewal for this subscription's current period can be retried.
    /// A `canceled` or `paused` subscription has no live billing period to
    /// charge, so a retry is a conflict with its state.
    pub fn ensure_renewal_retryable(&self) -> Result<(), UserError> {
        match self.status {
            SubscriptionStatus::Active | SubscriptionStatus::PastDue => Ok(()),
            SubscriptionStatus::Canceled => Err(UserError::RenewalNotRetryable(
                "subscription is canceled".to_string(),
            )),
            SubscriptionStatus::Paused => Err(UserError::RenewalNotRetryable(
                "subscription is paused".to_string(),
            )),
        }
    }
}
