use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCreated {
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

impl UserCreated {
    pub fn new(user_id: Uuid, email: String, created_at: DateTime<Utc>) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            user_id,
            email,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserLoggedIn {
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub logged_in_at: DateTime<Utc>,
}

impl UserLoggedIn {
    pub fn new(user_id: Uuid, email: String, logged_in_at: DateTime<Utc>) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            user_id,
            email,
            logged_in_at,
        }
    }
}

/// A renewal charge succeeded — the subscription is extended one period.
/// docs/sagas/renewal-subscriptions.md §3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRenewed {
    pub event_id: Uuid,
    pub subscription_id: Uuid,
    pub user_id: Uuid,
    pub plan_id: String,
    pub renewal_attempt_id: Uuid,
    pub period_start: NaiveDate,
    pub new_period_end: NaiveDate,
    pub amount_minor: i64,
    pub currency: String,
    pub provider_charge_id: String,
    pub attempt_count: i32,
    pub renewed_at: DateTime<Utc>,
}

/// A renewal charge was declined — the customer is now in dunning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPaymentFailed {
    pub event_id: Uuid,
    pub subscription_id: Uuid,
    pub user_id: Uuid,
    pub plan_id: String,
    pub renewal_attempt_id: Uuid,
    pub period_end: NaiveDate,
    pub amount_minor: i64,
    pub currency: String,
    pub decline_code: String,
    pub dunning_attempt: i32,
    pub dunning_max: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
}

/// Dunning was exhausted — the subscription is involuntarily canceled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionCanceled {
    pub event_id: Uuid,
    pub subscription_id: Uuid,
    pub user_id: Uuid,
    pub plan_id: String,
    pub renewal_attempt_id: Uuid,
    pub period_end: NaiveDate,
    pub reason: String,
    pub dunning_attempts: i32,
    pub canceled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    UserCreated(UserCreated),
    UserLoggedIn(UserLoggedIn),
    SubscriptionRenewed(SubscriptionRenewed),
    SubscriptionPaymentFailed(SubscriptionPaymentFailed),
    SubscriptionCanceled(SubscriptionCanceled),
}

impl DomainEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.event_id,
            DomainEvent::UserLoggedIn(e) => e.event_id,
            DomainEvent::SubscriptionRenewed(e) => e.event_id,
            DomainEvent::SubscriptionPaymentFailed(e) => e.event_id,
            DomainEvent::SubscriptionCanceled(e) => e.event_id,
        }
    }

    /// The Kafka message key — the SMT maps this to the partition key, so all
    /// events for one subscription stay ordered.
    pub fn aggregate_id(&self) -> Uuid {
        match self {
            DomainEvent::UserCreated(e) => e.user_id,
            DomainEvent::UserLoggedIn(e) => e.user_id,
            DomainEvent::SubscriptionRenewed(e) => e.subscription_id,
            DomainEvent::SubscriptionPaymentFailed(e) => e.subscription_id,
            DomainEvent::SubscriptionCanceled(e) => e.subscription_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            DomainEvent::UserCreated(_) => "UserCreated",
            DomainEvent::UserLoggedIn(_) => "UserLoggedIn",
            DomainEvent::SubscriptionRenewed(_) => "SubscriptionRenewed",
            DomainEvent::SubscriptionPaymentFailed(_) => "SubscriptionPaymentFailed",
            DomainEvent::SubscriptionCanceled(_) => "SubscriptionCanceled",
        }
    }

    /// Routes to `<aggregate_type>.events` via the Debezium Outbox Event Router.
    pub fn aggregate_type(&self) -> &'static str {
        match self {
            DomainEvent::UserCreated(_) | DomainEvent::UserLoggedIn(_) => "user",
            DomainEvent::SubscriptionRenewed(_)
            | DomainEvent::SubscriptionPaymentFailed(_)
            | DomainEvent::SubscriptionCanceled(_) => "subscription",
        }
    }

    pub fn payload(&self) -> serde_json::Value {
        match self {
            DomainEvent::UserCreated(e) => {
                serde_json::to_value(e).expect("UserCreated is serializable")
            }
            DomainEvent::UserLoggedIn(e) => {
                serde_json::to_value(e).expect("UserLoggedIn is serializable")
            }
            DomainEvent::SubscriptionRenewed(e) => {
                serde_json::to_value(e).expect("SubscriptionRenewed is serializable")
            }
            DomainEvent::SubscriptionPaymentFailed(e) => {
                serde_json::to_value(e).expect("SubscriptionPaymentFailed is serializable")
            }
            DomainEvent::SubscriptionCanceled(e) => {
                serde_json::to_value(e).expect("SubscriptionCanceled is serializable")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn events_route_to_their_aggregate_topic_keyed_by_the_right_id() {
        let ts = Utc.with_ymd_and_hms(2031, 3, 4, 5, 6, 7).unwrap();
        let date = NaiveDate::from_ymd_opt(2031, 3, 31).unwrap();
        let user_id = Uuid::new_v4();
        let subscription_id = Uuid::new_v4();

        let created = DomainEvent::UserCreated(UserCreated::new(user_id, "a@b.c".to_string(), ts));
        let logged_in =
            DomainEvent::UserLoggedIn(UserLoggedIn::new(user_id, "a@b.c".to_string(), ts));
        let renewed = DomainEvent::SubscriptionRenewed(SubscriptionRenewed {
            event_id: Uuid::new_v4(),
            subscription_id,
            user_id,
            plan_id: "pro".to_string(),
            renewal_attempt_id: Uuid::new_v4(),
            period_start: date,
            new_period_end: date,
            amount_minor: 1,
            currency: "USD".to_string(),
            provider_charge_id: "ch_1".to_string(),
            attempt_count: 1,
            renewed_at: ts,
        });
        let failed = DomainEvent::SubscriptionPaymentFailed(SubscriptionPaymentFailed {
            event_id: Uuid::new_v4(),
            subscription_id,
            user_id,
            plan_id: "pro".to_string(),
            renewal_attempt_id: Uuid::new_v4(),
            period_end: date,
            amount_minor: 1,
            currency: "USD".to_string(),
            decline_code: "card_declined".to_string(),
            dunning_attempt: 1,
            dunning_max: 4,
            next_attempt_at: ts,
            failed_at: ts,
        });
        let canceled = DomainEvent::SubscriptionCanceled(SubscriptionCanceled {
            event_id: Uuid::new_v4(),
            subscription_id,
            user_id,
            plan_id: "pro".to_string(),
            renewal_attempt_id: Uuid::new_v4(),
            period_end: date,
            reason: "dunning_exhausted".to_string(),
            dunning_attempts: 4,
            canceled_at: ts,
        });

        for (event, event_type, aggregate_type, aggregate_id) in [
            (&created, "UserCreated", "user", user_id),
            (&logged_in, "UserLoggedIn", "user", user_id),
            (
                &renewed,
                "SubscriptionRenewed",
                "subscription",
                subscription_id,
            ),
            (
                &failed,
                "SubscriptionPaymentFailed",
                "subscription",
                subscription_id,
            ),
            (
                &canceled,
                "SubscriptionCanceled",
                "subscription",
                subscription_id,
            ),
        ] {
            assert_eq!(event.event_type(), event_type);
            assert_eq!(event.aggregate_type(), aggregate_type);
            assert_eq!(event.aggregate_id(), aggregate_id);
            assert_eq!(event.payload()["event_id"], event.event_id().to_string());
        }
    }
}
