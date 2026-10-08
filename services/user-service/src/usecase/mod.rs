pub mod create_subscription;
pub mod enqueue_due_renewals;
pub mod get_subscription;
pub mod get_user_profile;
pub mod list_subscriptions;
pub mod list_users;
pub mod login_user;
pub mod process_renewal_attempt;
pub mod register_user;
pub mod retry_renewal_now;
pub mod send_dunning_email;

pub use create_subscription::{CreateSubscriptionInput, CreateSubscriptionUseCase};
pub use enqueue_due_renewals::{EnqueueDueRenewalsUseCase, EnqueueOutcome};
pub use get_subscription::GetSubscriptionUseCase;
pub use get_user_profile::GetUserProfileUseCase;
pub use list_subscriptions::ListSubscriptionsUseCase;
pub use list_users::ListUsersUseCase;
pub use login_user::LoginUserUseCase;
pub use process_renewal_attempt::{ProcessOutcome, ProcessRenewalAttemptUseCase};
pub use register_user::RegisterUserUseCase;
pub use retry_renewal_now::RetryRenewalNowUseCase;
pub use send_dunning_email::SendDunningEmailUseCase;

#[cfg(test)]
pub(crate) mod fixtures {
    use chrono::{NaiveDate, Utc};
    use uuid::Uuid;

    use crate::domain::{
        BillingInterval, RenewalAttempt, RenewalAttemptStatus, Subscription, SubscriptionStatus,
        User,
    };

    pub fn user(email: &str) -> User {
        User::from_persisted(
            Uuid::new_v4(),
            email.to_string(),
            "stored-hash".to_string(),
            Utc::now(),
        )
    }

    pub fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    pub fn subscription(status: SubscriptionStatus, current_period_end: NaiveDate) -> Subscription {
        Subscription {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            plan_id: "pro".to_string(),
            status,
            current_period_end,
            billing_interval: BillingInterval::Month,
            price_minor: 1999,
            currency: "USD".to_string(),
            payment_method_id: "pm_1".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub fn attempt(
        subscription: &Subscription,
        status: RenewalAttemptStatus,
        attempt_count: i32,
        dunning_attempt_count: i32,
    ) -> RenewalAttempt {
        let now = Utc::now();
        RenewalAttempt {
            id: Uuid::new_v4(),
            subscription_id: subscription.id,
            period_end: subscription.current_period_end,
            idempotency_key: RenewalAttempt::idempotency_key_for(
                subscription.id,
                subscription.current_period_end,
            ),
            status,
            attempt_count,
            dunning_attempt_count,
            next_attempt_at: now,
            provider_charge_id: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }
}
