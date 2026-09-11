pub mod create_subscription;
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
pub use get_subscription::GetSubscriptionUseCase;
pub use get_user_profile::GetUserProfileUseCase;
pub use list_subscriptions::ListSubscriptionsUseCase;
pub use list_users::ListUsersUseCase;
pub use login_user::LoginUserUseCase;
pub use process_renewal_attempt::{ProcessOutcome, ProcessRenewalAttemptUseCase};
pub use register_user::RegisterUserUseCase;
pub use retry_renewal_now::RetryRenewalNowUseCase;
pub use send_dunning_email::SendDunningEmailUseCase;

use crate::domain::UserError;

pub(crate) fn tx_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}
