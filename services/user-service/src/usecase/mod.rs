pub mod get_user_profile;
pub mod list_users;
pub mod login_user;
pub mod register_user;

pub use get_user_profile::GetUserProfileUseCase;
pub use list_users::ListUsersUseCase;
pub use login_user::LoginUserUseCase;
pub use register_user::RegisterUserUseCase;

use crate::domain::UserError;

pub(crate) fn tx_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}
