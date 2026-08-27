use std::sync::Arc;

use crate::domain::{Pagination, User, UserError, UserRepository};

/// Lists registered users, one page at a time. One flow; depends only on the
/// `UserRepository` port, injected via `new`.
pub struct ListUsersUseCase {
    user_repository: Arc<dyn UserRepository>,
}

impl ListUsersUseCase {
    pub fn new(user_repository: Arc<dyn UserRepository>) -> Self {
        Self { user_repository }
    }

    /// Returns the page of users and the total match count. Parses nothing
    /// itself — the HTTP layer hands it an already-validated [`Pagination`]. The
    /// single read-only transaction spanning the count + page lives in the
    /// repository implementation.
    pub async fn execute(&self, pagination: Pagination) -> Result<(Vec<User>, i64), UserError> {
        self.user_repository.list(pagination).await
    }
}
