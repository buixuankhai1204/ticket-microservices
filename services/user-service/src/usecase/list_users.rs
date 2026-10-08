use std::sync::Arc;

use crate::domain::{Pagination, User, UserError};
use crate::platform::port::{Transactor, UserRepository};

pub struct ListUsersUseCase {
    db: Arc<dyn Transactor>,
    user_repository: Arc<dyn UserRepository>,
}

impl ListUsersUseCase {
    pub fn new(db: Arc<dyn Transactor>, user_repository: Arc<dyn UserRepository>) -> Self {
        Self {
            db,
            user_repository,
        }
    }

    pub async fn execute(&self, pagination: Pagination) -> Result<(Vec<User>, i64), UserError> {
        let mut tx = self.db.begin_read_only().await?;
        let page = self.user_repository.list(&mut tx, pagination).await?;
        tx.commit().await?;
        Ok(page)
    }
}
