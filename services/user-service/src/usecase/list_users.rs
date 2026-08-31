use std::sync::Arc;

use sqlx::PgPool;

use super::tx_err;
use crate::domain::{Pagination, User, UserError};
use crate::platform::port::UserRepository;

pub struct ListUsersUseCase {
    db_pool: PgPool,
    user_repository: Arc<dyn UserRepository>,
}

impl ListUsersUseCase {
    pub fn new(db_pool: PgPool, user_repository: Arc<dyn UserRepository>) -> Self {
        Self {
            db_pool,
            user_repository,
        }
    }

    pub async fn execute(&self, pagination: Pagination) -> Result<(Vec<User>, i64), UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let page = self.user_repository.list(&mut tx, pagination).await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(page)
    }
}
