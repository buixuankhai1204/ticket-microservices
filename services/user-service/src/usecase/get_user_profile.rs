use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::tx_err;
use crate::domain::{User, UserError};
use crate::platform::port::UserRepository;

pub struct GetUserProfileUseCase {
    db_pool: PgPool,
    user_repository: Arc<dyn UserRepository>,
}

impl GetUserProfileUseCase {
    pub fn new(db_pool: PgPool, user_repository: Arc<dyn UserRepository>) -> Self {
        Self {
            db_pool,
            user_repository,
        }
    }

    pub async fn execute(&self, id: Uuid) -> Result<User, UserError> {
        let mut tx = self.db_pool.begin().await.map_err(tx_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(tx_err)?;
        let user = self.user_repository.find_by_id(&mut tx, id).await?;
        tx.commit().await.map_err(tx_err)?;
        Ok(user)
    }
}
