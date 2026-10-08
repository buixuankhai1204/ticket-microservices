use std::sync::Arc;

use uuid::Uuid;

use crate::domain::{User, UserError};
use crate::platform::port::{Transactor, UserRepository};

pub struct GetUserProfileUseCase {
    db: Arc<dyn Transactor>,
    user_repository: Arc<dyn UserRepository>,
}

impl GetUserProfileUseCase {
    pub fn new(db: Arc<dyn Transactor>, user_repository: Arc<dyn UserRepository>) -> Self {
        Self {
            db,
            user_repository,
        }
    }

    pub async fn execute(&self, id: Uuid) -> Result<User, UserError> {
        let mut tx = self.db.begin_read_only().await?;
        let user = self.user_repository.find_by_id(&mut tx, id).await?;
        tx.commit().await?;
        Ok(user)
    }
}
