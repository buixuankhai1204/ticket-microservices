use std::sync::Arc;
use uuid::Uuid;

use crate::domain::{User, UserError, UserRepository};

pub struct GetUserProfileUseCase {
    user_repository: Arc<dyn UserRepository>,
}

impl GetUserProfileUseCase {
    pub fn new(user_repository: Arc<dyn UserRepository>) -> Self {
        Self { user_repository }
    }

    pub async fn execute(&self, id: Uuid) -> Result<User, UserError> {
        self.user_repository.find_by_id(id).await
    }
}
