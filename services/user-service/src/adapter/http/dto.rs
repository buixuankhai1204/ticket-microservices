use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::{Pagination, User};

#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
}

impl From<&User> for UserResponse {
    fn from(user: &User) -> Self {
        Self {
            id: user.id,
            email: user.email.clone(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationMeta {
    pub limit: i64,
    pub offset: i64,
    pub total: i64,
    pub has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedUsersResponse {
    pub data: Vec<UserResponse>,
    pub pagination: PaginationMeta,
}

impl PaginatedUsersResponse {
    pub fn new(users: &[User], pagination: &Pagination, total: i64) -> Self {
        let data: Vec<UserResponse> = users.iter().map(UserResponse::from).collect();
        let has_more = pagination.has_more(data.len(), total);
        Self {
            data,
            pagination: PaginationMeta {
                limit: pagination.limit,
                offset: pagination.offset,
                total,
                has_more,
            },
        }
    }
}
