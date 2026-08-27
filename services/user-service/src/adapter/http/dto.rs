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

/// Named `domain::User` → wire mapper, kept next to the DTO. Borrows the entity
/// (rather than consuming it) so a handler can still use the `User` afterwards —
/// e.g. once a saga step needs the aggregate after responding.
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

/// Pagination envelope every list endpoint returns alongside its data, so a
/// client can tell a response is paginated without reading the docs (CLAUDE.md
/// list-endpoint convention).
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationMeta {
    pub limit: i64,
    pub offset: i64,
    pub total: i64,
    pub has_more: bool,
}

/// Wire shape for `GET /api/v1/users`: the page of users plus its pagination
/// metadata — never a bare array.
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedUsersResponse {
    pub data: Vec<UserResponse>,
    pub pagination: PaginationMeta,
}

impl PaginatedUsersResponse {
    /// The one `(domain users, pagination, total)` → wire mapper, kept next to
    /// the DTO — extends the response-mapper convention to the list envelope,
    /// not just the single-item DTO.
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
