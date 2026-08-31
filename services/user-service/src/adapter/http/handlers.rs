use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use uuid::Uuid;

use crate::domain::{Pagination, UserError, DEFAULT_LIMIT};
use crate::platform::db::DbPool;
use crate::usecase::{
    GetUserProfileUseCase, ListUsersUseCase, LoginUserUseCase, RegisterUserUseCase,
};

use super::dto::{
    ErrorResponse, LoginRequest, LoginResponse, PaginatedUsersResponse, RegisterRequest,
    UserResponse,
};

pub struct AppState {
    pub register_user: RegisterUserUseCase,
    pub login_user: LoginUserUseCase,
    pub get_user_profile: GetUserProfileUseCase,
    pub list_users: ListUsersUseCase,
    pub db_pool: DbPool,
}

fn map_error(err: UserError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match err {
        UserError::NotFound => StatusCode::NOT_FOUND,
        UserError::EmailAlreadyExists | UserError::InvalidEmail | UserError::InvalidPagination => {
            StatusCode::BAD_REQUEST
        }
        UserError::InvalidCredentials => StatusCode::UNAUTHORIZED,
        UserError::Repository(_) | UserError::Hashing(_) | UserError::Token(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (
        status,
        Json(ErrorResponse {
            error: err.to_string(),
        }),
    )
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/register",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User registered", body = UserResponse),
        (status = 400, description = "Email already registered or invalid", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<UserResponse>), (StatusCode, Json<ErrorResponse>)> {
    let user = state
        .register_user
        .execute(req.email, req.password)
        .await
        .map_err(map_error)?;

    Ok((StatusCode::CREATED, Json(UserResponse::from(&user))))
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Login successful", body = LoginResponse),
        (status = 401, description = "Invalid email or password", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    let token = state
        .login_user
        .execute(req.email, req.password)
        .await
        .map_err(map_error)?;

    Ok(Json(LoginResponse { token }))
}

#[utoipa::path(
    get,
    path = "/api/v1/users/{id}",
    params(
        ("id" = Uuid, Path, description = "User ID"),
    ),
    responses(
        (status = 200, description = "User found", body = UserResponse),
        (status = 404, description = "User not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_user(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<UserResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user = state
        .get_user_profile
        .execute(id)
        .await
        .map_err(map_error)?;

    Ok(Json(UserResponse::from(&user)))
}

#[derive(Debug, Deserialize)]
pub struct ListUsersParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/v1/users",
    params(
        ("limit" = Option<i64>, Query, description = "page size, default 20, max 100"),
        ("offset" = Option<i64>, Query, description = "rows to skip, default 0"),
    ),
    responses(
        (status = 200, description = "Page of users", body = PaginatedUsersResponse),
        (status = 400, description = "Invalid pagination parameters", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_users(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListUsersParams>,
) -> Result<Json<PaginatedUsersResponse>, (StatusCode, Json<ErrorResponse>)> {
    let pagination = Pagination::new(
        params.limit.unwrap_or(DEFAULT_LIMIT),
        params.offset.unwrap_or(0),
    )
    .map_err(map_error)?;

    let (users, total) = state
        .list_users
        .execute(pagination)
        .await
        .map_err(map_error)?;

    Ok(Json(PaginatedUsersResponse::new(
        &users,
        &pagination,
        total,
    )))
}

pub async fn healthz() -> StatusCode {
    StatusCode::OK
}

pub async fn readyz(State(state): State<Arc<AppState>>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&state.db_pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
