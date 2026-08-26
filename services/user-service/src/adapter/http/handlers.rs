use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use uuid::Uuid;

use crate::domain::UserError;
use crate::platform::db::DbPool;
use crate::usecase::{GetUserProfileUseCase, LoginUserUseCase, RegisterUserUseCase};

use super::dto::{ErrorResponse, LoginRequest, LoginResponse, RegisterRequest, UserResponse};

pub struct AppState {
    pub register_user: RegisterUserUseCase,
    pub login_user: LoginUserUseCase,
    pub get_user_profile: GetUserProfileUseCase,
    pub db_pool: DbPool,
}

fn map_error(err: UserError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match err {
        UserError::NotFound => StatusCode::NOT_FOUND,
        UserError::EmailAlreadyExists | UserError::InvalidEmail => StatusCode::BAD_REQUEST,
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

pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<UserResponse>), (StatusCode, Json<ErrorResponse>)> {
    let user = state
        .register_user
        .execute(req.email, req.password)
        .await
        .map_err(map_error)?;

    Ok((StatusCode::CREATED, Json(user.into())))
}

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

pub async fn get_user(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<UserResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user = state
        .get_user_profile
        .execute(id)
        .await
        .map_err(map_error)?;

    Ok(Json(user.into()))
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
