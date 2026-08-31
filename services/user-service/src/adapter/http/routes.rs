use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use super::handlers::{get_user, healthz, list_users, login, readyz, register, AppState};

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/auth/register", post(register))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/users", get(list_users))
        .route("/api/v1/users/:id", get(get_user))
        .with_state(state)
}
