use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use super::handlers::{
    create_subscription, get_subscription, get_user, healthz, list_subscriptions, list_users,
    login, readyz, register, retry_renewal_now, AppState,
};

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/auth/register", post(register))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/users", get(list_users))
        .route("/api/v1/users/:id", get(get_user))
        .route(
            "/api/v1/subscriptions",
            post(create_subscription).get(list_subscriptions),
        )
        .route("/api/v1/subscriptions/:id", get(get_subscription))
        .route(
            "/api/v1/subscriptions/:id/retry-renewal",
            post(retry_renewal_now),
        )
        .with_state(state)
}
