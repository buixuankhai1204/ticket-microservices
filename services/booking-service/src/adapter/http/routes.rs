use std::sync::Arc;

use axum::routing::get;
use axum::Router;

use super::handlers::{get_booking, healthz, readyz, AppState};

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/bookings/:id", get(get_booking))
        .with_state(state)
}
