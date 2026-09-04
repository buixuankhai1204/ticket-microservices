mod adapter;
mod domain;
mod platform;
mod usecase;

use std::env;
use std::sync::Arc;

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use adapter::http::{build_router, ApiDoc, AppState};
use adapter::repository::postgres::PostgresBookingRepository;
use platform::db;
use platform::port::BookingRepository;
use usecase::GetBookingUseCase;

#[tokio::main]
async fn main() {
    platform::logging::init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let max_connections: u32 = env::var("DB_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let pool = db::build_pool(&database_url, max_connections)
        .await
        .expect("failed to connect to postgres");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run database migrations");

    let booking_repository: Arc<dyn BookingRepository> = Arc::new(PostgresBookingRepository::new());

    let state = Arc::new(AppState {
        get_booking: GetBookingUseCase::new(pool.clone(), booking_repository),
        db_pool: pool,
    });

    let app = build_router(state)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8083);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind listener");

    tracing::info!(port, "booking-service listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl_c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, draining in-flight requests");
}
