use std::env;
use std::time::Duration;

use axum::middleware::from_fn;
use axum::routing::get;
use tokio_util::sync::CancellationToken;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use booking_service::adapter::health_reporter::run_booking_health_reporter;
use booking_service::adapter::http::{build_router, metrics, ApiDoc};
use booking_service::app::{self, App, AppConfig};
use booking_service::platform::{self, db};

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    platform::logging::init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = db::build_pool(&database_url, env_parse("DB_MAX_CONNECTIONS", 10))
        .await
        .expect("failed to connect to postgres");

    app::migrate(&pool)
        .await
        .expect("failed to run database migrations");

    let config = AppConfig {
        jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET must be set"),
        jwt_issuer: env::var("JWT_ISSUER").unwrap_or_else(|_| "user-service".to_string()),
        kafka_brokers: env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set"),
        seat_reservation_topic: env::var("KAFKA_SEAT_RESERVATION_EVENTS_TOPIC")
            .unwrap_or_else(|_| "seat_reservation.events".to_string()),
        consumer_group_suffix: env::var("KAFKA_GROUP_SUFFIX").unwrap_or_default(),
        consumer_max_attempts: env_parse("KAFKA_CONSUMER_MAX_ATTEMPTS", 5),
        pending_timeout_secs: env_parse("BOOKING_PENDING_TIMEOUT", 120),
        reaper_interval_secs: env_parse("BOOKING_REAPER_INTERVAL", 30),
    };
    let reaper_interval_secs = config.reaper_interval_secs;
    let health_interval_secs: u64 = env_parse("BOOKING_HEALTH_INTERVAL", 60);

    let metrics_pool = pool.clone();
    let application = App::new(pool, config);

    let confirm_consumer = application
        .confirm_consumer()
        .expect("failed to create SeatReserved consumer");
    let cancel_consumer = application
        .cancel_consumer()
        .expect("failed to create SeatReservationFailed consumer");
    let App {
        state,
        reaper,
        health_reporter,
        ..
    } = application;

    let metrics_handle = metrics::install_recorder();

    let shutdown = CancellationToken::new();
    let background_tasks = vec![
        tokio::spawn(confirm_consumer.run(shutdown.clone())),
        tokio::spawn(cancel_consumer.run(shutdown.clone())),
        tokio::spawn(reaper.run(shutdown.clone(), Duration::from_secs(reaper_interval_secs))),
        tokio::spawn(run_booking_health_reporter(
            health_reporter,
            shutdown.clone(),
            Duration::from_secs(health_interval_secs),
        )),
    ];

    let router = build_router(state)
        .route(
            "/metrics",
            get(move || {
                let pool = metrics_pool.clone();
                let handle = metrics_handle.clone();
                async move {
                    metrics::record_pool_stats(&pool);
                    handle.render()
                }
            }),
        )
        .layer(from_fn(metrics::track_metrics))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    let port: u16 = env_parse("PORT", 8083);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind listener");

    tracing::info!(port, "booking-service listening");

    let server_shutdown = shutdown.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            server_shutdown.cancel();
        })
        .await
        .expect("server error");

    for task in background_tasks {
        let _ = task.await;
    }
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
