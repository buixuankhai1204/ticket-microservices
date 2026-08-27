mod adapter;
mod domain;
mod platform;
mod usecase;

use std::env;
use std::sync::Arc;

use chrono::Duration;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio_util::sync::CancellationToken;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use adapter::http::{build_router, ApiDoc, AppState};
use adapter::messaging::kafka::OutboxRelay;
use adapter::repository::postgres::PostgresUserRepository;
use adapter::security::{Argon2PasswordHasher, JwtTokenIssuer};
use domain::{PasswordHasher, TokenIssuer, UserRepository};
use platform::db;
use usecase::{GetUserProfileUseCase, LoginUserUseCase, RegisterUserUseCase};

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

    // Choreography saga bus: the outbox relay drains `outbox_events` (written in
    // the same tx as each user insert) to Kafka. Runs for the whole process
    // lifetime and is stopped by the same shutdown signal as the HTTP server.
    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let user_created_topic =
        env::var("KAFKA_USER_CREATED_TOPIC").unwrap_or_else(|_| "user.created".to_string());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("failed to create kafka producer");
    let relay_cancel = CancellationToken::new();
    let relay_handle = tokio::spawn(
        OutboxRelay::new(pool.clone(), producer, user_created_topic).run(relay_cancel.clone()),
    );

    let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    let jwt_issuer_key = env::var("JWT_ISSUER").unwrap_or_else(|_| "user-service".to_string());

    let user_repository: Arc<dyn UserRepository> =
        Arc::new(PostgresUserRepository::new(pool.clone()));
    let password_hasher: Arc<dyn PasswordHasher> = Arc::new(Argon2PasswordHasher::new());
    let token_issuer: Arc<dyn TokenIssuer> = Arc::new(JwtTokenIssuer::new(
        &jwt_secret,
        Duration::hours(1),
        jwt_issuer_key,
    ));

    let state = Arc::new(AppState {
        register_user: RegisterUserUseCase::new(user_repository.clone(), password_hasher.clone()),
        login_user: LoginUserUseCase::new(user_repository.clone(), password_hasher, token_issuer),
        get_user_profile: GetUserProfileUseCase::new(user_repository),
        db_pool: pool,
    });

    let app = build_router(state)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8081);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind listener");

    tracing::info!(port, "user-service listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Tear the relay down on the same signal, then wait for its
            // in-flight batch to finish.
            relay_cancel.cancel();
        })
        .await
        .expect("server error");

    if let Err(err) = relay_handle.await {
        tracing::error!(error = %err, "outbox relay task panicked");
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
