mod adapter;
mod domain;
mod platform;
mod usecase;

use std::env;
use std::sync::Arc;

use chrono::Duration;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use adapter::http::{build_router, ApiDoc, AppState};
use adapter::repository::postgres::PostgresUserRepository;
use adapter::security::{Argon2PasswordHasher, JwtTokenIssuer};
use domain::{PasswordHasher, TokenIssuer};
use platform::db;
use platform::port::UserRepository;
use usecase::{GetUserProfileUseCase, ListUsersUseCase, LoginUserUseCase, RegisterUserUseCase};

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

    // Choreography saga bus: user-service publishes `UserCreated` through the
    // transactional outbox, but there is no relay process to run here any more.
    // The register-user flow writes (and immediately deletes) an `outbox_events`
    // row in the same tx as the user insert; a Debezium PostgreSQL connector on
    // Kafka Connect tails that table's WAL and publishes via the Outbox Event
    // Router SMT (routed to `<aggregate_type>.events`). See `debezium/` and the
    // `kafka-connect` service in docker-compose.

    let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    let jwt_issuer_key = env::var("JWT_ISSUER").unwrap_or_else(|_| "user-service".to_string());

    let user_repository: Arc<dyn UserRepository> = Arc::new(PostgresUserRepository::new());
    let password_hasher: Arc<dyn PasswordHasher> = Arc::new(Argon2PasswordHasher::new());
    let token_issuer: Arc<dyn TokenIssuer> = Arc::new(JwtTokenIssuer::new(
        &jwt_secret,
        Duration::hours(1),
        jwt_issuer_key,
    ));

    let state = Arc::new(AppState {
        register_user: RegisterUserUseCase::new(
            pool.clone(),
            user_repository.clone(),
            password_hasher.clone(),
        ),
        login_user: LoginUserUseCase::new(
            pool.clone(),
            user_repository.clone(),
            password_hasher,
            token_issuer,
        ),
        get_user_profile: GetUserProfileUseCase::new(pool.clone(), user_repository.clone()),
        list_users: ListUsersUseCase::new(pool.clone(), user_repository),
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
