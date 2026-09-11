mod adapter;
mod domain;
mod platform;
mod usecase;

use std::env;
use std::sync::Arc;

use chrono::Duration;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use adapter::email::StubEmailGateway;
use adapter::http::{build_router, ApiDoc, AppState};
use adapter::payment::{StubOutcome, StubPaymentGateway};
use adapter::repository::postgres::PostgresUserRepository;
use adapter::repository::renewal_attempt_postgres::PostgresRenewalAttemptRepository;
use adapter::repository::subscription_postgres::PostgresSubscriptionRepository;
use adapter::security::{Argon2PasswordHasher, JwtTokenIssuer};
use domain::{EmailGateway, PasswordHasher, PaymentGateway, RenewalPolicy, TokenIssuer};
use platform::db;
use platform::port::{RenewalAttemptRepository, SubscriptionRepository, UserRepository};
use usecase::{
    CreateSubscriptionUseCase, GetSubscriptionUseCase, GetUserProfileUseCase,
    ListSubscriptionsUseCase, ListUsersUseCase, LoginUserUseCase, ProcessOutcome,
    ProcessRenewalAttemptUseCase, RegisterUserUseCase, RetryRenewalNowUseCase,
    SendDunningEmailUseCase,
};

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn renewal_policy_from_env() -> RenewalPolicy {
    let d = RenewalPolicy::default();
    RenewalPolicy {
        max_transient_attempts: env_parse(
            "RENEWAL_MAX_TRANSIENT_ATTEMPTS",
            d.max_transient_attempts,
        ),
        backoff_base: Duration::seconds(env_parse(
            "RENEWAL_BACKOFF_BASE_SECS",
            d.backoff_base.num_seconds(),
        )),
        backoff_cap: Duration::seconds(env_parse(
            "RENEWAL_BACKOFF_CAP_SECS",
            d.backoff_cap.num_seconds(),
        )),
        dunning_schedule_days: env::var("RENEWAL_DUNNING_SCHEDULE_DAYS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|x| x.trim().parse::<i64>().ok())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(d.dunning_schedule_days),
    }
}

/// Renewal Job B (docs/sagas/renewal-subscriptions.md §2): an in-process ticker
/// that, each tick, reaps stale `charging` rows then drains up to
/// `RENEWAL_BATCH_SIZE` due attempts. No advisory lock — `FOR UPDATE SKIP
/// LOCKED` in the claim makes concurrent replicas safe. Not part of graceful
/// shutdown (matches the seat-reservation reaper pattern).
fn spawn_renewal_job_b(
    process: Arc<ProcessRenewalAttemptUseCase>,
    dunning: Arc<SendDunningEmailUseCase>,
) {
    let interval_secs: u64 = env_parse("RENEWAL_PROCESS_INTERVAL_SECS", 600);
    let batch: usize = env_parse("RENEWAL_BATCH_SIZE", 100);
    let charging_timeout_secs: i64 = env_parse("RENEWAL_CHARGING_TIMEOUT_SECS", 900);

    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
        loop {
            ticker.tick().await;

            match process
                .reap_stale_charging(Duration::seconds(charging_timeout_secs))
                .await
            {
                Ok(0) => {}
                Ok(n) => tracing::warn!(reaped = n, "reset stale 'charging' renewal attempts"),
                Err(e) => tracing::error!(error = %e, "renewal charging reaper failed"),
            }

            for _ in 0..batch {
                match process.execute().await {
                    Ok(ProcessOutcome::NothingDue) => break,
                    Ok(ProcessOutcome::Skipped) => {
                        tracing::debug!("renewal attempt skipped (row no longer charging)")
                    }
                    Ok(ProcessOutcome::Dunning { email }) => {
                        let subscription_id = email.subscription_id;
                        if let Err(e) = dunning.execute(email).await {
                            tracing::error!(error = %e, "dunning email use case failed");
                        }
                        tracing::info!(%subscription_id, "renewal dunning advanced");
                    }
                    Ok(ProcessOutcome::Renewed { subscription_id }) => {
                        tracing::info!(%subscription_id, "subscription renewed")
                    }
                    Ok(ProcessOutcome::RetryScheduled { subscription_id }) => {
                        tracing::info!(%subscription_id, "renewal retry scheduled")
                    }
                    Ok(ProcessOutcome::GaveUp { subscription_id }) => {
                        tracing::warn!(%subscription_id, "renewal gave up — provider outage")
                    }
                    Ok(ProcessOutcome::Canceled { subscription_id }) => {
                        tracing::warn!(%subscription_id, "subscription canceled — dunning exhausted")
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "renewal Job B iteration failed");
                        break;
                    }
                }
            }
        }
    });
}

#[tokio::main]
async fn main() {
    platform::logging::init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let max_connections: u32 = env_parse("DB_MAX_CONNECTIONS", 10);

    let pool = db::build_pool(&database_url, max_connections)
        .await
        .expect("failed to connect to postgres");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run database migrations");

    let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    let jwt_issuer_key = env::var("JWT_ISSUER").unwrap_or_else(|_| "user-service".to_string());

    let user_repository: Arc<dyn UserRepository> = Arc::new(PostgresUserRepository::new());
    let subscription_repository: Arc<dyn SubscriptionRepository> =
        Arc::new(PostgresSubscriptionRepository::new());
    let renewal_attempt_repository: Arc<dyn RenewalAttemptRepository> =
        Arc::new(PostgresRenewalAttemptRepository::new());
    let password_hasher: Arc<dyn PasswordHasher> = Arc::new(Argon2PasswordHasher::new());
    let token_issuer: Arc<dyn TokenIssuer> = Arc::new(JwtTokenIssuer::new(
        &jwt_secret,
        Duration::hours(1),
        jwt_issuer_key.clone(),
    ));

    // Outbound gateways for renewal Job B. Stubs — no real provider is wired
    // (docs/sagas/renewal-subscriptions.md §9). `PAYMENT_STUB_OUTCOME` and
    // `EMAIL_STUB_FAIL` let a test drive the failure paths.
    let payment_gateway: Arc<dyn PaymentGateway> = Arc::new(StubPaymentGateway::new(
        StubOutcome::from_env("PAYMENT_STUB_OUTCOME"),
    ));
    let email_gateway: Arc<dyn EmailGateway> =
        Arc::new(StubEmailGateway::from_env("EMAIL_STUB_FAIL"));

    // JWT verification for the service's own JWT-protected routes (Kong verifies
    // at the edge too; this is defence in depth and how a handler reads `sub`).
    let jwt_decoding_key = jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes());
    let mut jwt_validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    jwt_validation.set_issuer(&[jwt_issuer_key]);
    jwt_validation.validate_aud = false;

    spawn_renewal_job_b(
        Arc::new(ProcessRenewalAttemptUseCase::new(
            pool.clone(),
            renewal_attempt_repository.clone(),
            subscription_repository.clone(),
            payment_gateway,
            renewal_policy_from_env(),
        )),
        Arc::new(SendDunningEmailUseCase::new(
            pool.clone(),
            renewal_attempt_repository.clone(),
            email_gateway,
        )),
    );

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
        create_subscription: CreateSubscriptionUseCase::new(
            pool.clone(),
            subscription_repository.clone(),
        ),
        get_subscription: GetSubscriptionUseCase::new(
            pool.clone(),
            subscription_repository.clone(),
        ),
        list_subscriptions: ListSubscriptionsUseCase::new(
            pool.clone(),
            subscription_repository.clone(),
        ),
        retry_renewal_now: RetryRenewalNowUseCase::new(
            pool.clone(),
            subscription_repository,
            renewal_attempt_repository,
        ),
        db_pool: pool,
        jwt_decoding_key,
        jwt_validation,
    });

    let app = build_router(state)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    let port: u16 = env_parse("PORT", 8081);
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
