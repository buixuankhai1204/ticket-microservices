use std::env;
use std::sync::Arc;

use axum::middleware::from_fn;
use axum::routing::get;
use chrono::Duration;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use user_service::adapter::email::{HttpEmailGateway, StubEmailGateway};
use user_service::adapter::http::{build_router, metrics, ApiDoc};
use user_service::adapter::payment::{HttpPaymentGateway, StubOutcome, StubPaymentGateway};
use user_service::app::{self, run_renewal_batch, App, AppConfig, Gateways};
use user_service::domain::{EmailGateway, PaymentGateway, RenewalPolicy};
use user_service::platform::{self, db};
use user_service::usecase::{
    EnqueueDueRenewalsUseCase, EnqueueOutcome, ProcessRenewalAttemptUseCase,
    SendDunningEmailUseCase,
};

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn provider_base_url(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
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

fn spawn_renewal_job_a(enqueue: Arc<EnqueueDueRenewalsUseCase>) {
    let interval_secs: u64 = env_parse("RENEWAL_ENQUEUE_INTERVAL_SECS", 86400);

    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
        loop {
            ticker.tick().await;

            match enqueue.execute().await {
                Ok(EnqueueOutcome::LockNotHeld) => {}
                Ok(EnqueueOutcome::Enqueued(0)) => {}
                Ok(EnqueueOutcome::Enqueued(n)) => {
                    tracing::info!(enqueued = n, "renewal Job A enqueued due renewals")
                }
                Err(e) => tracing::error!(error = %e, "renewal Job A iteration failed"),
            }
        }
    });
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
            run_renewal_batch(
                &process,
                &dunning,
                batch,
                Duration::seconds(charging_timeout_secs),
            )
            .await;
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

    app::migrate(&pool)
        .await
        .expect("failed to run database migrations");

    let payment_gateway: Arc<dyn PaymentGateway> = match provider_base_url(
        "PAYMENT_PROVIDER_BASE_URL",
    ) {
        Some(base_url) => {
            let timeout_secs: u64 = env_parse("PAYMENT_TIMEOUT_SECS", 30);
            let charging_timeout_secs: u64 = env_parse("RENEWAL_CHARGING_TIMEOUT_SECS", 900);
            assert!(
                timeout_secs < charging_timeout_secs,
                "PAYMENT_TIMEOUT_SECS must be shorter than RENEWAL_CHARGING_TIMEOUT_SECS, otherwise the reaper can reset a row whose charge is still in flight"
            );
            let api_key = env::var("PAYMENT_PROVIDER_API_KEY").expect(
                "PAYMENT_PROVIDER_API_KEY must be set when PAYMENT_PROVIDER_BASE_URL is set",
            );
            Arc::new(
                HttpPaymentGateway::new(
                    &base_url,
                    api_key,
                    std::time::Duration::from_secs(timeout_secs),
                )
                .expect("failed to build the payment provider client"),
            )
        }
        None => Arc::new(StubPaymentGateway::new(StubOutcome::from_env(
            "PAYMENT_STUB_OUTCOME",
        ))),
    };
    let email_gateway: Arc<dyn EmailGateway> = match provider_base_url("EMAIL_PROVIDER_BASE_URL") {
        Some(base_url) => {
            let timeout_secs: u64 = env_parse("EMAIL_TIMEOUT_SECS", 10);
            let api_key = env::var("EMAIL_PROVIDER_API_KEY")
                .expect("EMAIL_PROVIDER_API_KEY must be set when EMAIL_PROVIDER_BASE_URL is set");
            Arc::new(
                HttpEmailGateway::new(
                    &base_url,
                    api_key,
                    std::time::Duration::from_secs(timeout_secs),
                )
                .expect("failed to build the email provider client"),
            )
        }
        None => Arc::new(StubEmailGateway::from_env("EMAIL_STUB_FAIL")),
    };

    let application = App::new(
        pool.clone(),
        AppConfig {
            jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET must be set"),
            jwt_issuer: env::var("JWT_ISSUER").unwrap_or_else(|_| "user-service".to_string()),
            renewal_policy: renewal_policy_from_env(),
        },
        Gateways {
            payment: payment_gateway,
            email: email_gateway,
        },
    );

    spawn_renewal_job_a(application.enqueue_due_renewals.clone());
    spawn_renewal_job_b(
        application.process_renewal.clone(),
        application.send_dunning_email.clone(),
    );

    let metrics_pool = pool.clone();
    let state = application.state.clone();

    let metrics_handle = metrics::install_recorder();

    let app = build_router(state)
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
