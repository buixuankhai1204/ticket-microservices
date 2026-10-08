use std::sync::Arc;

use chrono::Duration;

use crate::adapter::http::AppState;
use crate::adapter::repository::postgres::PostgresUserRepository;
use crate::adapter::repository::renewal_attempt_postgres::PostgresRenewalAttemptRepository;
use crate::adapter::repository::subscription_postgres::PostgresSubscriptionRepository;
use crate::adapter::security::{Argon2PasswordHasher, JwtTokenIssuer};
use crate::domain::{EmailGateway, PasswordHasher, PaymentGateway, RenewalPolicy, TokenIssuer};
use crate::platform::db::DbPool;
use crate::platform::port::{
    PgTransactor, RenewalAttemptRepository, SubscriptionRepository, Transactor, UserRepository,
};
use crate::usecase::{
    CreateSubscriptionUseCase, EnqueueDueRenewalsUseCase, GetSubscriptionUseCase,
    GetUserProfileUseCase, ListSubscriptionsUseCase, ListUsersUseCase, LoginUserUseCase,
    ProcessOutcome, ProcessRenewalAttemptUseCase, RegisterUserUseCase, RetryRenewalNowUseCase,
    SendDunningEmailUseCase,
};

pub struct AppConfig {
    pub jwt_secret: String,
    pub jwt_issuer: String,
    pub renewal_policy: RenewalPolicy,
}

pub struct Gateways {
    pub payment: Arc<dyn PaymentGateway>,
    pub email: Arc<dyn EmailGateway>,
}

pub struct App {
    pub state: Arc<AppState>,
    pub enqueue_due_renewals: Arc<EnqueueDueRenewalsUseCase>,
    pub process_renewal: Arc<ProcessRenewalAttemptUseCase>,
    pub send_dunning_email: Arc<SendDunningEmailUseCase>,
}

pub async fn migrate(pool: &DbPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

impl App {
    pub fn new(pool: DbPool, config: AppConfig, gateways: Gateways) -> Self {
        let user_repository: Arc<dyn UserRepository> = Arc::new(PostgresUserRepository::new());
        let subscription_repository: Arc<dyn SubscriptionRepository> =
            Arc::new(PostgresSubscriptionRepository::new());
        let renewal_attempt_repository: Arc<dyn RenewalAttemptRepository> =
            Arc::new(PostgresRenewalAttemptRepository::new());
        let password_hasher: Arc<dyn PasswordHasher> = Arc::new(Argon2PasswordHasher::new());
        let token_issuer: Arc<dyn TokenIssuer> = Arc::new(JwtTokenIssuer::new(
            &config.jwt_secret,
            Duration::hours(1),
            config.jwt_issuer.clone(),
        ));

        let jwt_decoding_key = jsonwebtoken::DecodingKey::from_secret(config.jwt_secret.as_bytes());
        let mut jwt_validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        jwt_validation.set_issuer(&[config.jwt_issuer.as_str()]);
        jwt_validation.validate_aud = false;

        let db: Arc<dyn Transactor> = Arc::new(PgTransactor::new(pool.clone()));

        let enqueue_due_renewals = Arc::new(EnqueueDueRenewalsUseCase::new(
            db.clone(),
            renewal_attempt_repository.clone(),
        ));
        let process_renewal = Arc::new(ProcessRenewalAttemptUseCase::new(
            db.clone(),
            renewal_attempt_repository.clone(),
            subscription_repository.clone(),
            gateways.payment,
            config.renewal_policy,
        ));
        let send_dunning_email = Arc::new(SendDunningEmailUseCase::new(
            db.clone(),
            renewal_attempt_repository.clone(),
            gateways.email,
        ));

        let state = Arc::new(AppState {
            register_user: RegisterUserUseCase::new(
                db.clone(),
                user_repository.clone(),
                password_hasher.clone(),
            ),
            login_user: LoginUserUseCase::new(
                db.clone(),
                user_repository.clone(),
                password_hasher,
                token_issuer,
            ),
            get_user_profile: GetUserProfileUseCase::new(db.clone(), user_repository.clone()),
            list_users: ListUsersUseCase::new(db.clone(), user_repository),
            create_subscription: CreateSubscriptionUseCase::new(
                db.clone(),
                subscription_repository.clone(),
            ),
            get_subscription: GetSubscriptionUseCase::new(
                db.clone(),
                subscription_repository.clone(),
            ),
            list_subscriptions: ListSubscriptionsUseCase::new(
                db.clone(),
                subscription_repository.clone(),
            ),
            retry_renewal_now: RetryRenewalNowUseCase::new(
                db,
                subscription_repository,
                renewal_attempt_repository,
            ),
            db_pool: pool,
            jwt_decoding_key,
            jwt_validation,
        });

        Self {
            state,
            enqueue_due_renewals,
            process_renewal,
            send_dunning_email,
        }
    }
}

pub async fn run_renewal_batch(
    process: &ProcessRenewalAttemptUseCase,
    dunning: &SendDunningEmailUseCase,
    batch: usize,
    charging_timeout: Duration,
) {
    match process.reap_stale_charging(charging_timeout).await {
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
