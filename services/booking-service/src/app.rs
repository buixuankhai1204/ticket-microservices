use std::sync::Arc;

use rdkafka::error::KafkaError;

use crate::adapter::http::AppState;
use crate::adapter::messaging::kafka::{CancelBookingHandler, ConfirmBookingHandler, SagaConsumer};
use crate::adapter::repository::postgres::PostgresBookingRepository;
use crate::platform::db::DbPool;
use crate::platform::port::BookingRepository;
use crate::usecase::{
    CancelBookingUseCase, ConfirmBookingUseCase, CreateBookingUseCase, GetBookingUseCase,
    ListBookingsUseCase, ReapPendingBookingsUseCase, ReportBookingHealthUseCase,
};

pub struct AppConfig {
    pub jwt_secret: String,
    pub jwt_issuer: String,
    pub kafka_brokers: String,
    pub seat_reservation_topic: String,
    pub consumer_group_suffix: String,
    pub consumer_max_attempts: u32,
    pub pending_timeout_secs: i64,
    pub reaper_interval_secs: u64,
}

pub struct App {
    pub state: Arc<AppState>,
    pub reaper: ReapPendingBookingsUseCase,
    pub health_reporter: ReportBookingHealthUseCase,
    confirm_booking: Arc<ConfirmBookingUseCase>,
    cancel_booking: Arc<CancelBookingUseCase>,
    config: AppConfig,
}

pub async fn migrate(pool: &DbPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

impl App {
    pub fn new(pool: DbPool, config: AppConfig) -> Self {
        let booking_repository: Arc<dyn BookingRepository> =
            Arc::new(PostgresBookingRepository::new());

        let confirm_booking = Arc::new(ConfirmBookingUseCase::new(
            pool.clone(),
            Arc::clone(&booking_repository),
        ));
        let cancel_booking = Arc::new(CancelBookingUseCase::new(
            pool.clone(),
            Arc::clone(&booking_repository),
        ));
        let reaper = ReapPendingBookingsUseCase::new(
            pool.clone(),
            Arc::clone(&booking_repository),
            config.pending_timeout_secs,
            100,
        );
        let health_reporter = ReportBookingHealthUseCase::new(
            pool.clone(),
            Arc::clone(&booking_repository),
            config.pending_timeout_secs + 2 * config.reaper_interval_secs as i64,
        );

        let state = Arc::new(AppState {
            get_booking: GetBookingUseCase::new(pool.clone(), Arc::clone(&booking_repository)),
            list_bookings: ListBookingsUseCase::new(pool.clone(), Arc::clone(&booking_repository)),
            create_booking: CreateBookingUseCase::new(
                pool.clone(),
                Arc::clone(&booking_repository),
            ),
            db_pool: pool,
            jwt_secret: config.jwt_secret.clone(),
            jwt_issuer: config.jwt_issuer.clone(),
        });

        Self {
            state,
            reaper,
            health_reporter,
            confirm_booking,
            cancel_booking,
            config,
        }
    }

    pub fn confirm_consumer(&self) -> Result<SagaConsumer<ConfirmBookingHandler>, KafkaError> {
        SagaConsumer::new(
            &self.config.kafka_brokers,
            &self.config.seat_reservation_topic,
            self.config.consumer_max_attempts,
            ConfirmBookingHandler::new(
                Arc::clone(&self.confirm_booking),
                &self.config.consumer_group_suffix,
            ),
        )
    }

    pub fn cancel_consumer(&self) -> Result<SagaConsumer<CancelBookingHandler>, KafkaError> {
        SagaConsumer::new(
            &self.config.kafka_brokers,
            &self.config.seat_reservation_topic,
            self.config.consumer_max_attempts,
            CancelBookingHandler::new(
                Arc::clone(&self.cancel_booking),
                &self.config.consumer_group_suffix,
            ),
        )
    }
}
