use std::time::Duration;

use metrics::gauge;
use tokio_util::sync::CancellationToken;

use crate::usecase::ReportBookingHealthUseCase;

pub async fn run_booking_health_reporter(
    use_case: ReportBookingHealthUseCase,
    shutdown: CancellationToken,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => match use_case.execute().await {
                Ok(health) => {
                    gauge!("bookings_oversold_seats").set(health.oversold_seats as f64);
                    gauge!("bookings_stuck_pending").set(health.stuck_pending as f64);
                }
                Err(e) => tracing::error!(err = %e, "booking health check failed"),
            },
        }
    }
}
