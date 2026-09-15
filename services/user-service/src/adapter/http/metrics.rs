use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::IntoResponse;
use metrics::{counter, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

const DEFAULT_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

const UNINSTRUMENTED_PATHS: &[&str] = &["/healthz", "/readyz", "/metrics"];

pub fn install_recorder() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets(DEFAULT_BUCKETS)
        .expect("invalid default histogram buckets")
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder")
}

pub async fn track_metrics(req: Request, next: Next) -> impl IntoResponse {
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    let method = req.method().to_string();

    let start = Instant::now();
    let response = next.run(req).await;

    let Some(path) = path else {
        return response;
    };
    if UNINSTRUMENTED_PATHS.contains(&path.as_str()) {
        return response;
    }

    let status = response.status().as_u16().to_string();
    let labels = [("method", method), ("path", path), ("status", status)];

    counter!("http_requests_total", &labels).increment(1);
    histogram!("http_requests_duration_seconds", &labels).record(start.elapsed().as_secs_f64());

    response
}
