use std::time::Instant;

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const QUIET_PATHS: &[&str] = &["/healthz", "/readyz", "/metrics"];
const MAX_REQUEST_ID_LEN: usize = 128;

fn is_valid_request_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= MAX_REQUEST_ID_LEN
        && candidate
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn request_id_from(req: &Request) -> String {
    req.headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_valid_request_id(value))
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub async fn log_requests(req: Request, next: Next) -> Response {
    let request_id = request_id_from(&req);
    let method = req.method().to_string();
    let path = req.uri().path().to_owned();
    let start = Instant::now();

    let span = tracing::info_span!("request", request_id = %request_id);
    let mut response = next.run(req).instrument(span).await;

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }

    if QUIET_PATHS.contains(&path.as_str()) {
        return response;
    }

    let status = response.status().as_u16();
    let duration_ms = start.elapsed().as_millis() as u64;
    if status >= 500 {
        tracing::error!(%request_id, %method, %path, status, duration_ms, "request failed");
    } else {
        tracing::info!(%request_id, %method, %path, status, duration_ms, "request completed");
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_short_header_safe_ids_are_reused() {
        assert!(is_valid_request_id("8e217c49-7d08-4400-bdc7-6ac4f0b14377"));
        assert!(is_valid_request_id("verify.obs_001"));
        assert!(is_valid_request_id(&"a".repeat(MAX_REQUEST_ID_LEN)));

        assert!(!is_valid_request_id(""));
        assert!(!is_valid_request_id(&"a".repeat(MAX_REQUEST_ID_LEN + 1)));
        assert!(!is_valid_request_id("two words"));
        assert!(!is_valid_request_id("quote\"or\nnewline"));
    }
}
