use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::Json;
use jsonwebtoken::decode;
use serde::Deserialize;
use uuid::Uuid;

use super::dto::ErrorResponse;
use super::handlers::AppState;

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

/// The caller identified by a verified `Authorization: Bearer <jwt>`. Kong also
/// verifies the token at the edge (see `kong/kong.yml`); the service re-verifies
/// signature + `exp` + `iss` as defence in depth and to read `sub`.
pub struct AuthUser {
    pub user_id: Uuid,
}

#[axum::async_trait]
impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let reject = |message: &str| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: message.to_string(),
                }),
            )
        };

        let token = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| reject("missing bearer token"))?;

        let data = decode::<Claims>(token, &state.jwt_decoding_key, &state.jwt_validation)
            .map_err(|_| reject("invalid or expired token"))?;

        let user_id = Uuid::parse_str(&data.claims.sub)
            .map_err(|_| reject("token subject is not a valid user id"))?;

        Ok(AuthUser { user_id })
    }
}
