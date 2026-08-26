use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use uuid::Uuid;

use crate::domain::{TokenIssuer, UserError};

#[derive(Serialize)]
struct Claims {
    sub: String,
    email: String,
    exp: usize,
}

pub struct JwtTokenIssuer {
    encoding_key: EncodingKey,
    ttl: Duration,
}

impl JwtTokenIssuer {
    pub fn new(secret: &str, ttl: Duration) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            ttl,
        }
    }
}

impl TokenIssuer for JwtTokenIssuer {
    fn issue(&self, user_id: Uuid, email: &str) -> Result<String, UserError> {
        let claims = Claims {
            sub: user_id.to_string(),
            email: email.to_string(),
            exp: (Utc::now() + self.ttl).timestamp() as usize,
        };

        encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| UserError::Token(e.to_string()))
    }
}
