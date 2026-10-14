use std::sync::OnceLock;

use crate::models::user::Claims;
use axum::{
    extract::FromRequestParts,
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
};
use jsonwebtoken::{DecodingKey, Validation, decode};

pub struct AuthUser {
    pub user_id: String,
}

/// The key that signs and checks login tokens: `JWT_SECRET`, unset when missing or empty. The
/// exchange and the warm replica refuse to start without it, so no request can ever be checked
/// against a guessable fallback key. The environment is read once, by the first call (the startup
/// check), and never again: it cannot change while the process runs.
pub fn jwt_secret() -> Option<&'static str> {
    static SECRET: OnceLock<Option<String>> = OnceLock::new();
    SECRET
        .get_or_init(|| {
            std::env::var("JWT_SECRET")
                .ok()
                .filter(|secret| !secret.is_empty())
        })
        .as_deref()
}

/// The checking key, built once from `jwt_secret`.
fn decoding_key() -> Option<&'static DecodingKey> {
    static KEY: OnceLock<Option<DecodingKey>> = OnceLock::new();
    KEY.get_or_init(|| jwt_secret().map(|secret| DecodingKey::from_secret(secret.as_bytes())))
        .as_ref()
}

impl<S> FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // 1. Get the Authorization header
        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or((StatusCode::UNAUTHORIZED, "Missing Authorization Header"))?;

        if !auth_header.starts_with("Bearer ") {
            return Err((StatusCode::UNAUTHORIZED, "Invalid token format"));
        }

        let token = &auth_header[7..];

        // 2. Decode the JWT token
        let key = decoding_key().ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "token signing key is not configured",
        ))?;
        let token_data = decode::<Claims>(token, key, &Validation::default())
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid or expired token"))?;

        // 3. Return the user ID
        Ok(AuthUser {
            user_id: token_data.claims.sub,
        })
    }
}
