use crate::{
    error::app_error::AppError,
    models::user::{Claims, LoginRequest, RegisterRequest, User},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, response::Json};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{Duration, Utc};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::Serialize;
use sqlx::PgPool;
#[derive(Serialize)]
pub struct UserResponse {
    pub id: u32,
    pub name: String,
}

pub async fn get_users() -> Json<Vec<UserResponse>> {
    Json(vec![
        UserResponse {
            id: 1,
            name: "Mohit".to_string(),
        },
        UserResponse {
            id: 2,
            name: "John".to_string(),
        },
    ])
}

pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<(StatusCode, String), AppError> {
    if payload.username.trim().is_empty() {
        return Err(AppError::Validation("username is required".to_string()));
    }

    if payload.email.trim().is_empty() {
        return Err(AppError::Validation("email is required".to_string()));
    }

    if payload.password.len() < 8 {
        return Err(AppError::Validation(
            "password must be at least 8 characters".to_string(),
        ));
    }

    let exists: Option<(i32,)> = sqlx::query_as("SELECT id FROM users WHERE email = $1")
        .bind(&payload.email)
        .fetch_optional(&state.db)
        .await?;

    if exists.is_some() {
        return Err(AppError::Validation("user already exists".to_string()));
    }

    let password_hash =
        hash(&payload.password, DEFAULT_COST).map_err(|e| AppError::Validation(e.to_string()))?;

    sqlx::query(
        r#"
        INSERT INTO users (username, email, password_hash)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(&payload.username)
    .bind(&payload.email)
    .bind(&password_hash)
    .execute(&state.db)
    .await?;

    Ok((StatusCode::CREATED, "User created successfully".to_string()))
}

pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<(StatusCode, String), AppError> {
    if payload.email.is_empty() {
        return Err(AppError::Validation("email is required".to_string()));
    }

    if payload.password.len() < 8 {
        return Err(AppError::Validation(
            "password must be at least 8 characters".to_string(),
        ));
    }

    let user: Option<User> = sqlx::query_as("SELECT * FROM users WHERE email = $1")
        .bind(&payload.email)
        .fetch_optional(&state.db)
        .await?;

    match user {
        Some(u) => {
            let password_match = verify(&payload.password, &u.password_hash)
                .map_err(|e| AppError::Validation(e.to_string()))?;
            if !password_match {
                return Err(AppError::Validation("invalid password".to_string()));
            }

            let claims = Claims {
                sub: u.id.to_string(),
                exp: (Utc::now() + Duration::hours(24)).timestamp() as usize,
            };
            let token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(
                    std::env::var("JWT_SECRET")
                        .expect("JWT_SECRET not set")
                        .as_bytes(),
                ),
            )
            .map_err(|e| AppError::Validation(e.to_string()))?;
            Ok((StatusCode::OK, token))
        }
        None => Err(AppError::Validation("user not found".to_string())),
    }
}
