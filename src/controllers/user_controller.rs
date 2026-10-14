use std::sync::{Arc, LazyLock};

use crate::{
    error::app_error::AppError,
    middleware::auth_middleware::jwt_secret,
    models::user::{Claims, LoginRequest, RegisterRequest, User},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, response::Json};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{Duration, Utc};
use jsonwebtoken::{EncodingKey, Header, encode};
use tokio::sync::Semaphore;

/// The answer to every failed login, whatever the reason: the same status and body for an
/// unknown email and for a wrong password, so the answer reveals nothing about which accounts
/// exist.
const LOGIN_REFUSED: &str = "invalid email or password";

/// bcrypt calls allowed to run at once: half the CPUs this process sees, at least one (2 on the
/// office machine's 4 hardware threads). One cost-12 call is about 229 ms of CPU there
/// (`docs/milestones/24-gateway/performance/02-bcrypt-cost.md`); without a cap, enough
/// simultaneous logins take every core, even off the Tokio worker threads.
static BCRYPT_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism().map_or(1, |cpus| cpus.get());
    Arc::new(Semaphore::new((cpus / 2).max(1)))
});

/// A real bcrypt hash at the cost real accounts use, of a password nobody has. A login for an
/// unknown email verifies against it, so it costs the same bcrypt as a login for a known one and
/// its answer arrives just as late. Computed once; `prepare` computes it before the port opens.
static DUMMY_HASH: LazyLock<String> = LazyLock::new(|| {
    hash("no account has this password", DEFAULT_COST).expect("bcrypt can hash a fixed password")
});

/// Does the one-time password work before customers can connect, so that no login pays for it.
pub async fn prepare() {
    tokio::task::spawn_blocking(|| LazyLock::force(&DUMMY_HASH).len())
        .await
        .expect("the dummy hash is computed");
}

/// Runs a bcrypt call on Tokio's blocking pool, at most `permits` at a time. The Tokio worker
/// threads, which parse and answer every customer request, never run bcrypt themselves. The
/// permit moves into the blocking closure, so it is released when the bcrypt call finishes, even
/// if the request that started it has been dropped (a client that disconnects mid-login cannot
/// free its permit early while its bcrypt is still running).
async fn bcrypt_off_the_workers<T: Send + 'static>(
    permits: Arc<Semaphore>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, AppError> {
    let permit = permits
        .acquire_owned()
        .await
        .map_err(|_| AppError::Unavailable("password checks are unavailable".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let result = work();
        drop(permit);
        result
    })
    .await
    .map_err(|_| AppError::Unavailable("password check failed to run".to_string()))
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

    let password = payload.password.clone();
    let password_hash = bcrypt_off_the_workers(Arc::clone(&BCRYPT_PERMITS), move || {
        hash(password, DEFAULT_COST)
    })
    .await?
    .map_err(|e| AppError::Validation(e.to_string()))?;

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
    // These two checks depend only on the request, never on whether the account exists.
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

    // Exactly one full bcrypt computation on every path: against the account's hash, or against the
    // dummy hash when there is no account. Both answers then take one bcrypt.
    let stored = user
        .as_ref()
        .map_or_else(|| DUMMY_HASH.clone(), |u| u.password_hash.clone());
    let password = payload.password.clone();
    let password_match = bcrypt_off_the_workers(Arc::clone(&BCRYPT_PERMITS), move || {
        verify(&password, &stored).unwrap_or_else(|error| {
            // Only an unreadable stored hash can fail here (the dummy hash never does), and bcrypt
            // rejects one before doing any work. Answered at once, or differently, it would reveal
            // that the account exists: it is logged, costs one full verify like every other path,
            // and is refused like a wrong password.
            eprintln!("login: the stored password hash could not be checked: {error}");
            let _ = verify(&password, DUMMY_HASH.as_str());
            false
        })
    })
    .await?;

    match user {
        Some(u) if password_match => {
            let claims = Claims {
                sub: u.id.to_string(),
                exp: (Utc::now() + Duration::hours(24)).timestamp() as usize,
            };
            let token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(
                    jwt_secret()
                        .expect("JWT_SECRET is checked at startup")
                        .as_bytes(),
                ),
            )
            .map_err(|e| AppError::Validation(e.to_string()))?;
            Ok((StatusCode::OK, token))
        }
        _ => Err(AppError::Unauthorized(LOGIN_REFUSED.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Eight blocking calls of 30 ms each, with a cap of 2, on a runtime with 2 worker threads.
    /// If the calls ran on the worker threads, both workers would be blocked for the whole
    /// 4 × 30 ms and the ticker beside them could not run. Off the workers, it keeps ticking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bcrypt_work_runs_off_the_worker_threads_and_never_beyond_the_cap() {
        let permits = Arc::new(Semaphore::new(2));
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = {
            let ticks = Arc::clone(&ticks);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    ticks.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let calls: Vec<_> = (0..8)
            .map(|_| {
                let (permits, running, most) = (
                    Arc::clone(&permits),
                    Arc::clone(&running),
                    Arc::clone(&most),
                );
                tokio::spawn(async move {
                    bcrypt_off_the_workers(permits, move || {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        most.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(30));
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await
                })
            })
            .collect();
        let ticks_before = ticks.load(Ordering::SeqCst);
        for call in calls {
            assert!(call.await.expect("task").is_ok(), "the call failed to run");
        }
        ticker.abort();
        assert_eq!(
            most.load(Ordering::SeqCst),
            2,
            "the cap of 2 was not reached or was exceeded"
        );
        // 120 ms of blocking work at about 2–3 ms a tick: 40 or more ticks if the workers stayed
        // free. Run on the workers, at most one tick fits between two calls: about 8.
        assert!(
            ticks.load(Ordering::SeqCst) - ticks_before >= 20,
            "the async ticker stalled: the blocking work occupied the worker threads"
        );
    }

    #[test]
    fn the_dummy_hash_is_a_real_hash_at_the_accounts_cost() {
        let parts: bcrypt::HashParts = DUMMY_HASH.parse().expect("a valid bcrypt hash");
        assert_eq!(parts.get_cost(), DEFAULT_COST);
    }
}
