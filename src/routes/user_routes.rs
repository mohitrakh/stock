use axum::{
    Router,
    routing::{get, post},
};

use crate::{controllers::user_controller, state::AppState};

pub fn user_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(user_controller::get_users))
        .route("/register", post(user_controller::register))
        .route("/login", post(user_controller::login))
}
