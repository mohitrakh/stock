use axum::{Router, routing::post};

use crate::{controllers::user_controller, state::AppState};

pub fn user_routes() -> Router<AppState> {
    Router::new()
        .route("/register", post(user_controller::register))
        .route("/login", post(user_controller::login))
}
