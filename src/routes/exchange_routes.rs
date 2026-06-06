use crate::{controllers::exchange_controller, state::AppState};
use axum::{Router, routing::post};

pub fn exchange_routes() -> Router<AppState> {
    Router::new()
        .route("/deposit", post(exchange_controller::deposit))
        .route("/orders", post(exchange_controller::place_order))
        .route("/orders/cancel", post(exchange_controller::cancel_order))
}
