use crate::{controllers::exchange_controller, state::AppState};
use axum::{
    Router,
    routing::{get, post},
};

pub fn exchange_routes() -> Router<AppState> {
    Router::new()
        .route("/deposit", post(exchange_controller::deposit))
        .route("/orders", post(exchange_controller::place_order))
        .route("/orders/cancel", post(exchange_controller::cancel_order))
        .route("/shares/deposit", post(exchange_controller::deposit_shares))
        .route("/balance", get(exchange_controller::get_balance))
        .route("/positions", get(exchange_controller::get_positions))
        .route("/executions", get(exchange_controller::get_executions))
        .route(
            "/risk/limits",
            post(exchange_controller::set_risk_limit).get(exchange_controller::get_risk_limit),
        )
        .route("/orders/{order_id}", get(exchange_controller::get_order))
        // Market data is the one public read: it is aggregate L2 depth and carries no user
        // identity, matching the target design's split between private trading and public data.
        .route(
            "/orderbook/{symbol}",
            get(exchange_controller::get_order_book),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_build() {
        // Axum validates path syntax when the route is added, so a malformed capture (the 0.7
        // `:param` spelling, a duplicated path) panics here rather than at the first request.
        let _ = exchange_routes();
    }
}
