use crate::{controllers::exchange_controller, state::AppState};
use axum::{
    Router,
    routing::{get, post},
};

pub fn exchange_routes() -> Router<AppState> {
    let place_order = post(exchange_controller::place_order);
    // The order route alone is timed: its middleware stamps each request for the handler.
    #[cfg(feature = "stage-timing")]
    let place_order = place_order.layer(axum::middleware::from_fn(
        crate::exchange::stage_timing::http_layer,
    ));
    Router::new()
        .route("/deposit", post(exchange_controller::deposit))
        .route("/orders", place_order)
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
