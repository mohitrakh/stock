use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

mod error;
mod middleware;
mod sequencer;
mod types;
use axum::{Router, routing::get};
use dotenvy::dotenv;
use tokio::net::TcpListener;

mod routes {
    pub mod exchange_routes;
    pub mod user_routes;
}

mod controllers {
    pub mod exchange_controller;
    pub mod user_controller;
}

mod models {
    pub mod user;
}
mod db;
mod state;
use state::AppState;

use crate::types::order_manager::OrderManager;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db = db::connect_db().await;
    let order_manager = Arc::new(Mutex::new(OrderManager::new()));
    let state = AppState {
        db,
        order_manager: order_manager,
    };

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/users", routes::user_routes::user_routes())
        .nest("/exchange", routes::exchange_routes::exchange_routes()) // New routes
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 4000));

    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("Server is listening on port 4000");
    axum::serve(listener, app).await.unwrap();
}
