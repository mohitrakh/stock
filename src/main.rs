use std::net::SocketAddr;

mod error;
mod types;
mod sequencer;

use axum::{Router, routing::get};
use dotenvy::dotenv;
use tokio::net::TcpListener;

mod routes {
    pub mod user_routes;
}

mod controllers {
    pub mod user_controller;
}

mod models {
    pub mod user;
}
mod db;
mod state;
use state::AppState;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db = db::connect_db().await;

    let state = AppState { db };

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/users", routes::user_routes::user_routes())
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 4000));

    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("Server is listening on port 4000");
    axum::serve(listener, app).await.unwrap();
}
