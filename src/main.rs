use std::{net::SocketAddr, thread};

mod error;
mod exchange;
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

use crate::exchange::runtime::recover_runtime;

const EXCHANGE_COMMAND_QUEUE_SIZE: usize = 10_000;
const DEFAULT_EVENT_LOG_PATH: &str = "exchange-events.log";

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db = db::connect_db().await;
    let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);

    let event_log_path =
        std::env::var("EVENT_LOG_PATH").unwrap_or_else(|_| DEFAULT_EVENT_LOG_PATH.to_string());

    // Recovery happens before the listener binds, and on the main thread. History that cannot be
    // trusted must stop the process, not kill a worker thread and leave a server answering
    // requests it can never fulfil.
    let runtime = recover_runtime(rx, &event_log_path).unwrap_or_else(|err| {
        eprintln!("refusing to start: {}", err);
        eprintln!("event log: {}", event_log_path);
        eprintln!(
            "The exchange will not start on history it cannot replay. Move the file aside to \
             start a new exchange, understanding that its history is then abandoned."
        );
        std::process::exit(1);
    });

    println!(
        "Event log {} recovered with {} events",
        event_log_path,
        runtime.event_log().len()
    );

    thread::spawn(move || runtime.run());

    let state = AppState { db, tx };

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
