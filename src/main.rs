use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

mod error;
mod exchange;
mod middleware;
mod sequencer;
mod types;
use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
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

use crate::exchange::runtime::recover_runtime_with_stream;

const EXCHANGE_COMMAND_QUEUE_SIZE: usize = 10_000;
const DEFAULT_EVENT_LOG_PATH: &str = "exchange-events.log";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--event-probe") {
        if let Err(error) = exchange::event_probe::run(&args[1..]) {
            eprintln!("event probe: {error}");
            std::process::exit(1);
        }
        return;
    }
    if args.first().is_some_and(|arg| arg == "--market-data") {
        if let Err(error) = exchange::market_data::run(&args[1..]).await {
            eprintln!("market data: {error}");
            std::process::exit(1);
        }
        return;
    }
    if !args.is_empty() {
        eprintln!(
            "usage: stock [--event-probe JOURNAL STREAM [CHECKPOINT_JSON] [--once] | --market-data JOURNAL STREAM STATE_FILE [LISTEN_ADDR]]"
        );
        std::process::exit(1);
    }
    dotenv().ok();
    let db = db::connect_db().await;
    let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);

    let event_log_path =
        std::env::var("EVENT_LOG_PATH").unwrap_or_else(|_| DEFAULT_EVENT_LOG_PATH.to_string());
    let event_stream_path =
        std::env::var("EVENT_STREAM_PATH").unwrap_or_else(|_| format!("{event_log_path}.mmap"));

    // Recovery happens before the listener binds, and on the main thread. History that cannot be
    // trusted must stop the process, not kill a worker thread and leave a server answering
    // requests it can never fulfil.
    let runtime = recover_runtime_with_stream(rx, &event_log_path, &event_stream_path)
        .unwrap_or_else(|err| {
            eprintln!("refusing to start: {}", err);
            eprintln!("event log: {}", event_log_path);
            eprintln!("event stream: {}", event_stream_path);
            eprintln!(
                "Resolve the reported error before restarting; preserve the durable history."
            );
            std::process::exit(1);
        });

    println!(
        "Event log {} recovered with {} events",
        event_log_path,
        runtime.event_log().len()
    );

    let exchange_available = Arc::new(AtomicBool::new(true));
    let worker_availability = Arc::clone(&exchange_available);
    thread::spawn(move || {
        runtime.run();
        worker_availability.store(false, Ordering::Release);
    });

    let state = AppState {
        db,
        tx,
        exchange_available,
    };

    let app = Router::new()
        .route("/health", get(health))
        .nest("/users", routes::user_routes::user_routes())
        .nest("/exchange", routes::exchange_routes::exchange_routes()) // New routes
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 4000));

    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("Server is listening on port 4000");
    axum::serve(listener, app).await.unwrap();
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    if state.exchange_available.load(Ordering::Acquire) {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "exchange unavailable")
    }
}
