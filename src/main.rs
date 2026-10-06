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

use crate::exchange::runtime::{
    DEFAULT_SNAPSHOT_INTERVAL, ExchangeRuntime, promote_replica_with_stream_and_snapshot,
    recover_runtime_with_stream_and_snapshot,
};

const EXCHANGE_COMMAND_QUEUE_SIZE: usize = 10_000;
const DEFAULT_EVENT_LOG_PATH: &str = "exchange-events.log";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    dotenv().ok();
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
    if args.first().is_some_and(|arg| arg == "--reporter") {
        if let Err(error) = exchange::reporter::run(&args[1..]).await {
            eprintln!("reporter: {error}");
            std::process::exit(1);
        }
        return;
    }
    if args.first().is_some_and(|arg| arg == "--bench") {
        if let Err(error) = exchange::bench::run(&args[1..]).await {
            eprintln!("bench: {error}");
            std::process::exit(1);
        }
        return;
    }
    if args.first().is_some_and(|arg| arg == "--warm-replica") {
        // A promoted warm replica checks login tokens: refuse now, not after it has fenced the
        // old primary.
        require_jwt_secret();
        let promotion = exchange::warm_replica::run(&args[1..], snapshot_interval_or_exit())
            .await
            .unwrap_or_else(|error| {
                eprintln!("warm replica: {error}");
                std::process::exit(1);
            });
        let exchange::warm_replica::WarmPromotion {
            store,
            recovered,
            journal_path,
            stream_path,
            snapshot_path,
        } = promotion;
        let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);
        let runtime = promote_replica_with_stream_and_snapshot(
            rx,
            store,
            recovered,
            &journal_path,
            &stream_path,
            &snapshot_path,
        )
        .unwrap_or_else(|error| {
            eprintln!("refusing to promote: {error}");
            eprintln!("event log: {}", journal_path.display());
            eprintln!("event stream: {}", stream_path.display());
            eprintln!("event snapshot: {}", snapshot_path.display());
            eprintln!("Preserve the durable history and resolve the reported error.");
            std::process::exit(1);
        });
        println!(
            "Warm replica promoted through event sequence {}",
            runtime.next_event_sequence().saturating_sub(1)
        );
        serve_primary(runtime, tx).await;
        return;
    }
    if !args.is_empty() {
        eprintln!(
            "usage: stock [--event-probe JOURNAL STREAM [CHECKPOINT_JSON] [--once] | --market-data JOURNAL STREAM STATE_FILE [LISTEN_ADDR] | --reporter JOURNAL STREAM [LISTEN_ADDR] | --warm-replica JOURNAL STREAM SNAPSHOT [LISTEN_ADDR] | --bench EMPTY_DIR [OPTIONS]]"
        );
        std::process::exit(1);
    }
    require_jwt_secret();

    let event_log_path =
        std::env::var("EVENT_LOG_PATH").unwrap_or_else(|_| DEFAULT_EVENT_LOG_PATH.to_string());
    let event_stream_path =
        std::env::var("EVENT_STREAM_PATH").unwrap_or_else(|_| format!("{event_log_path}.mmap"));
    let event_snapshot_path = std::env::var("EVENT_SNAPSHOT_PATH")
        .unwrap_or_else(|_| format!("{event_log_path}.snapshot"));
    let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);

    // Recovery happens before the listener binds, and on the main thread. History that cannot be
    // trusted must stop the process, not kill a worker thread and leave a server answering
    // requests it can never fulfil.
    let runtime = recover_runtime_with_stream_and_snapshot(
        rx,
        &event_log_path,
        &event_stream_path,
        &event_snapshot_path,
    )
    .unwrap_or_else(|err| {
        eprintln!("refusing to start: {}", err);
        eprintln!("event log: {}", event_log_path);
        eprintln!("event stream: {}", event_stream_path);
        eprintln!("event snapshot: {}", event_snapshot_path);
        eprintln!("Resolve the reported error before restarting; preserve the durable history.");
        std::process::exit(1);
    });

    println!(
        "Event log {} recovered through event sequence {}",
        event_log_path,
        runtime.next_event_sequence().saturating_sub(1)
    );
    serve_primary(runtime, tx).await;
}

/// The primary signs and checks login tokens with `JWT_SECRET`. Without it, refuse to start, before
/// the journal is opened, rather than check tokens against a guessable key.
fn require_jwt_secret() {
    if middleware::auth_middleware::jwt_secret().is_none() {
        eprintln!("JWT_SECRET must be set to a non-empty secret: it signs and checks login tokens");
        std::process::exit(1);
    }
}

/// Commands between the snapshots the warm replica writes. The primary no longer snapshots while
/// trading; it writes one snapshot at startup.
fn snapshot_interval_or_exit() -> u64 {
    std::env::var("EVENT_SNAPSHOT_INTERVAL")
        .map(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|&value| value > 0)
                .ok_or(())
        })
        .unwrap_or(Ok(DEFAULT_SNAPSHOT_INTERVAL))
        .unwrap_or_else(|_| {
            eprintln!("EVENT_SNAPSHOT_INTERVAL must be a positive integer");
            std::process::exit(1);
        })
}

async fn serve_primary(
    runtime: ExchangeRuntime,
    tx: tokio::sync::mpsc::Sender<crate::types::types::ExchangeCommand>,
) {
    // The runtime has already recovered and, in a promotion, fenced the old writer. Database
    // availability must not decide whether untrusted durable history is accepted.
    let db = db::connect_db().await;

    let exchange_available = Arc::new(AtomicBool::new(true));
    let worker_availability = Arc::clone(&exchange_available);
    thread::spawn(move || {
        runtime.run();
        worker_availability.store(false, Ordering::Release);
    });

    // The operator opens and closes the market through its own loopback-only port.
    let operator_address = exchange::operator::address().unwrap_or_else(|error| {
        eprintln!("operator port: {error}");
        std::process::exit(1);
    });
    let operator_listener = TcpListener::bind(operator_address)
        .await
        .unwrap_or_else(|error| {
            eprintln!("could not bind the operator port {operator_address}: {error}");
            std::process::exit(1);
        });
    println!("Operator port is listening on {operator_address}");
    let operator = exchange::operator::router(tx.clone());
    tokio::spawn(async move { axum::serve(operator_listener, operator).await });

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
