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

use crate::exchange::{
    replication::Replication,
    runtime::{
        DEFAULT_SNAPSHOT_GROWTH, ExchangeRuntime, promote_replica, recover_replicated_runtime,
        recover_runtime_with_stream_and_snapshot,
    },
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
    if args.first().is_some_and(|arg| arg == "--replica") {
        // The second machine's copy of the journal: it has no exchange core, no customer port
        // and no database. It runs until the primary refuses it.
        if let Err(error) = exchange::replica::run(&args[1..]) {
            eprintln!("replica: {error}");
            std::process::exit(1);
        }
        return;
    }
    if args.first().is_some_and(|arg| arg == "--warm-replica") {
        // A promoted warm replica checks login tokens and may replicate: refuse a missing secret
        // or a bad address now, not after it has fenced the old primary.
        require_jwt_secret();
        let replicating = replication_address_or_exit();
        let promotion = exchange::warm_replica::run(&args[1..], snapshot_growth_or_exit())
            .await
            .unwrap_or_else(|error| {
                eprintln!("warm replica: {error}");
                std::process::exit(1);
            });
        let exchange::warm_replica::WarmPromotion {
            store,
            replica,
            suffix,
            journal_path,
            stream_path,
        } = promotion;
        let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);
        let mut runtime = promote_replica(
            rx,
            store,
            replica,
            suffix,
            &stream_path,
            replicating.is_some(),
        )
        .unwrap_or_else(|error| {
            eprintln!("refusing to promote: {error}");
            eprintln!("event log: {}", journal_path.display());
            eprintln!("event stream: {}", stream_path.display());
            eprintln!("Preserve the durable history and resolve the reported error.");
            std::process::exit(1);
        });
        println!(
            "Warm replica promoted through event sequence {}",
            runtime.next_event_sequence().saturating_sub(1)
        );
        let replication = replicate_or_exit(&mut runtime, replicating);
        serve_primary(runtime, tx, replication).await;
        return;
    }
    if !args.is_empty() {
        eprintln!(
            "usage: stock [--event-probe JOURNAL STREAM [CHECKPOINT_JSON] [--once] | --market-data JOURNAL STREAM STATE_FILE [LISTEN_ADDR] | --reporter JOURNAL STREAM [LISTEN_ADDR] | --replica PRIMARY_ADDR JOURNAL STREAM | --warm-replica JOURNAL STREAM SNAPSHOT [LISTEN_ADDR] | --bench EMPTY_DIR [OPTIONS]]"
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
    // requests it can never fulfil. A replicated journal publishes nothing its replica may lack.
    let replicating = replication_address_or_exit();
    let recovered = if replicating.is_some() {
        recover_replicated_runtime(
            rx,
            &event_log_path,
            &event_stream_path,
            &event_snapshot_path,
        )
    } else {
        recover_runtime_with_stream_and_snapshot(
            rx,
            &event_log_path,
            &event_stream_path,
            &event_snapshot_path,
        )
    };
    let mut runtime = recovered.unwrap_or_else(|err| {
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
    let replication = replicate_or_exit(&mut runtime, replicating);
    serve_primary(runtime, tx, replication).await;
}

/// `REPLICATION_LISTEN_ADDR`: where the primary listens for its replica on the other machine.
/// Unset, the journal is not replicated. The link is neither authenticated nor encrypted: use a
/// private network.
fn replication_address_or_exit() -> Option<SocketAddr> {
    let value = std::env::var("REPLICATION_LISTEN_ADDR").ok()?;
    Some(value.parse().unwrap_or_else(|error| {
        eprintln!("REPLICATION_LISTEN_ADDR is not a socket address: {error}");
        std::process::exit(1);
    }))
}

fn replicate_or_exit(
    runtime: &mut ExchangeRuntime,
    address: Option<SocketAddr>,
) -> Option<Arc<Replication>> {
    let address = address?;
    let replication = runtime.replicate(address).unwrap_or_else(|error| {
        eprintln!("could not listen for the replica on {address}: {error}");
        std::process::exit(1);
    });
    println!(
        "Replicating the journal: waiting for the replica on {}",
        replication.address()
    );
    Some(replication)
}

/// The primary signs and checks login tokens with `JWT_SECRET`. Without it, refuse to start, before
/// the journal is opened, rather than check tokens against a guessable key.
fn require_jwt_secret() {
    if middleware::auth_middleware::jwt_secret().is_none() {
        eprintln!("JWT_SECRET must be set to a non-empty secret: it signs and checks login tokens");
        std::process::exit(1);
    }
}

/// How much the journal grows between the snapshots the warm replica writes, as a multiple of the
/// last snapshot's size (0 writes one after every command). The primary does not snapshot while
/// trading: a normal startup writes one snapshot, and a promotion writes none.
fn snapshot_growth_or_exit() -> u64 {
    std::env::var("EVENT_SNAPSHOT_GROWTH")
        .map(|value| value.parse::<u64>())
        .unwrap_or(Ok(DEFAULT_SNAPSHOT_GROWTH))
        .unwrap_or_else(|_| {
            eprintln!("EVENT_SNAPSHOT_GROWTH must be a whole number");
            std::process::exit(1);
        })
}

async fn serve_primary(
    runtime: ExchangeRuntime,
    tx: tokio::sync::mpsc::Sender<crate::types::types::ExchangeCommand>,
    replication: Option<Arc<Replication>>,
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
    let operator = exchange::operator::router(tx.clone(), replication.clone());
    tokio::spawn(async move { axum::serve(operator_listener, operator).await });

    let state = AppState {
        db,
        tx,
        exchange_available,
        replication,
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
    if !state.exchange_available.load(Ordering::Acquire) {
        (StatusCode::SERVICE_UNAVAILABLE, "exchange unavailable")
    } else if state
        .replication
        .as_ref()
        .is_some_and(|replication| replication.paused())
    {
        // Nothing can be acknowledged until the replica confirms or the operator lets this
        // primary run alone.
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "paused: waiting for the replica",
        )
    } else {
        (StatusCode::OK, "OK")
    }
}
