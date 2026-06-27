use std::{net::SocketAddr, thread};

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

use crate::types::{order_manager::OrderManager, types::ExchangeCommand};

const EXCHANGE_COMMAND_QUEUE_SIZE: usize = 10_000;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db = db::connect_db().await;
    let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_COMMAND_QUEUE_SIZE);

    thread::spawn(move || run_exchange_worker(rx));

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

fn run_exchange_worker(mut rx: tokio::sync::mpsc::Receiver<ExchangeCommand>) {
    let mut order_manager = OrderManager::new();

    while let Some(command) = rx.blocking_recv() {
        match command {
            ExchangeCommand::Deposit {
                user_id,
                amount,
                respond_to,
            } => {
                order_manager.wallet.deposit(user_id, amount);
                let _ = respond_to.send(Ok(()));
            }
            ExchangeCommand::PlaceOrder { order, respond_to } => {
                let order_id = order.order_id.clone();
                let result = order_manager
                    .add_order(order)
                    .map(|_| order_id)
                    .map_err(|err| format!("{:?}", err));

                let _ = respond_to.send(result);
            }
            ExchangeCommand::CancelOrder {
                order_id,
                user_id,
                respond_to,
            } => {
                let result = order_manager
                    .cancel_order_for_user(&order_id, &user_id)
                    .map_err(|err| format!("{:?}", err));

                let _ = respond_to.send(result);
            }
        }
    }
}
