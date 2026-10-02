//! The operator port: a loopback-only listener that opens and closes the market.
//!
//! Opening and closing are exchange commands like any other: the single worker processes them, they
//! are journaled, and replay reproduces them. This listener only turns an operator's HTTP request
//! into that command. Like the warm replica's management port it is unauthenticated, so it accepts
//! loopback addresses only.

use std::net::SocketAddr;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::NaiveDate;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};

use crate::types::types::{ExchangeCommand, SessionView};

pub const DEFAULT_ADDR: &str = "127.0.0.1:4004";

/// `EXCHANGE_OPERATOR_ADDR`, or the default. It must be a loopback address.
pub fn address() -> Result<SocketAddr, String> {
    let address: SocketAddr = std::env::var("EXCHANGE_OPERATOR_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()
        .map_err(|error| format!("EXCHANGE_OPERATOR_ADDR is not a socket address: {error}"))?;
    if !address.ip().is_loopback() {
        return Err("the operator port must use a loopback address".to_string());
    }
    Ok(address)
}

type Exchange = mpsc::Sender<ExchangeCommand>;

#[derive(Deserialize)]
struct OpenRequest {
    trading_day: NaiveDate,
}

pub fn router(exchange: Exchange) -> Router {
    Router::new()
        .route("/session", get(session))
        .route("/session/open", post(open))
        .route("/session/close", post(close))
        .with_state(exchange)
}

async fn session(State(exchange): State<Exchange>) -> Response {
    let (respond_to, reply) = oneshot::channel();
    if exchange
        .send(ExchangeCommand::GetSession { respond_to })
        .await
        .is_err()
    {
        return unavailable();
    }
    match reply.await {
        Ok(view) => Json(view).into_response(),
        Err(_) => unavailable(),
    }
}

async fn open(State(exchange): State<Exchange>, Json(request): Json<OpenRequest>) -> Response {
    change(&exchange, |respond_to| ExchangeCommand::OpenMarket {
        trading_day: request.trading_day,
        respond_to,
    })
    .await
}

async fn close(State(exchange): State<Exchange>) -> Response {
    change(&exchange, |respond_to| ExchangeCommand::CloseMarket {
        respond_to,
    })
    .await
}

/// 200 with the new session; 409 when the session refused the change, which is journaled like any
/// business rejection; 503 when the exchange worker is not running.
async fn change(
    exchange: &Exchange,
    make: impl FnOnce(oneshot::Sender<Result<SessionView, String>>) -> ExchangeCommand,
) -> Response {
    let (respond_to, reply) = oneshot::channel();
    if exchange.send(make(respond_to)).await.is_err() {
        return unavailable();
    }
    match reply.await {
        Ok(Ok(view)) => Json(view).into_response(),
        Ok(Err(reason)) if reason.starts_with("exchange unavailable:") => {
            (StatusCode::SERVICE_UNAVAILABLE, reason).into_response()
        }
        Ok(Err(reason)) => (StatusCode::CONFLICT, reason).into_response(),
        Err(_) => unavailable(),
    }
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "exchange unavailable").into_response()
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        thread,
    };

    use super::*;
    use crate::exchange::runtime::recover_runtime;

    fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(address).unwrap();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        let status = response.split_whitespace().nth(1).unwrap().parse().unwrap();
        let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    fn session_of(journal: &std::path::Path) -> SessionView {
        let (exchange, rx) = mpsc::channel(1);
        let runtime = recover_runtime(rx, journal).unwrap();
        let worker = thread::spawn(move || runtime.run());
        let (respond_to, reply) = oneshot::channel();
        exchange
            .blocking_send(ExchangeCommand::GetSession { respond_to })
            .unwrap();
        let view = reply.blocking_recv().unwrap();
        drop(exchange);
        worker.join().unwrap();
        view
    }

    #[test]
    fn the_operator_opens_and_closes_a_journaled_trading_day() {
        let journal =
            std::env::temp_dir().join(format!("stock-operator-{}.log", uuid::Uuid::new_v4()));
        let (exchange, rx) = mpsc::channel(8);
        let runtime = recover_runtime(rx, &journal).unwrap();
        let worker = thread::spawn(move || runtime.run());
        let server = tokio::runtime::Runtime::new().unwrap();
        let listener = server
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        let app = router(exchange.clone());
        server.spawn(async move { axum::serve(listener, app).await });

        assert_eq!(
            request(address, "GET", "/session", ""),
            (200, r#"{"trading_day":null,"open":false}"#.to_string())
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/session/open",
                r#"{"trading_day":"2026-10-01"}"#
            ),
            (
                200,
                r#"{"trading_day":"2026-10-01","open":true}"#.to_string()
            )
        );
        let (status, reason) = request(
            address,
            "POST",
            "/session/open",
            r#"{"trading_day":"2026-10-02"}"#,
        );
        assert_eq!((status, reason.as_str()), (409, "AlreadyOpen"));
        assert_eq!(
            request(address, "POST", "/session/close", ""),
            (
                200,
                r#"{"trading_day":"2026-10-01","open":false}"#.to_string()
            )
        );
        let (status, reason) = request(
            address,
            "POST",
            "/session/open",
            r#"{"trading_day":"2026-09-30"}"#,
        );
        assert_eq!(status, 409);
        assert!(reason.starts_with("NotAfterLastTradingDay"));
        // Not a date: refused by the request parser, before it reaches the exchange.
        assert_eq!(
            request(
                address,
                "POST",
                "/session/open",
                r#"{"trading_day":"tomorrow"}"#
            )
            .0,
            422
        );

        drop(server);
        drop(exchange);
        worker.join().unwrap();

        // Every change, refusals included, is in the journal: a restart rebuilds the same session.
        assert_eq!(
            session_of(&journal),
            SessionView {
                trading_day: NaiveDate::from_ymd_opt(2026, 10, 1),
                open: false
            }
        );
        std::fs::remove_file(journal).unwrap();
    }
}
