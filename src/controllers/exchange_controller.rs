use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    error::app_error::AppError,
    middleware::auth_middleware::AuthUser,
    state::AppState,
    types::types::{
        BalanceView, ExchangeCommand, ExecutionView, Order, OrderBookView, OrderView, PositionView,
        RiskLimitView,
    },
};

/// Default and ceiling for L2 depth. The cap is a response-size bound on a public read, not a
/// correctness rule — a client asking for 10,000 levels gets 50.
const DEFAULT_BOOK_DEPTH: usize = 10;
const MAX_BOOK_DEPTH: usize = 50;

#[derive(Deserialize)]
pub struct DepositeRequest {
    pub amount: u64,
}

#[derive(Deserialize)]
pub struct ShareDepositRequest {
    pub symbol: String,
    pub quantity: u64,
}

/// Bound on a client-supplied order id. It becomes a map key held for the life of the order, so it
/// is checked at the edge rather than trusted.
const MAX_CLIENT_ORDER_ID_LEN: usize = 64;

#[derive(Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: String,
    pub price: u64,
    pub quantity: u32,
    /// Optional client-supplied order id, the FIX `ClOrdID` idea. Supply one and a retry of the
    /// same request is rejected as a duplicate instead of opening a second order — which matters
    /// now that the exchange can durably accept an order and still lose the HTTP response. Omit it
    /// and the server mints a uuid, which is convenient but gives a retry no way to be recognised.
    pub client_order_id: Option<String>,
}

#[derive(Deserialize)]
pub struct CancelRequest {
    pub order_id: String,
}

#[derive(Deserialize)]
pub struct BookQuery {
    pub depth: Option<usize>,
}

#[derive(Deserialize)]
pub struct RiskLimitRequest {
    pub symbol: String,
    pub max_daily_quantity: u64,
}

#[derive(Deserialize)]
pub struct RiskLimitQuery {
    pub symbol: String,
}

/// All filters optional, matching the target design's execution query. Times are epoch seconds on
/// the same scale as an order's `creation_time`, and both bounds are inclusive.
#[derive(Deserialize)]
pub struct ExecutionQuery {
    pub symbol: Option<String>,
    pub order_id: Option<String>,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
}

/// Sends one command to the single exchange worker and waits for its reply. Every handler shares
/// this because the failure modes are the same three lines each time: the queue is gone, or the
/// worker dropped the response channel.
async fn ask<T>(
    state: &AppState,
    make_command: impl FnOnce(oneshot::Sender<T>) -> ExchangeCommand,
) -> Result<T, AppError> {
    let (respond_to, response_rx) = oneshot::channel();

    state
        .tx
        .send(make_command(respond_to))
        .await
        .map_err(|_| AppError::Validation("exchange worker is unavailable".to_string()))?;

    response_rx
        .await
        .map_err(|_| AppError::Validation("exchange worker dropped response".to_string()))
}

pub async fn deposit(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<DepositeRequest>,
) -> Result<StatusCode, AppError> {
    ask(&state, |respond_to| ExchangeCommand::Deposit {
        user_id: auth.user_id,
        amount: payload.amount,
        respond_to,
    })
    .await?
    .map_err(AppError::Validation)?;

    Ok(StatusCode::OK)
}

pub async fn place_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<OrderRequest>,
) -> Result<(StatusCode, Json<OrderView>), AppError> {
    // A client-supplied id is what makes a retry recognisable as a retry; a generated one is
    // unique every time, so the duplicate check can never fire on it.
    let order_id = match payload.client_order_id {
        Some(client_order_id) => {
            let trimmed = client_order_id.trim();

            if trimmed.is_empty() || trimmed.len() > MAX_CLIENT_ORDER_ID_LEN {
                return Err(AppError::Validation(format!(
                    "client_order_id must be 1 to {} characters",
                    MAX_CLIENT_ORDER_ID_LEN
                )));
            }

            trimmed.to_string()
        }
        None => Uuid::new_v4().to_string(),
    };

    let order = Order::new(
        order_id,
        auth.user_id.clone(),
        payload.symbol,
        &payload.side,
        payload.price,
        payload.quantity,
        None, // leaves_qty defaults to quantity
        chrono::Utc::now().timestamp() as f64,
        0, // seq_num is set dynamically inside add_order
    )
    .map_err(AppError::Validation)?;

    let view = ask(&state, |respond_to| ExchangeCommand::PlaceOrder {
        order,
        respond_to,
    })
    .await?
    .map_err(|err| {
        // A reused client order id is the retry case, not a malformed request.
        if err.contains("AlreadyExists") {
            AppError::Conflict(err)
        } else {
            AppError::Validation(err)
        }
    })?;

    Ok((StatusCode::CREATED, Json(view)))
}

pub async fn deposit_shares(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<ShareDepositRequest>,
) -> Result<StatusCode, AppError> {
    let symbol = payload.symbol.trim();

    if symbol.is_empty() {
        return Err(AppError::Validation("symbol is required".to_string()));
    }

    if payload.quantity == 0 {
        return Err(AppError::Validation(
            "quantity must be positive".to_string(),
        ));
    }

    ask(&state, |respond_to| ExchangeCommand::DepositShares {
        user_id: auth.user_id,
        symbol: symbol.to_string(),
        quantity: payload.quantity,
        respond_to,
    })
    .await?
    .map_err(AppError::Validation)?;

    Ok(StatusCode::OK)
}

pub async fn get_positions(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<Vec<PositionView>>, AppError> {
    let views = ask(&state, |respond_to| ExchangeCommand::GetPositions {
        user_id: auth.user_id,
        respond_to,
    })
    .await?;

    Ok(Json(views))
}

/// Sets the caller's own daily cap. A real exchange would make this a compliance action rather than
/// something a trader can raise for themselves; it is a placeholder in the same spirit as letting
/// anyone deposit themselves any amount of cash.
pub async fn set_risk_limit(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<RiskLimitRequest>,
) -> Result<StatusCode, AppError> {
    let symbol = payload.symbol.trim();

    if symbol.is_empty() {
        return Err(AppError::Validation("symbol is required".to_string()));
    }

    ask(&state, |respond_to| ExchangeCommand::SetRiskLimit {
        user_id: auth.user_id,
        symbol: symbol.to_string(),
        max_daily_quantity: payload.max_daily_quantity,
        respond_to,
    })
    .await?
    .map_err(AppError::Validation)?;

    Ok(StatusCode::OK)
}

pub async fn get_risk_limit(
    State(state): State<AppState>,
    auth: AuthUser,
    Query(query): Query<RiskLimitQuery>,
) -> Result<Json<RiskLimitView>, AppError> {
    let symbol = query.symbol.trim();

    if symbol.is_empty() {
        return Err(AppError::Validation("symbol is required".to_string()));
    }

    let view = ask(&state, |respond_to| ExchangeCommand::GetRiskLimit {
        user_id: auth.user_id,
        symbol: symbol.to_string(),
        respond_to,
    })
    .await?;

    Ok(Json(view))
}

pub async fn get_executions(
    State(state): State<AppState>,
    auth: AuthUser,
    Query(query): Query<ExecutionQuery>,
) -> Result<Json<Vec<ExecutionView>>, AppError> {
    let views = ask(&state, |respond_to| ExchangeCommand::GetExecutions {
        user_id: auth.user_id,
        symbol: query.symbol,
        order_id: query.order_id,
        start_time: query.start_time,
        end_time: query.end_time,
        respond_to,
    })
    .await?;

    Ok(Json(views))
}

pub async fn get_balance(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<BalanceView>, AppError> {
    let view = ask(&state, |respond_to| ExchangeCommand::GetBalance {
        user_id: auth.user_id,
        respond_to,
    })
    .await?;

    Ok(Json(view))
}

pub async fn get_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(order_id): Path<String>,
) -> Result<Json<OrderView>, AppError> {
    let view = ask(&state, |respond_to| ExchangeCommand::GetOrder {
        order_id,
        user_id: auth.user_id,
        respond_to,
    })
    .await?
    .ok_or(AppError::NotFound)?;

    Ok(Json(view))
}

pub async fn get_order_book(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<BookQuery>,
) -> Result<Json<OrderBookView>, AppError> {
    let depth = query
        .depth
        .unwrap_or(DEFAULT_BOOK_DEPTH)
        .clamp(1, MAX_BOOK_DEPTH);

    let view = ask(&state, |respond_to| ExchangeCommand::GetOrderBook {
        symbol,
        depth,
        respond_to,
    })
    .await?
    .ok_or(AppError::NotFound)?;

    Ok(Json(view))
}

pub async fn cancel_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<CancelRequest>,
) -> Result<StatusCode, AppError> {
    ask(&state, |respond_to| ExchangeCommand::CancelOrder {
        order_id: payload.order_id,
        user_id: auth.user_id,
        respond_to,
    })
    .await?
    .map_err(|err| {
        if err.contains("OrderNotFound") {
            AppError::NotFound
        } else {
            AppError::Validation(err)
        }
    })?;

    Ok(StatusCode::OK)
}
