use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::app_error::AppError,
    middleware::auth_middleware::AuthUser,
    state::AppState,
    types::types::{ExchangeCommand, Order},
};

#[derive(Deserialize)]
pub struct DepositeRequest {
    pub amount: u64,
}
#[derive(Serialize)]
pub struct BalancedResponse {
    pub user_id: String,
    pub balance: u64,
}

#[derive(Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub quantity: u32,
}

#[derive(Deserialize)]
pub struct CancelRequest {
    pub order_id: String,
}

pub async fn deposit(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<DepositeRequest>,
) -> Result<StatusCode, AppError> {
    let (respond_to, response_rx) = tokio::sync::oneshot::channel();

    state
        .tx
        .send(ExchangeCommand::Deposit {
            user_id: auth.user_id,
            amount: payload.amount,
            respond_to,
        })
        .await
        .map_err(|_| AppError::Validation("exchange worker is unavailable".to_string()))?;

    response_rx
        .await
        .map_err(|_| AppError::Validation("exchange worker dropped response".to_string()))?
        .map_err(AppError::Validation)?;

    Ok(StatusCode::OK)
}

pub async fn place_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<OrderRequest>,
) -> Result<(StatusCode, String), AppError> {
    // Generate a unique order id
    let order_id = Uuid::new_v4().to_string();

    // Construct the order structure
    let order = Order::new(
        order_id.clone(),
        auth.user_id.clone(),
        payload.symbol,
        &payload.side,
        payload.price,
        payload.quantity,
        None, // leaves_qty defaults to quantity
        chrono::Utc::now().timestamp() as f64,
        0, // seq_num is set dynamically inside add_order
    )
    .map_err(|err| AppError::Validation(err))?;

    let (respond_to, response_rx) = tokio::sync::oneshot::channel();

    state
        .tx
        .send(ExchangeCommand::PlaceOrder { order, respond_to })
        .await
        .map_err(|_| AppError::Validation("exchange worker is unavailable".to_string()))?;

    let created_order_id = response_rx
        .await
        .map_err(|_| AppError::Validation("exchange worker dropped response".to_string()))?
        .map_err(AppError::Validation)?;

    Ok((StatusCode::CREATED, created_order_id))
}

pub async fn cancel_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<CancelRequest>,
) -> Result<StatusCode, AppError> {
    let (respond_to, response_rx) = tokio::sync::oneshot::channel();

    state
        .tx
        .send(ExchangeCommand::CancelOrder {
            order_id: payload.order_id,
            user_id: auth.user_id,
            respond_to,
        })
        .await
        .map_err(|_| AppError::Validation("exchange worker is unavailable".to_string()))?;

    response_rx
        .await
        .map_err(|_| AppError::Validation("exchange worker dropped response".to_string()))?
        .map_err(|err| {
            if err == "Order not found" {
                AppError::NotFound
            } else {
                AppError::Validation(err)
            }
        })?;

    Ok(StatusCode::OK)
}
