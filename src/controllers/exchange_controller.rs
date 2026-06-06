use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::app_error::AppError, middleware::auth_middleware::AuthUser, state::AppState,
    types::types::Order,
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
    let mut om = state.order_manager.lock().unwrap();
    om.wallet.deposit(auth.user_id, payload.amount);
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
    let mut om = state.order_manager.lock().unwrap();
    om.add_order(order)
        .map_err(|err| AppError::Validation(format!("{:?}", err)))?;
    Ok((StatusCode::CREATED, order_id))
}

pub async fn cancel_order(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(payload): Json<CancelRequest>,
) -> Result<StatusCode, AppError> {
    let mut om = state.order_manager.lock().unwrap();

    // Check if the order exists and belongs to this user first
    if let Some(managed) = om.orders.get(&payload.order_id) {
        if managed.order.user_id != auth.user_id {
            return Err(AppError::Validation(
                "Unauthorized to cancel this order".to_string(),
            ));
        }
    } else {
        return Err(AppError::NotFound);
    }
    om.cancel_order(&payload.order_id)
        .map_err(|err| AppError::Validation(format!("{:?}", err)))?;
    Ok(StatusCode::OK)
}
