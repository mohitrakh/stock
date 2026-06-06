use std::sync::{Arc, Mutex};

use sqlx::PgPool;

use crate::types::order_manager::OrderManager;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub order_manager: Arc<Mutex<OrderManager>>,
}
