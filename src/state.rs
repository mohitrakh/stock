use std::sync::{Arc, atomic::AtomicBool};

use crate::types::types::ExchangeCommand;
use sqlx::PgPool;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub tx: Sender<ExchangeCommand>,
    pub exchange_available: Arc<AtomicBool>,
}
