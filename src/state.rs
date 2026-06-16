use sqlx::PgPool;
use crossbeam_channel::Sender;
use crate::types::types::ExchangeCommand;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub tx: Sender<ExchangeCommand>,
}
