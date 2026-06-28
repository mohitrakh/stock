use super::types::{Execution, Order};

#[derive(Debug, Clone)]
pub enum ExchangeEvent {
    NewOrderRequested { order: Order },
    CancelOrderRequested { order_id: String, user_id: String },
    FundsDepositRequested { user_id: String, amount: u64 },
    FundsDeposited { user_id: String, amount: u64 },
    OrderAccepted { order_id: String, seq_num: u64 },
    OrderRejected { order_id: String, reason: String },
    OrderCanceled { order_id: String, seq_num: u64 },
    ExecutionCreated { execution: Execution },
}

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub seq_num: u64,
    pub event: ExchangeEvent,
}
