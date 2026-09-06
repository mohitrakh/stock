use super::types::{Execution, Order};

#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeInputEvent {
    NewOrderRequested { order: Order },
    CancelOrderRequested { order_id: String, user_id: String },
    FundsDepositRequested { user_id: String, amount: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeOutputEvent {
    FundsDeposited { user_id: String, amount: u64 },
    OrderAccepted { order_id: String, seq_num: u64 },
    OrderRejected { order_id: String, reason: String },
    OrderCanceled { order_id: String, seq_num: u64 },
    CancelRejected { order_id: String, reason: String },
    ExecutionCreated { execution: Execution },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeEvent {
    Input(ExchangeInputEvent),
    Output(ExchangeOutputEvent),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope {
    pub seq_num: u64,
    pub event: ExchangeEvent,
}
