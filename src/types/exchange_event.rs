use super::types::{Execution, Order};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ExchangeInputEvent {
    NewOrderRequested {
        order: Order,
    },
    CancelOrderRequested {
        order_id: String,
        user_id: String,
    },
    FundsDepositRequested {
        user_id: String,
        amount: u64,
    },
    /// How shares enter the exchange, the counterpart to a funds deposit. Without it nobody could
    /// ever sell, because a sell order must now be backed by shares the seller holds.
    SharesDepositRequested {
        user_id: String,
        symbol: String,
        quantity: u64,
    },
    /// A change to a daily trading cap. Limits are events rather than configuration so that replay
    /// reproduces them exactly — a limit read from the environment would make the same log rebuild
    /// different state on a differently configured machine.
    RiskLimitSetRequested {
        user_id: String,
        symbol: String,
        max_daily_quantity: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ExchangeOutputEvent {
    FundsDeposited {
        user_id: String,
        amount: u64,
    },
    SharesDeposited {
        user_id: String,
        symbol: String,
        quantity: u64,
    },
    FundsDepositRejected {
        user_id: String,
        reason: String,
    },
    SharesDepositRejected {
        user_id: String,
        symbol: String,
        reason: String,
    },
    RiskLimitSet {
        user_id: String,
        symbol: String,
        max_daily_quantity: u64,
    },
    OrderAccepted {
        order_id: String,
        seq_num: u64,
    },
    OrderRejected {
        order_id: String,
        reason: String,
    },
    OrderCanceled {
        order_id: String,
        seq_num: u64,
    },
    CancelRejected {
        order_id: String,
        reason: String,
    },
    ExecutionCreated {
        execution: Execution,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "direction", content = "event", rename_all = "snake_case")]
pub enum ExchangeEvent {
    Input(ExchangeInputEvent),
    Output(ExchangeOutputEvent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub seq_num: u64,
    pub event: ExchangeEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_event_batch_round_trips_through_json() {
        let batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                }),
            },
        ];

        let bytes = serde_json::to_vec(&batch).unwrap();
        let recovered: Vec<EventEnvelope> = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(recovered, batch);
    }
}
