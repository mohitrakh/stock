use super::types::{Execution, Order};
use chrono::NaiveDate;
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
    /// Starts a trading day. The operator names the day, and the command is journaled rather than
    /// derived from a clock, so replay rebuilds exactly the same days on any machine at any time.
    MarketOpenRequested {
        trading_day: NaiveDate,
    },
    /// Ends the current trading day.
    MarketCloseRequested,
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
    MarketOpened {
        trading_day: NaiveDate,
    },
    /// The close of a trading day. Its record also holds one `OrderExpired` for every order still
    /// resting, oldest first.
    MarketClosed {
        trading_day: NaiveDate,
    },
    /// A resting order expired at the close. Like a cancellation, it consumes a matching sequence.
    OrderExpired {
        order_id: String,
        seq_num: u64,
    },
    /// An open or close the exchange refused, such as opening an open market or a close too large
    /// for one journal record. Nothing changed.
    SessionRejected {
        reason: String,
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

    #[test]
    fn session_events_round_trip_with_an_iso_trading_day() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        let batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::MarketOpenRequested {
                    trading_day: day,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::MarketOpened {
                    trading_day: day,
                }),
            },
            EventEnvelope {
                seq_num: 3,
                event: ExchangeEvent::Input(ExchangeInputEvent::MarketCloseRequested),
            },
            EventEnvelope {
                seq_num: 4,
                event: ExchangeEvent::Output(ExchangeOutputEvent::MarketClosed {
                    trading_day: day,
                }),
            },
            EventEnvelope {
                seq_num: 5,
                event: ExchangeEvent::Output(ExchangeOutputEvent::OrderExpired {
                    order_id: "resting".to_string(),
                    seq_num: 7,
                }),
            },
        ];

        let bytes = serde_json::to_vec(&batch).unwrap();
        let json = String::from_utf8_lossy(&bytes);
        assert!(json.contains(r#""trading_day":"2026-10-01""#));
        assert!(
            json.contains(r#"{"kind":"order_expired","data":{"order_id":"resting","seq_num":7}}"#)
        );
        let recovered: Vec<EventEnvelope> = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(recovered, batch);
    }
}
