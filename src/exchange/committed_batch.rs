//! Shared structural interpretation of one complete committed exchange command.
//! Consumers keep their own projection rules; this module only establishes what a batch means.

use std::collections::HashSet;

use chrono::NaiveDate;

use crate::types::{
    exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
    types::{Execution, Order},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPair {
    /// The durable identity of this trade. It is the first of the two adjacent execution events.
    pub first_sequence: u64,
    pub first: Execution,
    pub second: Execution,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NewOrderOutcome {
    Accepted {
        matching_sequence: u64,
        executions: Vec<ExecutionPair>,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    Canceled { matching_sequence: u64 },
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpiredOrder {
    pub order_id: String,
    pub matching_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommittedCommand {
    NewOrder {
        order: Order,
        outcome: NewOrderOutcome,
    },
    Cancellation {
        order_id: String,
        /// Who asked. A rejected cancellation may come from someone other than the owner.
        user_id: String,
        outcome: CancelOutcome,
    },
    /// A trading day opened. A client order id is unique within a trading day only.
    MarketOpened { trading_day: NaiveDate },
    /// A trading day closed, and every order still resting expired, oldest first.
    MarketClosed {
        trading_day: NaiveDate,
        expired: Vec<ExpiredOrder>,
    },
    /// Anything that changes no order: deposits, risk limits, a refused open or close.
    Other,
}

pub fn decode(batch: &[EventEnvelope]) -> Result<CommittedCommand, String> {
    let first = batch.first().ok_or("empty committed batch")?;
    let input = match &first.event {
        ExchangeEvent::Input(input) => input,
        ExchangeEvent::Output(_) => return Err("committed batch does not start with input".into()),
    };
    let outputs = batch[1..]
        .iter()
        .map(|envelope| match &envelope.event {
            ExchangeEvent::Output(output) => Ok((envelope.seq_num, output)),
            ExchangeEvent::Input(_) => Err("committed batch contains a second input".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if outputs.is_empty() {
        return Err("committed batch has no output".into());
    }

    match input {
        ExchangeInputEvent::NewOrderRequested { order } => decode_new_order(order, &outputs),
        ExchangeInputEvent::CancelOrderRequested { order_id, user_id } => {
            decode_cancellation(order_id, user_id, &outputs)
        }
        ExchangeInputEvent::MarketOpenRequested { trading_day } => {
            decode_open(*trading_day, &outputs)
        }
        ExchangeInputEvent::MarketCloseRequested => decode_close(&outputs),
        ExchangeInputEvent::FundsDepositRequested { .. }
        | ExchangeInputEvent::SharesDepositRequested { .. }
        | ExchangeInputEvent::RiskLimitSetRequested { .. } => Ok(CommittedCommand::Other),
    }
}

fn decode_open(
    requested: NaiveDate,
    outputs: &[(u64, &ExchangeOutputEvent)],
) -> Result<CommittedCommand, String> {
    match outputs {
        [(_, ExchangeOutputEvent::MarketOpened { trading_day })] if *trading_day == requested => {
            Ok(CommittedCommand::MarketOpened {
                trading_day: requested,
            })
        }
        [(_, ExchangeOutputEvent::SessionRejected { .. })] => Ok(CommittedCommand::Other),
        _ => Err("open batch has no matching open or refusal".into()),
    }
}

fn decode_close(outputs: &[(u64, &ExchangeOutputEvent)]) -> Result<CommittedCommand, String> {
    let trading_day = match outputs[0].1 {
        ExchangeOutputEvent::SessionRejected { .. } if outputs.len() == 1 => {
            return Ok(CommittedCommand::Other);
        }
        ExchangeOutputEvent::MarketClosed { trading_day } => *trading_day,
        _ => return Err("close batch has no matching close or refusal".into()),
    };
    let mut order_ids = HashSet::new();
    let mut expired = Vec::with_capacity(outputs.len() - 1);
    for (_, output) in &outputs[1..] {
        let ExchangeOutputEvent::OrderExpired { order_id, seq_num } = output else {
            return Err("non-expiry output follows the close".into());
        };
        // Expiries take consecutive matching sequences, like a run of cancellations.
        let consecutive = match expired.last() {
            None => *seq_num > 0,
            Some(ExpiredOrder {
                matching_sequence, ..
            }) => matching_sequence.checked_add(1) == Some(*seq_num),
        };
        if !consecutive || !order_ids.insert(order_id.as_str()) {
            return Err("close expiries are out of sequence or repeat an order".into());
        }
        expired.push(ExpiredOrder {
            order_id: order_id.clone(),
            matching_sequence: *seq_num,
        });
    }
    Ok(CommittedCommand::MarketClosed {
        trading_day,
        expired,
    })
}

fn decode_new_order(
    order: &Order,
    outputs: &[(u64, &ExchangeOutputEvent)],
) -> Result<CommittedCommand, String> {
    match outputs[0].1 {
        ExchangeOutputEvent::OrderRejected { order_id, reason } => {
            if outputs.len() != 1 || order_id != &order.order_id {
                return Err("order rejection does not match its input".into());
            }
            Ok(CommittedCommand::NewOrder {
                order: order.clone(),
                outcome: NewOrderOutcome::Rejected {
                    reason: reason.clone(),
                },
            })
        }
        ExchangeOutputEvent::OrderAccepted { order_id, seq_num }
            if order_id == &order.order_id && *seq_num > 0 =>
        {
            let executions = decode_execution_pairs(&outputs[1..])?;
            Ok(CommittedCommand::NewOrder {
                order: order.clone(),
                outcome: NewOrderOutcome::Accepted {
                    matching_sequence: *seq_num,
                    executions,
                },
            })
        }
        _ => Err("new-order batch has no matching acceptance or rejection".into()),
    }
}

fn decode_execution_pairs(
    outputs: &[(u64, &ExchangeOutputEvent)],
) -> Result<Vec<ExecutionPair>, String> {
    if !outputs.len().is_multiple_of(2) {
        return Err("accepted order has an odd number of execution outputs".into());
    }
    let mut execution_ids = HashSet::new();
    let mut pairs = Vec::with_capacity(outputs.len() / 2);
    for pair in outputs.chunks_exact(2) {
        let first = match pair[0].1 {
            ExchangeOutputEvent::ExecutionCreated { execution } => execution,
            _ => return Err("non-execution output follows order acceptance".into()),
        };
        let second = match pair[1].1 {
            ExchangeOutputEvent::ExecutionCreated { execution } => execution,
            _ => return Err("incomplete two-sided execution pair".into()),
        };
        if !execution_ids.insert(first.execution_id.as_str())
            || !execution_ids.insert(second.execution_id.as_str())
        {
            return Err("accepted order contains a duplicate execution id".into());
        }
        validate_execution_pair(first, second)?;
        pairs.push(ExecutionPair {
            first_sequence: pair[0].0,
            first: first.clone(),
            second: second.clone(),
        });
    }
    Ok(pairs)
}

fn decode_cancellation(
    requested_order_id: &str,
    user_id: &str,
    outputs: &[(u64, &ExchangeOutputEvent)],
) -> Result<CommittedCommand, String> {
    if outputs.len() != 1 {
        return Err("cancellation batch must contain exactly one output".into());
    }
    let outcome = match outputs[0].1 {
        ExchangeOutputEvent::OrderCanceled { order_id, seq_num }
            if order_id == requested_order_id && *seq_num > 0 =>
        {
            CancelOutcome::Canceled {
                matching_sequence: *seq_num,
            }
        }
        ExchangeOutputEvent::CancelRejected { order_id, reason }
            if order_id == requested_order_id =>
        {
            CancelOutcome::Rejected {
                reason: reason.clone(),
            }
        }
        _ => return Err("cancellation output does not match its input".into()),
    };
    Ok(CommittedCommand::Cancellation {
        order_id: requested_order_id.to_string(),
        user_id: user_id.to_string(),
        outcome,
    })
}

fn validate_execution_pair(first: &Execution, second: &Execution) -> Result<(), String> {
    let same_trade = first.buy_order_id == second.buy_order_id
        && first.sell_order_id == second.sell_order_id
        && first.symbol == second.symbol
        && first.price == second.price
        && first.quantity == second.quantity
        && first.timestamp.to_bits() == second.timestamp.to_bits();
    if first.execution_id == second.execution_id || !same_trade {
        return Err("two-sided execution pair is inconsistent".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::types::{Price, Side};

    fn order() -> Order {
        Order {
            order_id: "order-1".into(),
            user_id: "user".into(),
            symbol: "AAPL".into(),
            side: Side::Buy,
            price: Price::new(100).unwrap(),
            quantity: 2,
            leaves_qty: 2,
            timestamp: 1.0,
            seq_num: 0,
        }
    }

    fn envelope(seq_num: u64, event: ExchangeEvent) -> EventEnvelope {
        EventEnvelope { seq_num, event }
    }

    #[test]
    fn decodes_a_rejected_order_and_rejects_a_second_input() {
        let order = order();
        let batch = vec![
            envelope(
                1,
                ExchangeEvent::Input(ExchangeInputEvent::NewOrderRequested {
                    order: order.clone(),
                }),
            ),
            envelope(
                2,
                ExchangeEvent::Output(ExchangeOutputEvent::OrderRejected {
                    order_id: order.order_id.clone(),
                    reason: "no funds".into(),
                }),
            ),
        ];
        assert!(matches!(
            decode(&batch).unwrap(),
            CommittedCommand::NewOrder {
                outcome: NewOrderOutcome::Rejected { .. },
                ..
            }
        ));
        let mut malformed = batch;
        malformed.push(envelope(
            3,
            ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                user_id: "user".into(),
                amount: 1,
            }),
        ));
        assert!(decode(&malformed).is_err());
    }

    fn close(outputs: Vec<ExchangeOutputEvent>) -> Vec<EventEnvelope> {
        std::iter::once(ExchangeEvent::Input(
            ExchangeInputEvent::MarketCloseRequested,
        ))
        .chain(outputs.into_iter().map(ExchangeEvent::Output))
        .enumerate()
        .map(|(index, event)| envelope(index as u64 + 1, event))
        .collect()
    }

    fn expiry(order_id: &str, seq_num: u64) -> ExchangeOutputEvent {
        ExchangeOutputEvent::OrderExpired {
            order_id: order_id.into(),
            seq_num,
        }
    }

    #[test]
    fn a_close_lists_its_expiries_and_a_refused_close_changes_nothing() {
        let trading_day = NaiveDate::from_ymd_opt(2026, 10, 5).unwrap();
        let closed = ExchangeOutputEvent::MarketClosed { trading_day };
        assert_eq!(
            decode(&close(vec![
                closed.clone(),
                expiry("old", 7),
                expiry("new", 8)
            ]))
            .unwrap(),
            CommittedCommand::MarketClosed {
                trading_day,
                expired: vec![
                    ExpiredOrder {
                        order_id: "old".into(),
                        matching_sequence: 7
                    },
                    ExpiredOrder {
                        order_id: "new".into(),
                        matching_sequence: 8
                    },
                ],
            }
        );
        assert_eq!(
            decode(&close(vec![ExchangeOutputEvent::SessionRejected {
                reason: "AlreadyClosed".into()
            }]))
            .unwrap(),
            CommittedCommand::Other
        );

        // A gap, a repeated order, or an expiry without its close is not a committed close.
        for malformed in [
            vec![closed.clone(), expiry("old", 7), expiry("new", 9)],
            vec![closed.clone(), expiry("old", 7), expiry("old", 8)],
            vec![expiry("old", 7)],
        ] {
            assert!(decode(&close(malformed)).is_err());
        }
    }

    #[test]
    fn an_open_names_its_day_and_a_refused_open_changes_nothing() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 6).unwrap();
        let open = |output| {
            vec![
                envelope(
                    1,
                    ExchangeEvent::Input(ExchangeInputEvent::MarketOpenRequested {
                        trading_day: day,
                    }),
                ),
                envelope(2, ExchangeEvent::Output(output)),
            ]
        };
        assert_eq!(
            decode(&open(ExchangeOutputEvent::MarketOpened {
                trading_day: day
            }))
            .unwrap(),
            CommittedCommand::MarketOpened { trading_day: day }
        );
        assert_eq!(
            decode(&open(ExchangeOutputEvent::SessionRejected {
                reason: "AlreadyOpen".into()
            }))
            .unwrap(),
            CommittedCommand::Other
        );
        // An open of some other day is not this command's outcome.
        let other_day = NaiveDate::from_ymd_opt(2026, 10, 7).unwrap();
        assert!(
            decode(&open(ExchangeOutputEvent::MarketOpened {
                trading_day: other_day
            }))
            .is_err()
        );
    }

    #[test]
    fn a_cancellation_carries_who_asked() {
        let batch = vec![
            envelope(
                1,
                ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                    order_id: "order-1".into(),
                    user_id: "intruder".into(),
                }),
            ),
            envelope(
                2,
                ExchangeEvent::Output(ExchangeOutputEvent::CancelRejected {
                    order_id: "order-1".into(),
                    reason: "Unauthorized".into(),
                }),
            ),
        ];
        assert_eq!(
            decode(&batch).unwrap(),
            CommittedCommand::Cancellation {
                order_id: "order-1".into(),
                user_id: "intruder".into(),
                outcome: CancelOutcome::Rejected {
                    reason: "Unauthorized".into()
                },
            }
        );
    }
}
