use std::path::Path;

use crate::{
    exchange::{
        core::ExchangeCore,
        event_store::{EventStore, EventStoreError},
    },
    types::{
        exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::{ExchangeCommand, OrderView},
    },
};

pub struct ExchangeRuntime {
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    core: ExchangeCore,
    event_log: Vec<EventEnvelope>,
    next_event_seq: u64,
    /// `None` runs the exchange in memory only, which is what the unit tests and
    /// `ExchangeRuntime::new` want. The server always supplies a store.
    store: Option<EventStore>,
}

/// Why the exchange refused to start. Both variants mean the same thing operationally: there is
/// history on disk that cannot be trusted, so starting would either lose it or build state that
/// disagrees with it.
#[derive(Debug)]
pub enum StartupError {
    Store(EventStoreError),
    Replay(ReplayError),
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(err) => write!(f, "{}", err),
            Self::Replay(err) => write!(
                f,
                "stored history did not replay deterministically: {:?}",
                err
            ),
        }
    }
}

enum InputEventResult {
    Deposit(Result<(), String>),
    DepositShares(Result<(), String>),
    SetRiskLimit(Result<(), String>),
    PlaceOrder(Result<OrderView, String>),
    CancelOrder(Result<(), String>),
}
struct ProcessedInput {
    result: InputEventResult,
    output_events: Vec<ExchangeOutputEvent>,
}

#[derive(Debug, PartialEq)]
pub enum ReplayError {
    EventSequenceMismatch {
        expected: u64,
        actual: u64,
    },
    UnexpectedOutput {
        seq_num: u64,
        actual: ExchangeOutputEvent,
    },
    MissingOutput {
        seq_num: u64,
        expected: ExchangeOutputEvent,
    },
    OutputMismatch {
        seq_num: u64,
        expected: ExchangeOutputEvent,
        actual: ExchangeOutputEvent,
    },
}

impl InputEventResult {
    fn into_deposit_result(self) -> Result<(), String> {
        match self {
            Self::Deposit(result) => result,
            _ => unreachable!("expected deposit result"),
        }
    }

    fn into_deposit_shares_result(self) -> Result<(), String> {
        match self {
            Self::DepositShares(result) => result,
            _ => unreachable!("expected share deposit result"),
        }
    }

    fn into_set_risk_limit_result(self) -> Result<(), String> {
        match self {
            Self::SetRiskLimit(result) => result,
            _ => unreachable!("expected risk limit result"),
        }
    }

    fn into_place_order_result(self) -> Result<OrderView, String> {
        match self {
            Self::PlaceOrder(result) => result,
            _ => unreachable!("expected place order result"),
        }
    }

    fn into_cancel_order_result(self) -> Result<(), String> {
        match self {
            Self::CancelOrder(result) => result,
            _ => unreachable!("expected cancel order result"),
        }
    }
}

fn process_input_event(core: &mut ExchangeCore, event: ExchangeInputEvent) -> ProcessedInput {
    match event {
        ExchangeInputEvent::FundsDepositRequested { user_id, amount } => {
            match core.deposit(user_id.clone(), amount) {
                Ok(()) => ProcessedInput {
                    result: InputEventResult::Deposit(Ok(())),
                    output_events: vec![ExchangeOutputEvent::FundsDeposited { user_id, amount }],
                },

                Err(err) => {
                    let reason = format!("{:?}", err);

                    ProcessedInput {
                        result: InputEventResult::Deposit(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::FundsDepositRejected {
                            user_id,
                            reason,
                        }],
                    }
                }
            }
        }

        ExchangeInputEvent::RiskLimitSetRequested {
            user_id,
            symbol,
            max_daily_quantity,
        } => {
            core.set_risk_limit(user_id.clone(), symbol.clone(), max_daily_quantity);

            ProcessedInput {
                result: InputEventResult::SetRiskLimit(Ok(())),
                output_events: vec![ExchangeOutputEvent::RiskLimitSet {
                    user_id,
                    symbol,
                    max_daily_quantity,
                }],
            }
        }

        ExchangeInputEvent::SharesDepositRequested {
            user_id,
            symbol,
            quantity,
        } => match core.deposit_shares(&user_id, &symbol, quantity) {
            Ok(()) => ProcessedInput {
                result: InputEventResult::DepositShares(Ok(())),
                output_events: vec![ExchangeOutputEvent::SharesDeposited {
                    user_id,
                    symbol,
                    quantity,
                }],
            },

            Err(err) => {
                let reason = format!("{:?}", err);

                ProcessedInput {
                    result: InputEventResult::DepositShares(Err(reason.clone())),
                    output_events: vec![ExchangeOutputEvent::SharesDepositRejected {
                        user_id,
                        symbol,
                        reason,
                    }],
                }
            }
        },

        ExchangeInputEvent::NewOrderRequested { order } => {
            let order_id = order.order_id.clone();

            match core.add_order(order) {
                Ok(outcome) => {
                    // The view rides the live reply only. Output events stay byte-identical to
                    // what they were, so a log written before this change still replays.
                    let view = outcome.view;

                    let mut output_events = vec![ExchangeOutputEvent::OrderAccepted {
                        order_id: outcome.order_id,
                        seq_num: outcome.seq_num,
                    }];

                    output_events.extend(
                        outcome
                            .executions
                            .into_iter()
                            .map(|execution| ExchangeOutputEvent::ExecutionCreated { execution }),
                    );

                    ProcessedInput {
                        result: InputEventResult::PlaceOrder(Ok(view)),
                        output_events,
                    }
                }

                Err(err) => {
                    let reason = format!("{:?}", err);

                    ProcessedInput {
                        result: InputEventResult::PlaceOrder(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::OrderRejected {
                            order_id,
                            reason,
                        }],
                    }
                }
            }
        }

        ExchangeInputEvent::CancelOrderRequested { order_id, user_id } => {
            match core.cancel_order_for_user(&order_id, &user_id) {
                Ok(seq_num) => ProcessedInput {
                    result: InputEventResult::CancelOrder(Ok(())),
                    output_events: vec![ExchangeOutputEvent::OrderCanceled { order_id, seq_num }],
                },

                Err(err) => {
                    let reason = format!("{:?}", err);

                    ProcessedInput {
                        result: InputEventResult::CancelOrder(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::CancelRejected {
                            order_id,
                            reason,
                        }],
                    }
                }
            }
        }
    }
}

pub fn replay_event_log(event_log: &[EventEnvelope]) -> Result<ExchangeCore, ReplayError> {
    for (index, envelope) in event_log.iter().enumerate() {
        let expected = index as u64 + 1;

        if envelope.seq_num != expected {
            return Err(ReplayError::EventSequenceMismatch {
                expected,
                actual: envelope.seq_num,
            });
        }
    }

    let mut core = ExchangeCore::new();
    let mut index = 0;

    while index < event_log.len() {
        let input_envelope = &event_log[index];

        let input = match &input_envelope.event {
            ExchangeEvent::Input(input) => input.clone(),

            ExchangeEvent::Output(actual) => {
                return Err(ReplayError::UnexpectedOutput {
                    seq_num: input_envelope.seq_num,
                    actual: actual.clone(),
                });
            }
        };

        index += 1;

        let processed = process_input_event(&mut core, input);

        for expected_output in processed.output_events {
            let Some(output_envelope) = event_log.get(index) else {
                return Err(ReplayError::MissingOutput {
                    seq_num: index as u64 + 1,
                    expected: expected_output,
                });
            };

            let actual_output = match &output_envelope.event {
                ExchangeEvent::Output(actual) => actual,

                ExchangeEvent::Input(_) => {
                    return Err(ReplayError::MissingOutput {
                        seq_num: output_envelope.seq_num,
                        expected: expected_output,
                    });
                }
            };

            if actual_output != &expected_output {
                return Err(ReplayError::OutputMismatch {
                    seq_num: output_envelope.seq_num,
                    expected: expected_output,
                    actual: actual_output.clone(),
                });
            }

            index += 1;
        }
    }

    Ok(core)
}

impl ExchangeRuntime {
    pub fn new(rx: tokio::sync::mpsc::Receiver<ExchangeCommand>) -> Self {
        Self {
            rx,
            core: ExchangeCore::new(),
            event_log: Vec::new(),
            next_event_seq: 1,
            store: None,
        }
    }

    pub fn from_event_log(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        event_log: Vec<EventEnvelope>,
    ) -> Result<Self, ReplayError> {
        Self::rebuild(rx, event_log, None)
    }

    /// Rebuilds from validated history and keeps writing to the store the history came from.
    pub fn from_store(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        store: EventStore,
        event_log: Vec<EventEnvelope>,
    ) -> Result<Self, ReplayError> {
        Self::rebuild(rx, event_log, Some(store))
    }

    fn rebuild(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        event_log: Vec<EventEnvelope>,
        store: Option<EventStore>,
    ) -> Result<Self, ReplayError> {
        let core = replay_event_log(&event_log)?;

        let next_event_seq = event_log
            .last()
            .map(|envelope| envelope.seq_num + 1)
            .unwrap_or(1);

        Ok(Self {
            rx,
            core,
            event_log,
            next_event_seq,
            store,
        })
    }

    pub fn run(mut self) {
        while let Some(command) = self.rx.blocking_recv() {
            if let Err(err) = self.handle_command(command) {
                // Fail closed. The core has already applied this command in memory but the record
                // never reached disk, so memory is ahead of durable history and there is no
                // rollback. Accepting more work would widen the disagreement; dropping the
                // receiver makes every later request fail fast instead.
                eprintln!(
                    "exchange worker halted: could not write the event log ({}). \
                     In-memory state is ahead of durable history; no further commands accepted.",
                    err
                );
                break;
            }
        }
    }

    pub fn event_log(&self) -> &[EventEnvelope] {
        &self.event_log
    }

    fn handle_command(&mut self, command: ExchangeCommand) -> Result<(), EventStoreError> {
        match command {
            ExchangeCommand::Deposit {
                user_id,
                amount,
                respond_to,
            } => {
                match self.record_and_process_input_event(
                    ExchangeInputEvent::FundsDepositRequested { user_id, amount },
                ) {
                    Ok(result) => {
                        let _ = respond_to.send(result.into_deposit_result());
                        Ok(())
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(halted_message(&err)));
                        Err(err)
                    }
                }
            }
            ExchangeCommand::DepositShares {
                user_id,
                symbol,
                quantity,
                respond_to,
            } => {
                match self.record_and_process_input_event(
                    ExchangeInputEvent::SharesDepositRequested {
                        user_id,
                        symbol,
                        quantity,
                    },
                ) {
                    Ok(result) => {
                        let _ = respond_to.send(result.into_deposit_shares_result());
                        Ok(())
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(halted_message(&err)));
                        Err(err)
                    }
                }
            }
            ExchangeCommand::SetRiskLimit {
                user_id,
                symbol,
                max_daily_quantity,
                respond_to,
            } => {
                match self.record_and_process_input_event(
                    ExchangeInputEvent::RiskLimitSetRequested {
                        user_id,
                        symbol,
                        max_daily_quantity,
                    },
                ) {
                    Ok(result) => {
                        let _ = respond_to.send(result.into_set_risk_limit_result());
                        Ok(())
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(halted_message(&err)));
                        Err(err)
                    }
                }
            }
            ExchangeCommand::PlaceOrder { order, respond_to } => {
                match self
                    .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested { order })
                {
                    Ok(result) => {
                        let _ = respond_to.send(result.into_place_order_result());
                        Ok(())
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(halted_message(&err)));
                        Err(err)
                    }
                }
            }
            ExchangeCommand::CancelOrder {
                order_id,
                user_id,
                respond_to,
            } => {
                match self.record_and_process_input_event(
                    ExchangeInputEvent::CancelOrderRequested { order_id, user_id },
                ) {
                    Ok(result) => {
                        let _ = respond_to.send(result.into_cancel_order_result());
                        Ok(())
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(halted_message(&err)));
                        Err(err)
                    }
                }
            }

            // Reads are answered straight from the core. They never call
            // `record_and_process_input_event`, so they never reach the event log: they mutate
            // nothing, and logging them would make every future replay longer for no change in
            // outcome.
            ExchangeCommand::GetBalance {
                user_id,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.balance_view(&user_id));
                Ok(())
            }

            ExchangeCommand::GetPositions {
                user_id,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.position_views(&user_id));
                Ok(())
            }

            ExchangeCommand::GetExecutions {
                user_id,
                symbol,
                order_id,
                start_time,
                end_time,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.execution_views(
                    &user_id,
                    symbol.as_deref(),
                    order_id.as_deref(),
                    start_time,
                    end_time,
                ));
                Ok(())
            }

            ExchangeCommand::GetRiskLimit {
                user_id,
                symbol,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.risk_limit_view(&user_id, &symbol));
                Ok(())
            }

            ExchangeCommand::GetOrder {
                order_id,
                user_id,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.order_view(&order_id, &user_id));
                Ok(())
            }

            ExchangeCommand::GetOrderBook {
                symbol,
                depth,
                respond_to,
            } => {
                let _ = respond_to.send(self.core.l2_snapshot(&symbol, depth));
                Ok(())
            }
        }
    }

    /// Applies one input, then makes the whole command durable before it is acknowledged.
    ///
    /// The input and every output it generated are numbered and written as **one** record. That is
    /// the property replay depends on: a crash can lose the last command entirely, but it can never
    /// leave an input on disk whose outputs are missing.
    ///
    /// The core is mutated before the write, because the outputs are not known until the command
    /// has been processed. A write failure therefore leaves memory ahead of disk with no way back,
    /// which is why the error is fatal to the worker rather than merely returned to one caller.
    fn record_and_process_input_event(
        &mut self,
        event: ExchangeInputEvent,
    ) -> Result<InputEventResult, EventStoreError> {
        let ProcessedInput {
            result,
            output_events,
        } = process_input_event(&mut self.core, event.clone());

        let mut seq_num = self.next_event_seq;
        let mut batch = Vec::with_capacity(1 + output_events.len());

        batch.push(EventEnvelope {
            seq_num,
            event: ExchangeEvent::Input(event),
        });
        seq_num += 1;

        for output_event in output_events {
            batch.push(EventEnvelope {
                seq_num,
                event: ExchangeEvent::Output(output_event),
            });
            seq_num += 1;
        }

        if let Some(store) = self.store.as_mut() {
            store.append(&batch)?;
        }

        // Only now is the command part of history: durable first, then visible.
        self.next_event_seq = seq_num;
        self.event_log.extend(batch);

        Ok(result)
    }
}

fn halted_message(err: &EventStoreError) -> String {
    format!(
        "exchange halted: the event log could not be written ({})",
        err
    )
}

/// Opens the event log at `path`, replays whatever history it holds, and returns a runtime ready to
/// continue it. An absent or empty log starts a new exchange; anything unreadable, out of sequence,
/// or failing deterministic replay is an error, never a silent fresh start.
pub fn recover_runtime(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    path: impl AsRef<Path>,
) -> Result<ExchangeRuntime, StartupError> {
    let (store, recovered) = EventStore::open(path).map_err(StartupError::Store)?;

    ExchangeRuntime::from_store(rx, store, recovered).map_err(StartupError::Replay)
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;
    use crate::types::types::Order;

    fn runtime() -> ExchangeRuntime {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        ExchangeRuntime::new(rx)
    }

    /// A sell order must now be backed by shares, so histories that contain one have to fund the
    /// seller first — and that funding has to be an event, or replay would rebuild a seller with
    /// no inventory and reject the sell it is meant to reproduce.
    fn share_deposit(user: &str, quantity: u64) -> ExchangeInputEvent {
        ExchangeInputEvent::SharesDepositRequested {
            user_id: user.to_string(),
            symbol: "AAPL".to_string(),
            quantity,
        }
    }

    fn order(id: &str, user: &str, side: &str, price: u64, quantity: u32) -> Order {
        Order::new(
            id.to_string(),
            user.to_string(),
            "AAPL".to_string(),
            side,
            price,
            quantity,
            None,
            1.0,
            0,
        )
        .unwrap()
    }

    #[test]
    fn deposit_appends_requested_and_deposited_events() {
        let mut runtime = runtime();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::Deposit {
                user_id: "buyer".to_string(),
                amount: 1_000,
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);
        assert_eq!(runtime.event_log[0].seq_num, 1);
        assert_eq!(runtime.event_log[1].seq_num, 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn place_order_appends_accepted_after_successful_core_processing() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::NewOrderRequested { order }) => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 1);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn place_order_appends_rejected_after_failed_core_processing() {
        let mut runtime = runtime();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderRejected { order_id, reason }) => {
                assert_eq!(order_id, "buy-1");
                assert!(reason.contains("WalletRejected"));
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn cancel_order_appends_canceled_after_successful_core_processing() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "buy-1".to_string(),
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(user_id, "buyer");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 2);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn matching_order_appends_execution_created_events() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime.core.deposit_shares("seller", "AAPL", 5).unwrap();
        runtime
            .core
            .add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 5),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 4);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::NewOrderRequested { order }) => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 2);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        for event in &runtime.event_log[2..] {
            match &event.event {
                ExchangeEvent::Output(ExchangeOutputEvent::ExecutionCreated { execution }) => {
                    assert_eq!(execution.buy_order_id, "buy-1");
                    assert_eq!(execution.sell_order_id, "sell-1");
                    assert_eq!(execution.price.minor_units(), 10);
                    assert_eq!(execution.quantity, 5);
                }
                other => panic!("unexpected event: {:?}", other),
            }
        }
    }

    #[test]
    fn event_log_can_be_consumed_in_sequence() {
        let mut runtime = runtime();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::Deposit {
                user_id: "buyer".to_string(),
                amount: 1_000,
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::DepositShares {
                user_id: "seller".to_string(),
                symbol: "AAPL".to_string(),
                quantity: 5,
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("sell-1", "seller", "SELL", 10, 5),
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 5),
                respond_to,
            })
            .unwrap();

        let consumed_events = runtime.event_log();
        assert_eq!(consumed_events.len(), 10);

        for (idx, envelope) in consumed_events.iter().enumerate() {
            assert_eq!(envelope.seq_num, (idx + 1) as u64);
        }

        match &consumed_events[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &consumed_events[9].event {
            ExchangeEvent::Output(ExchangeOutputEvent::ExecutionCreated { execution }) => {
                assert_eq!(execution.buy_order_id, "buy-1");
                assert_eq!(execution.sell_order_id, "sell-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }
    #[test]
    fn rejected_cancellation_is_recorded_and_returned() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();

        let (respond_to, response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "buy-1".to_string(),
                user_id: "wrong-user".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);
        assert_eq!(runtime.event_log[0].seq_num, 1);
        assert_eq!(runtime.event_log[1].seq_num, 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(user_id, "wrong-user");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::CancelRejected { order_id, reason }) => {
                assert_eq!(order_id, "buy-1");
                assert!(reason.contains("Unauthorized"));
            }
            other => panic!("unexpected event: {:?}", other),
        }

        let response = response_rx.blocking_recv().unwrap();
        assert!(matches!(
            response,
            Err(reason) if reason.contains("Unauthorized")
        ));
    }
    #[test]
    fn same_input_sequence_produces_same_output_events() {
        let inputs = vec![
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            },
            share_deposit("seller", 5),
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell-1", "seller", "SELL", 10, 5),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy-1", "buyer", "BUY", 10, 5),
            },
        ];

        let mut first_core = ExchangeCore::new();
        let mut second_core = ExchangeCore::new();

        let mut first_outputs = Vec::new();
        let mut second_outputs = Vec::new();

        for input in inputs {
            let first_processed = process_input_event(&mut first_core, input.clone());
            let second_processed = process_input_event(&mut second_core, input);

            first_outputs.extend(first_processed.output_events);
            second_outputs.extend(second_processed.output_events);
        }

        assert_eq!(first_outputs.len(), 6);
        assert_eq!(first_outputs, second_outputs);
    }
    #[test]
    fn replay_rebuilds_matching_state_and_sequence() {
        let mut original = runtime();

        let _ =
            original.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            });

        let _ = original.record_and_process_input_event(share_deposit("seller", 10));

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("sell-1", "seller", "SELL", 10, 10),
        });

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("buy-1", "buyer", "BUY", 10, 5),
        });

        assert_eq!(original.event_log().len(), 10);

        let mut rebuilt_core = replay_event_log(original.event_log()).unwrap();

        let continuation = process_input_event(
            &mut rebuilt_core,
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
            },
        );

        assert_eq!(
            continuation.output_events,
            vec![ExchangeOutputEvent::OrderCanceled {
                order_id: "sell-1".to_string(),
                seq_num: 3,
            }]
        );
    }
    #[test]
    fn replay_rejects_event_sequence_gap() {
        let event_log = vec![EventEnvelope {
            seq_num: 2,
            event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::EventSequenceMismatch {
                expected: 1,
                actual: 2,
            })
        ));
    }

    #[test]
    fn replay_rejects_missing_output() {
        let event_log = vec![EventEnvelope {
            seq_num: 1,
            event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::MissingOutput {
                seq_num: 2,
                expected: ExchangeOutputEvent::FundsDeposited { .. },
            })
        ));
    }

    #[test]
    fn replay_rejects_unexpected_output() {
        let event_log = vec![EventEnvelope {
            seq_num: 1,
            event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::UnexpectedOutput { seq_num: 1, .. })
        ));
    }

    #[test]
    fn replay_rejects_modified_output() {
        let event_log = vec![
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
                    amount: 999,
                }),
            },
        ];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::OutputMismatch { seq_num: 2, .. })
        ));
    }

    #[test]
    fn recovered_runtime_continues_event_and_matching_sequences() {
        let mut original = runtime();

        let _ =
            original.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            });

        let _ = original.record_and_process_input_event(share_deposit("seller", 10));

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("sell-1", "seller", "SELL", 10, 10),
        });

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("buy-1", "buyer", "BUY", 10, 5),
        });

        let recorded_log = original.event_log().to_vec();
        assert_eq!(recorded_log.len(), 10);

        let (_tx, rx) = tokio::sync::mpsc::channel(1);

        let mut recovered = ExchangeRuntime::from_event_log(rx, recorded_log).unwrap();

        let (respond_to, response_rx) = oneshot::channel();

        recovered
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(response_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(recovered.event_log().len(), 12);

        assert_eq!(recovered.event_log()[10].seq_num, 11);
        assert_eq!(recovered.event_log()[11].seq_num, 12);

        match &recovered.event_log()[10].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(user_id, "seller");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &recovered.event_log()[11].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(*seq_num, 3);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn queries_do_not_append_to_the_event_log() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let length_before_reads = runtime.event_log().len();
        assert_eq!(length_before_reads, 2);

        let (respond_to, balance_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetBalance {
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        let (respond_to, order_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetOrder {
                order_id: "buy-1".to_string(),
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        let (respond_to, book_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetOrderBook {
                symbol: "AAPL".to_string(),
                depth: 10,
                respond_to,
            })
            .unwrap();

        // The reads answered, and the log is exactly where it was.
        let balance = balance_rx.blocking_recv().unwrap();
        assert_eq!(balance.balance, 1_000);
        assert_eq!(balance.locked, 100);
        assert_eq!(balance.available, 900);

        assert!(order_rx.blocking_recv().unwrap().is_some());
        assert!(book_rx.blocking_recv().unwrap().is_some());

        assert_eq!(runtime.event_log().len(), length_before_reads);
    }

    #[test]
    fn place_order_reply_reports_fill_state() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime.core.deposit_shares("seller", "AAPL", 4).unwrap();
        runtime
            .core
            .add_order(order("sell-1", "seller", "SELL", 10, 4))
            .unwrap();

        let (respond_to, response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let view = response_rx.blocking_recv().unwrap().unwrap();

        assert_eq!(view.order_id, "buy-1");
        assert_eq!(view.quantity, 10);
        assert_eq!(view.filled_quantity, 4);
        assert_eq!(view.remaining_quantity, 6);
        assert_eq!(view.status, "partially_filled");
        assert_eq!(view.price, 10);
    }

    fn temp_log_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "exchange-runtime-{}-{}.log",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn a_durable_exchange_survives_a_restart_and_continues_both_sequences() {
        let path = temp_log_path("restart");

        // First run: fund a buyer, rest a sell, partially fill it.
        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();

            for command in [
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-1", "seller", "SELL", 10, 10),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("buy-1", "buyer", "BUY", 10, 5),
                },
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }

            assert_eq!(runtime.event_log().len(), 10);
        }

        // Second run: same file, brand-new process state.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted = recover_runtime(rx, &path).unwrap();

        assert_eq!(restarted.event_log().len(), 10);

        // Wallet, locks and order state all came back.
        let balance = restarted.core.balance_view("buyer");
        assert_eq!(balance.balance, 950);
        assert_eq!(balance.locked, 0);
        assert_eq!(restarted.core.balance_view("seller").balance, 50);

        // So did both share ledgers: the seller delivered 5 and still has 5 reserved behind the
        // resting half of the order, and the buyer is holding what it bought.
        let seller_position = &restarted.core.position_views("seller")[0];
        assert_eq!(seller_position.symbol, "AAPL");
        assert_eq!(seller_position.quantity, 5);
        assert_eq!(seller_position.locked, 5);
        assert_eq!(seller_position.available, 0);
        assert_eq!(restarted.core.position_views("buyer")[0].quantity, 5);

        let resting = restarted.core.order_view("sell-1", "seller").unwrap();
        assert_eq!(resting.status, "partially_filled");
        assert_eq!(resting.remaining_quantity, 5);

        // The order book survived too.
        let book = restarted.core.l2_snapshot("AAPL", 10).unwrap();
        assert_eq!(
            book.asks,
            vec![crate::types::types::L2Level {
                price: 10,
                quantity: 5
            }]
        );

        // Cancelling the resting quantity works, and both counters continue where they left off:
        // event sequences 9 and 10, matching sequence 3.
        let (respond_to, response_rx) = oneshot::channel();
        restarted
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(response_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(restarted.event_log().len(), 12);
        assert_eq!(restarted.event_log()[10].seq_num, 11);

        match &restarted.event_log()[11].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(*seq_num, 3);
            }
            other => panic!("unexpected event: {:?}", other),
        }
        drop(restarted);

        // And the cancellation itself is durable: a third start sees it, with the seller's
        // reservation released back to available.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let third = recover_runtime(rx, &path).unwrap();
        assert_eq!(third.event_log().len(), 12);
        assert_eq!(
            third.core.order_view("sell-1", "seller").unwrap().status,
            "canceled"
        );

        let seller_position = &third.core.position_views("seller")[0];
        assert_eq!(seller_position.quantity, 5);
        assert_eq!(seller_position.locked, 0);
        assert_eq!(seller_position.available, 5);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn risk_limits_and_executions_survive_a_restart() {
        let path = temp_log_path("risk-and-fills");

        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();

            for command in [
                ExchangeInputEvent::RiskLimitSetRequested {
                    user_id: "buyer".to_string(),
                    symbol: "AAPL".to_string(),
                    max_daily_quantity: 8,
                },
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-1", "seller", "SELL", 10, 10),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("buy-1", "buyer", "BUY", 10, 5),
                },
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let restarted = recover_runtime(rx, &path).unwrap();

        // The limit was set by an event, so replay rebuilds it — a limit read from configuration
        // could not have survived this, and worse, would have replayed differently elsewhere.
        let limit = restarted.core.risk_limit_view("buyer", "AAPL");
        assert_eq!(limit.max_daily_quantity, 8);
        assert_eq!(limit.used_today, 5);

        // Fills are rebuilt by settlement running again during replay, for both parties.
        let buyer_fills = restarted
            .core
            .execution_views("buyer", None, None, None, None);
        assert_eq!(buyer_fills.len(), 1);
        assert_eq!(buyer_fills[0].order_id, "buy-1");
        assert_eq!(buyer_fills[0].quantity, 5);

        assert_eq!(
            restarted
                .core
                .execution_views("seller", None, None, None, None)
                .len(),
            1
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_rejected_order_is_recorded_so_replay_reproduces_the_rejection() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        runtime
            .record_and_process_input_event(ExchangeInputEvent::RiskLimitSetRequested {
                user_id: "buyer".to_string(),
                symbol: "AAPL".to_string(),
                max_daily_quantity: 3,
            })
            .unwrap();

        runtime
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("too-big", "buyer", "BUY", 10, 4),
            })
            .unwrap();

        match &runtime.event_log()[3].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderRejected { order_id, reason }) => {
                assert_eq!(order_id, "too-big");
                assert!(reason.contains("RiskRejected"));
            }
            other => panic!("unexpected event: {:?}", other),
        }

        // The rejection is part of history, so replaying it regenerates the same refusal.
        replay_event_log(runtime.event_log()).unwrap();
    }

    #[test]
    fn startup_refuses_history_that_does_not_replay() {
        let path = temp_log_path("mismatch");

        // A structurally perfect record — correct framing, correct checksum — whose recorded
        // outcome disagrees with what the core regenerates: the deposit asks for 1000, the stored
        // outcome claims 999. Checksums cannot catch this; only deterministic replay can.
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store
                .append(&[
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
                            amount: 999,
                        }),
                    },
                ])
                .unwrap();
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        assert!(matches!(
            recover_runtime(rx, &path),
            Err(StartupError::Replay(_))
        ));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_users_order_is_not_readable_by_anyone_else() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let (respond_to, order_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetOrder {
                order_id: "buy-1".to_string(),
                user_id: "someone-else".to_string(),
                respond_to,
            })
            .unwrap();

        assert!(order_rx.blocking_recv().unwrap().is_none());
    }
}
