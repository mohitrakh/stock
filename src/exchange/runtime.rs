use crate::{
    exchange::core::ExchangeCore,
    types::{
        exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::ExchangeCommand,
    },
};

pub struct ExchangeRuntime {
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    core: ExchangeCore,
    event_log: Vec<EventEnvelope>,
    next_event_seq: u64,
}

enum InputEventResult {
    Deposit(Result<(), String>),
    PlaceOrder(Result<String, String>),
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

    fn into_place_order_result(self) -> Result<String, String> {
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
            core.deposit(user_id.clone(), amount);

            ProcessedInput {
                result: InputEventResult::Deposit(Ok(())),
                output_events: vec![ExchangeOutputEvent::FundsDeposited { user_id, amount }],
            }
        }

        ExchangeInputEvent::NewOrderRequested { order } => {
            let order_id = order.order_id.clone();

            match core.add_order(order) {
                Ok(outcome) => {
                    let response_order_id = outcome.order_id.clone();

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
                        result: InputEventResult::PlaceOrder(Ok(response_order_id)),
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
        }
    }

    pub fn run(mut self) {
        while let Some(command) = self.rx.blocking_recv() {
            self.handle_command(command);
        }
    }

    pub fn event_log(&self) -> &[EventEnvelope] {
        &self.event_log
    }

    fn handle_command(&mut self, command: ExchangeCommand) {
        match command {
            ExchangeCommand::Deposit {
                user_id,
                amount,
                respond_to,
            } => {
                let result = self.record_and_process_input_event(
                    ExchangeInputEvent::FundsDepositRequested {
                        user_id: user_id.clone(),
                        amount,
                    },
                );

                let _ = respond_to.send(result.into_deposit_result());
            }
            ExchangeCommand::PlaceOrder { order, respond_to } => {
                let result =
                    self.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                        order: order.clone(),
                    });

                let _ = respond_to.send(result.into_place_order_result());
            }
            ExchangeCommand::CancelOrder {
                order_id,
                user_id,
                respond_to,
            } => {
                let result =
                    self.record_and_process_input_event(ExchangeInputEvent::CancelOrderRequested {
                        order_id: order_id.clone(),
                        user_id: user_id.clone(),
                    });

                let _ = respond_to.send(result.into_cancel_order_result());
            }
        }
    }

    fn record_and_process_input_event(&mut self, event: ExchangeInputEvent) -> InputEventResult {
        self.append_event(ExchangeEvent::Input(event.clone()));
        let ProcessedInput {
            result,
            output_events,
        } = process_input_event(&mut self.core, event);
        for output_event in output_events {
            self.append_event(ExchangeEvent::Output(output_event));
        }
        result
    }

    fn append_event(&mut self, event: ExchangeEvent) -> u64 {
        let seq_num = self.next_event_seq;
        self.next_event_seq += 1;
        self.event_log.push(EventEnvelope { seq_num, event });
        seq_num
    }
}

pub fn run_exchange_worker(rx: tokio::sync::mpsc::Receiver<ExchangeCommand>) {
    ExchangeRuntime::new(rx).run();
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

        runtime.handle_command(ExchangeCommand::Deposit {
            user_id: "buyer".to_string(),
            amount: 1_000,
            respond_to,
        });

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
        runtime.core.deposit("buyer".to_string(), 1_000);
        let (respond_to, _response_rx) = oneshot::channel();

        runtime.handle_command(ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 10),
            respond_to,
        });

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

        runtime.handle_command(ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 10),
            respond_to,
        });

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
        runtime.core.deposit("buyer".to_string(), 1_000);
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime.handle_command(ExchangeCommand::CancelOrder {
            order_id: "buy-1".to_string(),
            user_id: "buyer".to_string(),
            respond_to,
        });

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
        runtime.core.deposit("buyer".to_string(), 1_000);
        runtime
            .core
            .add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime.handle_command(ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 5),
            respond_to,
        });

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
        runtime.handle_command(ExchangeCommand::Deposit {
            user_id: "buyer".to_string(),
            amount: 1_000,
            respond_to,
        });

        let (respond_to, _response_rx) = oneshot::channel();
        runtime.handle_command(ExchangeCommand::PlaceOrder {
            order: order("sell-1", "seller", "SELL", 10, 5),
            respond_to,
        });

        let (respond_to, _response_rx) = oneshot::channel();
        runtime.handle_command(ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 5),
            respond_to,
        });

        let consumed_events = runtime.event_log();
        assert_eq!(consumed_events.len(), 8);

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

        match &consumed_events[7].event {
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
        runtime.core.deposit("buyer".to_string(), 1_000);
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();

        let (respond_to, response_rx) = oneshot::channel();

        runtime.handle_command(ExchangeCommand::CancelOrder {
            order_id: "buy-1".to_string(),
            user_id: "wrong-user".to_string(),
            respond_to,
        });

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

        assert_eq!(first_outputs.len(), 5);
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

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("sell-1", "seller", "SELL", 10, 10),
        });

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("buy-1", "buyer", "BUY", 10, 5),
        });

        assert_eq!(original.event_log().len(), 8);

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
}
