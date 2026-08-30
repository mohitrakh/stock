use crate::{
    exchange::core::ExchangeCore,
    types::{
        exchange_event::{EventEnvelope, ExchangeEvent},
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
                let result =
                    self.record_and_process_input_event(ExchangeEvent::FundsDepositRequested {
                        user_id: user_id.clone(),
                        amount,
                    });

                let _ = respond_to.send(result.into_deposit_result());
            }
            ExchangeCommand::PlaceOrder { order, respond_to } => {
                let result =
                    self.record_and_process_input_event(ExchangeEvent::NewOrderRequested {
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
                    self.record_and_process_input_event(ExchangeEvent::CancelOrderRequested {
                        order_id: order_id.clone(),
                        user_id: user_id.clone(),
                    });

                let _ = respond_to.send(result.into_cancel_order_result());
            }
        }
    }

    fn record_and_process_input_event(&mut self, event: ExchangeEvent) -> InputEventResult {
        self.append_event(event.clone());
        self.process_input_event(event)
    }

    fn process_input_event(&mut self, event: ExchangeEvent) -> InputEventResult {
        match event {
            ExchangeEvent::FundsDepositRequested { user_id, amount } => {
                self.core.deposit(user_id.clone(), amount);
                self.append_event(ExchangeEvent::FundsDeposited { user_id, amount });
                InputEventResult::Deposit(Ok(()))
            }
            ExchangeEvent::NewOrderRequested { order } => {
                let order_id = order.order_id.clone();

                let result = match self.core.add_order(order) {
                    Ok(outcome) => {
                        self.append_event(ExchangeEvent::OrderAccepted {
                            order_id: outcome.order_id.clone(),
                            seq_num: outcome.seq_num,
                        });

                        for execution in outcome.executions {
                            self.append_event(ExchangeEvent::ExecutionCreated { execution });
                        }

                        Ok(outcome.order_id)
                    }
                    Err(err) => {
                        let reason = format!("{:?}", err);
                        self.append_event(ExchangeEvent::OrderRejected {
                            order_id,
                            reason: reason.clone(),
                        });
                        Err(reason)
                    }
                };

                InputEventResult::PlaceOrder(result)
            }
            ExchangeEvent::CancelOrderRequested { order_id, user_id } => {
                let result = self
                    .core
                    .cancel_order_for_user(&order_id, &user_id)
                    .map(|seq_num| {
                        self.append_event(ExchangeEvent::OrderCanceled { order_id, seq_num });
                    })
                    .map_err(|err| format!("{:?}", err));

                InputEventResult::CancelOrder(result)
            }
            event => unreachable!("cannot process output event as input: {:?}", event),
        }
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
            ExchangeEvent::FundsDepositRequested { user_id, amount } => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::FundsDeposited { user_id, amount } => {
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
            ExchangeEvent::NewOrderRequested { order } => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::OrderAccepted { order_id, seq_num } => {
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
            ExchangeEvent::OrderRejected { order_id, reason } => {
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
            ExchangeEvent::CancelOrderRequested { order_id, user_id } => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(user_id, "buyer");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::OrderCanceled { order_id, seq_num } => {
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
            ExchangeEvent::NewOrderRequested { order } => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::OrderAccepted { order_id, seq_num } => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 2);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        for event in &runtime.event_log[2..] {
            match &event.event {
                ExchangeEvent::ExecutionCreated { execution } => {
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
            ExchangeEvent::FundsDepositRequested { user_id, amount } => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &consumed_events[7].event {
            ExchangeEvent::ExecutionCreated { execution } => {
                assert_eq!(execution.buy_order_id, "buy-1");
                assert_eq!(execution.sell_order_id, "sell-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }
}
