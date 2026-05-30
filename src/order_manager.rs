use std::collections::HashMap;

use crate::{
    matching_engine::MatchingEngine,
    sequencer::Sequencer,
    types::{Execution, Order, OrderState},
};

pub struct OrderManager {
    pub engine: MatchingEngine,
    pub sequencer: Sequencer,
    pub order_states: HashMap<String, OrderState>,
    execution_callbacks: Vec<Box<dyn Fn(Execution)>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
            order_states: HashMap::new(),
            execution_callbacks: Vec::new(),
        }
    }

    pub fn place_order(&mut self, mut order: Order) -> Result<Vec<Execution>, String> {
        order.seq_num = self.sequencer.next();

        let order_id = order.order_id.clone();
        let original_qty = order.quantity;
        let fills = self.engine.process_order(order)?;

        // After engine.process_order() returns fills:

        // -- Update state for the incoming order itself
        let incoming_id = order_id.clone();
        if self.engine.is_resting(&incoming_id) {
            let leaves = self
                .engine
                .get_order_leaves(&incoming_id)
                .unwrap_or(original_qty);
            self.order_states
                .insert(incoming_id, OrderState::PartialFill { leaves_qty: leaves });
        } else {
            self.order_states.insert(incoming_id, OrderState::Filled);
        }

        // -- Update states for any resting orders that were involved in fills
        for exec in &fills {
            // Update the sell side
            if !self.engine.is_resting(&exec.sell_order_id) {
                self.order_states
                    .insert(exec.sell_order_id.clone(), OrderState::Filled);
            } else {
                if let Some(leaves) = self.engine.get_order_leaves(&exec.sell_order_id) {
                    self.order_states.insert(
                        exec.sell_order_id.clone(),
                        OrderState::PartialFill { leaves_qty: leaves },
                    );
                }
            }
            // Update the buy side (if it was a resting order, not the incoming one)
            if exec.buy_order_id != order_id {
                // avoid double‑updating the incoming order
                if !self.engine.is_resting(&exec.buy_order_id) {
                    self.order_states
                        .insert(exec.buy_order_id.clone(), OrderState::Filled);
                } else {
                    if let Some(leaves) = self.engine.get_order_leaves(&exec.buy_order_id) {
                        self.order_states.insert(
                            exec.buy_order_id.clone(),
                            OrderState::PartialFill { leaves_qty: leaves },
                        );
                    }
                }
            }
        }
        for exec in &fills {
            for cb in &self.execution_callbacks {
                cb(exec.clone());
            }
        }
        Ok(fills)
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<Option<Order>, String> {
        let cancel_seq = self.sequencer.next();
        let removed = self.engine.cancel_order(order_id, cancel_seq)?;

        if removed.is_some() {
            self.order_states
                .insert(order_id.to_string(), OrderState::Canceled);
        }

        Ok(removed)
    }

    pub fn get_order_state(&self, order_id: &str) -> Option<OrderState> {
        self.order_states.get(order_id).copied()
    }

    pub fn best_bid_ask(&self, symbol: &str) -> Option<((f64, u32), (f64, u32))> {
        self.engine.best_bid_ask(symbol)
    }

    /// Registers a callback that will be invoked for every execution.
    pub fn subscribe<F: Fn(Execution) + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }
}
