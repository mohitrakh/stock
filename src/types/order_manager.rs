use std::collections::HashMap;

use super::matching_engine::MatchingEngine;
use super::risk_manager::RiskManager;
use super::types::{Execution, Order};
use super::wallet::Wallet;
use crate::sequencer::Sequencer;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
}

#[derive(Debug, PartialEq)]
pub enum OrderManagerError {
    AlreadyExists(String),     // duplicate order_id
    OrderNotFound(String),     // no such order
    InvalidTransition(String), // e.g. cancel a Filled order
    OverFill(String),          // fill_qty > remaining
    RiskRejected(String),
    WalletRejected(String),
}

pub struct ManagedOrder {
    pub order: Order,
    pub state: OrderState,
    pub remaining_quantity: u32,
}

pub struct OrderManager {
    pub orders: HashMap<String, ManagedOrder>,
    pub risk_manager: RiskManager,
    pub wallet: Wallet,
    pub engine: MatchingEngine,
    pub sequencer: Sequencer,
    execution_callbacks: Vec<Box<dyn Fn(Execution) + Send + Sync>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            risk_manager: RiskManager::new(),
            wallet: Wallet::new(),
            engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
            execution_callbacks: Vec::new(),
        }
    }

    pub fn add_order(&mut self, mut order: Order) -> Result<(), OrderManagerError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(OrderManagerError::AlreadyExists(order.order_id.clone()));
        }
        self.risk_manager
            .check_and_record(&order)
            .map_err(|e| OrderManagerError::RiskRejected(format!("{:?}", e)))?;
        self.wallet
            .check_and_lock(
                &order.user_id,
                &order.side,
                order.price,
                order.quantity as u64,
            )
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;

        let new_seq = self.sequencer.next();

        order.seq_num = new_seq;

        let order_id = order.order_id.clone();
        let original_qty = order.quantity;
        let og_order = order.clone();
        let fills = self
            .engine
            .process_order(order)
            .map_err(OrderManagerError::RiskRejected)?;

        self.orders.insert(
            order_id,
            ManagedOrder {
                order: og_order,
                state: OrderState::New,
                remaining_quantity: original_qty,
            },
        );
        for chunk in fills.chunks(2) {
            if chunk.len() == 2 {
                let buy_exec = &chunk[0];
                self.record_fill(&buy_exec.buy_order_id, buy_exec.quantity)?;
                self.record_fill(&buy_exec.sell_order_id, buy_exec.quantity)?;

                // Settle trade cash: transfer cash from the buyer to the seller!
                let buyer_user_id = self
                    .orders
                    .get(&buy_exec.buy_order_id)
                    .map(|o| o.order.user_id.clone());
                let seller_user_id = self
                    .orders
                    .get(&buy_exec.sell_order_id)
                    .map(|o| o.order.user_id.clone());

                if let (Some(_buyer), Some(seller)) = (buyer_user_id, seller_user_id) {
                    let cash_amount = (buy_exec.price * buy_exec.quantity as f64) as u64;
                    self.wallet.deposit(seller, cash_amount);
                }
            }
        }

        // Trigger subscriber callbacks for both parties
        for fill in &fills {
            for cb in &self.execution_callbacks {
                cb(fill.clone());
            }
        }

        Ok(())
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<(), OrderManagerError> {
        // 1. Fetch the order locally first to verify its current state and avoid borrow issues
        let (state, user_id, side, price, remaining) = {
            let managed = self
                .orders
                .get(order_id)
                .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;
            (
                managed.state,
                managed.order.user_id.clone(),
                managed.order.side.clone(),
                managed.order.price,
                managed.remaining_quantity,
            )
        };

        // 2. Reject if the order is already in a terminal state
        match state {
            OrderState::Filled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "order {} is already Filled",
                    order_id
                )));
            }
            OrderState::Canceled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "order {} is already Canceled",
                    order_id
                )));
            }
            _ => {}
        }

        // 3. Call the matching engine to cancel it there (if it's resting)
        let cancel_seq = self.sequencer.next();
        let _ = self
            .engine
            .cancel_order(order_id, cancel_seq)
            .map_err(OrderManagerError::OrderNotFound)?;

        // 4. If matching engine cancellation succeeds, unlock funds and transition state
        let _ = self
            .wallet
            .unlock_funds(&user_id, &side, price, remaining as u64);

        if let Some(managed) = self.orders.get_mut(order_id) {
            managed.state = OrderState::Canceled;
        }

        Ok(())
    }

    fn record_fill(&mut self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError> {
        let managed = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        let order = &managed.order;
        let user_id = order.user_id.clone();
        let _ = self
            .wallet
            .commit_fill(&user_id, &order.side, order.price, filled_qty as u64);

        // Cannot record fill for already Canceled or Filled order
        match managed.state {
            OrderState::Filled | OrderState::Canceled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "order {} is terminal, cannot fill",
                    order_id
                )));
            }
            _ => {} // ok to continue
        }

        // Cannot record fill larger than remaining quantity
        if filled_qty > managed.remaining_quantity {
            return Err(OrderManagerError::OverFill(format!(
                "filled_qty {} > remaining_quantity {}",
                filled_qty, managed.remaining_quantity
            )));
        }

        managed.remaining_quantity -= filled_qty;
        if managed.remaining_quantity == 0 {
            managed.state = OrderState::Filled;
        } else {
            managed.state = OrderState::PartiallyFilled;
        }

        Ok(())
    }

    pub fn get_state(&self, order_id: &str) -> Option<OrderState> {
        self.orders.get(order_id).map(|m| m.state)
    }

    pub fn subscribe<F: Fn(Execution) + Send + Sync + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }
}
