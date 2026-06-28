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
    AlreadyExists(String),
    OrderNotFound(String),
    InvalidTransition(String),
    OverFill(String),
    Unauthorized(String),
    RiskRejected(String),
    WalletRejected(String),
    MatchingRejected(String),
}
pub struct ManagedOrder {
    pub order: Order,
    pub state: OrderState,
    pub remaining_quantity: u32,
}

pub struct AddOrderOutcome {
    pub order_id: String,
    pub seq_num: u64,
    pub executions: Vec<Execution>,
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

    pub fn add_order(&mut self, mut order: Order) -> Result<AddOrderOutcome, OrderManagerError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(OrderManagerError::AlreadyExists(order.order_id.clone()));
        }
        self.risk_manager
            .check(&order)
            .map_err(|e| OrderManagerError::RiskRejected(format!("{:?}", e)))?;
        self.wallet
            .check_and_lock(
                &order.user_id,
                &order.side,
                order.price,
                order.quantity as u64,
            )
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;
        self.risk_manager.record(&order);

        let new_seq = self.sequencer.next();

        order.seq_num = new_seq;

        let order_id = order.order_id.clone();
        let original_qty = order.quantity;
        let og_order = order.clone();
        self.orders.insert(
            order_id.clone(),
            ManagedOrder {
                order: og_order,
                state: OrderState::New,
                remaining_quantity: original_qty,
            },
        );
        let fills = self
            .engine
            .process_order(order)
            .map_err(OrderManagerError::MatchingRejected)?;

        for chunk in fills.chunks(2) {
            if chunk.len() == 2 {
                self.apply_execution(&chunk[0])?;
            }
        }

        // Trigger subscriber callbacks for both parties
        for fill in &fills {
            for cb in &self.execution_callbacks {
                cb(fill.clone());
            }
        }

        Ok(AddOrderOutcome {
            order_id,
            seq_num: new_seq,
            executions: fills,
        })
    }

    fn apply_execution(&mut self, execution: &Execution) -> Result<(), OrderManagerError> {
        self.validate_fill(&execution.buy_order_id, execution.quantity)?;
        self.validate_fill(&execution.sell_order_id, execution.quantity)?;

        let (buyer_user_id, buyer_limit_price) = self.fill_context(&execution.buy_order_id)?;
        let (seller_user_id, _) = self.fill_context(&execution.sell_order_id)?;

        self.wallet
            .commit_buy_fill(
                &buyer_user_id,
                buyer_limit_price,
                execution.price,
                execution.quantity as u64,
            )
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;

        let cash_amount = (execution.price * execution.quantity as f64) as u64;
        self.wallet.deposit(seller_user_id, cash_amount);

        self.record_fill(&execution.buy_order_id, execution.quantity)?;
        self.record_fill(&execution.sell_order_id, execution.quantity)?;

        Ok(())
    }

    fn fill_context(&self, order_id: &str) -> Result<(String, f64), OrderManagerError> {
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        Ok((managed.order.user_id.clone(), managed.order.price))
    }

    pub fn cancel_order_for_user(
        &mut self,
        order_id: &str,
        user_id: &str,
    ) -> Result<u64, OrderManagerError> {
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        if managed.order.user_id != user_id {
            return Err(OrderManagerError::Unauthorized(format!(
                "user {} cannot cancel order {}",
                user_id, order_id
            )));
        }

        self.cancel_order(order_id)
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<u64, OrderManagerError> {
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
        let removed = self
            .engine
            .cancel_order(order_id, cancel_seq)
            .map_err(OrderManagerError::OrderNotFound)?;

        if removed.is_none() {
            return Err(OrderManagerError::OrderNotFound(order_id.to_string()));
        }

        self.wallet
            .unlock_funds(&user_id, &side, price, remaining as u64)
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;

        if let Some(managed) = self.orders.get_mut(order_id) {
            managed.state = OrderState::Canceled;
        }

        Ok(cancel_seq)
    }

    fn record_fill(&mut self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError> {
        self.validate_fill(order_id, filled_qty)?;

        let managed = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        managed.remaining_quantity -= filled_qty;

        if managed.remaining_quantity == 0 {
            managed.state = OrderState::Filled;
        } else {
            managed.state = OrderState::PartiallyFilled;
        }

        Ok(())
    }

    fn validate_fill(&self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError> {
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        match managed.state {
            OrderState::Filled | OrderState::Canceled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "Order {} is terminal, cannot fill",
                    order_id
                )));
            }

            _ => {}
        }

        if filled_qty > managed.remaining_quantity {
            return Err(OrderManagerError::OverFill(format!(
                "filled_qty {} > remaining_quantity {}",
                filled_qty, managed.remaining_quantity
            )));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn order(id: &str, user: &str, side: &str, price: f64, quantity: u32) -> Order {
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
    fn buy_order_rests_when_there_is_no_seller() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 1_000);

        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 10))
            .unwrap();

        assert_eq!(manager.get_state("buy-1"), Some(OrderState::New));
        assert_eq!(manager.orders["buy-1"].remaining_quantity, 10);
        assert!(manager.engine.is_resting("buy-1"));
        assert_eq!(manager.wallet.balance("buyer"), 1_000);
        assert_eq!(manager.wallet.locked("buyer"), 100);
        assert_eq!(manager.wallet.available("buyer"), 900);
    }

    #[test]
    fn sell_order_rests_when_there_is_no_buyer() {
        let mut manager = OrderManager::new();

        manager
            .add_order(order("sell-1", "seller", "SELL", 10.0, 10))
            .unwrap();

        assert_eq!(manager.get_state("sell-1"), Some(OrderState::New));
        assert_eq!(manager.orders["sell-1"].remaining_quantity, 10);
        assert!(manager.engine.is_resting("sell-1"));
    }

    #[test]
    fn buy_fully_matches_resting_sell() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 50);

        manager
            .add_order(order("sell-1", "seller", "SELL", 10.0, 5))
            .unwrap();
        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 5))
            .unwrap();

        assert_eq!(manager.get_state("sell-1"), Some(OrderState::Filled));
        assert_eq!(manager.get_state("buy-1"), Some(OrderState::Filled));
        assert_eq!(manager.orders["sell-1"].remaining_quantity, 0);
        assert_eq!(manager.orders["buy-1"].remaining_quantity, 0);
        assert!(!manager.engine.is_resting("sell-1"));
        assert!(!manager.engine.is_resting("buy-1"));
        assert_eq!(manager.wallet.balance("buyer"), 0);
        assert_eq!(manager.wallet.locked("buyer"), 0);
        assert_eq!(manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn buy_pays_execution_price_and_releases_price_improvement_lock() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 60);

        manager
            .add_order(order("sell-1", "seller", "SELL", 10.0, 5))
            .unwrap();
        manager
            .add_order(order("buy-1", "buyer", "BUY", 12.0, 5))
            .unwrap();

        assert_eq!(manager.get_state("sell-1"), Some(OrderState::Filled));
        assert_eq!(manager.get_state("buy-1"), Some(OrderState::Filled));
        assert_eq!(manager.wallet.balance("buyer"), 10);
        assert_eq!(manager.wallet.locked("buyer"), 0);
        assert_eq!(manager.wallet.available("buyer"), 10);
        assert_eq!(manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn buy_partially_matches_resting_sell() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 50);

        manager
            .add_order(order("sell-1", "seller", "SELL", 10.0, 10))
            .unwrap();
        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 5))
            .unwrap();

        assert_eq!(
            manager.get_state("sell-1"),
            Some(OrderState::PartiallyFilled)
        );
        assert_eq!(manager.get_state("buy-1"), Some(OrderState::Filled));
        assert_eq!(manager.orders["sell-1"].remaining_quantity, 5);
        assert_eq!(manager.orders["buy-1"].remaining_quantity, 0);
        assert!(manager.engine.is_resting("sell-1"));
        assert!(!manager.engine.is_resting("buy-1"));
        assert_eq!(manager.wallet.balance("buyer"), 0);
        assert_eq!(manager.wallet.locked("buyer"), 0);
        assert_eq!(manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn sell_fully_matches_resting_buy() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 100);

        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 5))
            .unwrap();
        manager
            .add_order(order("sell-1", "seller", "SELL", 10.0, 5))
            .unwrap();

        assert_eq!(manager.get_state("buy-1"), Some(OrderState::Filled));
        assert_eq!(manager.get_state("sell-1"), Some(OrderState::Filled));
        assert_eq!(manager.orders["buy-1"].remaining_quantity, 0);
        assert_eq!(manager.orders["sell-1"].remaining_quantity, 0);
        assert!(!manager.engine.is_resting("buy-1"));
        assert!(!manager.engine.is_resting("sell-1"));
        assert_eq!(manager.wallet.balance("buyer"), 50);
        assert_eq!(manager.wallet.locked("buyer"), 0);
        assert_eq!(manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn cancel_resting_buy_unlocks_remaining_funds() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 100);

        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 5))
            .unwrap();
        manager.cancel_order_for_user("buy-1", "buyer").unwrap();

        assert_eq!(manager.get_state("buy-1"), Some(OrderState::Canceled));
        assert_eq!(manager.wallet.balance("buyer"), 100);
        assert_eq!(manager.wallet.locked("buyer"), 0);
        assert_eq!(manager.wallet.available("buyer"), 100);
        assert!(!manager.engine.is_resting("buy-1"));
    }

    #[test]
    fn cancel_by_wrong_user_is_rejected() {
        let mut manager = OrderManager::new();
        manager.wallet.deposit("buyer".to_string(), 100);

        manager
            .add_order(order("buy-1", "buyer", "BUY", 10.0, 5))
            .unwrap();

        let result = manager.cancel_order_for_user("buy-1", "not-buyer");

        assert!(matches!(result, Err(OrderManagerError::Unauthorized(_))));
        assert_eq!(manager.get_state("buy-1"), Some(OrderState::New));
        assert_eq!(manager.wallet.locked("buyer"), 50);
        assert!(manager.engine.is_resting("buy-1"));
    }

    #[test]
    fn insufficient_funds_rejects_buy_order() {
        let mut manager = OrderManager::new();

        let result = manager.add_order(order("buy-1", "buyer", "BUY", 10.0, 5));

        assert!(matches!(result, Err(OrderManagerError::WalletRejected(_))));
        assert!(!manager.orders.contains_key("buy-1"));
        assert!(!manager.engine.is_resting("buy-1"));
        assert_eq!(manager.wallet.locked("buyer"), 0);
    }

    #[test]
    fn wallet_rejected_order_does_not_consume_risk_limit() {
        let mut manager = OrderManager::new();
        manager
            .risk_manager
            .set_limit("buyer".to_string(), "AAPL".to_string(), 5);

        let rejected = manager.add_order(order("buy-1", "buyer", "BUY", 10.0, 5));
        assert!(matches!(
            rejected,
            Err(OrderManagerError::WalletRejected(_))
        ));

        manager.wallet.deposit("buyer".to_string(), 50);
        let accepted = manager.add_order(order("buy-2", "buyer", "BUY", 10.0, 5));

        assert!(accepted.is_ok());
        assert_eq!(manager.get_state("buy-2"), Some(OrderState::New));
        assert!(manager.engine.is_resting("buy-2"));
    }

    #[test]
    fn duplicate_order_id_is_rejected() {
        let mut manager = OrderManager::new();

        manager
            .add_order(order("same-id", "seller", "SELL", 10.0, 5))
            .unwrap();
        let result = manager.add_order(order("same-id", "seller", "SELL", 11.0, 5));

        assert!(matches!(result, Err(OrderManagerError::AlreadyExists(_))));
        assert_eq!(manager.orders["same-id"].order.price, 10.0);
    }
}
