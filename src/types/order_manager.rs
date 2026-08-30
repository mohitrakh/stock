use std::collections::HashMap;

use super::risk_manager::RiskManager;
use super::types::{Execution, Order, Price};
use super::wallet::Wallet;

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

pub struct OrderManager {
    pub orders: HashMap<String, ManagedOrder>,
    pub risk_manager: RiskManager,
    pub wallet: Wallet,
    execution_callbacks: Vec<Box<dyn Fn(Execution) + Send + Sync>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            risk_manager: RiskManager::new(),
            wallet: Wallet::new(),
            execution_callbacks: Vec::new(),
        }
    }

    pub(crate) fn prepare_order(&mut self, order: Order) -> Result<Order, OrderManagerError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(OrderManagerError::AlreadyExists(order.order_id.clone()));
        }

        self.risk_manager
            .check(&order)
            .map_err(|err| OrderManagerError::RiskRejected(format!("{:?}", err)))?;

        self.wallet
            .check_and_lock(
                &order.user_id,
                &order.side,
                order.price,
                order.quantity as u64,
            )
            .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))?;

        self.risk_manager.record(&order);

        Ok(order)
    }
    pub(crate) fn register_order(&mut self, order: Order) {
        let order_id = order.order_id.clone();
        let original_quantity = order.quantity;

        self.orders.insert(
            order_id,
            ManagedOrder {
                order,
                state: OrderState::New,
                remaining_quantity: original_quantity,
            },
        );
    }

    pub(crate) fn apply_executions(
        &mut self,
        executions: &[Execution],
    ) -> Result<(), OrderManagerError> {
        for chunk in executions.chunks(2) {
            if chunk.len() == 2 {
                self.apply_execution(&chunk[0])?;
            }
        }

        for execution in executions {
            for callback in &self.execution_callbacks {
                callback(execution.clone());
            }
        }

        Ok(())
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

        let cash_amount = execution
            .price
            .checked_notional(execution.quantity as u64)
            .ok_or_else(|| OrderManagerError::WalletRejected("Overflow".to_string()))?;
        self.wallet.deposit(seller_user_id, cash_amount);

        self.record_fill(&execution.buy_order_id, execution.quantity)?;
        self.record_fill(&execution.sell_order_id, execution.quantity)?;

        Ok(())
    }

    fn fill_context(&self, order_id: &str) -> Result<(String, Price), OrderManagerError> {
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        Ok((managed.order.user_id.clone(), managed.order.price))
    }

    pub(crate) fn validate_cancel_for_user(
        &self,
        order_id: &str,
        user_id: &str,
    ) -> Result<(), OrderManagerError> {
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

        match managed.state {
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

        Ok(())
    }

    pub(crate) fn complete_cancel(&mut self, order_id: &str) -> Result<(), OrderManagerError> {
        let (user_id, side, price, remaining) = {
            let managed = self
                .orders
                .get(order_id)
                .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

            (
                managed.order.user_id.clone(),
                managed.order.side.clone(),
                managed.order.price,
                managed.remaining_quantity,
            )
        };

        self.wallet
            .unlock_funds(&user_id, &side, price, remaining as u64)
            .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))?;

        if let Some(managed) = self.orders.get_mut(order_id) {
            managed.state = OrderState::Canceled;
        }

        Ok(())
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
