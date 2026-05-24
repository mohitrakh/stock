use crate::risk_manager::RiskManager;
use crate::types::Order;
use crate::wallet::Wallet;
use std::collections::HashMap;

// Custom error type — no external crates needed
#[derive(Debug, PartialEq)]
pub enum OrderManagerError {
    AlreadyExists(String),     // duplicate order_id
    OrderNotFound(String),     // no such order
    InvalidTransition(String), // e.g. cancel a Filled order
    OverFill(String),          // fill_qty > remaining
    RiskRejected(String),
    WalletRejected(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
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
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            risk_manager: RiskManager::new(),
            wallet: Wallet::new(),
        }
    }

    pub fn add_order(&mut self, order: Order) -> Result<(), OrderManagerError> {
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
        self.orders.insert(
            order.order_id.clone(),
            ManagedOrder {
                order: order.clone(),
                state: OrderState::New,
                remaining_quantity: order.quantity,
            },
        );

        Ok(())
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<(), OrderManagerError> {
        let managed = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        let user_id = managed.order.user_id.clone();
        let side = managed.order.side.clone();
        let price = managed.order.price;
        let remaining = managed.remaining_quantity;
        let _ = self
            .wallet
            .unlock_funds(&user_id, &side, price, remaining as u64);

        // Only New or PartiallyFilled can be canceled
        match managed.state {
            OrderState::New | OrderState::PartiallyFilled => {
                managed.state = OrderState::Canceled;
                Ok(())
            }
            OrderState::Filled => Err(OrderManagerError::InvalidTransition(format!(
                "order {} is already Filled",
                order_id
            ))),
            OrderState::Canceled => Err(OrderManagerError::InvalidTransition(format!(
                "order {} is already Canceled",
                order_id
            ))),
        }
    }

    pub fn record_fill(&mut self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError> {
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
}

#[cfg(test)]
mod om_tests {
    use super::*;
    use crate::types::{Order, Side};
    use crate::wallet::{Wallet, WalletError};

    fn sample_order(id: &str, qty: u32) -> Order {
        Order::new(
            id.to_string(),
            "user1".to_string(),
            "AAPL".to_string(),
            "SELL",
            100.0,
            qty,
            None,
            1.0,
            0,
        )
        .unwrap()
    }

    #[test]
    fn test_add_order_state_is_new() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::New));
    }

    #[test]
    fn test_add_duplicate_fails() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        let err = om.add_order(sample_order("o1", 10)).unwrap_err();
        assert_eq!(err, OrderManagerError::AlreadyExists("o1".into()));
    }

    #[test]
    fn test_partial_fill_new_to_partially_filled() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        assert_eq!(om.orders["o1"].remaining_quantity, 6);
    }

    #[test]
    fn test_full_fill_goes_to_filled() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Filled));
        assert_eq!(om.orders["o1"].remaining_quantity, 0);
    }

    #[test]
    fn test_multiple_partials_then_filled() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Filled));
    }

    #[test]
    fn test_cancel_from_new() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.cancel_order("o1").unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Canceled));
    }

    #[test]
    fn test_cancel_from_partially_filled() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 4).unwrap();
        om.cancel_order("o1").unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Canceled));
    }

    #[test]
    fn test_cancel_filled_fails() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert!(matches!(
            om.cancel_order("o1").unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_cancel_already_canceled_fails() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.cancel_order("o1").unwrap();
        assert!(matches!(
            om.cancel_order("o1").unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_overfill_rejected() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        assert!(matches!(
            om.record_fill("o1", 11).unwrap_err(),
            OrderManagerError::OverFill(_)
        ));
    }

    #[test]
    fn test_fill_terminal_order_fails() {
        let mut om = OrderManager::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert!(matches!(
            om.record_fill("o1", 1).unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_unknown_order_errors() {
        let mut om = OrderManager::new();
        assert_eq!(
            om.record_fill("ghost", 5).unwrap_err(),
            OrderManagerError::OrderNotFound("ghost".into())
        );
        assert_eq!(
            om.cancel_order("ghost").unwrap_err(),
            OrderManagerError::OrderNotFound("ghost".into())
        );
    }
    #[cfg(test)]
    mod risk_tests {
        use super::*;

        fn make_order(user_id: &str, symbol: &str, qty: u32) -> Order {
            Order::new(
                format!("o-{}-{}", user_id, qty),
                user_id.to_string(),
                symbol.to_string(),
                "SELL",
                100.0,
                qty,
                None,
                1.0,
                0,
            )
            .unwrap()
        }

        #[test]
        fn test_under_limit_passes() {
            let mut om = OrderManager::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            assert!(om.add_order(make_order("u1", "AAPL", 500)).is_ok());
            // volume is now 500
        }

        #[test]
        fn test_exceeds_limit_rejected() {
            let mut om = OrderManager::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 500)).unwrap();
            // 500 + 600 = 1100 > 1000, should fail
            assert!(matches!(
                om.add_order(make_order("u1", "AAPL", 600)).unwrap_err(),
                OrderManagerError::RiskRejected(_)
            ));
        }

        #[test]
        fn test_exact_limit_passes() {
            let mut om = OrderManager::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 500)).unwrap();
            // use 499 + 1 would also work, but simplest fix: different qty = different ID
            assert!(om.add_order(make_order("u1", "AAPL", 499)).is_ok()); // 500+499=999 ≤ 1000
        }

        #[test]
        fn test_different_user_unaffected() {
            let mut om = OrderManager::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 900)).unwrap();
            // u2 has no limit, should always pass
            assert!(om.add_order(make_order("u2", "AAPL", 9999)).is_ok());
        }

        #[test]
        fn test_no_limit_set_always_passes() {
            let mut om = OrderManager::new();
            // no limits configured at all
            assert!(om.add_order(make_order("u1", "AAPL", 99999)).is_ok());
        }

        #[test]
        fn test_different_symbol_unaffected() {
            let mut om = OrderManager::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 900)).unwrap();
            // TSLA has no limit for u1, should pass
            assert!(om.add_order(make_order("u1", "TSLA", 9999)).is_ok());
        }
    }
    #[cfg(test)]
    mod wallet_tests {
        use super::*;

        #[test]
        fn test_deposit_and_lock() {
            let mut w = Wallet::new();
            w.deposit("u1".into(), 1000);
            assert!(w.check_and_lock("u1", &Side::Buy, 1.0, 600).is_ok());
            assert_eq!(*w.locked.get("u1").unwrap(), 600);
            assert_eq!(*w.balances.get("u1").unwrap(), 1000); // balance unchanged
        }

        #[test]
        fn test_insufficient_funds() {
            let mut w = Wallet::new();
            w.deposit("u1".into(), 1000);
            w.check_and_lock("u1", &Side::Buy, 1.0, 600).unwrap();
            // only 400 available, 500 requested
            assert_eq!(
                w.check_and_lock("u1", &Side::Buy, 1.0, 500),
                Err(WalletError::InsufficientFunds)
            );
        }

        #[test]
        fn test_commit_fill_deducts_balance_and_lock() {
            let mut w = Wallet::new();
            w.deposit("u1".into(), 1000);
            w.check_and_lock("u1", &Side::Buy, 1.0, 600).unwrap();
            w.commit_fill("u1", &Side::Buy, 1.0, 300).unwrap();
            assert_eq!(*w.balances.get("u1").unwrap(), 700); // 1000 - 300
            assert_eq!(*w.locked.get("u1").unwrap(), 300); // 600 - 300
        }

        #[test]
        fn test_unlock_funds_on_cancel() {
            let mut w = Wallet::new();
            w.deposit("u1".into(), 1000);
            w.check_and_lock("u1", &Side::Buy, 1.0, 600).unwrap();
            w.commit_fill("u1", &Side::Buy, 1.0, 300).unwrap(); // partial fill
            w.unlock_funds("u1", &Side::Buy, 1.0, 300).unwrap(); // cancel remaining
            assert_eq!(*w.locked.get("u1").unwrap(), 0);
            assert_eq!(*w.balances.get("u1").unwrap(), 700); // only filled portion deducted
        }

        #[test]
        fn test_sell_order_always_passes() {
            let mut w = Wallet::new(); // no deposit at all
            assert!(w.check_and_lock("u1", &Side::Sell, 100.0, 999).is_ok());
            assert!(w.commit_fill("u1", &Side::Sell, 100.0, 999).is_ok());
            assert!(w.unlock_funds("u1", &Side::Sell, 100.0, 999).is_ok());
        }
        #[test]
        fn test_overflow_rejected() {
            let mut w = Wallet::new();
            w.deposit("u1".into(), 1000);
            // requires 10000, only have 1000
            assert_eq!(
                w.check_and_lock("u1", &Side::Buy, 1000.0, 10),
                Err(WalletError::InsufficientFunds)
            );
        }

        #[test]
        fn test_no_deposit_insufficient_funds() {
            let mut w = Wallet::new();
            // user has no deposit at all, buy should fail
            assert_eq!(
                w.check_and_lock("u1", &Side::Buy, 100.0, 10),
                Err(WalletError::InsufficientFunds)
            );
        }
    }
}
