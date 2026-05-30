mod execution;
mod matching_engine;
mod order;
mod order_book;
mod order_manager;
mod price_level;
mod risk_manager;
mod sequencer;
mod types;
mod wallet;

use crate::{
    order_manager::OrderManager,
    types::{Execution, Order},
};

fn main() {
    let mut om = OrderManager::new();

    // Subscribe to all executions
    om.subscribe(|exec| {
        println!(
            "EXECUTION: {} @ {} qty {}",
            exec.symbol, exec.price, exec.quantity
        );
    });

    // Place a few orders
    let o1 = Order::new(
        "o1".into(),
        "u1".into(),
        "AAPL".into(),
        "SELL",
        100.0,
        10,
        None,
        1.0,
        0,
    )
    .unwrap();
    let o2 = Order::new(
        "o2".into(),
        "u2".into(),
        "AAPL".into(),
        "BUY",
        100.0,
        7,
        None,
        2.0,
        0,
    )
    .unwrap();

    let fills = om.place_order(o1).unwrap();
    println!("After o1: {:?}", fills);
    println!("State o1: {:?}", om.get_order_state("o1"));

    let fills = om.place_order(o2).unwrap();
    println!("After o2: {:?}", fills);
    println!("State o1: {:?}", om.get_order_state("o1"));
    println!("State o2: {:?}", om.get_order_state("o2"));

    // Cancel remaining o1
    let cancelled = om.cancel_order("o1").unwrap();
    println!("Cancelled o1: {:?}", cancelled);
    println!("State o1 after cancel: {:?}", om.get_order_state("o1"));
}

#[cfg(test)]
mod om_tests {
    use super::*;
    use crate::types::{Order, OrderState};

    fn sample_order(id: &str, user_id: &str, side: &str, qty: u32, price: f64) -> Order {
        Order::new(
            id.to_string(),
            user_id.to_string(),
            "AAPL".to_string(),
            side,
            price,
            qty,
            None,
            1.0,
            0,
        )
        .unwrap()
    }

    #[test]
    fn test_place_resting_order() {
        let mut om = OrderManager::new();
        let o1 = sample_order("o1", "u1", "SELL", 10, 100.0);
        let fills = om.place_order(o1).unwrap();
        assert!(fills.is_empty());
        assert_eq!(
            om.get_order_state("o1"),
            Some(OrderState::PartialFill { leaves_qty: 10 })
        );
    }

    #[test]
    fn test_order_matching_and_fills() {
        let mut om = OrderManager::new();
        let o1 = sample_order("o1", "u1", "SELL", 10, 100.0);
        om.place_order(o1).unwrap();

        let o2 = sample_order("o2", "u2", "BUY", 7, 100.0);
        let fills = om.place_order(o2).unwrap();
        
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].quantity, 7);
        assert_eq!(fills[0].price, 100.0);
        assert_eq!(fills[0].buy_order_id, "o2");
        assert_eq!(fills[0].sell_order_id, "o1");

        // Buyer is fully filled
        assert_eq!(om.get_order_state("o2"), Some(OrderState::Filled));
        // Seller is partially filled with 3 remaining
        assert_eq!(
            om.get_order_state("o1"),
            Some(OrderState::PartialFill { leaves_qty: 3 })
        );
    }

    #[test]
    fn test_cancel_order() {
        let mut om = OrderManager::new();
        let o1 = sample_order("o1", "u1", "SELL", 10, 100.0);
        om.place_order(o1).unwrap();
        
        let cancelled = om.cancel_order("o1").unwrap();
        assert!(cancelled.is_some());
        assert_eq!(om.get_order_state("o1"), Some(OrderState::Canceled));
    }

    #[test]
    fn test_subscribe_execution() {
        use std::sync::{Arc, Mutex};
        let mut om = OrderManager::new();
        let o1 = sample_order("o1", "u1", "SELL", 10, 100.0);
        om.place_order(o1).unwrap();

        let execs = Arc::new(Mutex::new(Vec::new()));
        let execs_clone = execs.clone();
        om.subscribe(move |exec| {
            execs_clone.lock().unwrap().push(exec);
        });

        let o2 = sample_order("o2", "u2", "BUY", 10, 100.0);
        om.place_order(o2).unwrap();

        let recorded = execs.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].quantity, 10);
    }
}

#[cfg(test)]
mod risk_tests {
    use crate::risk_manager::{RiskError, RiskManager};
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
        let mut rm = RiskManager::new();
        rm.set_limit("u1".into(), "AAPL".into(), 1000);
        assert!(rm.check_and_record(&make_order("u1", "AAPL", 500)).is_ok());
    }

    #[test]
    fn test_exceeds_limit_rejected() {
        let mut rm = RiskManager::new();
        rm.set_limit("u1".into(), "AAPL".into(), 1000);
        rm.check_and_record(&make_order("u1", "AAPL", 500)).unwrap();
        assert!(matches!(
            rm.check_and_record(&make_order("u1", "AAPL", 600)).unwrap_err(),
            RiskError::LimitExceeded { .. }
        ));
    }

    #[test]
    fn test_exact_limit_passes() {
        let mut rm = RiskManager::new();
        rm.set_limit("u1".into(), "AAPL".into(), 1000);
        rm.check_and_record(&make_order("u1", "AAPL", 500)).unwrap();
        assert!(rm.check_and_record(&make_order("u1", "AAPL", 500)).is_ok());
    }

    #[test]
    fn test_different_user_unaffected() {
        let mut rm = RiskManager::new();
        rm.set_limit("u1".into(), "AAPL".into(), 1000);
        rm.check_and_record(&make_order("u1", "AAPL", 900)).unwrap();
        assert!(rm.check_and_record(&make_order("u2", "AAPL", 9999)).is_ok());
    }

    #[test]
    fn test_no_limit_set_always_passes() {
        let mut rm = RiskManager::new();
        assert!(rm.check_and_record(&make_order("u1", "AAPL", 99999)).is_ok());
    }

    #[test]
    fn test_different_symbol_unaffected() {
        let mut rm = RiskManager::new();
        rm.set_limit("u1".into(), "AAPL".into(), 1000);
        rm.check_and_record(&make_order("u1", "AAPL", 900)).unwrap();
        assert!(rm.check_and_record(&make_order("u1", "TSLA", 9999)).is_ok());
    }
}

#[cfg(test)]
mod wallet_tests {
    use crate::{
        types::Side,
        wallet::{Wallet, WalletError},
    };

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
        assert_eq!(
            w.check_and_lock("u1", &Side::Buy, 1000.0, 10),
            Err(WalletError::InsufficientFunds)
        );
    }

    #[test]
    fn test_no_deposit_insufficient_funds() {
        let mut w = Wallet::new();
        assert_eq!(
            w.check_and_lock("u1", &Side::Buy, 100.0, 10),
            Err(WalletError::InsufficientFunds)
        );
    }
}
