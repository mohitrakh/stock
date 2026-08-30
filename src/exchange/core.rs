use crate::{
    sequencer::Sequencer,
    types::{
        matching_engine::MatchingEngine,
        order_manager::{OrderManager, OrderManagerError},
        types::{Execution, Order},
    },
};

pub struct AddOrderOutcome {
    pub order_id: String,
    pub seq_num: u64,
    pub executions: Vec<Execution>,
}

pub struct ExchangeCore {
    order_manager: OrderManager,
    matching_engine: MatchingEngine,
    sequencer: Sequencer,
}

impl ExchangeCore {
    pub fn new() -> Self {
        Self {
            order_manager: OrderManager::new(),
            matching_engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
        }
    }

    pub fn deposit(&mut self, user_id: String, amount: u64) {
        self.order_manager.wallet.deposit(user_id, amount);
    }

    pub fn add_order(&mut self, order: Order) -> Result<AddOrderOutcome, OrderManagerError> {
        let mut order = self.order_manager.prepare_order(order)?;

        let seq_num = self.sequencer.next();
        order.seq_num = seq_num;

        let order_id = order.order_id.clone();
        self.order_manager.register_order(order.clone());

        let executions = self
            .matching_engine
            .process_order(order)
            .map_err(OrderManagerError::MatchingRejected)?;

        self.order_manager.apply_executions(&executions)?;

        Ok(AddOrderOutcome {
            order_id,
            seq_num,
            executions,
        })
    }

    pub fn cancel_order_for_user(
        &mut self,
        order_id: &str,
        user_id: &str,
    ) -> Result<u64, OrderManagerError> {
        self.order_manager
            .validate_cancel_for_user(order_id, user_id)?;

        let cancel_seq = self.sequencer.next();
        let removed = self
            .matching_engine
            .cancel_order(order_id, cancel_seq)
            .map_err(OrderManagerError::OrderNotFound)?;

        if removed.is_none() {
            return Err(OrderManagerError::OrderNotFound(order_id.to_string()));
        }

        self.order_manager.complete_cancel(order_id)?;

        Ok(cancel_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::order_manager::OrderState;

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
    fn buy_order_rests_when_there_is_no_seller() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 1_000);

        core.add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();

        assert_eq!(core.order_manager.get_state("buy-1"), Some(OrderState::New));
        assert_eq!(core.order_manager.orders["buy-1"].remaining_quantity, 10);
        assert!(core.matching_engine.is_resting("buy-1"));
        assert_eq!(core.order_manager.wallet.balance("buyer"), 1_000);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 100);
        assert_eq!(core.order_manager.wallet.available("buyer"), 900);
    }

    #[test]
    fn sell_order_rests_when_there_is_no_buyer() {
        let mut core = ExchangeCore::new();

        core.add_order(order("sell-1", "seller", "SELL", 10, 10))
            .unwrap();

        assert_eq!(
            core.order_manager.get_state("sell-1"),
            Some(OrderState::New)
        );
        assert_eq!(core.order_manager.orders["sell-1"].remaining_quantity, 10);
        assert!(core.matching_engine.is_resting("sell-1"));
    }

    #[test]
    fn buy_fully_matches_resting_sell() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 50);

        core.add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();

        assert_eq!(
            core.order_manager.get_state("sell-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(core.order_manager.orders["sell-1"].remaining_quantity, 0);
        assert_eq!(core.order_manager.orders["buy-1"].remaining_quantity, 0);
        assert!(!core.matching_engine.is_resting("sell-1"));
        assert!(!core.matching_engine.is_resting("buy-1"));
        assert_eq!(core.order_manager.wallet.balance("buyer"), 0);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn buy_pays_execution_price_and_releases_price_improvement_lock() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 60);

        core.add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 12, 5))
            .unwrap();

        assert_eq!(
            core.order_manager.get_state("sell-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(core.order_manager.wallet.balance("buyer"), 10);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.available("buyer"), 10);
        assert_eq!(core.order_manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn buy_partially_matches_resting_sell() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 50);

        core.add_order(order("sell-1", "seller", "SELL", 10, 10))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();

        assert_eq!(
            core.order_manager.get_state("sell-1"),
            Some(OrderState::PartiallyFilled)
        );
        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(core.order_manager.orders["sell-1"].remaining_quantity, 5);
        assert_eq!(core.order_manager.orders["buy-1"].remaining_quantity, 0);
        assert!(core.matching_engine.is_resting("sell-1"));
        assert!(!core.matching_engine.is_resting("buy-1"));
        assert_eq!(core.order_manager.wallet.balance("buyer"), 0);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn sell_fully_matches_resting_buy() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 100);

        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();
        core.add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();

        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(
            core.order_manager.get_state("sell-1"),
            Some(OrderState::Filled)
        );
        assert_eq!(core.order_manager.orders["buy-1"].remaining_quantity, 0);
        assert_eq!(core.order_manager.orders["sell-1"].remaining_quantity, 0);
        assert!(!core.matching_engine.is_resting("buy-1"));
        assert!(!core.matching_engine.is_resting("sell-1"));
        assert_eq!(core.order_manager.wallet.balance("buyer"), 50);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.balance("seller"), 50);
    }

    #[test]
    fn cancel_resting_buy_unlocks_remaining_funds() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 100);

        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();
        core.cancel_order_for_user("buy-1", "buyer").unwrap();

        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Canceled)
        );
        assert_eq!(core.order_manager.wallet.balance("buyer"), 100);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.available("buyer"), 100);
        assert!(!core.matching_engine.is_resting("buy-1"));
    }

    #[test]
    fn cancel_by_wrong_user_is_rejected() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 100);

        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();

        let result = core.cancel_order_for_user("buy-1", "not-buyer");

        assert!(matches!(result, Err(OrderManagerError::Unauthorized(_))));
        assert_eq!(core.order_manager.get_state("buy-1"), Some(OrderState::New));
        assert_eq!(core.order_manager.wallet.locked("buyer"), 50);
        assert!(core.matching_engine.is_resting("buy-1"));
    }

    #[test]
    fn insufficient_funds_rejects_buy_order() {
        let mut core = ExchangeCore::new();

        let result = core.add_order(order("buy-1", "buyer", "BUY", 10, 5));

        assert!(matches!(result, Err(OrderManagerError::WalletRejected(_))));
        assert!(!core.order_manager.orders.contains_key("buy-1"));
        assert!(!core.matching_engine.is_resting("buy-1"));
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
    }

    #[test]
    fn wallet_rejected_order_does_not_consume_risk_limit() {
        let mut core = ExchangeCore::new();
        core.order_manager
            .risk_manager
            .set_limit("buyer".to_string(), "AAPL".to_string(), 5);

        let rejected = core.add_order(order("buy-1", "buyer", "BUY", 10, 5));
        assert!(matches!(
            rejected,
            Err(OrderManagerError::WalletRejected(_))
        ));

        core.deposit("buyer".to_string(), 50);
        let accepted = core.add_order(order("buy-2", "buyer", "BUY", 10, 5));

        assert!(accepted.is_ok());
        assert_eq!(core.order_manager.get_state("buy-2"), Some(OrderState::New));
        assert!(core.matching_engine.is_resting("buy-2"));
    }

    #[test]
    fn duplicate_order_id_is_rejected() {
        let mut core = ExchangeCore::new();

        core.add_order(order("same-id", "seller", "SELL", 10, 5))
            .unwrap();
        let result = core.add_order(order("same-id", "seller", "SELL", 11, 5));

        assert!(matches!(result, Err(OrderManagerError::AlreadyExists(_))));
        assert_eq!(
            core.order_manager.orders["same-id"]
                .order
                .price
                .minor_units(),
            10
        );
    }

    #[test]
    fn rejected_operations_do_not_consume_matching_sequence() {
        let mut core = ExchangeCore::new();

        let rejected_order = core.add_order(order("buy-1", "buyer", "BUY", 10, 5));

        assert!(matches!(
            rejected_order,
            Err(OrderManagerError::WalletRejected(_))
        ));

        let first_accepted = core
            .add_order(order("sell-1", "seller", "SELL", 20, 5))
            .unwrap();

        assert_eq!(first_accepted.seq_num, 1);

        let rejected_cancel = core.cancel_order_for_user("sell-1", "intruder");

        assert!(matches!(
            rejected_cancel,
            Err(OrderManagerError::Unauthorized(_))
        ));

        let second_accepted = core
            .add_order(order("sell-2", "seller", "SELL", 21, 5))
            .unwrap();

        assert_eq!(second_accepted.seq_num, 2);
    }

    #[test]
    fn exact_minor_unit_price_survives_partial_fill_and_cancel() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 3_075);

        core.add_order(order("sell-1", "seller", "SELL", 1_025, 1))
            .unwrap();
        let outcome = core
            .add_order(order("buy-1", "buyer", "BUY", 1_025, 3))
            .unwrap();

        assert_eq!(outcome.executions.len(), 2);
        assert!(
            outcome
                .executions
                .iter()
                .all(|execution| execution.price.minor_units() == 1_025)
        );
        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::PartiallyFilled)
        );
        assert_eq!(core.order_manager.wallet.balance("buyer"), 2_050);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 2_050);
        assert_eq!(core.order_manager.wallet.balance("seller"), 1_025);

        core.cancel_order_for_user("buy-1", "buyer").unwrap();

        assert_eq!(
            core.order_manager.get_state("buy-1"),
            Some(OrderState::Canceled)
        );
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert_eq!(core.order_manager.wallet.available("buyer"), 2_050);
    }

    #[test]
    fn overflowing_notional_is_rejected_without_locking_funds() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), u64::MAX);

        let result = core.add_order(order("buy-1", "buyer", "BUY", u64::MAX, 2));

        assert!(matches!(
            result,
            Err(OrderManagerError::WalletRejected(reason)) if reason == "Overflow"
        ));
        assert_eq!(core.order_manager.wallet.balance("buyer"), u64::MAX);
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
        assert!(!core.order_manager.orders.contains_key("buy-1"));
        assert!(!core.matching_engine.is_resting("buy-1"));
    }

    #[test]
    fn adjacent_minor_unit_prices_remain_distinct_book_levels() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 1_025);

        core.add_order(order("sell-1", "seller", "SELL", 1_026, 1))
            .unwrap();
        let buy_outcome = core
            .add_order(order("buy-1", "buyer", "BUY", 1_025, 1))
            .unwrap();

        assert!(buy_outcome.executions.is_empty());
        assert!(core.matching_engine.is_resting("sell-1"));
        assert!(core.matching_engine.is_resting("buy-1"));

        let ((bid, bid_quantity), (ask, ask_quantity)) =
            core.matching_engine.best_bid_ask("AAPL").unwrap();

        assert_eq!(bid.minor_units(), 1_025);
        assert_eq!(ask.minor_units(), 1_026);
        assert_eq!(bid_quantity, 1);
        assert_eq!(ask_quantity, 1);
    }
}
