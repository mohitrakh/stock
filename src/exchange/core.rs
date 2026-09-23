use crate::{
    sequencer::Sequencer,
    types::{
        matching_engine::MatchingEngine,
        order_manager::{OrderManager, OrderManagerError},
        types::{
            BalanceView, Execution, ExecutionView, Order, OrderBookView, OrderView, PositionView,
            RiskLimitView,
        },
    },
};

pub struct AddOrderOutcome {
    pub order_id: String,
    pub seq_num: u64,
    pub executions: Vec<Execution>,
    /// The order's post-match state, read back from `OrderManager` rather than summed from
    /// `executions` — the manager is the one authority on fill quantity and lifecycle state.
    pub view: OrderView,
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

    pub fn deposit(&mut self, user_id: String, amount: u64) -> Result<(), OrderManagerError> {
        self.order_manager.deposit_funds(user_id, amount)
    }

    pub fn set_risk_limit(&mut self, user_id: String, symbol: String, limit: u64) {
        self.order_manager.set_risk_limit(user_id, symbol, limit);
    }

    pub fn risk_limit_view(&self, user_id: &str, symbol: &str) -> RiskLimitView {
        self.order_manager.risk_limit_view(user_id, symbol)
    }

    pub fn execution_views(
        &self,
        user_id: &str,
        symbol: Option<&str>,
        order_id: Option<&str>,
        start_time: Option<f64>,
        end_time: Option<f64>,
    ) -> Vec<ExecutionView> {
        self.order_manager
            .execution_views(user_id, symbol, order_id, start_time, end_time)
    }

    pub fn deposit_shares(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), OrderManagerError> {
        self.order_manager.deposit_shares(user_id, symbol, quantity)
    }

    pub fn add_order(&mut self, order: Order) -> Result<AddOrderOutcome, OrderManagerError> {
        let mut order = self.order_manager.prepare_order(order)?;

        let seq_num = self.sequencer.next();
        order.seq_num = seq_num;

        let order_id = order.order_id.clone();
        let user_id = order.user_id.clone();
        self.order_manager.register_order(order.clone());

        let executions = self
            .matching_engine
            .process_order(order)
            .map_err(OrderManagerError::MatchingRejected)?;

        self.order_manager.apply_executions(&executions)?;

        let view = self
            .order_manager
            .order_view(&order_id, &user_id)
            .expect("order was registered above, so its view must exist");

        Ok(AddOrderOutcome {
            order_id,
            seq_num,
            executions,
            view,
        })
    }

    pub fn balance_view(&self, user_id: &str) -> BalanceView {
        self.order_manager.balance_view(user_id)
    }

    pub fn position_views(&self, user_id: &str) -> Vec<PositionView> {
        self.order_manager.position_views(user_id)
    }

    pub fn order_view(&self, order_id: &str, user_id: &str) -> Option<OrderView> {
        self.order_manager.order_view(order_id, user_id)
    }

    pub fn l2_snapshot(&self, symbol: &str, depth: usize) -> Option<OrderBookView> {
        self.matching_engine.l2_snapshot(symbol, depth)
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
    use crate::types::{order_manager::OrderState, types::Side};

    /// A core whose test users already hold AAPL, since a sell order now has to be backed by
    /// shares. Tests about matching, wallets and lifecycle should not each have to fund an
    /// inventory first; rejection of an unbacked sell has its own tests below.
    fn funded_core() -> ExchangeCore {
        let mut core = ExchangeCore::new();

        for user in ["seller", "alice", "bob"] {
            core.deposit_shares(user, "AAPL", 1_000).unwrap();
        }

        core
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
    fn buy_order_rests_when_there_is_no_seller() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();

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
        let mut core = funded_core();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 50).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 60).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 50).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 100).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 100).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 100).unwrap();

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
        let mut core = funded_core();

        let result = core.add_order(order("buy-1", "buyer", "BUY", 10, 5));

        assert!(matches!(result, Err(OrderManagerError::WalletRejected(_))));
        assert!(!core.order_manager.orders.contains_key("buy-1"));
        assert!(!core.matching_engine.is_resting("buy-1"));
        assert_eq!(core.order_manager.wallet.locked("buyer"), 0);
    }

    #[test]
    fn wallet_rejected_order_does_not_consume_risk_limit() {
        let mut core = funded_core();
        core.order_manager
            .risk_manager
            .set_limit("buyer".to_string(), "AAPL".to_string(), 5);

        let rejected = core.add_order(order("buy-1", "buyer", "BUY", 10, 5));
        assert!(matches!(
            rejected,
            Err(OrderManagerError::WalletRejected(_))
        ));

        core.deposit("buyer".to_string(), 50).unwrap();
        let accepted = core.add_order(order("buy-2", "buyer", "BUY", 10, 5));

        assert!(accepted.is_ok());
        assert_eq!(core.order_manager.get_state("buy-2"), Some(OrderState::New));
        assert!(core.matching_engine.is_resting("buy-2"));
    }

    #[test]
    fn duplicate_order_id_is_rejected() {
        let mut core = funded_core();

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
        let mut core = funded_core();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 3_075).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), u64::MAX).unwrap();

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
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_025).unwrap();

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

    #[test]
    fn a_sell_without_shares_is_rejected() {
        let mut core = ExchangeCore::new();

        let result = core.add_order(order("sell-1", "seller", "SELL", 10, 1));

        assert!(matches!(
            result,
            Err(OrderManagerError::PositionRejected(_))
        ));
        assert!(!core.matching_engine.is_resting("sell-1"));
        // And nothing was paid for shares that do not exist.
        assert_eq!(core.order_manager.wallet.balance("seller"), 0);
    }

    #[test]
    fn a_sell_larger_than_the_holding_is_rejected() {
        let mut core = ExchangeCore::new();
        core.deposit_shares("seller", "AAPL", 5).unwrap();

        assert!(matches!(
            core.add_order(order("sell-1", "seller", "SELL", 10, 6)),
            Err(OrderManagerError::PositionRejected(_))
        ));

        // Exactly the holding is fine, and then there is nothing left to sell again.
        core.add_order(order("sell-2", "seller", "SELL", 10, 5))
            .unwrap();
        assert!(matches!(
            core.add_order(order("sell-3", "seller", "SELL", 10, 1)),
            Err(OrderManagerError::PositionRejected(_))
        ));
    }

    #[test]
    fn a_fill_creates_no_cash_and_no_shares() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 1_000).unwrap();
        core.deposit_shares("seller", "AAPL", 10).unwrap();

        let cash_before = core.order_manager.wallet.balance("buyer")
            + core.order_manager.wallet.balance("seller");
        let shares_before = core.order_manager.positions.holding("buyer", "AAPL")
            + core.order_manager.positions.holding("seller", "AAPL");

        core.add_order(order("sell-1", "seller", "SELL", 10, 10))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 6))
            .unwrap();

        let cash_after = core.order_manager.wallet.balance("buyer")
            + core.order_manager.wallet.balance("seller");
        let shares_after = core.order_manager.positions.holding("buyer", "AAPL")
            + core.order_manager.positions.holding("seller", "AAPL");

        // The whole point of this milestone: a trade moves value between two parties, it does not
        // manufacture any. Before the position ledger existed, `cash_after` was 60 higher than
        // `cash_before` on exactly this sequence.
        assert_eq!(cash_before, cash_after);
        assert_eq!(shares_before, shares_after);

        // And it moved the right amounts in the right directions.
        assert_eq!(core.order_manager.wallet.balance("buyer"), 940);
        assert_eq!(core.order_manager.wallet.balance("seller"), 60);
        assert_eq!(core.order_manager.positions.holding("buyer", "AAPL"), 6);
        assert_eq!(core.order_manager.positions.holding("seller", "AAPL"), 4);

        // The seller's unsold 4 are still reserved behind the resting half of the order.
        assert_eq!(core.order_manager.positions.locked("seller", "AAPL"), 4);
        assert_eq!(core.order_manager.positions.available("seller", "AAPL"), 0);
    }

    #[test]
    fn cancelling_a_sell_returns_the_shares_to_available() {
        let mut core = ExchangeCore::new();
        core.deposit_shares("seller", "AAPL", 10).unwrap();

        core.add_order(order("sell-1", "seller", "SELL", 10, 10))
            .unwrap();
        assert_eq!(core.order_manager.positions.available("seller", "AAPL"), 0);

        core.cancel_order_for_user("sell-1", "seller").unwrap();

        assert_eq!(core.order_manager.positions.holding("seller", "AAPL"), 10);
        assert_eq!(core.order_manager.positions.locked("seller", "AAPL"), 0);
        assert_eq!(core.order_manager.positions.available("seller", "AAPL"), 10);

        // Which means they can be sold again.
        core.add_order(order("sell-2", "seller", "SELL", 10, 10))
            .unwrap();
    }

    #[test]
    fn shares_bought_can_then_be_sold() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();

        core.add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();

        // The buyer held nothing at the start; the fill is the only thing that can back this sell.
        assert_eq!(core.order_manager.positions.holding("buyer", "AAPL"), 5);
        core.add_order(order("resell-1", "buyer", "SELL", 11, 5))
            .unwrap();

        assert!(core.matching_engine.is_resting("resell-1"));
    }

    #[test]
    fn the_daily_limit_rejects_an_order_that_would_exceed_it() {
        let mut core = funded_core();
        core.deposit("trader".to_string(), 1_000_000).unwrap();
        core.set_risk_limit("trader".to_string(), "AAPL".to_string(), 10);

        core.add_order(order("buy-1", "trader", "BUY", 1, 6))
            .unwrap();

        let rejected = core.add_order(order("buy-2", "trader", "BUY", 1, 5));
        assert!(matches!(rejected, Err(OrderManagerError::RiskRejected(_))));

        // Rejected pre-trade: no order, no reservation, no sequence number consumed.
        assert!(!core.matching_engine.is_resting("buy-2"));
        assert_eq!(core.order_manager.wallet.locked("trader"), 6);

        let view = core.risk_limit_view("trader", "AAPL");
        assert_eq!(view.max_daily_quantity, 10);
        assert_eq!(view.used_today, 6);
    }

    #[test]
    fn the_default_limit_applies_without_anyone_setting_one() {
        let mut core = funded_core();

        let view = core.risk_limit_view("trader", "AAPL");
        assert_eq!(view.max_daily_quantity, 1_000_000);

        // A single order past the cap is refused even though no limit was ever configured.
        core.deposit("trader".to_string(), u64::MAX / 2).unwrap();
        assert!(matches!(
            core.add_order(order("huge", "trader", "BUY", 1, 1_000_001)),
            Err(OrderManagerError::RiskRejected(_))
        ));
    }

    #[test]
    fn cancelling_returns_the_days_risk_allowance() {
        let mut core = funded_core();
        core.deposit("trader".to_string(), 1_000).unwrap();
        core.set_risk_limit("trader".to_string(), "AAPL".to_string(), 10);

        core.add_order(order("buy-1", "trader", "BUY", 1, 10))
            .unwrap();
        assert_eq!(core.risk_limit_view("trader", "AAPL").used_today, 10);

        core.cancel_order_for_user("buy-1", "trader").unwrap();

        // Nothing traded, so nothing should stay counted against the day.
        assert_eq!(core.risk_limit_view("trader", "AAPL").used_today, 0);
        core.add_order(order("buy-2", "trader", "BUY", 1, 10))
            .unwrap();
    }

    #[test]
    fn executions_are_recorded_for_both_sides_with_their_own_side_and_order() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();

        core.add_order(order("sell-1", "seller", "SELL", 10, 10))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 4))
            .unwrap();

        let buyer_fills = core.execution_views("buyer", None, None, None, None);
        assert_eq!(buyer_fills.len(), 1);
        assert_eq!(buyer_fills[0].order_id, "buy-1");
        assert_eq!(buyer_fills[0].side, Side::Buy);
        assert_eq!(buyer_fills[0].quantity, 4);
        assert_eq!(buyer_fills[0].price, 10);

        // The same match, seen from the other side: the seller's own order id and side.
        let seller_fills = core.execution_views("seller", None, None, None, None);
        assert_eq!(seller_fills.len(), 1);
        assert_eq!(seller_fills[0].order_id, "sell-1");
        assert_eq!(seller_fills[0].side, Side::Sell);

        // And nobody sees anyone else's.
        assert!(
            core.execution_views("stranger", None, None, None, None)
                .is_empty()
        );
    }

    #[test]
    fn executions_can_be_filtered() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 10_000).unwrap();
        core.deposit_shares("seller", "MSFT", 100).unwrap();

        core.add_order(order("sell-aapl", "seller", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy-aapl", "buyer", "BUY", 10, 5))
            .unwrap();

        let msft_sell = Order::new(
            "sell-msft".to_string(),
            "seller".to_string(),
            "MSFT".to_string(),
            "SELL",
            20,
            5,
            None,
            50.0,
            0,
        )
        .unwrap();
        let msft_buy = Order::new(
            "buy-msft".to_string(),
            "buyer".to_string(),
            "MSFT".to_string(),
            "BUY",
            20,
            5,
            None,
            50.0,
            0,
        )
        .unwrap();
        core.add_order(msft_sell).unwrap();
        core.add_order(msft_buy).unwrap();

        assert_eq!(
            core.execution_views("buyer", None, None, None, None).len(),
            2
        );
        assert_eq!(
            core.execution_views("buyer", Some("MSFT"), None, None, None)
                .len(),
            1
        );
        assert_eq!(
            core.execution_views("buyer", None, Some("buy-aapl"), None, None)
                .len(),
            1
        );
        // The AAPL fill is stamped at 1.0 and the MSFT one at 50.0; bounds are inclusive.
        assert_eq!(
            core.execution_views("buyer", None, None, Some(2.0), None)
                .len(),
            1
        );
        assert_eq!(
            core.execution_views("buyer", None, None, None, Some(1.0))
                .len(),
            1
        );
        assert!(
            core.execution_views("buyer", None, None, Some(100.0), None)
                .is_empty()
        );
    }

    #[test]
    fn views_serialize_to_the_documented_json_shape() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();
        core.add_order(order("sell-1", "seller", "SELL", 10, 4))
            .unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();

        let order_json =
            serde_json::to_string(&core.order_view("buy-1", "buyer").unwrap()).unwrap();
        assert_eq!(
            order_json,
            r#"{"order_id":"buy-1","symbol":"AAPL","side":"buy","price":10,"quantity":10,"filled_quantity":4,"remaining_quantity":6,"status":"partially_filled","creation_time":1.0}"#
        );

        let balance_json = serde_json::to_string(&core.balance_view("buyer")).unwrap();
        assert_eq!(
            balance_json,
            r#"{"user_id":"buyer","balance":960,"locked":60,"available":900}"#
        );

        let book_json = serde_json::to_string(&core.l2_snapshot("AAPL", 10).unwrap()).unwrap();
        assert_eq!(
            book_json,
            r#"{"symbol":"AAPL","bids":[{"price":10,"quantity":6}],"asks":[]}"#
        );
    }

    #[test]
    fn self_trade_prevention_skips_own_order_and_fills_the_next_user() {
        let mut core = funded_core();
        core.deposit("alice".to_string(), 100_000).unwrap();

        // Alice is first in the queue at 100, so a naive self-trade check that stops matching
        // would hide Bob behind her and leave the book crossed.
        core.add_order(order("sell-alice", "alice", "SELL", 100, 5))
            .unwrap();
        core.add_order(order("sell-bob", "bob", "SELL", 100, 5))
            .unwrap();

        let outcome = core
            .add_order(order("buy-alice", "alice", "BUY", 100, 5))
            .unwrap();

        assert_eq!(outcome.executions.len(), 2);
        assert_eq!(outcome.executions[0].sell_order_id, "sell-bob");
        assert_eq!(outcome.executions[0].quantity, 5);

        assert_eq!(
            core.order_manager.get_state("buy-alice"),
            Some(OrderState::Filled)
        );
        assert_eq!(
            core.order_manager.get_state("sell-bob"),
            Some(OrderState::Filled)
        );
        // Alice's own resting sell is untouched by her buy.
        assert_eq!(
            core.order_manager.get_state("sell-alice"),
            Some(OrderState::New)
        );
        assert!(core.matching_engine.is_resting("sell-alice"));
    }

    #[test]
    fn aggressor_never_rests_across_a_matchable_counterparty() {
        let mut core = funded_core();
        core.deposit("alice".to_string(), 100_000).unwrap();

        core.add_order(order("sell-alice", "alice", "SELL", 100, 5))
            .unwrap();
        core.add_order(order("sell-bob", "bob", "SELL", 100, 5))
            .unwrap();
        core.add_order(order("buy-alice", "alice", "BUY", 100, 5))
            .unwrap();

        // Only Alice's own sell is left, so there is no bid at all — nothing crossed.
        let book = core.l2_snapshot("AAPL", 10).unwrap();
        assert!(book.bids.is_empty());
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.asks[0].price, 100);
        assert_eq!(book.asks[0].quantity, 5);
    }

    #[test]
    fn a_level_of_only_own_orders_does_not_block_a_worse_level() {
        let mut core = funded_core();
        core.deposit("alice".to_string(), 100_000).unwrap();

        core.add_order(order("sell-alice", "alice", "SELL", 100, 5))
            .unwrap();
        core.add_order(order("sell-bob", "bob", "SELL", 101, 5))
            .unwrap();

        // Alice bids through her own level at 100 and must still reach Bob at 101.
        let outcome = core
            .add_order(order("buy-alice", "alice", "BUY", 101, 5))
            .unwrap();

        assert_eq!(outcome.executions.len(), 2);
        assert_eq!(outcome.executions[0].sell_order_id, "sell-bob");
        assert_eq!(outcome.executions[0].price.minor_units(), 101);
    }
}
