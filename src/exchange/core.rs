use crate::{
    sequencer::Sequencer,
    types::{
        matching_engine::{MAX_RESTING_ORDERS, MatchingEngine},
        order_manager::{OrderManager, OrderManagerError, PreparedCancel, PreparedNewOrder},
        types::{
            BalanceView, Execution, ExecutionView, Order, OrderBookView, OrderView, PositionView,
            RiskLimitView, SessionView,
        },
    },
};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// The trading session. Only journaled open and close commands change it, so replay rebuilds it
/// exactly. A new exchange has never opened: no trading day, and closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Session {
    trading_day: Option<NaiveDate>,
    open: bool,
}

/// Why an open or close was refused. These are ordinary business rejections: they are journaled
/// as `SessionRejected` and change nothing.
#[derive(Debug, PartialEq)]
pub enum SessionError {
    AlreadyOpen,
    /// Trading days only move forward, so a replayed history can never revisit a day.
    NotAfterLastTradingDay(NaiveDate),
    AlreadyClosed,
    /// The close's single journal record, with one expiry for each of these resting orders, would
    /// exceed the record size limit. The resting-order cap prevents this for orders that passed
    /// the gateway's id check; it remains a safety net for any other entry point.
    TooManyRestingOrders(usize),
}

/// The close's expiry of every resting order, planned without changing anything.
pub(crate) struct PreparedExpiry {
    /// Every resting order, oldest acceptance first, with the matching sequence its expiry
    /// consumes, as a cancellation's does.
    pub(crate) expired: Vec<(String, u64)>,
    manager: crate::types::order_manager::PreparedExpiry,
}

pub struct AddOrderOutcome {
    pub order_id: String,
    pub seq_num: u64,
    pub executions: Vec<Execution>,
    /// The order's post-match state, read back from `OrderManager` rather than summed from
    /// `executions` — the manager is the one authority on fill quantity and lifecycle state.
    pub view: OrderView,
}

#[derive(Debug)]
pub enum CoreError {
    Business(OrderManagerError),
    Internal(String),
}

pub(crate) struct PreparedAddOrder {
    pub(crate) order: Order,
    pub(crate) seq_num: u64,
    pub(crate) executions: Vec<Execution>,
    pub(crate) view: OrderView,
    pub(crate) matching: crate::types::matching_engine::PreparedOrder,
    pub(crate) manager: PreparedNewOrder,
}

pub(crate) struct PreparedCancelOrder {
    pub(crate) order_id: String,
    pub(crate) seq_num: u64,
    pub(crate) matching: crate::types::matching_engine::PreparedCancel,
    pub(crate) manager: PreparedCancel,
}

pub struct ExchangeCore {
    order_manager: OrderManager,
    matching_engine: MatchingEngine,
    sequencer: Sequencer,
    session: Session,
}

/// Complete deterministic state required to resume the single-owner exchange core. It has no
/// filesystem, queue, HTTP, callback, or mmap handles; those remain runtime concerns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CoreSnapshot {
    order_manager: crate::types::order_manager::OrderManagerSnapshot,
    matching_engine: crate::types::matching_engine::MatchingEngineSnapshot,
    next_matching_sequence: u64,
    session: Session,
}

impl ExchangeCore {
    pub fn new() -> Self {
        Self {
            order_manager: OrderManager::new(),
            matching_engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
            session: Session::default(),
        }
    }

    /// Whether new orders are accepted. The runtime checks this before preparing an order; core
    /// methods such as `add_order` do not, so unit tests can exercise matching on their own.
    pub fn is_market_open(&self) -> bool {
        self.session.open
    }

    pub fn session_view(&self) -> SessionView {
        SessionView {
            trading_day: self.session.trading_day,
            open: self.session.open,
        }
    }

    /// A trading day opens only while the market is closed, and only for a day later than the
    /// last one.
    pub(crate) fn prepare_open_market(&self, trading_day: NaiveDate) -> Result<(), SessionError> {
        if self.session.open {
            return Err(SessionError::AlreadyOpen);
        }
        if let Some(last) = self.session.trading_day
            && trading_day <= last
        {
            return Err(SessionError::NotAfterLastTradingDay(last));
        }
        Ok(())
    }

    /// Starts the day: the previous day's finished orders and fills leave memory, and daily risk
    /// usage restarts from zero. Balances, positions, risk limits, and each symbol's book with its
    /// execution counter stay, so an execution id never repeats.
    pub(crate) fn commit_open_market(&mut self, trading_day: NaiveDate) {
        self.session = Session {
            trading_day: Some(trading_day),
            open: true,
        };
        self.order_manager.start_trading_day();
    }

    /// Returns the day being closed.
    pub(crate) fn prepare_close_market(&self) -> Result<NaiveDate, SessionError> {
        match self.session {
            Session {
                trading_day: Some(day),
                open: true,
            } => Ok(day),
            _ => Err(SessionError::AlreadyClosed),
        }
    }

    /// Plans the close's expiry of every resting order. They expire oldest first, by the
    /// matching sequence that accepted them, because the books live in a hash map whose order
    /// differs between processes and replay must reproduce the same sequences. The order id breaks
    /// a tie, which only a damaged snapshot could create. An error means the ledgers disagree with
    /// the books: an internal fault.
    pub(crate) fn prepare_expiry(&self) -> Result<PreparedExpiry, String> {
        let mut resting = self.matching_engine.resting_orders();
        resting.sort_by(|left, right| {
            (left.seq_num, &left.order_id).cmp(&(right.seq_num, &right.order_id))
        });
        let manager = self
            .order_manager
            .prepare_expiry(resting.iter().map(|order| order.order_id.as_str()))
            .map_err(|error| format!("{:?}", error))?;
        let expired = resting
            .into_iter()
            .zip(self.sequencer.peek()..)
            .map(|(order, seq_num)| (order.order_id, seq_num))
            .collect();
        Ok(PreparedExpiry { expired, manager })
    }

    /// Closes the market. Every resting order expires and leaves its book.
    pub(crate) fn commit_close_market(&mut self, expiry: PreparedExpiry) {
        if let Some(&(_, last_seq)) = expiry.expired.last() {
            for (_, seq_num) in &expiry.expired {
                self.sequencer.commit(*seq_num);
            }
            self.matching_engine.commit_expire_all(last_seq);
        }
        self.order_manager.commit_expiry(expiry.manager);
        self.session.open = false;
    }

    #[cfg(test)]
    pub(crate) fn open_market(&mut self, trading_day: NaiveDate) -> Result<(), SessionError> {
        self.prepare_open_market(trading_day)?;
        self.commit_open_market(trading_day);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn close_market(&mut self) -> Result<NaiveDate, SessionError> {
        let day = self.prepare_close_market()?;
        let expiry = self
            .prepare_expiry()
            .expect("test ledgers agree with the books");
        self.commit_close_market(expiry);
        Ok(day)
    }

    pub fn deposit(&mut self, user_id: String, amount: u64) -> Result<(), OrderManagerError> {
        self.order_manager.deposit_funds(user_id, amount)
    }

    pub(crate) fn validate_deposit(
        &self,
        user_id: &str,
        amount: u64,
    ) -> Result<(), OrderManagerError> {
        self.order_manager
            .wallet
            .validate_deposit(user_id, amount)
            .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))
    }

    pub(crate) fn commit_deposit(&mut self, user_id: String, amount: u64) {
        self.order_manager.wallet.commit_deposit(user_id, amount);
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

    pub(crate) fn notify_executions(&self, executions: &[Execution]) {
        self.order_manager.notify_executions(executions);
    }

    #[cfg(test)]
    pub(crate) fn subscribe<F: Fn(Execution) + Send + Sync + 'static>(&mut self, callback: F) {
        self.order_manager.subscribe(callback);
    }

    pub fn deposit_shares(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), OrderManagerError> {
        self.order_manager.deposit_shares(user_id, symbol, quantity)
    }

    pub(crate) fn validate_share_deposit(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), OrderManagerError> {
        self.order_manager
            .positions
            .validate_credit(user_id, symbol, quantity)
            .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))
    }

    pub(crate) fn commit_share_deposit(&mut self, user_id: &str, symbol: &str, quantity: u64) {
        self.order_manager
            .positions
            .commit_credit(user_id, symbol, quantity);
    }

    pub(crate) fn commit_risk_limit(&mut self, user_id: String, symbol: String, limit: u64) {
        self.set_risk_limit(user_id, symbol, limit);
    }

    pub(crate) fn prepare_add_order(&self, order: Order) -> Result<PreparedAddOrder, CoreError> {
        self.prepare_add_order_within(order, MAX_RESTING_ORDERS)
    }

    /// `max_resting` is `MAX_RESTING_ORDERS`, except in tests that cannot build that many orders.
    fn prepare_add_order_within(
        &self,
        mut order: Order,
        max_resting: usize,
    ) -> Result<PreparedAddOrder, CoreError> {
        let seq_num = self.sequencer.peek();
        order.seq_num = seq_num;
        self.order_manager
            .validate_new_order(&order)
            .map_err(CoreError::Business)?;

        let matching = self
            .matching_engine
            .prepare_order(order.clone())
            .map_err(CoreError::Internal)?;
        // The cap keeps the close's single journal record within its size limit.
        if self
            .matching_engine
            .exceeds_capacity(&matching.plan, max_resting)
        {
            return Err(CoreError::Business(OrderManagerError::BookFull));
        }
        let executions = matching.plan.executions.clone();
        let manager = self
            .order_manager
            .prepare_new_order(order.clone(), &executions)
            .map_err(|error| match error {
                OrderManagerError::Internal(reason)
                | OrderManagerError::MatchingRejected(reason) => CoreError::Internal(reason),
                other => CoreError::Business(other),
            })?;
        let view = self
            .order_manager
            .planned_order_view(&order, &manager.settlement);

        Ok(PreparedAddOrder {
            order,
            seq_num,
            executions,
            view,
            matching,
            manager,
        })
    }

    pub(crate) fn commit_add_order(&mut self, prepared: PreparedAddOrder) {
        self.order_manager.commit_new_order(prepared.manager);
        self.matching_engine.commit_order(prepared.matching);
        self.sequencer.commit(prepared.seq_num);
    }

    pub(crate) fn prepare_cancel_order(
        &self,
        order_id: &str,
        user_id: &str,
    ) -> Result<PreparedCancelOrder, CoreError> {
        let seq_num = self.sequencer.peek();
        let manager = self
            .order_manager
            .prepare_cancel_plan(order_id, user_id)
            .map_err(|error| match error {
                OrderManagerError::Internal(reason)
                | OrderManagerError::MatchingRejected(reason) => CoreError::Internal(reason),
                other => CoreError::Business(other),
            })?;
        let matching = self
            .matching_engine
            .prepare_cancel(order_id, seq_num)
            .map_err(CoreError::Internal)?;

        Ok(PreparedCancelOrder {
            order_id: order_id.to_string(),
            seq_num,
            matching,
            manager,
        })
    }

    pub(crate) fn commit_cancel_order(&mut self, prepared: PreparedCancelOrder) {
        self.order_manager.commit_cancel_plan(prepared.manager);
        self.matching_engine.commit_cancel(prepared.matching);
        self.sequencer.commit(prepared.seq_num);
    }

    pub fn add_order(&mut self, order: Order) -> Result<AddOrderOutcome, OrderManagerError> {
        let prepared = self.prepare_add_order(order).map_err(|error| match error {
            CoreError::Business(error) => error,
            CoreError::Internal(reason) => OrderManagerError::Internal(reason),
        })?;
        let outcome = AddOrderOutcome {
            order_id: prepared.order.order_id.clone(),
            seq_num: prepared.seq_num,
            executions: prepared.executions.clone(),
            view: prepared.view.clone(),
        };
        self.commit_add_order(prepared);

        Ok(outcome)
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
        let prepared =
            self.prepare_cancel_order(order_id, user_id)
                .map_err(|error| match error {
                    CoreError::Business(error) => error,
                    CoreError::Internal(reason) => OrderManagerError::Internal(reason),
                })?;
        let cancel_seq = prepared.seq_num;
        self.commit_cancel_order(prepared);
        Ok(cancel_seq)
    }

    pub(crate) fn snapshot(&self) -> CoreSnapshot {
        CoreSnapshot {
            order_manager: self.order_manager.snapshot(),
            matching_engine: self.matching_engine.snapshot(),
            next_matching_sequence: self.sequencer.next_sequence(),
            session: self.session,
        }
    }

    pub(crate) fn from_snapshot(snapshot: CoreSnapshot) -> Result<Self, String> {
        if snapshot.next_matching_sequence == 0 {
            return Err("core snapshot has an invalid next matching sequence".to_string());
        }
        if snapshot.session.open && snapshot.session.trading_day.is_none() {
            return Err("core snapshot has an open market without a trading day".to_string());
        }
        let order_manager = OrderManager::from_snapshot(snapshot.order_manager)?;
        let matching_engine = MatchingEngine::from_snapshot(snapshot.matching_engine)?;
        if matching_engine
            .last_sequence()
            .checked_add(1)
            .filter(|sequence| *sequence == snapshot.next_matching_sequence)
            .is_none()
        {
            return Err("core snapshot has inconsistent matching sequence state".to_string());
        }

        let core = Self {
            order_manager,
            matching_engine,
            sequencer: Sequencer::new(snapshot.next_matching_sequence),
            session: snapshot.session,
        };
        core.validate_snapshot()?;
        Ok(core)
    }

    fn validate_snapshot(&self) -> Result<(), String> {
        self.order_manager.validate_collateral()?;

        let mut resting = std::collections::HashMap::new();
        for order in self.matching_engine.resting_orders() {
            if resting.insert(order.order_id.clone(), order).is_some() {
                return Err("core snapshot contains a duplicated resting order".to_string());
            }
        }
        // The close expires every resting order, and none is accepted while closed.
        if !self.session.open && !resting.is_empty() {
            return Err("core snapshot has resting orders while the market is closed".to_string());
        }

        for managed in self.order_manager.orders.values() {
            let is_resting = managed.state.is_resting();
            let book_order = resting.remove(&managed.order.order_id);
            if is_resting != book_order.is_some() {
                return Err(format!(
                    "core snapshot disagrees about whether order {} is resting",
                    managed.order.order_id
                ));
            }
            if let Some(book_order) = book_order
                && (book_order.user_id != managed.order.user_id
                    || book_order.symbol != managed.order.symbol
                    || book_order.side != managed.order.side
                    || book_order.price != managed.order.price
                    || book_order.quantity != managed.order.quantity
                    || book_order.leaves_qty != managed.remaining_quantity)
            {
                return Err(format!(
                    "core snapshot has inconsistent resting order {}",
                    managed.order.order_id
                ));
            }
        }
        if let Some(order_id) = resting.keys().next() {
            return Err(format!(
                "core snapshot has resting order {} missing from order management",
                order_id
            ));
        }

        Ok(())
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

    fn day(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 10, day).unwrap()
    }

    /// Every order is a day order: the close expires whatever still rests, and leaves the ledgers
    /// exactly where cancelling each of those orders would have left them.
    #[test]
    fn the_close_expires_every_resting_order_exactly_like_a_cancellation() {
        let trade_then_rest = |core: &mut ExchangeCore| {
            core.deposit("buyer".to_string(), 1_000).unwrap();
            core.open_market(day(1)).unwrap();
            core.add_order(order("sell", "seller", "SELL", 10, 10))
                .unwrap();
            // Takes 4 of the sell, which keeps resting with 6.
            core.add_order(order("buy-filled", "buyer", "BUY", 10, 4))
                .unwrap();
            core.add_order(order("buy-resting", "buyer", "BUY", 9, 5))
                .unwrap();
            core.add_order(order("alice-sell", "alice", "SELL", 12, 3))
                .unwrap();
        };
        let mut expired = funded_core();
        trade_then_rest(&mut expired);
        let mut canceled = funded_core();
        trade_then_rest(&mut canceled);

        assert_eq!(expired.close_market(), Ok(day(1)));
        for (order_id, user) in [
            ("sell", "seller"),
            ("buy-resting", "buyer"),
            ("alice-sell", "alice"),
        ] {
            canceled.cancel_order_for_user(order_id, user).unwrap();
        }
        canceled.close_market().unwrap();

        for user in ["buyer", "seller", "alice"] {
            assert_eq!(expired.balance_view(user), canceled.balance_view(user));
            assert_eq!(expired.position_views(user), canceled.position_views(user));
            assert_eq!(
                expired.risk_limit_view(user, "AAPL"),
                canceled.risk_limit_view(user, "AAPL")
            );
        }
        assert_eq!(expired.balance_view("buyer").locked, 0);
        assert_eq!(expired.position_views("seller")[0].locked, 0);
        // The fill still counts against the day it traded on; the expired quantity does not.
        assert_eq!(expired.risk_limit_view("buyer", "AAPL").used_today, 4);
        assert_eq!(expired.risk_limit_view("seller", "AAPL").used_today, 4);

        assert_eq!(
            expired.order_manager.get_state("sell"),
            Some(OrderState::Expired)
        );
        assert_eq!(
            expired.order_manager.get_state("buy-filled"),
            Some(OrderState::Filled)
        );
        let sell = expired.order_view("sell", "seller").unwrap();
        assert_eq!(
            (sell.status.as_str(), sell.remaining_quantity),
            ("expired", 6)
        );
        let book = expired.l2_snapshot("AAPL", 10).unwrap();
        assert!(book.bids.is_empty() && book.asks.is_empty());
    }

    #[test]
    fn expiries_take_sequences_oldest_first_and_the_next_day_continues_both_counters() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();
        core.open_market(day(1)).unwrap();
        core.add_order(order("first", "seller", "SELL", 12, 2))
            .unwrap();
        core.add_order(order("second", "buyer", "BUY", 9, 1))
            .unwrap();
        let trade = core
            .add_order(order("third", "buyer", "BUY", 12, 1))
            .unwrap();
        assert_eq!(trade.executions[0].execution_id, "exec_0");

        // The book holds "second" (a bid) ahead of "first" (an ask). Expiry goes by acceptance,
        // and each expiry consumes the next matching sequence.
        let expiry = core.prepare_expiry().unwrap();
        assert_eq!(
            expiry.expired,
            vec![("first".to_string(), 4), ("second".to_string(), 5)]
        );
        core.commit_close_market(expiry);

        assert!(matches!(
            core.cancel_order_for_user("first", "seller"),
            Err(OrderManagerError::InvalidTransition(_))
        ));
        let restored = ExchangeCore::from_snapshot(core.snapshot()).unwrap();
        assert_eq!(restored.snapshot(), core.snapshot());

        // The next day continues the matching sequence, and each book's execution ids.
        core.open_market(day(2)).unwrap();
        core.add_order(order("ask", "alice", "SELL", 10, 1))
            .unwrap();
        let next = core.add_order(order("bid", "buyer", "BUY", 10, 1)).unwrap();
        assert_eq!(next.seq_num, 7);
        assert_eq!(next.executions[0].execution_id, "exec_2");
    }

    #[test]
    fn the_next_open_clears_the_previous_day_and_its_ids_return() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();
        core.open_market(day(1)).unwrap();
        core.add_order(order("sell", "seller", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy", "buyer", "BUY", 10, 3)).unwrap();
        core.close_market().unwrap();

        // Until the next open, the day just closed can still be read.
        assert_eq!(core.order_view("sell", "seller").unwrap().status, "expired");
        assert_eq!(
            core.execution_views("buyer", None, None, None, None).len(),
            1
        );

        core.open_market(day(2)).unwrap();
        assert!(core.order_manager.orders.is_empty());
        assert!(core.order_view("buy", "buyer").is_none());
        assert!(
            core.execution_views("buyer", None, None, None, None)
                .is_empty()
        );
        // Cash, shares and each book's execution counter carry over.
        assert_eq!(core.balance_view("buyer").balance, 970);
        assert_eq!(core.position_views("buyer")[0].quantity, 3);
        core.add_order(order("sell", "seller", "SELL", 10, 1))
            .unwrap();
        let again = core.add_order(order("buy", "buyer", "BUY", 10, 1)).unwrap();
        assert_eq!(again.executions[0].execution_id, "exec_2");
        assert_eq!(
            ExchangeCore::from_snapshot(core.snapshot())
                .unwrap()
                .snapshot(),
            core.snapshot()
        );
    }

    #[test]
    fn an_order_that_would_rest_beyond_the_cap_is_refused_but_trading_goes_on() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 1_000).unwrap();
        // A cap of two resting orders stands in for the real 200,000.
        let add = |core: &mut ExchangeCore, order: Order| {
            core.prepare_add_order_within(order, 2)
                .map(|prepared| core.commit_add_order(prepared))
        };
        add(&mut core, order("ask-10", "seller", "SELL", 10, 1)).unwrap();
        add(&mut core, order("ask-11", "alice", "SELL", 11, 1)).unwrap();

        assert!(matches!(
            add(&mut core, order("ask-12", "bob", "SELL", 12, 1)),
            Err(CoreError::Business(OrderManagerError::BookFull))
        ));
        assert!(!core.matching_engine.is_resting("ask-12"));
        // A full book still trades: an order that does not rest is accepted, which frees a place.
        add(&mut core, order("buy-10", "buyer", "BUY", 10, 1)).unwrap();
        add(&mut core, order("bid-9", "buyer", "BUY", 9, 1)).unwrap();
        // Full again. An order that takes a resting order out as it rests keeps the count level.
        add(&mut core, order("buy-11", "buyer", "BUY", 11, 2)).unwrap();
        assert!(core.matching_engine.is_resting("buy-11"));
        assert!(matches!(
            add(&mut core, order("bid-8", "buyer", "BUY", 8, 1)),
            Err(CoreError::Business(OrderManagerError::BookFull))
        ));
    }

    #[test]
    fn a_snapshot_cannot_hold_resting_orders_while_the_market_is_closed() {
        let mut core = funded_core();
        core.open_market(day(1)).unwrap();
        core.add_order(order("sell", "seller", "SELL", 10, 1))
            .unwrap();
        let mut snapshot = core.snapshot();
        snapshot.session.open = false;

        assert!(ExchangeCore::from_snapshot(snapshot).is_err());
    }

    #[test]
    fn a_new_trading_day_expires_the_previous_days_traded_usage() {
        let mut core = funded_core();
        core.deposit("trader".to_string(), 1_000).unwrap();
        core.set_risk_limit("trader".to_string(), "AAPL".to_string(), 4);

        core.open_market(day(1)).unwrap();
        core.add_order(order("sell", "seller", "SELL", 10, 4))
            .unwrap();
        core.add_order(order("buy", "trader", "BUY", 10, 4))
            .unwrap();
        assert!(matches!(
            core.add_order(order("more", "trader", "BUY", 10, 1)),
            Err(OrderManagerError::RiskRejected(_))
        ));
        core.close_market().unwrap();

        // The day is the operator's trading day, not the order timestamp, so nothing about the
        // orders themselves decides when the allowance comes back.
        core.open_market(day(2)).unwrap();
        assert_eq!(core.risk_limit_view("trader", "AAPL").used_today, 0);
        core.add_order(order("next-day", "trader", "BUY", 10, 4))
            .unwrap();
    }

    #[test]
    fn the_session_opens_only_forward_and_closes_only_when_open() {
        let mut core = ExchangeCore::new();
        assert_eq!(
            core.session_view(),
            SessionView {
                trading_day: None,
                open: false
            }
        );
        assert_eq!(core.close_market(), Err(SessionError::AlreadyClosed));

        core.open_market(day(2)).unwrap();
        assert!(core.is_market_open());
        assert_eq!(core.open_market(day(3)), Err(SessionError::AlreadyOpen));
        assert_eq!(core.close_market(), Ok(day(2)));
        assert_eq!(core.close_market(), Err(SessionError::AlreadyClosed));

        assert_eq!(
            core.open_market(day(2)),
            Err(SessionError::NotAfterLastTradingDay(day(2)))
        );
        assert_eq!(
            core.open_market(day(1)),
            Err(SessionError::NotAfterLastTradingDay(day(2)))
        );
        core.open_market(day(3)).unwrap();
        assert_eq!(
            core.session_view(),
            SessionView {
                trading_day: Some(day(3)),
                open: true
            }
        );
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

    #[test]
    fn late_seller_credit_overflow_leaves_the_command_uncommitted() {
        let mut core = funded_core();
        core.deposit("seller".to_string(), u64::MAX).unwrap();
        core.deposit("buyer".to_string(), 10).unwrap();
        core.add_order(order("sell-1", "seller", "SELL", 10, 1))
            .unwrap();

        let result = core.add_order(order("buy-1", "buyer", "BUY", 10, 1));

        assert!(matches!(result, Err(OrderManagerError::Internal(_))));
        assert_eq!(core.sequencer.peek(), 2);
        assert!(!core.order_manager.orders.contains_key("buy-1"));
        assert_eq!(core.order_manager.wallet.balance("seller"), u64::MAX);
        assert_eq!(core.order_manager.wallet.balance("buyer"), 10);
        assert_eq!(
            core.order_manager.positions.holding("seller", "AAPL"),
            1_000
        );
        assert_eq!(core.order_manager.positions.locked("seller", "AAPL"), 1);
        assert!(core.matching_engine.is_resting("sell-1"));
    }

    #[test]
    fn buyer_position_overflow_leaves_the_command_uncommitted() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 10).unwrap();
        core.deposit_shares("buyer", "AAPL", u64::MAX).unwrap();
        core.deposit_shares("seller", "AAPL", 1).unwrap();
        core.add_order(order("sell-1", "seller", "SELL", 10, 1))
            .unwrap();

        let result = core.add_order(order("buy-1", "buyer", "BUY", 10, 1));

        assert!(matches!(result, Err(OrderManagerError::Internal(_))));
        assert_eq!(core.sequencer.peek(), 2);
        assert_eq!(
            core.order_manager.positions.holding("buyer", "AAPL"),
            u64::MAX
        );
        assert_eq!(core.order_manager.positions.locked("seller", "AAPL"), 1);
        assert!(core.matching_engine.is_resting("sell-1"));
    }

    #[test]
    fn later_fill_failure_does_not_commit_earlier_fills() {
        let mut core = ExchangeCore::new();
        core.deposit("buyer".to_string(), 20).unwrap();
        core.deposit_shares("buyer", "AAPL", u64::MAX - 1).unwrap();
        core.deposit_shares("seller-a", "AAPL", 1).unwrap();
        core.deposit_shares("seller-b", "AAPL", 1).unwrap();
        core.add_order(order("sell-a", "seller-a", "SELL", 10, 1))
            .unwrap();
        core.add_order(order("sell-b", "seller-b", "SELL", 10, 1))
            .unwrap();

        let result = core.add_order(order("buy-1", "buyer", "BUY", 10, 2));

        assert!(matches!(result, Err(OrderManagerError::Internal(_))));
        assert_eq!(core.sequencer.peek(), 3);
        assert_eq!(
            core.order_manager.positions.holding("buyer", "AAPL"),
            u64::MAX - 1
        );
        assert!(core.matching_engine.is_resting("sell-a"));
        assert!(core.matching_engine.is_resting("sell-b"));
        assert!(!core.order_manager.orders.contains_key("buy-1"));
    }

    #[test]
    fn cancellation_failure_does_not_remove_the_book_order() {
        let mut core = funded_core();
        core.deposit("buyer".to_string(), 100).unwrap();
        core.add_order(order("buy-1", "buyer", "BUY", 10, 5))
            .unwrap();

        // Simulate an internal ledger inconsistency after placement. Preparation must detect it
        // before the matching-engine removal is committed.
        core.order_manager
            .wallet
            .unlock_funds("buyer", crate::types::types::Price::new(10).unwrap(), 5)
            .unwrap();

        let result = core.cancel_order_for_user("buy-1", "buyer");

        assert!(matches!(result, Err(OrderManagerError::Internal(_))));
        assert_eq!(core.sequencer.peek(), 2);
        assert_eq!(core.order_manager.get_state("buy-1"), Some(OrderState::New));
        assert!(core.matching_engine.is_resting("buy-1"));
    }

    #[test]
    fn normalized_snapshot_restores_fifo_books_ledgers_and_sequences() {
        let mut core = ExchangeCore::new();
        core.open_market(day(1)).unwrap();
        core.deposit("buyer".to_string(), 100).unwrap();
        core.deposit_shares("seller-a", "AAPL", 5).unwrap();
        core.deposit_shares("seller-b", "AAPL", 5).unwrap();
        core.add_order(order("sell-a", "seller-a", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("sell-b", "seller-b", "SELL", 10, 5))
            .unwrap();
        core.add_order(order("buy", "buyer", "BUY", 10, 7)).unwrap();

        let snapshot = core.snapshot();
        let mut restored = ExchangeCore::from_snapshot(snapshot.clone()).unwrap();
        assert_eq!(restored.snapshot(), snapshot);
        assert_eq!(
            restored.l2_snapshot("AAPL", 10).unwrap().asks,
            vec![crate::types::types::L2Level {
                price: 10,
                quantity: 3,
            }]
        );
        assert_eq!(
            restored.order_view("sell-a", "seller-a").unwrap().status,
            "filled"
        );
        assert_eq!(
            restored
                .order_view("sell-b", "seller-b")
                .unwrap()
                .remaining_quantity,
            3
        );
        assert_eq!(
            restored
                .execution_views("buyer", None, None, None, None)
                .len(),
            2
        );

        // The new order must receive the sequence after the three accepted commands in the
        // checkpoint. This also proves that the reconstructed index can continue matching.
        let outcome = restored
            .add_order(order("buyer-rest", "buyer", "BUY", 9, 1))
            .unwrap();
        assert_eq!(outcome.seq_num, 4);
    }

    #[test]
    fn snapshot_rejects_inconsistent_matching_sequence() {
        let core = ExchangeCore::new();
        let mut snapshot = core.snapshot();
        snapshot.next_matching_sequence = 2;
        assert!(ExchangeCore::from_snapshot(snapshot).is_err());
    }
}
