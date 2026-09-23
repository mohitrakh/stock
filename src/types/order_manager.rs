use std::collections::HashMap;

use super::positions::Positions;
use super::risk_manager::RiskManager;
use super::types::{
    BalanceView, Execution, ExecutionView, Order, OrderView, PositionView, Price, RiskLimitView,
    Side,
};
use super::wallet::Wallet;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
}

impl OrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Canceled => "canceled",
        }
    }
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
    /// The seller does not hold enough unreserved shares to back the order.
    PositionRejected(String),
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
    pub positions: Positions,
    /// Every fill each user was a party to, in the order they happened. Built as settlement runs
    /// rather than scanned out of the event log on each request, because this layer already knows
    /// which side of the trade each party was on.
    executions: HashMap<String, Vec<ExecutionView>>,
    execution_callbacks: Vec<Box<dyn Fn(Execution) + Send + Sync>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            risk_manager: RiskManager::new(),
            wallet: Wallet::new(),
            positions: Positions::new(),
            executions: HashMap::new(),
            execution_callbacks: Vec::new(),
        }
    }

    pub(crate) fn prepare_order(&mut self, order: Order) -> Result<Order, OrderManagerError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(OrderManagerError::AlreadyExists(order.order_id.clone()));
        }

        // Pre-trade, before any collateral moves, as the target design orders it: risk check, then
        // funds, then matching.
        self.risk_manager
            .check(&order)
            .map_err(|err| OrderManagerError::RiskRejected(format!("{:?}", err)))?;

        // Both sides must post collateral before the order is allowed to rest: a buyer reserves the
        // cash it would cost, a seller reserves the shares it would deliver. The sell branch is what
        // stops the exchange paying out for shares that never existed.
        match order.side {
            Side::Buy => self
                .wallet
                .check_and_lock(&order.user_id, order.price, order.quantity as u64)
                .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))?,

            Side::Sell => self
                .positions
                .check_and_lock(&order.user_id, &order.symbol, order.quantity as u64)
                .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))?,
        }

        self.risk_manager.record(&order);

        Ok(order)
    }

    pub(crate) fn deposit_shares(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), OrderManagerError> {
        self.positions
            .credit(user_id, symbol, quantity)
            .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))
    }

    pub(crate) fn deposit_funds(
        &mut self,
        user_id: String,
        amount: u64,
    ) -> Result<(), OrderManagerError> {
        self.wallet
            .deposit(user_id, amount)
            .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))
    }

    pub(crate) fn set_risk_limit(&mut self, user_id: String, symbol: String, limit: u64) {
        self.risk_manager.set_limit(user_id, symbol, limit);
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
        // The matching engine emits two records per match, one for each side, and settlement reads
        // the first of each pair. An odd count means that invariant broke upstream; it used to be
        // ignored silently, which would have meant a fill that never settled.
        for chunk in executions.chunks(2) {
            if chunk.len() != 2 {
                return Err(OrderManagerError::MatchingRejected(format!(
                    "executions must arrive in buy/sell pairs, got {} for {}",
                    executions.len(),
                    chunk[0].execution_id
                )));
            }

            self.apply_execution(&chunk[0])?;
        }

        for execution in executions {
            for callback in &self.execution_callbacks {
                callback(execution.clone());
            }
        }

        Ok(())
    }

    /// Settles one match across both ledgers.
    ///
    /// A fill has four legs, and all four must move or the books stop balancing: the buyer's cash
    /// out, the seller's cash in, the seller's shares out, the buyer's shares in. Cash paid equals
    /// cash received and shares delivered equals shares received, so neither total changes — which
    /// is the property `a_fill_creates_no_cash_and_no_shares` pins down.
    fn apply_execution(&mut self, execution: &Execution) -> Result<(), OrderManagerError> {
        self.validate_fill(&execution.buy_order_id, execution.quantity)?;
        self.validate_fill(&execution.sell_order_id, execution.quantity)?;

        let (buyer_user_id, buyer_limit_price) = self.fill_context(&execution.buy_order_id)?;
        let (seller_user_id, _) = self.fill_context(&execution.sell_order_id)?;

        let cash_amount = execution
            .price
            .checked_notional(execution.quantity as u64)
            .ok_or_else(|| OrderManagerError::WalletRejected("Overflow".to_string()))?;
        let quantity = execution.quantity as u64;

        // Shares first. The seller's reservation was taken at placement, so this cannot fail on a
        // well-formed book — and if it ever does, it fails before any cash has moved.
        self.positions
            .commit_sell_fill(&seller_user_id, &execution.symbol, quantity)
            .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))?;
        self.positions
            .credit(&buyer_user_id, &execution.symbol, quantity)
            .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))?;

        self.wallet
            .commit_buy_fill(&buyer_user_id, buyer_limit_price, execution.price, quantity)
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;
        self.wallet
            .deposit(seller_user_id.clone(), cash_amount)
            .map_err(|e| OrderManagerError::WalletRejected(format!("{:?}", e)))?;

        self.record_execution(
            &buyer_user_id,
            &execution.buy_order_id,
            Side::Buy,
            execution,
        );
        self.record_execution(
            &seller_user_id,
            &execution.sell_order_id,
            Side::Sell,
            execution,
        );

        self.record_fill(&execution.buy_order_id, execution.quantity)?;
        self.record_fill(&execution.sell_order_id, execution.quantity)?;

        Ok(())
    }

    fn record_execution(
        &mut self,
        user_id: &str,
        order_id: &str,
        side: Side,
        execution: &Execution,
    ) {
        self.executions
            .entry(user_id.to_string())
            .or_default()
            .push(ExecutionView {
                execution_id: execution.execution_id.clone(),
                order_id: order_id.to_string(),
                symbol: execution.symbol.clone(),
                side,
                price: execution.price.minor_units(),
                quantity: execution.quantity,
                timestamp: execution.timestamp,
            });
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
        let (user_id, symbol, side, price, remaining) = {
            let managed = self
                .orders
                .get(order_id)
                .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

            (
                managed.order.user_id.clone(),
                managed.order.symbol.clone(),
                managed.order.side.clone(),
                managed.order.price,
                managed.remaining_quantity,
            )
        };

        // Release whichever collateral the order was resting on — cash for a buy, shares for a
        // sell — for the quantity that never traded.
        match side {
            Side::Buy => self
                .wallet
                .unlock_funds(&user_id, price, remaining as u64)
                .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))?,

            Side::Sell => self
                .positions
                .unlock(&user_id, &symbol, remaining as u64)
                .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))?,
        }

        // The day's risk allowance is collateral too: quantity that never traded should not stay
        // counted against the cap.
        if let Some(managed) = self.orders.get(order_id) {
            let order = managed.order.clone();
            self.risk_manager.release(&order, remaining);
        }

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

    /// The client-facing view of one order, or `None` when it does not exist or belongs to
    /// someone else. Non-owners get the same answer as a missing order so a caller cannot probe
    /// for the existence of other users' order ids.
    pub fn order_view(&self, order_id: &str, user_id: &str) -> Option<OrderView> {
        let managed = self.orders.get(order_id)?;

        if managed.order.user_id != user_id {
            return None;
        }

        Some(OrderView {
            order_id: managed.order.order_id.clone(),
            symbol: managed.order.symbol.clone(),
            side: managed.order.side.clone(),
            price: managed.order.price.minor_units(),
            quantity: managed.order.quantity,
            filled_quantity: managed.order.quantity - managed.remaining_quantity,
            remaining_quantity: managed.remaining_quantity,
            status: managed.state.as_str().to_string(),
            creation_time: managed.order.timestamp,
        })
    }

    pub fn balance_view(&self, user_id: &str) -> BalanceView {
        BalanceView {
            user_id: user_id.to_string(),
            balance: self.wallet.balance(user_id),
            locked: self.wallet.locked(user_id),
            available: self.wallet.available(user_id),
        }
    }

    /// One user's fills, newest last, narrowed by any filter the caller supplied. Time bounds are
    /// inclusive and use the same epoch-seconds scale as `Order.timestamp`.
    pub fn execution_views(
        &self,
        user_id: &str,
        symbol: Option<&str>,
        order_id: Option<&str>,
        start_time: Option<f64>,
        end_time: Option<f64>,
    ) -> Vec<ExecutionView> {
        self.executions
            .get(user_id)
            .map(|rows| {
                rows.iter()
                    .filter(|row| symbol.is_none_or(|wanted| row.symbol == wanted))
                    .filter(|row| order_id.is_none_or(|wanted| row.order_id == wanted))
                    .filter(|row| start_time.is_none_or(|from| row.timestamp >= from))
                    .filter(|row| end_time.is_none_or(|to| row.timestamp <= to))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn risk_limit_view(&self, user_id: &str, symbol: &str) -> RiskLimitView {
        RiskLimitView {
            symbol: symbol.to_string(),
            max_daily_quantity: self.risk_manager.limit_for(user_id, symbol),
            used_today: self.risk_manager.used_today(user_id, symbol),
        }
    }

    pub fn position_views(&self, user_id: &str) -> Vec<PositionView> {
        self.positions
            .holdings_for(user_id)
            .into_iter()
            .map(|(symbol, quantity, locked)| PositionView {
                symbol,
                quantity,
                locked,
                available: quantity.saturating_sub(locked),
            })
            .collect()
    }

    pub fn subscribe<F: Fn(Execution) + Send + Sync + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }
}
