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
    Internal(String),
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

pub(crate) struct WalletSettlement {
    pub(crate) user_id: String,
    pub(crate) balance: u64,
    pub(crate) locked: u64,
}

pub(crate) struct PositionSettlement {
    pub(crate) user_id: String,
    pub(crate) symbol: String,
    pub(crate) holding: u64,
    pub(crate) locked: u64,
}

pub(crate) struct PreparedSettlement {
    pub(crate) wallet: Vec<WalletSettlement>,
    pub(crate) positions: Vec<PositionSettlement>,
    pub(crate) order_fills: HashMap<String, u32>,
    pub(crate) execution_views: Vec<(String, ExecutionView)>,
}

pub(crate) struct PreparedNewOrder {
    pub(crate) order: Order,
    pub(crate) settlement: PreparedSettlement,
}

pub(crate) struct PreparedCancel {
    pub(crate) order_id: String,
    pub(crate) user_id: String,
    pub(crate) symbol: String,
    pub(crate) side: Side,
    pub(crate) price: Price,
    pub(crate) remaining: u32,
}

#[derive(Default)]
struct WalletAccum {
    debit: u64,
    credit: u64,
    reserved: u64,
}

#[derive(Default)]
struct PositionAccum {
    sold: u64,
    bought: u64,
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

    pub(crate) fn validate_new_order(&self, order: &Order) -> Result<(), OrderManagerError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(OrderManagerError::AlreadyExists(order.order_id.clone()));
        }

        self.risk_manager
            .check_read_only(order)
            .map_err(|err| OrderManagerError::RiskRejected(format!("{:?}", err)))?;

        match order.side {
            Side::Buy => self
                .wallet
                .validate_lock(&order.user_id, order.price, order.quantity as u64)
                .map_err(|err| OrderManagerError::WalletRejected(format!("{:?}", err)))?,
            Side::Sell => self
                .positions
                .validate_lock(&order.user_id, &order.symbol, order.quantity as u64)
                .map_err(|err| OrderManagerError::PositionRejected(format!("{:?}", err)))?,
        }

        Ok(())
    }

    pub(crate) fn prepare_new_order(
        &self,
        order: Order,
        executions: &[Execution],
    ) -> Result<PreparedNewOrder, OrderManagerError> {
        self.validate_new_order(&order)?;
        let settlement = self.prepare_settlement(Some(&order), executions)?;
        Ok(PreparedNewOrder { order, settlement })
    }

    fn context(
        &self,
        order_id: &str,
        candidate: Option<&Order>,
    ) -> Result<(String, String, Side, Price, OrderState, u32), OrderManagerError> {
        if let Some(order) = candidate.filter(|order| order.order_id == order_id) {
            return Ok((
                order.user_id.clone(),
                order.symbol.clone(),
                order.side.clone(),
                order.price,
                OrderState::New,
                order.quantity,
            ));
        }

        let managed = self.orders.get(order_id).ok_or_else(|| {
            OrderManagerError::Internal(format!("missing matched order {}", order_id))
        })?;
        Ok((
            managed.order.user_id.clone(),
            managed.order.symbol.clone(),
            managed.order.side.clone(),
            managed.order.price,
            managed.state,
            managed.remaining_quantity,
        ))
    }

    fn prepare_settlement(
        &self,
        candidate: Option<&Order>,
        executions: &[Execution],
    ) -> Result<PreparedSettlement, OrderManagerError> {
        if !executions.len().is_multiple_of(2) {
            return Err(OrderManagerError::Internal(format!(
                "executions must arrive in buy/sell pairs, got {}",
                executions.len()
            )));
        }

        let mut wallet_accum: HashMap<String, WalletAccum> = HashMap::new();
        let mut position_accum: HashMap<(String, String), PositionAccum> = HashMap::new();
        let mut order_fills: HashMap<String, u32> = HashMap::new();
        let mut execution_views = Vec::with_capacity(executions.len());

        for pair in executions.chunks(2) {
            let execution = &pair[0];
            let duplicate = &pair[1];
            if execution.buy_order_id != duplicate.buy_order_id
                || execution.sell_order_id != duplicate.sell_order_id
                || execution.symbol != duplicate.symbol
                || execution.price != duplicate.price
                || execution.quantity != duplicate.quantity
                || execution.timestamp != duplicate.timestamp
            {
                return Err(OrderManagerError::Internal(
                    "execution pair contains different trade data".to_string(),
                ));
            }

            let (buyer_user, buyer_symbol, buyer_side, buyer_limit, buyer_state, buyer_remaining) =
                self.context(&execution.buy_order_id, candidate)?;
            let (
                seller_user,
                seller_symbol,
                seller_side,
                _seller_limit,
                seller_state,
                seller_remaining,
            ) = self.context(&execution.sell_order_id, candidate)?;

            if buyer_symbol != execution.symbol
                || seller_symbol != execution.symbol
                || buyer_side != Side::Buy
                || seller_side != Side::Sell
                || matches!(buyer_state, OrderState::Filled | OrderState::Canceled)
                || matches!(seller_state, OrderState::Filled | OrderState::Canceled)
            {
                return Err(OrderManagerError::Internal(
                    "execution references an invalid order state".to_string(),
                ));
            }

            for order_id in [&execution.buy_order_id, &execution.sell_order_id] {
                let filled = order_fills.entry(order_id.clone()).or_default();
                *filled = filled.checked_add(execution.quantity).ok_or_else(|| {
                    OrderManagerError::Internal("filled quantity overflow".to_string())
                })?;
                let remaining = if *order_id == execution.buy_order_id {
                    buyer_remaining
                } else {
                    seller_remaining
                };
                if *filled > remaining {
                    return Err(OrderManagerError::Internal(format!(
                        "order {} would be over-filled",
                        order_id
                    )));
                }
            }

            let quantity = execution.quantity as u64;
            let spent = execution.price.checked_notional(quantity).ok_or_else(|| {
                OrderManagerError::Internal("execution notional overflow".to_string())
            })?;
            let reserved = buyer_limit.checked_notional(quantity).ok_or_else(|| {
                OrderManagerError::Internal("reserved notional overflow".to_string())
            })?;

            let buyer_cash = wallet_accum.entry(buyer_user.clone()).or_default();
            buyer_cash.debit = buyer_cash
                .debit
                .checked_add(spent)
                .ok_or_else(|| OrderManagerError::Internal("buyer debit overflow".to_string()))?;
            buyer_cash.reserved = buyer_cash.reserved.checked_add(reserved).ok_or_else(|| {
                OrderManagerError::Internal("buyer reservation overflow".to_string())
            })?;

            let seller_cash = wallet_accum.entry(seller_user.clone()).or_default();
            seller_cash.credit = seller_cash
                .credit
                .checked_add(spent)
                .ok_or_else(|| OrderManagerError::Internal("seller credit overflow".to_string()))?;

            let seller_position = position_accum
                .entry((seller_user.clone(), execution.symbol.clone()))
                .or_default();
            seller_position.sold = seller_position
                .sold
                .checked_add(quantity)
                .ok_or_else(|| OrderManagerError::Internal("sold quantity overflow".to_string()))?;

            let buyer_position = position_accum
                .entry((buyer_user.clone(), execution.symbol.clone()))
                .or_default();
            buyer_position.bought =
                buyer_position.bought.checked_add(quantity).ok_or_else(|| {
                    OrderManagerError::Internal("bought quantity overflow".to_string())
                })?;

            execution_views.push((
                buyer_user.clone(),
                ExecutionView {
                    execution_id: execution.execution_id.clone(),
                    order_id: execution.buy_order_id.clone(),
                    symbol: execution.symbol.clone(),
                    side: Side::Buy,
                    price: execution.price.minor_units(),
                    quantity: execution.quantity,
                    timestamp: execution.timestamp,
                },
            ));
            execution_views.push((
                seller_user,
                ExecutionView {
                    execution_id: execution.execution_id.clone(),
                    order_id: execution.sell_order_id.clone(),
                    symbol: execution.symbol.clone(),
                    side: Side::Sell,
                    price: execution.price.minor_units(),
                    quantity: execution.quantity,
                    timestamp: execution.timestamp,
                },
            ));
        }

        let candidate_wallet_lock = candidate
            .filter(|order| matches!(order.side, Side::Buy))
            .map(|order| {
                order
                    .price
                    .checked_notional(order.quantity as u64)
                    .ok_or_else(|| {
                        OrderManagerError::Internal("candidate wallet lock overflow".to_string())
                    })
            })
            .transpose()?
            .unwrap_or(0);
        let candidate_position_lock = candidate
            .filter(|order| matches!(order.side, Side::Sell))
            .map(|order| order.quantity as u64)
            .unwrap_or(0);

        let wallet = wallet_accum
            .into_iter()
            .map(|(user_id, accum)| {
                let balance = self
                    .wallet
                    .balance(&user_id)
                    .checked_sub(accum.debit)
                    .and_then(|value| value.checked_add(accum.credit))
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!(
                            "wallet balance invalid for {}",
                            user_id
                        ))
                    })?;
                let candidate_lock = candidate
                    .filter(|order| order.user_id == user_id && matches!(order.side, Side::Buy))
                    .map(|_| candidate_wallet_lock)
                    .unwrap_or(0);
                let locked = self
                    .wallet
                    .locked(&user_id)
                    .checked_add(candidate_lock)
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!("wallet lock invalid for {}", user_id))
                    })?
                    .checked_sub(accum.reserved)
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!(
                            "wallet reservation invalid for {}",
                            user_id
                        ))
                    })?;
                Ok(WalletSettlement {
                    user_id,
                    balance,
                    locked,
                })
            })
            .collect::<Result<Vec<_>, OrderManagerError>>()?;

        let positions = position_accum
            .into_iter()
            .map(|((user_id, symbol), accum)| {
                let holding = self
                    .positions
                    .holding(&user_id, &symbol)
                    .checked_sub(accum.sold)
                    .and_then(|value| value.checked_add(accum.bought))
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!(
                            "position holding invalid for {}",
                            user_id
                        ))
                    })?;
                let candidate_lock = candidate
                    .filter(|order| {
                        order.user_id == user_id
                            && order.symbol == symbol
                            && matches!(order.side, Side::Sell)
                    })
                    .map(|_| candidate_position_lock)
                    .unwrap_or(0);
                let locked = self
                    .positions
                    .locked(&user_id, &symbol)
                    .checked_add(candidate_lock)
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!(
                            "position lock invalid for {}",
                            user_id
                        ))
                    })?
                    .checked_sub(accum.sold)
                    .ok_or_else(|| {
                        OrderManagerError::Internal(format!(
                            "position reservation invalid for {}",
                            user_id
                        ))
                    })?;
                Ok(PositionSettlement {
                    user_id,
                    symbol,
                    holding,
                    locked,
                })
            })
            .collect::<Result<Vec<_>, OrderManagerError>>()?;

        Ok(PreparedSettlement {
            wallet,
            positions,
            order_fills,
            execution_views,
        })
    }

    pub(crate) fn planned_order_view(
        &self,
        order: &Order,
        settlement: &PreparedSettlement,
    ) -> OrderView {
        let filled_quantity = settlement
            .order_fills
            .get(&order.order_id)
            .copied()
            .unwrap_or(0);
        let remaining_quantity = order.quantity - filled_quantity;
        let status = if remaining_quantity == 0 {
            OrderState::Filled
        } else if filled_quantity > 0 {
            OrderState::PartiallyFilled
        } else {
            OrderState::New
        };

        OrderView {
            order_id: order.order_id.clone(),
            symbol: order.symbol.clone(),
            side: order.side.clone(),
            price: order.price.minor_units(),
            quantity: order.quantity,
            filled_quantity,
            remaining_quantity,
            status: status.as_str().to_string(),
            creation_time: order.timestamp,
        }
    }

    pub(crate) fn commit_new_order(&mut self, prepared: PreparedNewOrder) {
        let order = prepared.order;
        match order.side {
            Side::Buy => {
                self.wallet
                    .commit_lock(&order.user_id, order.price, order.quantity as u64)
            }
            Side::Sell => {
                self.positions
                    .commit_lock(&order.user_id, &order.symbol, order.quantity as u64)
            }
        }
        self.risk_manager.record(&order);
        self.register_order(order);
        self.commit_settlement(prepared.settlement);
    }

    pub(crate) fn prepare_cancel_plan(
        &self,
        order_id: &str,
        user_id: &str,
    ) -> Result<PreparedCancel, OrderManagerError> {
        self.validate_cancel_for_user(order_id, user_id)?;
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;
        let remaining = managed.remaining_quantity;

        match managed.order.side {
            Side::Buy => self
                .wallet
                .validate_unlock(
                    &managed.order.user_id,
                    managed.order.price,
                    remaining as u64,
                )
                .map_err(|err| OrderManagerError::Internal(format!("{:?}", err)))?,
            Side::Sell => self
                .positions
                .validate_unlock(
                    &managed.order.user_id,
                    &managed.order.symbol,
                    remaining as u64,
                )
                .map_err(|err| OrderManagerError::Internal(format!("{:?}", err)))?,
        }

        Ok(PreparedCancel {
            order_id: order_id.to_string(),
            user_id: managed.order.user_id.clone(),
            symbol: managed.order.symbol.clone(),
            side: managed.order.side.clone(),
            price: managed.order.price,
            remaining,
        })
    }

    pub(crate) fn commit_cancel_plan(&mut self, prepared: PreparedCancel) {
        match prepared.side {
            Side::Buy => self.wallet.commit_unlock(
                &prepared.user_id,
                prepared.price,
                prepared.remaining as u64,
            ),
            Side::Sell => self.positions.commit_unlock(
                &prepared.user_id,
                &prepared.symbol,
                prepared.remaining as u64,
            ),
        }

        if let Some(managed) = self.orders.get(&prepared.order_id) {
            self.risk_manager
                .release(&managed.order, prepared.remaining);
        }
        if let Some(managed) = self.orders.get_mut(&prepared.order_id) {
            managed.state = OrderState::Canceled;
        }
    }

    fn commit_settlement(&mut self, prepared: PreparedSettlement) {
        for update in prepared.wallet {
            self.wallet
                .commit_settlement(update.user_id, update.balance, update.locked);
        }
        for update in prepared.positions {
            self.positions.commit_settlement(
                update.user_id,
                update.symbol,
                update.holding,
                update.locked,
            );
        }
        for (order_id, filled_qty) in prepared.order_fills {
            let managed = self
                .orders
                .get_mut(&order_id)
                .expect("prepared settlement order must exist at commit");
            managed.remaining_quantity -= filled_qty;
            managed.state = if managed.remaining_quantity == 0 {
                OrderState::Filled
            } else {
                OrderState::PartiallyFilled
            };
        }
        for (user_id, view) in prepared.execution_views {
            self.executions.entry(user_id).or_default().push(view);
        }
    }

    pub(crate) fn notify_executions(&self, executions: &[Execution]) {
        for execution in executions {
            for callback in &self.execution_callbacks {
                callback(execution.clone());
            }
        }
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
