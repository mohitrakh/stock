use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};
mod order;
mod sequencer;

use crate::sequencer::Sequencer;

// ─────────────────────────────────────────────
// Price
// ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Price(f64);

impl Price {
    pub fn new(val: f64) -> Self {
        assert!(!val.is_nan(), "Price cannot be NaN");
        Price(val)
    }
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl Eq for Price {}

impl PartialOrd for Price {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Price {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

// ─────────────────────────────────────────────
// Side
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "BUY" => Ok(Side::Buy),
            "SELL" => Ok(Side::Sell),
            _ => Err("side must be BUY or SELL".to_string()),
        }
    }
}

// ─────────────────────────────────────────────
// Order
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub quantity: u32,
    pub leaves_qty: u32,
    pub timestamp: f64,
    pub seq_num: u64,
}

impl Order {
    pub fn new(
        order_id: String,
        user_id: String,
        symbol: String,
        side: &str,
        price: f64,
        quantity: u32,
        leaves_qty: Option<u32>,
        timestamp: f64,
        seq_num: u64,
    ) -> Result<Self, String> {
        let side = Side::from_str(side)?;

        if price <= 0.0 || quantity == 0 {
            return Err("price and quantity must be positive".to_string());
        }

        let leaves_qty = leaves_qty.unwrap_or(quantity);

        Ok(Order {
            order_id,
            user_id,
            symbol,
            side,
            price,
            quantity,
            leaves_qty,
            timestamp,
            seq_num,
        })
    }
}

// ─────────────────────────────────────────────
// Execution
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Execution {
    pub execution_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub symbol: String,
    pub price: f64,
    pub quantity: u32,
    pub timestamp: f64,
}

// ─────────────────────────────────────────────
// PriceLevel  (doubly-linked list via Vec<Node>)
// ─────────────────────────────────────────────

#[derive(Debug)]
struct Node {
    order: Option<Order>,
    prev_idx: Option<usize>,
    next_idx: Option<usize>,
}

#[derive(Debug)]
pub struct PriceLevel {
    pub price: f64,
    nodes: Vec<Node>,
    head_idx: Option<usize>,
    tail_idx: Option<usize>,
    order_map: HashMap<String, usize>, // order_id -> index
}

impl PriceLevel {
    pub fn new(price: f64) -> Self {
        PriceLevel {
            price,
            nodes: Vec::new(),
            head_idx: None,
            tail_idx: None,
            order_map: HashMap::new(),
        }
    }

    pub fn append(&mut self, order: Order) {
        let order_id = order.order_id.clone();
        let new_idx = self.nodes.len();

        self.nodes.push(Node {
            order: Some(order),
            prev_idx: self.tail_idx,
            next_idx: None,
        });

        if let Some(tail_idx) = self.tail_idx {
            self.nodes[tail_idx].next_idx = Some(new_idx);
        }

        if self.head_idx.is_none() {
            self.head_idx = Some(new_idx);
        }

        self.tail_idx = Some(new_idx);
        self.order_map.insert(order_id, new_idx);
    }

    pub fn remove(&mut self, order_id: &str) -> Option<Order> {
        let &idx = self.order_map.get(order_id)?;
        let node = &mut self.nodes[idx];
        let order = node.order.take()?;
        let prev = node.prev_idx;
        let next = node.next_idx;

        if let Some(prev_idx) = prev {
            self.nodes[prev_idx].next_idx = next;
        } else {
            self.head_idx = next;
        }

        if let Some(next_idx) = next {
            self.nodes[next_idx].prev_idx = prev;
        } else {
            self.tail_idx = prev;
        }

        self.nodes[idx].prev_idx = None;
        self.nodes[idx].next_idx = None;
        self.order_map.remove(order_id);
        Some(order)
    }

    pub fn peek_front(&self) -> Option<&Order> {
        self.head_idx.and_then(|idx| self.nodes[idx].order.as_ref())
    }

    pub fn pop_front(&mut self) -> Option<Order> {
        let head_order_id = self.peek_front()?.order_id.clone();
        self.remove(&head_order_id)
    }

    pub fn is_empty(&self) -> bool {
        self.head_idx.is_none()
    }

    pub fn total_quantity(&self) -> u32 {
        let mut total = 0;
        let mut current_idx = self.head_idx;
        while let Some(idx) = current_idx {
            if let Some(ref order) = self.nodes[idx].order {
                total += order.leaves_qty;
            }
            current_idx = self.nodes[idx].next_idx;
        }
        total
    }

    pub fn peek_front_mut(&mut self) -> Option<&mut Order> {
        let head_idx = self.head_idx?;
        self.nodes[head_idx].order.as_mut()
    }
}

// ─────────────────────────────────────────────
// OrderBook
// ─────────────────────────────────────────────

#[derive(Debug)]
pub struct OrderBook {
    pub symbol: String,
    buy_levels: BTreeMap<Reverse<Price>, PriceLevel>,
    sell_levels: BTreeMap<Price, PriceLevel>,
    order_map: HashMap<String, (Price, Side)>,
    exec_counter: u64,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        OrderBook {
            symbol,
            buy_levels: BTreeMap::new(),
            sell_levels: BTreeMap::new(),
            order_map: HashMap::new(),
            exec_counter: 0,
        }
    }

    pub fn best_bid(&self) -> Option<(f64, u32)> {
        self.buy_levels
            .first_key_value()
            .map(|(rev_price, level)| (rev_price.0.as_f64(), level.total_quantity()))
    }

    pub fn best_ask(&self) -> Option<(f64, u32)> {
        self.sell_levels
            .first_key_value()
            .map(|(price, level)| (price.as_f64(), level.total_quantity()))
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Option<Order> {
        let (price, side) = self.order_map.remove(order_id)?;

        let level = match side {
            Side::Buy => self.buy_levels.get_mut(&Reverse(price)),
            Side::Sell => self.sell_levels.get_mut(&price),
        }?;

        let removed_order = level.remove(order_id);

        if level.is_empty() {
            match side {
                Side::Buy => self.buy_levels.remove(&Reverse(price)),
                Side::Sell => self.sell_levels.remove(&price),
            };
        }

        removed_order
    }

    fn match_order(&mut self, order: &mut Order) -> Vec<Execution> {
        let mut executions = Vec::new();

        match order.side {
            Side::Buy => {
                while order.leaves_qty > 0 {
                    let best_ask_price = match self.sell_levels.first_key_value() {
                        Some((&price, _)) => price,
                        None => break,
                    };
                    if best_ask_price.as_f64() > order.price {
                        break;
                    }

                    let level = self.sell_levels.get_mut(&best_ask_price).unwrap();
                    let sell_resting = match level.peek_front_mut() {
                        Some(o) => o,
                        None => {
                            self.sell_levels.remove(&best_ask_price);
                            continue;
                        }
                    };

                    if sell_resting.user_id == order.user_id {
                        break; // self-trade prevention
                    }

                    let trade_qty = order.leaves_qty.min(sell_resting.leaves_qty);
                    let trade_price = sell_resting.price;

                    let exec_id_buy = format!("exec_{}", self.exec_counter);
                    self.exec_counter += 1;
                    let exec_id_sell = format!("exec_{}", self.exec_counter);
                    self.exec_counter += 1;

                    executions.push(Execution {
                        execution_id: exec_id_buy,
                        buy_order_id: order.order_id.clone(),
                        sell_order_id: sell_resting.order_id.clone(),
                        symbol: self.symbol.clone(),
                        price: trade_price,
                        quantity: trade_qty,
                        timestamp: order.timestamp.max(sell_resting.timestamp),
                    });
                    executions.push(Execution {
                        execution_id: exec_id_sell,
                        buy_order_id: order.order_id.clone(),
                        sell_order_id: sell_resting.order_id.clone(),
                        symbol: self.symbol.clone(),
                        price: trade_price,
                        quantity: trade_qty,
                        timestamp: order.timestamp.max(sell_resting.timestamp),
                    });

                    order.leaves_qty -= trade_qty;
                    sell_resting.leaves_qty -= trade_qty;

                    if sell_resting.leaves_qty == 0 {
                        let sell_id = sell_resting.order_id.clone();
                        level.remove(&sell_id);
                        self.order_map.remove(&sell_id);
                        if level.is_empty() {
                            self.sell_levels.remove(&best_ask_price);
                        }
                    }
                }
            }
            Side::Sell => {
                while order.leaves_qty > 0 {
                    let best_bid_key = match self.buy_levels.first_key_value() {
                        Some((&rev_price, _)) => rev_price,
                        None => break,
                    };
                    let best_bid_price = best_bid_key.0;
                    if best_bid_price.as_f64() < order.price {
                        break;
                    }

                    let level = self.buy_levels.get_mut(&best_bid_key).unwrap();
                    let buy_resting = match level.peek_front_mut() {
                        Some(o) => o,
                        None => {
                            self.buy_levels.remove(&best_bid_key);
                            continue;
                        }
                    };

                    if buy_resting.user_id == order.user_id {
                        break; // self-trade prevention
                    }

                    let trade_qty = order.leaves_qty.min(buy_resting.leaves_qty);
                    let trade_price = buy_resting.price;

                    let exec_id_buy = format!("exec_{}", self.exec_counter);
                    self.exec_counter += 1;
                    let exec_id_sell = format!("exec_{}", self.exec_counter);
                    self.exec_counter += 1;

                    executions.push(Execution {
                        execution_id: exec_id_buy,
                        buy_order_id: buy_resting.order_id.clone(),
                        sell_order_id: order.order_id.clone(),
                        symbol: self.symbol.clone(),
                        price: trade_price,
                        quantity: trade_qty,
                        timestamp: order.timestamp.max(buy_resting.timestamp),
                    });
                    executions.push(Execution {
                        execution_id: exec_id_sell,
                        buy_order_id: buy_resting.order_id.clone(),
                        sell_order_id: order.order_id.clone(),
                        symbol: self.symbol.clone(),
                        price: trade_price,
                        quantity: trade_qty,
                        timestamp: order.timestamp.max(buy_resting.timestamp),
                    });

                    order.leaves_qty -= trade_qty;
                    buy_resting.leaves_qty -= trade_qty;

                    if buy_resting.leaves_qty == 0 {
                        let buy_id = buy_resting.order_id.clone();
                        level.remove(&buy_id);
                        self.order_map.remove(&buy_id);
                        if level.is_empty() {
                            self.buy_levels.remove(&best_bid_key);
                        }
                    }
                }
            }
        }

        executions
    }

    pub fn place_order(&mut self, mut order: Order) -> Vec<Execution> {
        let executions = self.match_order(&mut order);

        if order.leaves_qty > 0 {
            let price = Price::new(order.price);
            let order_id = order.order_id.clone();
            match order.side {
                Side::Buy => {
                    let level = self
                        .buy_levels
                        .entry(Reverse(price))
                        .or_insert_with(|| PriceLevel::new(order.price));
                    level.append(order);
                    self.order_map.insert(order_id, (price, Side::Buy));
                }
                Side::Sell => {
                    let level = self
                        .sell_levels
                        .entry(price)
                        .or_insert_with(|| PriceLevel::new(order.price));
                    level.append(order);
                    self.order_map.insert(order_id, (price, Side::Sell));
                }
            }
        }

        executions
    }

    pub fn is_resting(&self, order_id: &str) -> bool {
        self.order_map.contains_key(order_id)
    }
}

// ─────────────────────────────────────────────
// MatchingEngine
// ─────────────────────────────────────────────

#[derive(Debug)]
pub struct MatchingEngine {
    order_books: HashMap<String, OrderBook>,
    order_location: HashMap<String, String>, // order_id -> symbol
    last_seq: u64,
}

impl MatchingEngine {
    pub fn new() -> Self {
        MatchingEngine {
            order_books: HashMap::new(),
            order_location: HashMap::new(),
            last_seq: 0,
        }
    }

    pub fn process_order(&mut self, order: Order) -> Result<Vec<Execution>, String> {
        if order.seq_num <= self.last_seq {
            return Err(format!(
                "Sequence violation: received seq {} but last was {}",
                order.seq_num, self.last_seq
            ));
        }

        let symbol = order.symbol.clone();
        let order_id = order.order_id.clone();
        let seq_num = order.seq_num;

        let book = self
            .order_books
            .entry(symbol.clone())
            .or_insert_with(|| OrderBook::new(symbol.clone()));

        let fills = book.place_order(order);

        self.last_seq = seq_num;

        if book.is_resting(&order_id) {
            self.order_location.insert(order_id, symbol);
        }

        Ok(fills)
    }

    pub fn best_bid_ask(&self, symbol: &str) -> Option<((f64, u32), (f64, u32))> {
        let book = self.order_books.get(symbol)?;
        let bid = book.best_bid()?;
        let ask = book.best_ask()?;
        Some((bid, ask))
    }

    pub fn cancel_order(
        &mut self,
        order_id: &str,
        cancel_seq: u64,
    ) -> Result<Option<Order>, String> {
        if cancel_seq <= self.last_seq {
            return Err(format!(
                "Sequence violation on cancel: received seq {} but last was {}",
                cancel_seq, self.last_seq
            ));
        }

        let symbol = self
            .order_location
            .remove(order_id)
            .ok_or_else(|| format!("Order {} not found for cancellation", order_id))?;

        let book = self
            .order_books
            .get_mut(&symbol)
            .ok_or_else(|| format!("Order book for symbol {} not found", symbol))?;

        let removed = book.cancel_order(order_id);
        self.last_seq = cancel_seq;
        Ok(removed)
    }

    pub fn is_resting(&self, order_id: &str) -> bool {
        self.order_location.contains_key(order_id)
    }

    pub fn get_order_leaves(&self, order_id: &str) -> Option<u32> {
        let symbol = self.order_location.get(order_id)?;
        let book = self.order_books.get(symbol)?;
        let (price, side) = book.order_map.get(order_id)?;
        let level = match side {
            Side::Buy => book.buy_levels.get(&Reverse(*price)),
            Side::Sell => book.sell_levels.get(price),
        }?;
        let idx = level.order_map.get(order_id)?;
        let node = &level.nodes[*idx];
        node.order.as_ref().map(|o| o.leaves_qty)
    }
}

// ─────────────────────────────────────────────
// OrderState
// ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
}

// ─────────────────────────────────────────────
// OrderManagerError  (typed, no external crates)
// ─────────────────────────────────────────────

#[derive(Debug, PartialEq)]
pub enum OrderManagerError {
    /// An order with this ID was submitted but already exists in the manager.
    AlreadyExists(String),
    /// No managed order was found for the given ID.
    OrderNotFound(String),
    /// The requested state transition is not allowed from the current state.
    InvalidTransition {
        order_id: String,
        from: OrderState,
        attempted: &'static str,
    },
    /// A fill quantity would exceed the order's remaining quantity.
    OverFill {
        order_id: String,
        remaining: u32,
        requested: u32,
    },
    /// The matching engine returned an error (e.g. sequence violation).
    EngineError(String),
}

impl std::fmt::Display for OrderManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderManagerError::AlreadyExists(id) => {
                write!(f, "Order '{}' already exists", id)
            }
            OrderManagerError::OrderNotFound(id) => {
                write!(f, "Order '{}' not found", id)
            }
            OrderManagerError::InvalidTransition { order_id, from, attempted } => {
                write!(
                    f,
                    "Order '{}' cannot '{}' from state {:?}",
                    order_id, attempted, from
                )
            }
            OrderManagerError::OverFill { order_id, remaining, requested } => {
                write!(
                    f,
                    "Order '{}' overfill: remaining={}, requested={}",
                    order_id, remaining, requested
                )
            }
            OrderManagerError::EngineError(msg) => write!(f, "Engine error: {}", msg),
        }
    }
}

// ─────────────────────────────────────────────
// ManagedOrder  — wrapper tracking lifecycle state
// ─────────────────────────────────────────────

#[derive(Debug)]
pub struct ManagedOrder {
    pub order: Order,
    pub state: OrderState,
    /// Tracks how many units are still open. Starts at order.quantity and
    /// decrements with each fill. Kept here so the matching engine's copy
    /// remains untouched.
    pub remaining_quantity: u32,
}

impl ManagedOrder {
    fn new(order: Order) -> Self {
        let qty = order.quantity;
        ManagedOrder {
            order,
            state: OrderState::New,
            remaining_quantity: qty,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.state, OrderState::Filled | OrderState::Canceled)
    }
}

// ─────────────────────────────────────────────
// OrderManager
// ─────────────────────────────────────────────

pub struct OrderManager {
    /// All orders ever submitted, keyed by order_id.
    orders: HashMap<String, ManagedOrder>,
    engine: MatchingEngine,
    sequencer: Sequencer,
    execution_callbacks: Vec<Box<dyn Fn(Execution)>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
            execution_callbacks: Vec::new(),
        }
    }

    // ── Public API ──────────────────────────────

    /// Register an execution callback invoked for every fill.
    pub fn subscribe<F: Fn(Execution) + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }

    /// Insert a new order with state `New`.
    /// Returns `AlreadyExists` if the order ID has been seen before (idempotency guard).
    pub fn add_order(&mut self, order: Order) -> Result<(), OrderManagerError> {
        let id = order.order_id.clone();
        if self.orders.contains_key(&id) {
            return Err(OrderManagerError::AlreadyExists(id));
        }
        self.orders.insert(id, ManagedOrder::new(order));
        Ok(())
    }

    /// Place an order end-to-end: add → sequence → match → update states → fire callbacks.
    /// Returns the list of executions produced.
    pub fn place_order(&mut self, mut order: Order) -> Result<Vec<Execution>, OrderManagerError> {
        // Idempotency check — reject duplicate IDs before touching the engine.
        let id = order.order_id.clone();
        if self.orders.contains_key(&id) {
            return Err(OrderManagerError::AlreadyExists(id));
        }

        // Stamp the sequence number.
        order.seq_num = self.sequencer.next();

        // Insert as New before sending to engine.
        self.orders.insert(id.clone(), ManagedOrder::new(order.clone()));

        // Send to matching engine.
        let fills = self
            .engine
            .process_order(order)
            .map_err(OrderManagerError::EngineError)?;

        // Update states based on what the engine returned.
        self.apply_fills_to_states(&id, &fills);

        // Fire callbacks.
        for exec in &fills {
            for cb in &self.execution_callbacks {
                cb(exec.clone());
            }
        }

        Ok(fills)
    }

    /// Cancel an open order.
    /// Allowed only from `New` or `PartiallyFilled`. Returns `InvalidTransition`
    /// if the order is already terminal.
    pub fn cancel_order(&mut self, order_id: &str) -> Result<Option<Order>, OrderManagerError> {
        let managed = self
            .orders
            .get(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        if managed.is_terminal() {
            return Err(OrderManagerError::InvalidTransition {
                order_id: order_id.to_string(),
                from: managed.state,
                attempted: "cancel",
            });
        }

        let cancel_seq = self.sequencer.next();
        let removed = self
            .engine
            .cancel_order(order_id, cancel_seq)
            .map_err(OrderManagerError::EngineError)?;

        if removed.is_some() {
            self.orders
                .get_mut(order_id)
                .unwrap()
                .state = OrderState::Canceled;
        }

        Ok(removed)
    }

    /// Apply a fill of `filled_qty` units to a managed order.
    ///
    /// - `New` + partial fill → `PartiallyFilled`
    /// - `PartiallyFilled` + partial fill → stays `PartiallyFilled`
    /// - Any state + fill that exhausts remaining → `Filled`
    /// - `filled_qty > remaining_quantity` → `OverFill` error
    /// - Terminal order → `InvalidTransition` error
    pub fn record_fill(
        &mut self,
        order_id: &str,
        filled_qty: u32,
    ) -> Result<(), OrderManagerError> {
        let managed = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| OrderManagerError::OrderNotFound(order_id.to_string()))?;

        if managed.is_terminal() {
            return Err(OrderManagerError::InvalidTransition {
                order_id: order_id.to_string(),
                from: managed.state,
                attempted: "record_fill",
            });
        }

        if filled_qty > managed.remaining_quantity {
            return Err(OrderManagerError::OverFill {
                order_id: order_id.to_string(),
                remaining: managed.remaining_quantity,
                requested: filled_qty,
            });
        }

        managed.remaining_quantity -= filled_qty;

        managed.state = if managed.remaining_quantity == 0 {
            OrderState::Filled
        } else {
            OrderState::PartiallyFilled
        };

        Ok(())
    }

    /// Read the current state of an order.
    pub fn get_state(&self, order_id: &str) -> Option<OrderState> {
        self.orders.get(order_id).map(|m| m.state)
    }

    /// Convenience alias used in existing integration code.
    pub fn get_order_state(&self, order_id: &str) -> Option<OrderState> {
        self.get_state(order_id)
    }

    /// Best bid/ask for a symbol.
    pub fn best_bid_ask(&self, symbol: &str) -> Option<((f64, u32), (f64, u32))> {
        self.engine.best_bid_ask(symbol)
    }

    // ── Private helpers ──────────────────────────

    /// After the engine returns fills, update ManagedOrder states for every
    /// order touched by those fills (both the incoming order and any resting
    /// counterparts).
    fn apply_fills_to_states(&mut self, incoming_id: &str, fills: &[Execution]) {
        // Update the incoming order itself.
        if self.engine.is_resting(incoming_id) {
            // Still resting → partial fill (or still New if no fills at all).
            if !fills.is_empty() {
                let leaves = self.engine.get_order_leaves(incoming_id).unwrap_or(0);
                if let Some(m) = self.orders.get_mut(incoming_id) {
                    m.remaining_quantity = leaves;
                    m.state = OrderState::PartiallyFilled;
                }
            }
            // If fills is empty the order is pure-resting New; state stays New.
        } else {
            // No longer resting → fully matched.
            if let Some(m) = self.orders.get_mut(incoming_id) {
                m.remaining_quantity = 0;
                m.state = OrderState::Filled;
            }
        }

        // Update every counterpart order touched by fills.
        for exec in fills {
            for counterpart_id in [&exec.buy_order_id, &exec.sell_order_id] {
                if counterpart_id.as_str() == incoming_id {
                    continue; // already handled above
                }
                if self.engine.is_resting(counterpart_id) {
                    let leaves = self
                        .engine
                        .get_order_leaves(counterpart_id)
                        .unwrap_or(0);
                    if let Some(m) = self.orders.get_mut(counterpart_id.as_str()) {
                        m.remaining_quantity = leaves;
                        m.state = OrderState::PartiallyFilled;
                    }
                } else {
                    if let Some(m) = self.orders.get_mut(counterpart_id.as_str()) {
                        m.remaining_quantity = 0;
                        m.state = OrderState::Filled;
                    }
                }
            }
        }
    }
}

// ─────────────────────────────────────────────
// main  (smoke test / demo)
// ─────────────────────────────────────────────

fn main() {
    let mut om = OrderManager::new();

    om.subscribe(|exec| {
        println!(
            "EXECUTION: {} @ {} qty {}",
            exec.symbol, exec.price, exec.quantity
        );
    });

    let o1 = Order::new(
        "o1".into(), "u1".into(), "AAPL".into(),
        "SELL", 100.0, 10, None, 1.0, 0,
    ).unwrap();
    let o2 = Order::new(
        "o2".into(), "u2".into(), "AAPL".into(),
        "BUY", 100.0, 7, None, 2.0, 0,
    ).unwrap();

    let fills = om.place_order(o1).unwrap();
    println!("After o1: {:?}", fills);
    println!("State o1: {:?}", om.get_order_state("o1"));

    let fills = om.place_order(o2).unwrap();
    println!("After o2: {:?}", fills);
    println!("State o1: {:?}", om.get_order_state("o1"));
    println!("State o2: {:?}", om.get_order_state("o2"));

    let cancelled = om.cancel_order("o1").unwrap();
    println!("Cancelled o1: {:?}", cancelled);
    println!("State o1 after cancel: {:?}", om.get_order_state("o1"));
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ─────────────────────────────────

    fn make_order(id: &str, side: &str, price: f64, qty: u32, seq: u64) -> Order {
        Order::new(
            id.to_string(),
            "user".to_string(),
            "AAPL".to_string(),
            side,
            price,
            qty,
            None,
            seq as f64,
            seq,
        )
        .unwrap()
    }

    fn make_order_user(id: &str, user: &str, side: &str, price: f64, qty: u32, seq: u64) -> Order {
        Order::new(
            id.to_string(),
            user.to_string(),
            "AAPL".to_string(),
            side,
            price,
            qty,
            None,
            seq as f64,
            seq,
        )
        .unwrap()
    }

    fn make_engine() -> MatchingEngine {
        MatchingEngine::new()
    }

    fn make_om() -> OrderManager {
        OrderManager::new()
    }

    // ════════════════════════════════════════════
    // SECTION A — MatchingEngine (unchanged from Phase 1)
    // ════════════════════════════════════════════

    // --- A1. Sequence Validation ---

    #[test]
    fn test_sequence_accept_first() {
        let mut engine = make_engine();
        let order = make_order("o1", "BUY", 100.0, 10, 1);
        assert!(engine.process_order(order).is_ok());
        assert_eq!(engine.last_seq, 1);
    }

    #[test]
    fn test_sequence_reject_duplicate() {
        let mut engine = make_engine();
        engine.process_order(make_order("o1", "BUY", 100.0, 10, 1)).unwrap();
        assert!(engine.process_order(make_order("o2", "BUY", 100.0, 10, 1)).is_err());
    }

    #[test]
    fn test_sequence_reject_lower() {
        let mut engine = make_engine();
        engine.process_order(make_order("o1", "BUY", 100.0, 10, 5)).unwrap();
        assert!(engine.process_order(make_order("o2", "BUY", 100.0, 10, 3)).is_err());
    }

    #[test]
    fn test_sequence_last_seq_unchanged_on_error() {
        let mut engine = make_engine();
        engine.process_order(make_order("o1", "BUY", 100.0, 10, 10)).unwrap();
        assert_eq!(engine.last_seq, 10);
        assert!(engine.process_order(make_order("o2", "BUY", 100.0, 10, 5)).is_err());
        assert_eq!(engine.last_seq, 10);
    }

    // --- A2. Place & No Match ---

    #[test]
    fn test_place_order_no_match() {
        let mut engine = make_engine();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 10, 1)).unwrap();
        assert!(fills.is_empty());
        assert!(engine.cancel_order("b1", 2).unwrap().is_some());
    }

    #[test]
    fn test_best_bid_after_no_match() {
        let mut engine = make_engine();
        engine.process_order(make_order("b1", "BUY", 100.0, 10, 1)).unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 100.0);
        assert_eq!(bid_qty, 10);
    }

    // --- A3. Cancel Order ---

    #[test]
    fn test_cancel_resting_order() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        let removed = engine.cancel_order("s1", 2).unwrap().unwrap();
        assert_eq!(removed.order_id, "s1");
        assert!(engine.cancel_order("s1", 3).is_err());
    }

    #[test]
    fn test_cancel_updates_order_book() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        engine.cancel_order("s1", 2).unwrap();
        assert!(engine.best_bid_ask("AAPL").unwrap().1.1 == 0);
    }

    // --- A4. Full Fill ---

    #[test]
    fn test_full_fill_basic() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 10, 2)).unwrap();
        assert_eq!(fills.len(), 2);
        assert!(engine.cancel_order("s1", 3).is_err());
        assert!(engine.cancel_order("b1", 3).is_err());
    }

    #[test]
    fn test_full_fill_execution_details() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 10, 2)).unwrap();
        assert_eq!(fills[0].quantity, 10);
        assert_eq!(fills[1].quantity, 10);
        assert_eq!(fills[0].price, 100.0);
    }

    // --- A5. Partial Fill ---

    #[test]
    fn test_partial_fill_buy_less() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 20, 1)).unwrap();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 10, 2)).unwrap();
        assert_eq!(fills.len(), 2);
        let order = engine.cancel_order("s1", 3).unwrap().unwrap();
        assert_eq!(order.leaves_qty, 10);
    }

    #[test]
    fn test_partial_fill_buy_more() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 30, 2)).unwrap();
        assert_eq!(fills.len(), 2);
        assert!(engine.cancel_order("s1", 3).is_err());
        let buy = engine.cancel_order("b1", 3).unwrap().unwrap();
        assert_eq!(buy.leaves_qty, 20);
    }

    // --- A6. Multiple Partial Fills ---

    #[test]
    fn test_multiple_sells_partial_fill() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 5, 1)).unwrap();
        engine.process_order(make_order("s2", "SELL", 100.0, 10, 2)).unwrap();
        let fills = engine.process_order(make_order("b1", "BUY", 100.0, 12, 3)).unwrap();
        assert_eq!(fills.len(), 4);
        assert!(engine.cancel_order("s1", 4).is_err());
        let s2 = engine.cancel_order("s2", 4).unwrap().unwrap();
        assert_eq!(s2.leaves_qty, 3);
    }

    // --- A7. FIFO Price-Time Priority ---

    #[test]
    fn test_fifo_same_price() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 100.0, 10, 1)).unwrap();
        engine.process_order(make_order("s2", "SELL", 100.0, 10, 2)).unwrap();
        engine.process_order(make_order("b1", "BUY", 100.0, 15, 3)).unwrap();
        assert!(engine.cancel_order("s1", 4).is_err());
        let s2 = engine.cancel_order("s2", 4).unwrap().unwrap();
        assert_eq!(s2.leaves_qty, 5);
    }

    // --- A8. Best Bid/Ask ---

    #[test]
    fn test_best_bid_multiple_levels() {
        let mut engine = make_engine();
        engine.process_order(make_order("b1", "BUY", 100.0, 10, 1)).unwrap();
        engine.process_order(make_order("b2", "BUY", 101.0, 20, 2)).unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 101.0);
        assert_eq!(bid_qty, 20);
    }

    #[test]
    fn test_best_bid_after_removal() {
        let mut engine = make_engine();
        engine.process_order(make_order("b1", "BUY", 101.0, 20, 1)).unwrap();
        engine.process_order(make_order("b2", "BUY", 100.0, 10, 2)).unwrap();
        engine.cancel_order("b1", 3).unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 100.0);
        assert_eq!(bid_qty, 10);
    }

    #[test]
    fn test_best_ask_updates() {
        let mut engine = make_engine();
        engine.process_order(make_order("s1", "SELL", 105.0, 30, 1)).unwrap();
        engine.process_order(make_order("s2", "SELL", 102.0, 10, 2)).unwrap();
        let (ask_price, ask_qty) = engine.best_bid_ask("AAPL").unwrap().1;
        assert_eq!(ask_price, 102.0);
        assert_eq!(ask_qty, 10);
    }

    // --- A9. Edge Cases ---

    #[test]
    fn test_best_bid_ask_empty() {
        let engine = make_engine();
        assert!(engine.best_bid_ask("AAPL").is_none());
    }

    #[test]
    fn test_multi_symbol_isolation() {
        let mut engine = make_engine();
        engine.process_order(
            Order::new("a1".into(), "u1".into(), "AAPL".into(), "BUY", 100.0, 10, None, 1.0, 1).unwrap(),
        ).unwrap();
        engine.process_order(
            Order::new("t1".into(), "u1".into(), "TSLA".into(), "SELL", 200.0, 5, None, 2.0, 2).unwrap(),
        ).unwrap();
        let (aapl_bid, aapl_ask) = engine.best_bid_ask("AAPL").unwrap();
        assert_eq!(aapl_bid.0, 100.0);
        assert!(aapl_ask.1 == 0);
        let (tsla_bid, tsla_ask) = engine.best_bid_ask("TSLA").unwrap();
        assert!(tsla_bid.1 == 0);
        assert_eq!(tsla_ask.0, 200.0);
    }

    // ════════════════════════════════════════════
    // SECTION B — OrderManager state machine (new)
    // ════════════════════════════════════════════

    // --- B1. add_order ---

    #[test]
    fn test_add_order_state_is_new() {
        let mut om = make_om();
        let order = make_order("o1", "BUY", 100.0, 10, 0);
        om.add_order(order).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::New));
    }

    #[test]
    fn test_add_order_duplicate_fails() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        let err = om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap_err();
        assert_eq!(err, OrderManagerError::AlreadyExists("o1".into()));
    }

    // --- B2. place_order duplicate guard ---

    #[test]
    fn test_place_order_duplicate_id_rejected() {
        let mut om = make_om();
        om.place_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        let err = om.place_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap_err();
        assert_eq!(err, OrderManagerError::AlreadyExists("o1".into()));
    }

    // --- B3. record_fill: New → PartiallyFilled ---

    #[test]
    fn test_record_fill_partial_transitions_new_to_partially_filled() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        // Check remaining via ManagedOrder
        assert_eq!(om.orders["o1"].remaining_quantity, 6);
    }

    // --- B4. record_fill: New → Filled (full fill at once) ---

    #[test]
    fn test_record_fill_full_transitions_to_filled() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Filled));
        assert_eq!(om.orders["o1"].remaining_quantity, 0);
    }

    // --- B5. record_fill: PartiallyFilled → PartiallyFilled → Filled ---

    #[test]
    fn test_record_fill_multiple_partials_then_filled() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::PartiallyFilled));
        assert_eq!(om.orders["o1"].remaining_quantity, 4);
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Filled));
        assert_eq!(om.orders["o1"].remaining_quantity, 0);
    }

    // --- B6. record_fill: overfill is rejected ---

    #[test]
    fn test_record_fill_overfill_error() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        let err = om.record_fill("o1", 11).unwrap_err();
        assert!(matches!(err, OrderManagerError::OverFill { .. }));
    }

    // --- B7. record_fill: terminal order rejected ---

    #[test]
    fn test_record_fill_on_filled_order_fails() {
        let mut om = make_om();
        om.add_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.record_fill("o1", 10).unwrap();
        let err = om.record_fill("o1", 1).unwrap_err();
        assert!(matches!(err, OrderManagerError::InvalidTransition { .. }));
    }

    #[test]
    fn test_record_fill_on_canceled_order_fails() {
        let mut om = make_om();
        // Place then cancel via place_order + cancel_order so engine knows about it.
        om.place_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.cancel_order("o1").unwrap();
        let err = om.record_fill("o1", 1).unwrap_err();
        assert!(matches!(err, OrderManagerError::InvalidTransition { .. }));
    }

    // --- B8. record_fill: unknown order ---

    #[test]
    fn test_record_fill_unknown_order_fails() {
        let mut om = make_om();
        let err = om.record_fill("ghost", 5).unwrap_err();
        assert_eq!(err, OrderManagerError::OrderNotFound("ghost".into()));
    }

    // --- B9. cancel_order: valid transitions ---

    #[test]
    fn test_cancel_from_new_succeeds() {
        let mut om = make_om();
        om.place_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::New));
        om.cancel_order("o1").unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::Canceled));
    }

    #[test]
    fn test_cancel_from_partially_filled_succeeds() {
        let mut om = make_om();
        // Place a sell to rest, then place a partial-matching buy.
        om.place_order(make_order_user("s1", "seller", "SELL", 100.0, 20, 0)).unwrap();
        om.place_order(make_order_user("b1", "buyer", "BUY", 100.0, 5, 0)).unwrap();
        // s1 should be PartiallyFilled (20 - 5 = 15 remaining).
        assert_eq!(om.get_state("s1"), Some(OrderState::PartiallyFilled));
        om.cancel_order("s1").unwrap();
        assert_eq!(om.get_state("s1"), Some(OrderState::Canceled));
    }

    // --- B10. cancel_order: invalid transitions ---

    #[test]
    fn test_cancel_filled_order_fails() {
        let mut om = make_om();
        om.place_order(make_order_user("s1", "seller", "SELL", 100.0, 10, 0)).unwrap();
        om.place_order(make_order_user("b1", "buyer", "BUY", 100.0, 10, 0)).unwrap();
        assert_eq!(om.get_state("s1"), Some(OrderState::Filled));
        let err = om.cancel_order("s1").unwrap_err();
        assert!(matches!(err, OrderManagerError::InvalidTransition { .. }));
    }

    #[test]
    fn test_cancel_already_canceled_fails() {
        let mut om = make_om();
        om.place_order(make_order("o1", "BUY", 100.0, 10, 0)).unwrap();
        om.cancel_order("o1").unwrap();
        let err = om.cancel_order("o1").unwrap_err();
        assert!(matches!(err, OrderManagerError::InvalidTransition { .. }));
    }

    #[test]
    fn test_cancel_unknown_order_fails() {
        let mut om = make_om();
        let err = om.cancel_order("ghost").unwrap_err();
        assert_eq!(err, OrderManagerError::OrderNotFound("ghost".into()));
    }

    // --- B11. get_state ---

    #[test]
    fn test_get_state_none_for_unknown() {
        let om = make_om();
        assert_eq!(om.get_state("no-such-order"), None);
    }

    // --- B12. place_order state integration ---

    #[test]
    fn test_place_order_fully_matched_state_is_filled() {
        let mut om = make_om();
        om.place_order(make_order_user("s1", "seller", "SELL", 100.0, 10, 0)).unwrap();
        om.place_order(make_order_user("b1", "buyer", "BUY", 100.0, 10, 0)).unwrap();
        assert_eq!(om.get_state("s1"), Some(OrderState::Filled));
        assert_eq!(om.get_state("b1"), Some(OrderState::Filled));
    }

    #[test]
    fn test_place_order_no_match_state_is_new() {
        let mut om = make_om();
        om.place_order(make_order("o1", "BUY", 90.0, 10, 0)).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderState::New));
    }

    #[test]
    fn test_place_order_partial_match_incoming_partially_filled() {
        let mut om = make_om();
        // Sell 5, buy 10 → buy fills 5 and rests 5.
        om.place_order(make_order_user("s1", "seller", "SELL", 100.0, 5, 0)).unwrap();
        om.place_order(make_order_user("b1", "buyer", "BUY", 100.0, 10, 0)).unwrap();
        assert_eq!(om.get_state("b1"), Some(OrderState::PartiallyFilled));
        assert_eq!(om.orders["b1"].remaining_quantity, 5);
    }
}