use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};
mod order;
mod sequencer;

use crate::sequencer::Sequencer;

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

        // If the price level is empty, remove it from the book entirely
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
                    // Get best ask price (lowest sell)
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
                        break; // skip self‑trade (simplified)
                    }

                    let trade_qty = order.leaves_qty.min(sell_resting.leaves_qty);
                    let trade_price = sell_resting.price;

                    // Generate execution IDs
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

                    // Update quantities
                    order.leaves_qty -= trade_qty;
                    sell_resting.leaves_qty -= trade_qty;

                    // Remove fully filled resting order
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
                    // Get best bid price (highest buy) – key is Reverse(Price)
                    let best_bid_key = match self.buy_levels.first_key_value() {
                        Some((&rev_price, _)) => rev_price,
                        None => break,
                    };
                    let best_bid_price = best_bid_key.0; // unwrap Reverse
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
                        break;
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

#[derive(Debug)]
pub struct MatchingEngine {
    order_books: HashMap<String, OrderBook>, // symbol -> book
    order_location: HashMap<String, String>, // order_id -> symbol
    last_seq: u64,                           // last processed sequence number
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
        // 1. Validate sequence
        if order.seq_num <= self.last_seq {
            return Err(format!(
                "Sequence violation: received seq {} but last was {}",
                order.seq_num, self.last_seq
            ));
        }

        let symbol = order.symbol.clone();
        let order_id = order.order_id.clone();
        let seq_num = order.seq_num; // save before moving order

        // 2. Get or create the OrderBook for this symbol
        let book = self
            .order_books
            .entry(symbol.clone())
            .or_insert_with(|| OrderBook::new(symbol.clone()));

        // 3. Place the order and get fills
        let fills = book.place_order(order);

        // 4. Update last sequence
        self.last_seq = seq_num;
        // 5. If the order is still resting, remember its symbol for fast cancellation
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
        // 1. Validate sequence
        if cancel_seq <= self.last_seq {
            return Err(format!(
                "Sequence violation on cancel: received seq {} but last was {}",
                cancel_seq, self.last_seq
            ));
        }

        // 2. Find the symbol from order_location
        let symbol = self
            .order_location
            .remove(order_id)
            .ok_or_else(|| format!("Order {} not found for cancellation", order_id))?;

        // 3. Get the OrderBook for that symbol
        let book = self
            .order_books
            .get_mut(&symbol)
            .ok_or_else(|| format!("Order book for symbol {} not found", symbol))?;

        // 4. Cancel the order in the book
        let removed = book.cancel_order(order_id);

        // (last_seq update will go here in next chunk)
        // 5. Update last sequence
        self.last_seq = cancel_seq;

        Ok(removed)
    }
    /// Returns true if the order is still resting in an order book.
    pub fn is_resting(&self, order_id: &str) -> bool {
        self.order_location.contains_key(order_id)
    }

    /// Returns the remaining `leaves_qty` of a resting order, if it exists.
    pub fn get_order_leaves(&self, order_id: &str) -> Option<u32> {
        let symbol = self.order_location.get(order_id)?;
        let book = self.order_books.get(symbol)?;
        // We need to dig into the book’s order_map → PriceLevel → node
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartialFill { leaves_qty: u32 },
    Filled,
    Canceled,
}

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

pub struct OrderManager {
    engine: MatchingEngine,
    sequencer: Sequencer,
    order_states: HashMap<String, OrderState>,
    execution_callbacks: Vec<Box<dyn Fn(Execution)>>,
}

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

impl OrderManager {
    pub fn new() -> Self {
        Self {
            engine: MatchingEngine::new(),
            sequencer: Sequencer::new(1),
            order_states: HashMap::new(),
            execution_callbacks: Vec::new(),
        }
    }
    pub fn place_order(&mut self, mut order: Order) -> Result<Vec<Execution>, String> {
        order.seq_num = self.sequencer.next();

        let order_id = order.order_id.clone();
        let original_qty = order.quantity;
        let fills = self.engine.process_order(order)?;

        // After engine.process_order() returns fills:

        // -- Update state for the incoming order itself
        let incoming_id = order_id.clone();
        if self.engine.is_resting(&incoming_id) {
            let leaves = self
                .engine
                .get_order_leaves(&incoming_id)
                .unwrap_or(original_qty);
            self.order_states
                .insert(incoming_id, OrderState::PartialFill { leaves_qty: leaves });
        } else {
            self.order_states.insert(incoming_id, OrderState::Filled);
        }

        // -- Update states for any resting orders that were involved in fills
        for exec in &fills {
            // Update the sell side
            if !self.engine.is_resting(&exec.sell_order_id) {
                self.order_states
                    .insert(exec.sell_order_id.clone(), OrderState::Filled);
            } else {
                if let Some(leaves) = self.engine.get_order_leaves(&exec.sell_order_id) {
                    self.order_states.insert(
                        exec.sell_order_id.clone(),
                        OrderState::PartialFill { leaves_qty: leaves },
                    );
                }
            }
            // Update the buy side (if it was a resting order, not the incoming one)
            if exec.buy_order_id != order_id {
                // avoid double‑updating the incoming order
                if !self.engine.is_resting(&exec.buy_order_id) {
                    self.order_states
                        .insert(exec.buy_order_id.clone(), OrderState::Filled);
                } else {
                    if let Some(leaves) = self.engine.get_order_leaves(&exec.buy_order_id) {
                        self.order_states.insert(
                            exec.buy_order_id.clone(),
                            OrderState::PartialFill { leaves_qty: leaves },
                        );
                    }
                }
            }
        }
        for exec in &fills {
            for cb in &self.execution_callbacks {
                cb(exec.clone());
            }
        }
        Ok(fills)
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<Option<Order>, String> {
        let cancel_seq = self.sequencer.next();
        let removed = self.engine.cancel_order(order_id, cancel_seq)?;

        if removed.is_some() {
            self.order_states
                .insert(order_id.to_string(), OrderState::Canceled);
        }

        Ok(removed)
    }

    pub fn get_order_state(&self, order_id: &str) -> Option<OrderState> {
        self.order_states.get(order_id).copied()
    }

    pub fn best_bid_ask(&self, symbol: &str) -> Option<((f64, u32), (f64, u32))> {
        self.engine.best_bid_ask(symbol)
    }

    /// Registers a callback that will be invoked for every execution.
    pub fn subscribe<F: Fn(Execution) + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }
}

#[derive(Debug, PartialEq)]
pub enum RiskError {
    LimitExceeded {
        user_id: String,
        symbol: String,
        current_volume: u64,
        limit: u64,
    },
}

pub struct RiskManager {
    limits: HashMap<(String, String), u64>, // (user_id, symbol) -> max qty
    volumes: HashMap<(String, String), u64>, // (user_id, symbol) -> used qty today
}

impl RiskManager {
    pub fn new() -> Self {
        Self {
            limits: HashMap::new(),
            volumes: HashMap::new(),
        }
    }

    pub fn set_limit(&mut self, user_id: String, symbol: String, limit: u64) {
        self.limits.insert((user_id, symbol), limit);
    }

    pub fn check_and_record(&mut self, order: &Order) -> Result<(), RiskError> {
        let key = (order.user_id.clone(), order.symbol.clone());

        // No limit set = unlimited, always pass
        let limit = match self.limits.get(&key) {
            Some(&l) => l,
            None => return Ok(()),
        };

        let current_volume = *self.volumes.get(&key).unwrap_or(&0);
        let incoming_qty = order.quantity as u64;

        if current_volume + incoming_qty > limit {
            return Err(RiskError::LimitExceeded {
                user_id: order.user_id.clone(),
                symbol: order.symbol.clone(),
                current_volume,
                limit,
            });
        }

        *self.volumes.entry(key).or_insert(0) += incoming_qty;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderStateNew {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
}

pub struct ManagedOrder {
    pub order: Order,
    pub state: OrderStateNew,
    pub remaining_quantity: u32,
}

pub struct OrderManagerNew {
    pub orders: HashMap<String, ManagedOrder>,
    pub risk_manager: RiskManager,
    pub wallet: Wallet,
    pub engine: MatchingEngine,
    pub sequencer: Sequencer,
    execution_callbacks: Vec<Box<dyn Fn(Execution)>>,
}

impl OrderManagerNew {
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

    pub fn add_order(&mut self, mut order: Order) -> Result<(), OrderManagerError> {
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

        let new_seq = self.sequencer.next();

        order.seq_num = new_seq;

        let order_id = order.order_id.clone();
        let original_qty = order.quantity;
        let og_order = order.clone();
        let fills = self
            .engine
            .process_order(order)
            .map_err(OrderManagerError::RiskRejected)?;

        self.orders.insert(
            order_id,
            ManagedOrder {
                order: og_order,
                state: OrderStateNew::New,
                remaining_quantity: original_qty,
            },
        );
        for fill in fills {
            self.record_fill(&fill.buy_order_id, fill.quantity)?;
            self.record_fill(&fill.sell_order_id, fill.quantity)?;

            // Settle trade cash: transfer cash from the buyer to the seller!
            let buyer_user_id = self
                .orders
                .get(&fill.buy_order_id)
                .map(|o| o.order.user_id.clone());
            let seller_user_id = self
                .orders
                .get(&fill.sell_order_id)
                .map(|o| o.order.user_id.clone());

            if let (Some(_buyer), Some(seller)) = (buyer_user_id, seller_user_id) {
                let cash_amount = (fill.price * fill.quantity as f64) as u64;
                self.wallet.deposit(seller, cash_amount);
            }

            // Trigger subscriber callbacks
            for cb in &self.execution_callbacks {
                cb(fill.clone());
            }
        }

        Ok(())
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Result<(), OrderManagerError> {
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
            OrderStateNew::Filled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "order {} is already Filled",
                    order_id
                )));
            }
            OrderStateNew::Canceled => {
                return Err(OrderManagerError::InvalidTransition(format!(
                    "order {} is already Canceled",
                    order_id
                )));
            }
            _ => {}
        }

        // 3. Call the matching engine to cancel it there (if it's resting)
        let cancel_seq = self.sequencer.next();
        let _ = self
            .engine
            .cancel_order(order_id, cancel_seq)
            .map_err(OrderManagerError::OrderNotFound)?;

        // 4. If matching engine cancellation succeeds, unlock funds and transition state
        let _ = self
            .wallet
            .unlock_funds(&user_id, &side, price, remaining as u64);

        if let Some(managed) = self.orders.get_mut(order_id) {
            managed.state = OrderStateNew::Canceled;
        }

        Ok(())
    }

    fn record_fill(&mut self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError> {
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
            OrderStateNew::Filled | OrderStateNew::Canceled => {
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
            managed.state = OrderStateNew::Filled;
        } else {
            managed.state = OrderStateNew::PartiallyFilled;
        }

        Ok(())
    }

    pub fn get_state(&self, order_id: &str) -> Option<OrderStateNew> {
        self.orders.get(order_id).map(|m| m.state)
    }

    pub fn subscribe<F: Fn(Execution) + 'static>(&mut self, callback: F) {
        self.execution_callbacks.push(Box::new(callback));
    }
}

#[derive(Debug, PartialEq)]
pub enum WalletError {
    InsufficientFunds,
    Overlfow,
}

pub struct Wallet {
    balances: HashMap<String, u64>,
    locked: HashMap<String, u64>,
}

impl Wallet {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
            locked: HashMap::new(),
        }
    }

    pub fn deposit(&mut self, user_id: String, amount: u64) {
        *self.balances.entry(user_id).or_insert(0) += amount;
    }

    pub fn check_and_lock(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        quantity: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        let required = (price * quantity as f64) as u64;
        let balance = self.balances.get(user_id).copied().unwrap_or(0);
        let locked = self.locked.get(user_id).copied().unwrap_or(0);

        let available = balance.checked_sub(locked).unwrap_or(0);
        if available < required {
            return Err(WalletError::InsufficientFunds);
        }

        *self.locked.entry(user_id.to_string()).or_insert(0) += required;
        Ok(())
    }

    // Called on fill: money is actually spent
    pub fn commit_fill(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        qty_filled: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        let amount = (price * qty_filled as f64) as u64;

        let balance = self
            .balances
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *balance = balance
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        let locked = self
            .locked
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *locked = locked
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        Ok(())
    }

    // Called on cancel: just release the lock, no balance change
    pub fn unlock_funds(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        qty_unlocked: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        let amount = (price * qty_unlocked as f64) as u64;

        let locked = self
            .locked
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *locked = locked
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        Ok(())
    }
}
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
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::New));
    }

    #[test]
    fn test_add_duplicate_fails() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        let err = om.add_order(sample_order("o1", 10)).unwrap_err();
        assert_eq!(err, OrderManagerError::AlreadyExists("o1".into()));
    }

    #[test]
    fn test_partial_fill_new_to_partially_filled() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::PartiallyFilled));
        assert_eq!(om.orders["o1"].remaining_quantity, 6);
    }

    #[test]
    fn test_full_fill_goes_to_filled() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::Filled));
        assert_eq!(om.orders["o1"].remaining_quantity, 0);
    }

    #[test]
    fn test_multiple_partials_then_filled() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::PartiallyFilled));
        om.record_fill("o1", 3).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::PartiallyFilled));
        om.record_fill("o1", 4).unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::Filled));
    }

    #[test]
    fn test_cancel_from_new() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.cancel_order("o1").unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::Canceled));
    }

    #[test]
    fn test_cancel_from_partially_filled() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 4).unwrap();
        om.cancel_order("o1").unwrap();
        assert_eq!(om.get_state("o1"), Some(OrderStateNew::Canceled));
    }

    #[test]
    fn test_cancel_filled_fails() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert!(matches!(
            om.cancel_order("o1").unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_cancel_already_canceled_fails() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.cancel_order("o1").unwrap();
        assert!(matches!(
            om.cancel_order("o1").unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_overfill_rejected() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        assert!(matches!(
            om.record_fill("o1", 11).unwrap_err(),
            OrderManagerError::OverFill(_)
        ));
    }

    #[test]
    fn test_fill_terminal_order_fails() {
        let mut om = OrderManagerNew::new();
        om.add_order(sample_order("o1", 10)).unwrap();
        om.record_fill("o1", 10).unwrap();
        assert!(matches!(
            om.record_fill("o1", 1).unwrap_err(),
            OrderManagerError::InvalidTransition(_)
        ));
    }

    #[test]
    fn test_unknown_order_errors() {
        let mut om = OrderManagerNew::new();
        assert_eq!(
            om.record_fill("ghost", 5).unwrap_err(),
            OrderManagerError::OrderNotFound("ghost".into())
        );
        assert_eq!(
            om.cancel_order("ghost").unwrap_err(),
            OrderManagerError::OrderNotFound("ghost".into())
        );
    }

    #[test]
    fn test_connected_order_manager_matching_and_wallet_transfer() {
        let mut om = OrderManagerNew::new();
        
        // 1. Setup wallets
        om.wallet.deposit("u1".to_string(), 1000); // Buyer has 1000
        
        // 2. Setup subscription
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        let executions_count = Arc::new(AtomicU32::new(0));
        let exec_count_clone = executions_count.clone();
        om.subscribe(move |_exec| {
            exec_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // 3. Place resting SELL order
        let o_sell = Order::new(
            "o_sell".to_string(),
            "u2".to_string(),
            "AAPL".to_string(),
            "SELL",
            100.0,
            10,
            None,
            1.0,
            0,
        )
        .unwrap();
        om.add_order(o_sell).unwrap();

        // 4. Place matching BUY order
        let o_buy = Order::new(
            "o_buy".to_string(),
            "u1".to_string(),
            "AAPL".to_string(),
            "BUY",
            100.0,
            10,
            None,
            2.0,
            0,
        )
        .unwrap();
        om.add_order(o_buy).unwrap();

        // 5. Verify states
        assert_eq!(om.get_state("o_sell"), Some(OrderStateNew::Filled));
        assert_eq!(om.get_state("o_buy"), Some(OrderStateNew::Filled));

        // 6. Verify cash transfer
        assert_eq!(om.wallet.balances.get("u1").copied().unwrap_or(0), 0);
        assert_eq!(om.wallet.locked.get("u1").copied().unwrap_or(0), 0);
        assert_eq!(om.wallet.balances.get("u2").copied().unwrap_or(0), 1000);

        // 7. Verify subscription callbacks (2 executions: 1 buy fill, 1 sell fill)
        assert_eq!(executions_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_connected_cancellation_in_engine() {
        let mut om = OrderManagerNew::new();

        // 1. Place resting SELL order
        let o_sell = Order::new(
            "o_sell".to_string(),
            "u2".to_string(),
            "AAPL".to_string(),
            "SELL",
            100.0,
            10,
            None,
            1.0,
            0,
        )
        .unwrap();
        om.add_order(o_sell).unwrap();

        // 2. Cancel the order
        om.cancel_order("o_sell").unwrap();
        assert_eq!(om.get_state("o_sell"), Some(OrderStateNew::Canceled));

        // 3. Placing a matching BUY order should NOT trade since the SELL was canceled in the engine
        om.wallet.deposit("u1".to_string(), 1000);
        let o_buy = Order::new(
            "o_buy".to_string(),
            "u1".to_string(),
            "AAPL".to_string(),
            "BUY",
            100.0,
            10,
            None,
            2.0,
            0,
        )
        .unwrap();
        om.add_order(o_buy).unwrap();

        // BUY order should just rest as New because SELL order is gone
        assert_eq!(om.get_state("o_buy"), Some(OrderStateNew::New));
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
            let mut om = OrderManagerNew::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            assert!(om.add_order(make_order("u1", "AAPL", 500)).is_ok());
            // volume is now 500
        }

        #[test]
        fn test_exceeds_limit_rejected() {
            let mut om = OrderManagerNew::new();
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
            let mut om = OrderManagerNew::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 500)).unwrap();
            // use 499 + 1 would also work, but simplest fix: different qty = different ID
            assert!(om.add_order(make_order("u1", "AAPL", 499)).is_ok()); // 500+499=999 ≤ 1000
        }

        #[test]
        fn test_different_user_unaffected() {
            let mut om = OrderManagerNew::new();
            om.risk_manager.set_limit("u1".into(), "AAPL".into(), 1000);
            om.add_order(make_order("u1", "AAPL", 900)).unwrap();
            // u2 has no limit, should always pass
            assert!(om.add_order(make_order("u2", "AAPL", 9999)).is_ok());
        }

        #[test]
        fn test_no_limit_set_always_passes() {
            let mut om = OrderManagerNew::new();
            // no limits configured at all
            assert!(om.add_order(make_order("u1", "AAPL", 99999)).is_ok());
        }

        #[test]
        fn test_different_symbol_unaffected() {
            let mut om = OrderManagerNew::new();
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
