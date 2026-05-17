use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};

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
use std::time::Instant;

fn main() {
    let mut engine = MatchingEngine::new();
    let n_orders = 100_000;
    let mut seq: u64 = 1;

    // Generate a realistic mix of buy/sell orders with slightly varying prices
    let orders: Vec<Order> = (0..n_orders)
        .map(|i| {
            let side = if i % 2 == 0 { "BUY" } else { "SELL" };
            let price = 100.0 + ((i as f64 * 0.01) % 1.0) - 0.5; // 99.5 .. 100.5
            let qty = (i % 5 + 1) as u32;
            let seq_num = seq;
            seq += 1;
            Order::new(
                format!("order_{}", i),
                format!("user_{}", i % 100), // 100 distinct users
                "AAPL".into(),
                side,
                price,
                qty,
                None,
                i as f64 * 0.001, // increasing timestamps
                seq_num,
            )
            .expect("valid order")
        })
        .collect();

    let start = Instant::now();

    let mut total_executions = 0;
    for order in orders {
        match engine.process_order(order) {
            Ok(execs) => total_executions += execs.len(),
            Err(e) => {
                eprintln!("Error processing order: {}", e);
                std::process::exit(1);
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = n_orders as f64 / elapsed.as_secs_f64();

    println!(
        "Processed {} orders in {:.3} s",
        n_orders,
        elapsed.as_secs_f64()
    );
    println!("Throughput: {:.0} orders/sec", throughput);
    println!("Total executions generated: {}", total_executions);

    assert!(
        throughput > 10_000.0,
        "Throughput below target: {:.0}",
        throughput
    );
    println!("\n✅ Benchmark passed – Phase 1 throughput target met!");
}
#[cfg(test)]
mod tests {
    use super::*;

    // --- Helpers ---
    fn make_order(id: &str, side: &str, price: f64, qty: u32, seq: u64) -> Order {
        Order::new(
            id.to_string(),
            "user".to_string(),
            "AAPL".to_string(),
            side,
            price,
            qty,
            None,
            seq as f64, // timestamp = seq_num for ordering
            seq,
        )
        .unwrap()
    }

    fn make_engine() -> MatchingEngine {
        MatchingEngine::new()
    }

    // --- 1. Sequence Validation ---
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
        engine
            .process_order(make_order("o1", "BUY", 100.0, 10, 1))
            .unwrap();
        let order2 = make_order("o2", "BUY", 100.0, 10, 1); // same seq
        assert!(engine.process_order(order2).is_err());
    }

    #[test]
    fn test_sequence_reject_lower() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("o1", "BUY", 100.0, 10, 5))
            .unwrap();
        let order2 = make_order("o2", "BUY", 100.0, 10, 3); // lower seq
        assert!(engine.process_order(order2).is_err());
    }

    #[test]
    fn test_sequence_last_seq_unchanged_on_error() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("o1", "BUY", 100.0, 10, 10))
            .unwrap();
        assert_eq!(engine.last_seq, 10);
        let err = engine.process_order(make_order("o2", "BUY", 100.0, 10, 5));
        assert!(err.is_err());
        assert_eq!(engine.last_seq, 10); // unchanged
    }

    // --- 2. Place & No Match ---
    #[test]
    fn test_place_order_no_match() {
        let mut engine = make_engine();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 1))
            .unwrap();
        assert!(fills.is_empty());
        // Check that order is resting via cancel
        let cancelled = engine.cancel_order("b1", 2).unwrap();
        assert!(cancelled.is_some());
    }

    #[test]
    fn test_best_bid_after_no_match() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 1))
            .unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 100.0);
        assert_eq!(bid_qty, 10);
    }

    // --- 3. Cancel Order ---
    #[test]
    fn test_cancel_resting_order() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap();
        let removed = engine.cancel_order("s1", 2).unwrap().unwrap();
        assert_eq!(removed.order_id, "s1");
        // Cancel again fails
        assert!(engine.cancel_order("s1", 3).is_err());
    }

    #[test]
    fn test_cancel_updates_order_book() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap();
        engine.cancel_order("s1", 2).unwrap();
        // Best ask should be empty
        assert!(engine.best_bid_ask("AAPL").unwrap().1.1 == 0); // ask qty 0
    }

    // --- 4. Full Fill ---
    #[test]
    fn test_full_fill_basic() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 2))
            .unwrap();
        assert_eq!(fills.len(), 2); // two executions
        assert!(engine.cancel_order("s1", 3).is_err()); // filled
        assert!(engine.cancel_order("b1", 3).is_err()); // filled
    }

    #[test]
    fn test_full_fill_execution_details() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 2))
            .unwrap();
        assert_eq!(fills[0].quantity, 10);
        assert_eq!(fills[1].quantity, 10);
        assert_eq!(fills[0].price, 100.0);
    }

    // --- 5. Partial Fill ---
    #[test]
    fn test_partial_fill_buy_less() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 20, 1))
            .unwrap();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 2))
            .unwrap();
        assert_eq!(fills.len(), 2);
        // Sell still resting with 10 leaves
        let order = engine.cancel_order("s1", 3).unwrap().unwrap();
        assert_eq!(order.leaves_qty, 10);
    }

    #[test]
    fn test_partial_fill_buy_more() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 30, 2))
            .unwrap();
        assert_eq!(fills.len(), 2);
        // Sell is fully filled, buy resting with 20
        assert!(engine.cancel_order("s1", 3).is_err());
        let buy = engine.cancel_order("b1", 3).unwrap().unwrap();
        assert_eq!(buy.leaves_qty, 20);
    }

    // --- 6. Multiple Partial Fills ---
    #[test]
    fn test_multiple_sells_partial_fill() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 100.0, 5, 1))
            .unwrap();
        engine
            .process_order(make_order("s2", "SELL", 100.0, 10, 2))
            .unwrap();
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 12, 3))
            .unwrap();
        assert_eq!(fills.len(), 4); // 2 executions per match -> 4 total
        // s1 fully filled, s2 partially filled (8 leaves)
        assert!(engine.cancel_order("s1", 4).is_err());
        let s2 = engine.cancel_order("s2", 4).unwrap().unwrap();
        assert_eq!(s2.leaves_qty, 3); // 10 - 7? Wait: buy 12 matched s1(5) then s2(7), so s2 leaves 3
        // Actually: buy 12, first match s1 (5) -> buy leaves 7, s1 leaves 0. second match s2 (7) -> buy leaves 0, s2 leaves 3.
        assert_eq!(s2.leaves_qty, 3);
    }

    // --- 7. FIFO Price-Time Priority ---
    #[test]
    fn test_fifo_same_price() {
        let mut engine = make_engine();
        // Place two sells at same price, different timestamps (seq as timestamp)
        engine
            .process_order(make_order("s1", "SELL", 100.0, 10, 1))
            .unwrap(); // timestamp 1.0
        engine
            .process_order(make_order("s2", "SELL", 100.0, 10, 2))
            .unwrap(); // timestamp 2.0
        let fills = engine
            .process_order(make_order("b1", "BUY", 100.0, 15, 3))
            .unwrap();
        // s1 should be fully filled, s2 partially
        assert!(engine.cancel_order("s1", 4).is_err());
        let s2 = engine.cancel_order("s2", 4).unwrap().unwrap();
        assert_eq!(s2.leaves_qty, 5);
    }

    // --- 8. Best Bid/Ask Updates ---
    #[test]
    fn test_best_bid_multiple_levels() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("b1", "BUY", 100.0, 10, 1))
            .unwrap();
        engine
            .process_order(make_order("b2", "BUY", 101.0, 20, 2))
            .unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 101.0);
        assert_eq!(bid_qty, 20);
    }

    #[test]
    fn test_best_bid_after_removal() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("b1", "BUY", 101.0, 20, 1))
            .unwrap();
        engine
            .process_order(make_order("b2", "BUY", 100.0, 10, 2))
            .unwrap();
        // Remove b1 (cancel)
        engine.cancel_order("b1", 3).unwrap();
        let (bid_price, bid_qty) = engine.best_bid_ask("AAPL").unwrap().0;
        assert_eq!(bid_price, 100.0);
        assert_eq!(bid_qty, 10);
    }

    #[test]
    fn test_best_ask_updates() {
        let mut engine = make_engine();
        engine
            .process_order(make_order("s1", "SELL", 105.0, 30, 1))
            .unwrap();
        engine
            .process_order(make_order("s2", "SELL", 102.0, 10, 2))
            .unwrap();
        let (ask_price, ask_qty) = engine.best_bid_ask("AAPL").unwrap().1;
        assert_eq!(ask_price, 102.0);
        assert_eq!(ask_qty, 10);
    }

    // --- 9. Edge Cases ---
    #[test]
    fn test_best_bid_ask_empty() {
        let engine = make_engine();
        assert!(engine.best_bid_ask("AAPL").is_none());
    }

    #[test]
    fn test_multi_symbol_isolation() {
        let mut engine = make_engine();
        engine
            .process_order(
                Order::new(
                    "a1".into(),
                    "u1".into(),
                    "AAPL".into(),
                    "BUY",
                    100.0,
                    10,
                    None,
                    1.0,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        engine
            .process_order(
                Order::new(
                    "t1".into(),
                    "u1".into(),
                    "TSLA".into(),
                    "SELL",
                    200.0,
                    5,
                    None,
                    2.0,
                    2,
                )
                .unwrap(),
            )
            .unwrap();
        let (aapl_bid, aapl_ask) = engine.best_bid_ask("AAPL").unwrap();
        assert_eq!(aapl_bid.0, 100.0);
        assert!(aapl_ask.1 == 0);
        let (tsla_bid, tsla_ask) = engine.best_bid_ask("TSLA").unwrap();
        assert!(tsla_bid.1 == 0);
        assert_eq!(tsla_ask.0, 200.0);
    }
}
