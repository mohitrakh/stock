use std::cmp::Reverse;
use std::collections::HashMap;

use super::order_book::OrderBook;
use super::types::{Execution, Order, Side};

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
        let (fills, is_incoming_resting, no_longer_resting) = {
            let book = self
                .order_books
                .entry(symbol.clone())
                .or_insert_with(|| OrderBook::new(symbol.clone()));

            // 3. Place the order and get fills
            let fills = book.place_order(order);
            let is_incoming_resting = book.is_resting(&order_id);
            let mut no_longer_resting = Vec::new();

            for fill in &fills {
                for filled_order_id in [&fill.buy_order_id, &fill.sell_order_id] {
                    if !book.is_resting(filled_order_id) {
                        no_longer_resting.push(filled_order_id.clone());
                    }
                }
            }

            (fills, is_incoming_resting, no_longer_resting)
        };

        // 4. Update last sequence
        self.last_seq = seq_num;
        for filled_order_id in no_longer_resting {
            self.order_location.remove(&filled_order_id);
        }
        // 5. If the order is still resting, remember its symbol for fast cancellation
        if is_incoming_resting {
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
