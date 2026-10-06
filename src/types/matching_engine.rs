use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::order_book::{MatchPlan, OrderBook, OrderBookSnapshot};
use super::types::{L2Level, Order, OrderBookView, Price};

/// Most orders the books hold at once, across every symbol. The close expires every resting order
/// in one journal record, which may not exceed 64 MiB. With the gateway's longest order id (64
/// bytes, each escaped to two in JSON) and the widest sequence numbers, one expiry takes under 280
/// bytes there, so a close of this many orders, about 56 MB, always fits.
pub(crate) const MAX_RESTING_ORDERS: usize = 200_000;

/// Most resting orders one new order may trade against. Each trade adds two executions to the
/// order's journal record, which may not exceed 64 MiB. At the widest values (the gateway's longest
/// ids and symbol, each byte escaped to two in JSON, and the largest numbers) one trade takes 1,376
/// bytes there, so an order with this many, 13.8 MB, always fits. An order that would trade against
/// more is refused before anything changes; without the cap its record could not be written, and
/// the worker would halt.
pub(crate) const MAX_FILLS_PER_ORDER: usize = 10_000;

#[derive(Debug)]
pub struct MatchingEngine {
    order_books: HashMap<String, OrderBook>,
    order_location: HashMap<String, String>,
    last_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MatchingEngineSnapshot {
    books: Vec<OrderBookSnapshot>,
    last_seq: u64,
}

/// A new order's matching, worked out against the live book but not yet applied to it.
pub(crate) struct PreparedOrder {
    pub(crate) order: Order,
    pub(crate) plan: MatchPlan,
}

pub(crate) struct PreparedCancel {
    pub(crate) symbol: String,
    pub(crate) order_id: String,
    pub(crate) seq_num: u64,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            order_books: HashMap::new(),
            order_location: HashMap::new(),
            last_seq: 0,
        }
    }

    /// Plans the order against the live book. An order that would trade against more than
    /// `max_fills` resting orders gets a plan marked `MatchPlan::too_many_fills`, which the caller
    /// refuses.
    pub(crate) fn prepare_order(
        &self,
        order: Order,
        max_fills: usize,
    ) -> Result<PreparedOrder, String> {
        if order.seq_num <= self.last_seq {
            return Err(format!(
                "Sequence violation: received seq {} but last was {}",
                order.seq_num, self.last_seq
            ));
        }

        // Read the live book; never copy it. A symbol with no book yet has nothing to match.
        let plan = match self.order_books.get(&order.symbol) {
            Some(book) => book.plan_order(&order, max_fills),
            None => OrderBook::new(order.symbol.clone()).plan_order(&order, max_fills),
        };
        Ok(PreparedOrder { order, plan })
    }

    pub(crate) fn commit_order(&mut self, prepared: PreparedOrder) {
        let PreparedOrder { order, plan } = prepared;
        for filled_order_id in plan.filled_resting_orders() {
            self.order_location.remove(filled_order_id);
        }
        self.last_seq = order.seq_num;
        if plan.remaining > 0 {
            self.order_location
                .insert(order.order_id.clone(), order.symbol.clone());
        }
        self.order_books
            .entry(order.symbol.clone())
            .or_insert_with(|| OrderBook::new(order.symbol.clone()))
            .apply_plan(order, &plan);
    }

    pub(crate) fn prepare_cancel(
        &self,
        order_id: &str,
        cancel_seq: u64,
    ) -> Result<PreparedCancel, String> {
        if cancel_seq <= self.last_seq {
            return Err(format!(
                "Sequence violation on cancel: received seq {} but last was {}",
                cancel_seq, self.last_seq
            ));
        }

        let symbol = self
            .order_location
            .get(order_id)
            .ok_or_else(|| format!("Order {} not found for cancellation", order_id))?
            .clone();
        let book = self
            .order_books
            .get(&symbol)
            .ok_or_else(|| format!("Order book for symbol {} not found", symbol))?;
        if !book.is_resting(order_id) {
            return Err(format!("Order {} not found in its order book", order_id));
        }

        Ok(PreparedCancel {
            symbol,
            order_id: order_id.to_string(),
            seq_num: cancel_seq,
        })
    }

    pub(crate) fn commit_cancel(&mut self, prepared: PreparedCancel) {
        if let Some(book) = self.order_books.get_mut(&prepared.symbol) {
            book.cancel_order(&prepared.order_id);
        }
        self.order_location.remove(&prepared.order_id);
        self.last_seq = prepared.seq_num;
    }

    /// Expires every resting order at the close; `last_seq` is the last sequence the expiries
    /// consumed. The books stay, empty, so each keeps its execution counter.
    pub(crate) fn commit_expire_all(&mut self, last_seq: u64) {
        for book in self.order_books.values_mut() {
            book.clear();
        }
        self.order_location.clear();
        self.last_seq = last_seq;
    }

    pub fn best_bid_ask(&self, symbol: &str) -> Option<((Price, u64), (Price, u64))> {
        let book = self.order_books.get(symbol)?;
        Some((book.best_bid()?, book.best_ask()?))
    }

    pub fn l2_snapshot(&self, symbol: &str, depth: usize) -> Option<OrderBookView> {
        let book = self.order_books.get(symbol)?;
        let (bids, asks) = book.l2_snapshot(depth);
        let to_levels = |levels: Vec<(Price, u64)>| {
            levels
                .into_iter()
                .map(|(price, quantity)| L2Level {
                    price: price.minor_units(),
                    quantity,
                })
                .collect()
        };

        Some(OrderBookView {
            symbol: symbol.to_string(),
            bids: to_levels(bids),
            asks: to_levels(asks),
        })
    }

    pub fn is_resting(&self, order_id: &str) -> bool {
        self.order_location.contains_key(order_id)
    }

    /// Whether the new order this plan was made for would rest beyond `max_resting` resting
    /// orders. Orders that trade without resting are never refused, nor is one that takes as many
    /// resting orders out of the book as it adds.
    pub(crate) fn exceeds_capacity(&self, plan: &MatchPlan, max_resting: usize) -> bool {
        plan.remaining > 0
            && self.order_location.len() + 1 - plan.filled_resting_orders().count() > max_resting
    }

    pub(crate) fn snapshot(&self) -> MatchingEngineSnapshot {
        let mut books: Vec<_> = self.order_books.values().map(OrderBook::snapshot).collect();
        books.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        MatchingEngineSnapshot {
            books,
            last_seq: self.last_seq,
        }
    }

    pub(crate) fn from_snapshot(snapshot: MatchingEngineSnapshot) -> Result<Self, String> {
        let mut order_books = HashMap::new();
        let mut order_location = HashMap::new();

        for book_snapshot in snapshot.books {
            let symbol = book_snapshot.symbol.clone();
            let book = OrderBook::from_snapshot(book_snapshot)?;
            if order_books.insert(symbol.clone(), book).is_some() {
                return Err(format!(
                    "matching snapshot contains duplicate book for {}",
                    symbol
                ));
            }
        }

        for (symbol, book) in &order_books {
            for order in book.resting_orders() {
                if order_location
                    .insert(order.order_id.clone(), symbol.clone())
                    .is_some()
                {
                    return Err(format!(
                        "matching snapshot contains duplicate resting order {}",
                        order.order_id
                    ));
                }
            }
        }

        Ok(Self {
            order_books,
            order_location,
            last_seq: snapshot.last_seq,
        })
    }

    pub(crate) fn last_sequence(&self) -> u64 {
        self.last_seq
    }

    pub(crate) fn resting_orders(&self) -> Vec<Order> {
        self.order_books
            .values()
            .flat_map(OrderBook::resting_orders)
            .collect()
    }
}
