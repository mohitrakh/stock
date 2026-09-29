use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::order_book::{MatchPlan, OrderBook, OrderBookSnapshot};
use super::types::{L2Level, Order, OrderBookView, Price};

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

    pub(crate) fn prepare_order(&self, order: Order) -> Result<PreparedOrder, String> {
        if order.seq_num <= self.last_seq {
            return Err(format!(
                "Sequence violation: received seq {} but last was {}",
                order.seq_num, self.last_seq
            ));
        }

        // Read the live book; never copy it. A symbol with no book yet has nothing to match.
        let plan = match self.order_books.get(&order.symbol) {
            Some(book) => book.plan_order(&order),
            None => OrderBook::new(order.symbol.clone()).plan_order(&order),
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
