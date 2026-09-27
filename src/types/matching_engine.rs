use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::order_book::{OrderBook, OrderBookSnapshot};
use super::types::{Execution, L2Level, Order, OrderBookView, Price};

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

pub(crate) struct PreparedOrder {
    pub(crate) symbol: String,
    pub(crate) order_id: String,
    pub(crate) seq_num: u64,
    pub(crate) book: OrderBook,
    pub(crate) executions: Vec<Execution>,
    pub(crate) incoming_resting: bool,
    pub(crate) removed_order_ids: Vec<String>,
}

pub(crate) struct PreparedCancel {
    pub(crate) symbol: String,
    pub(crate) order_id: String,
    pub(crate) seq_num: u64,
    pub(crate) book: OrderBook,
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

        let symbol = order.symbol.clone();
        let order_id = order.order_id.clone();
        let seq_num = order.seq_num;
        let mut book = self
            .order_books
            .get(&symbol)
            .cloned()
            .unwrap_or_else(|| OrderBook::new(symbol.clone()));

        let executions = book.place_order(order);
        let incoming_resting = book.is_resting(&order_id);
        let mut removed_order_ids = Vec::new();

        for execution in &executions {
            for filled_order_id in [&execution.buy_order_id, &execution.sell_order_id] {
                if !book.is_resting(filled_order_id)
                    && !removed_order_ids.iter().any(|id| id == filled_order_id)
                {
                    removed_order_ids.push(filled_order_id.clone());
                }
            }
        }

        Ok(PreparedOrder {
            symbol,
            order_id,
            seq_num,
            book,
            executions,
            incoming_resting,
            removed_order_ids,
        })
    }

    pub(crate) fn commit_order(&mut self, prepared: PreparedOrder) {
        self.order_books
            .insert(prepared.symbol.clone(), prepared.book);
        self.last_seq = prepared.seq_num;

        for filled_order_id in prepared.removed_order_ids {
            self.order_location.remove(&filled_order_id);
        }

        if prepared.incoming_resting {
            self.order_location
                .insert(prepared.order_id, prepared.symbol);
        }
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
        let mut book = self
            .order_books
            .get(&symbol)
            .cloned()
            .ok_or_else(|| format!("Order book for symbol {} not found", symbol))?;
        book.cancel_order(order_id)
            .ok_or_else(|| format!("Order {} not found in its order book", order_id))?;

        Ok(PreparedCancel {
            symbol,
            order_id: order_id.to_string(),
            seq_num: cancel_seq,
            book,
        })
    }

    pub(crate) fn commit_cancel(&mut self, prepared: PreparedCancel) {
        self.order_books.insert(prepared.symbol, prepared.book);
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
