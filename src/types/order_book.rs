use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet},
};

use serde::{Deserialize, Serialize};

use super::price_level::PriceLevel;
use super::types::{Execution, Order, Price, Side};

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub(crate) buy_levels: BTreeMap<Reverse<Price>, PriceLevel>,
    pub(crate) sell_levels: BTreeMap<Price, PriceLevel>,
    pub(crate) order_map: HashMap<String, (Price, Side)>,
    exec_counter: u64,
}

/// Snapshot form of a price level. Orders are stored in FIFO order; the linked-node indexes are
/// an in-memory implementation detail and are regenerated on load.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PriceLevelSnapshot {
    price: Price,
    orders: Vec<Order>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrderBookSnapshot {
    pub(crate) symbol: String,
    buy_levels: Vec<PriceLevelSnapshot>,
    sell_levels: Vec<PriceLevelSnapshot>,
    exec_counter: u64,
}

/// A new order's effect on a book, worked out by `OrderBook::plan_order` without changing it.
#[derive(Debug, Clone)]
pub(crate) struct MatchPlan {
    fills: Vec<Fill>,
    /// Two per fill, one for each side, exactly as matching has always emitted them.
    pub(crate) executions: Vec<Execution>,
    /// Quantity the new order still has after matching. Above zero, it rests.
    pub(crate) remaining: u32,
}

impl MatchPlan {
    /// Resting orders this plan takes out of the book completely.
    pub(crate) fn filled_resting_orders(&self) -> impl Iterator<Item = &str> {
        self.fills
            .iter()
            .filter(|fill| fill.fills_resting_order)
            .map(|fill| fill.order_id.as_str())
    }
}

#[derive(Debug, Clone)]
struct Fill {
    order_id: String,
    price: Price,
    quantity: u32,
    /// The fill takes the resting order's whole remaining quantity, so it leaves the book.
    fills_resting_order: bool,
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
    pub fn best_bid(&self) -> Option<(Price, u64)> {
        self.buy_levels
            .first_key_value()
            .map(|(rev_price, level)| (rev_price.0, level.total_quantity()))
    }

    pub fn best_ask(&self) -> Option<(Price, u64)> {
        self.sell_levels
            .first_key_value()
            .map(|(price, level)| (*price, level.total_quantity()))
    }

    /// Aggregated resting quantity per price level, best price first, capped at `depth` levels
    /// per side. This is the L2 view: price points and their total size, no per-order identity.
    pub fn l2_snapshot(&self, depth: usize) -> (Vec<(Price, u64)>, Vec<(Price, u64)>) {
        let bids = self
            .buy_levels
            .iter()
            .take(depth)
            .map(|(rev_price, level)| (rev_price.0, level.total_quantity()))
            .collect();

        let asks = self
            .sell_levels
            .iter()
            .take(depth)
            .map(|(price, level)| (*price, level.total_quantity()))
            .collect();

        (bids, asks)
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

    /// Works out everything a new order will do to this book — which resting orders it trades
    /// with, the executions, and what is left over — WITHOUT changing the book.
    ///
    /// This is the half of matching that can be done read-only. The exchange prepares every
    /// command before it is allowed to change anything, and this lets it do that by reading the
    /// live book instead of copying it: the cost is the orders the new order actually reaches, not
    /// the size of the book. See `docs/performance/02-match-without-copying-the-book.md`.
    pub(crate) fn plan_order(&self, order: &Order) -> MatchPlan {
        let mut plan = MatchPlan {
            fills: Vec::new(),
            executions: Vec::new(),
            remaining: order.leaves_qty,
        };
        // Best price first, and only the levels the order's limit price crosses.
        match order.side {
            Side::Buy => self.plan_against(
                order,
                self.sell_levels
                    .range(..=order.price)
                    .map(|(_, level)| level),
                &mut plan,
            ),
            Side::Sell => self.plan_against(
                order,
                self.buy_levels
                    .range(..=Reverse(order.price))
                    .map(|(_, level)| level),
                &mut plan,
            ),
        }
        plan
    }

    fn plan_against<'a>(
        &self,
        order: &Order,
        levels: impl Iterator<Item = &'a PriceLevel>,
        plan: &mut MatchPlan,
    ) {
        let mut exec_counter = self.exec_counter;
        for level in levels {
            // Price/time priority: within a level, oldest first.
            for resting in level.iter() {
                if plan.remaining == 0 {
                    return;
                }
                // Self-trade prevention walks past the aggressor's own orders, so a resting
                // self-order can never hide a valid counterparty queued behind it.
                if resting.user_id == order.user_id {
                    continue;
                }
                let quantity = plan.remaining.min(resting.leaves_qty);
                plan.remaining -= quantity;
                plan.fills.push(Fill {
                    order_id: resting.order_id.clone(),
                    price: resting.price,
                    quantity,
                    fills_resting_order: quantity == resting.leaves_qty,
                });
                let (buy_order_id, sell_order_id) = match order.side {
                    Side::Buy => (order.order_id.clone(), resting.order_id.clone()),
                    Side::Sell => (resting.order_id.clone(), order.order_id.clone()),
                };
                // One match produces two fills: one for the buy side, one for the sell side.
                for _ in 0..2 {
                    plan.executions.push(Execution {
                        execution_id: format!("exec_{exec_counter}"),
                        buy_order_id: buy_order_id.clone(),
                        sell_order_id: sell_order_id.clone(),
                        symbol: self.symbol.clone(),
                        price: resting.price,
                        quantity,
                        timestamp: order.timestamp.max(resting.timestamp),
                    });
                    exec_counter += 1;
                }
            }
        }
    }

    /// Applies a plan made by `plan_order` for this same order against this same, unchanged
    /// book. The exchange's single worker guarantees nothing touches the book in between, so
    /// every planned resting order and level is still exactly where the plan found it.
    pub(crate) fn apply_plan(&mut self, mut order: Order, plan: &MatchPlan) {
        for fill in &plan.fills {
            let (level, level_gone) = match order.side {
                Side::Buy => {
                    let level = self
                        .sell_levels
                        .get_mut(&fill.price)
                        .expect("planned level is in the book");
                    Self::take_from_level(level, fill);
                    (fill.price, level.is_empty())
                }
                Side::Sell => {
                    let level = self
                        .buy_levels
                        .get_mut(&Reverse(fill.price))
                        .expect("planned level is in the book");
                    Self::take_from_level(level, fill);
                    (fill.price, level.is_empty())
                }
            };
            if fill.fills_resting_order {
                self.order_map.remove(&fill.order_id);
            }
            if level_gone {
                match order.side {
                    Side::Buy => self.sell_levels.remove(&level),
                    Side::Sell => self.buy_levels.remove(&Reverse(level)),
                };
            }
        }
        self.exec_counter += plan.executions.len() as u64;

        order.leaves_qty = plan.remaining;
        if order.leaves_qty > 0 {
            let price = order.price;
            let side = order.side.clone();
            let order_id = order.order_id.clone();
            let level = match side {
                Side::Buy => self
                    .buy_levels
                    .entry(Reverse(price))
                    .or_insert_with(|| PriceLevel::new(price)),
                Side::Sell => self
                    .sell_levels
                    .entry(price)
                    .or_insert_with(|| PriceLevel::new(price)),
            };
            level.append(order);
            self.order_map.insert(order_id, (price, side));
        }
    }

    fn take_from_level(level: &mut PriceLevel, fill: &Fill) {
        if fill.fills_resting_order {
            level.remove(&fill.order_id);
        } else {
            level
                .get_mut(&fill.order_id)
                .expect("planned resting order is in its level")
                .leaves_qty -= fill.quantity;
        }
    }

    /// Plan and apply in one step, for tests and tools that are not preparing a command.
    #[cfg(test)]
    pub fn place_order(&mut self, order: Order) -> Vec<Execution> {
        let plan = self.plan_order(&order);
        self.apply_plan(order, &plan);
        plan.executions
    }

    /// The matcher this book used before planning existed: it matches by mutating the book
    /// directly. Kept only as a test oracle, so the planned matcher can be proven to produce
    /// exactly the same executions and book on random order flow.
    #[cfg(test)]
    fn reference_place_order(&mut self, mut order: Order) -> Vec<Execution> {
        let mut executions = Vec::new();
        let crossing_prices: Vec<Price> = match order.side {
            Side::Buy => self
                .sell_levels
                .range(..=order.price)
                .map(|(price, _)| *price)
                .collect(),
            Side::Sell => self
                .buy_levels
                .range(..=Reverse(order.price))
                .map(|(rev_price, _)| rev_price.0)
                .collect(),
        };
        for price in crossing_prices {
            if order.leaves_qty == 0 {
                break;
            }
            self.reference_match_at_level(&mut order, price, &mut executions);
        }
        if order.leaves_qty > 0 {
            let price = order.price;
            let side = order.side.clone();
            let order_id = order.order_id.clone();
            match side {
                Side::Buy => self
                    .buy_levels
                    .entry(Reverse(price))
                    .or_insert_with(|| PriceLevel::new(price))
                    .append(order),
                Side::Sell => self
                    .sell_levels
                    .entry(price)
                    .or_insert_with(|| PriceLevel::new(price))
                    .append(order),
            }
            self.order_map.insert(order_id, (price, side));
        }
        executions
    }

    #[cfg(test)]
    fn reference_match_at_level(
        &mut self,
        order: &mut Order,
        price: Price,
        executions: &mut Vec<Execution>,
    ) {
        let aggressor_is_buy = matches!(order.side, Side::Buy);
        while order.leaves_qty > 0 {
            let level = match if aggressor_is_buy {
                self.sell_levels.get_mut(&price)
            } else {
                self.buy_levels.get_mut(&Reverse(price))
            } {
                Some(level) => level,
                None => return,
            };
            let Some(resting) = level.first_matchable_mut(&order.user_id) else {
                return;
            };
            let trade_qty = order.leaves_qty.min(resting.leaves_qty);
            let trade_price = resting.price;
            let resting_id = resting.order_id.clone();
            let timestamp = order.timestamp.max(resting.timestamp);
            resting.leaves_qty -= trade_qty;
            let resting_filled = resting.leaves_qty == 0;
            if resting_filled {
                level.remove(&resting_id);
            }
            let level_is_empty = level.is_empty();
            order.leaves_qty -= trade_qty;
            if resting_filled {
                self.order_map.remove(&resting_id);
            }
            if level_is_empty {
                if aggressor_is_buy {
                    self.sell_levels.remove(&price);
                } else {
                    self.buy_levels.remove(&Reverse(price));
                }
            }
            let (buy_order_id, sell_order_id) = if aggressor_is_buy {
                (order.order_id.clone(), resting_id)
            } else {
                (resting_id, order.order_id.clone())
            };
            for _ in 0..2 {
                let execution_id = format!("exec_{}", self.exec_counter);
                self.exec_counter += 1;
                executions.push(Execution {
                    execution_id,
                    buy_order_id: buy_order_id.clone(),
                    sell_order_id: sell_order_id.clone(),
                    symbol: self.symbol.clone(),
                    price: trade_price,
                    quantity: trade_qty,
                    timestamp,
                });
            }
        }
    }

    pub fn is_resting(&self, order_id: &str) -> bool {
        self.order_map.contains_key(order_id)
    }

    pub(crate) fn snapshot(&self) -> OrderBookSnapshot {
        let snapshot_level = |price: Price, level: &PriceLevel| PriceLevelSnapshot {
            price,
            orders: level.orders_in_queue(),
        };
        OrderBookSnapshot {
            symbol: self.symbol.clone(),
            buy_levels: self
                .buy_levels
                .iter()
                .map(|(price, level)| snapshot_level(price.0, level))
                .collect(),
            sell_levels: self
                .sell_levels
                .iter()
                .map(|(price, level)| snapshot_level(*price, level))
                .collect(),
            exec_counter: self.exec_counter,
        }
    }

    pub(crate) fn from_snapshot(snapshot: OrderBookSnapshot) -> Result<Self, String> {
        let mut book = Self::new(snapshot.symbol.clone());
        book.exec_counter = snapshot.exec_counter;
        let mut order_ids = HashSet::new();

        for level in snapshot.buy_levels {
            let price = level.price;
            if book.buy_levels.contains_key(&Reverse(price)) {
                return Err(format!(
                    "order-book snapshot contains duplicate buy level {}",
                    price.minor_units()
                ));
            }
            let restored = Self::restore_level(
                &snapshot.symbol,
                price,
                Side::Buy,
                level.orders,
                &mut order_ids,
            )?;
            book.buy_levels.insert(Reverse(price), restored);
        }

        for level in snapshot.sell_levels {
            let price = level.price;
            if book.sell_levels.contains_key(&price) {
                return Err(format!(
                    "order-book snapshot contains duplicate sell level {}",
                    price.minor_units()
                ));
            }
            let restored = Self::restore_level(
                &snapshot.symbol,
                price,
                Side::Sell,
                level.orders,
                &mut order_ids,
            )?;
            book.sell_levels.insert(price, restored);
        }

        for order in book.resting_orders() {
            if book
                .order_map
                .insert(order.order_id.clone(), (order.price, order.side))
                .is_some()
            {
                return Err(format!(
                    "order-book snapshot contains duplicate resting order {}",
                    order.order_id
                ));
            }
        }

        Ok(book)
    }

    fn restore_level(
        symbol: &str,
        price: Price,
        side: Side,
        orders: Vec<Order>,
        order_ids: &mut HashSet<String>,
    ) -> Result<PriceLevel, String> {
        if orders.is_empty() {
            return Err(format!(
                "order-book snapshot contains an empty {} level {}",
                match side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                },
                price.minor_units()
            ));
        }
        let mut level = PriceLevel::new(price);
        for order in orders {
            if order.symbol != symbol
                || order.side != side
                || order.price != price
                || order.leaves_qty == 0
                || order.leaves_qty > order.quantity
            {
                return Err(format!(
                    "order-book snapshot has an invalid resting order {}",
                    order.order_id
                ));
            }
            if !order_ids.insert(order.order_id.clone()) {
                return Err(format!(
                    "order-book snapshot contains duplicate resting order {}",
                    order.order_id
                ));
            }
            level.append(order);
        }
        Ok(level)
    }

    pub(crate) fn resting_orders(&self) -> Vec<Order> {
        self.buy_levels
            .values()
            .chain(self.sell_levels.values())
            .flat_map(PriceLevel::orders_in_queue)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Differential test: the planned matcher and the old in-place matcher see the same random
    /// order flow — several users so self-trade prevention is exercised, partial fills, orders
    /// sweeping several levels, and cancellations — and must agree on every execution and on the
    /// complete book after every step.
    #[test]
    fn planned_matching_is_identical_to_the_old_in_place_matcher() {
        let mut planned = OrderBook::new("AAPL".into());
        let mut reference = OrderBook::new("AAPL".into());
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let mut placed = Vec::new();
        for i in 0..20_000u64 {
            if next(4) == 0 && !placed.is_empty() {
                let id: String = placed.swap_remove(next(placed.len() as u64) as usize);
                assert_eq!(planned.cancel_order(&id), reference.cancel_order(&id));
            } else {
                let side = if next(2) == 0 { "BUY" } else { "SELL" };
                let order = Order::new(
                    format!("o{i}"),
                    format!("u{}", next(5)),
                    "AAPL".into(),
                    side,
                    95 + next(10),
                    1 + next(20) as u32,
                    None,
                    i as f64,
                    i + 1,
                )
                .unwrap();
                placed.push(order.order_id.clone());
                assert_eq!(
                    planned.place_order(order.clone()),
                    reference.reference_place_order(order),
                    "step {i}"
                );
            }
            assert_eq!(planned.snapshot(), reference.snapshot(), "step {i}");
            assert_eq!(planned.order_map, reference.order_map, "step {i}");
        }
    }

    #[test]
    fn planning_leaves_the_book_untouched() {
        let mut book = OrderBook::new("AAPL".into());
        let order = |id: &str, user: &str, side: &str, price: u64, quantity: u32| {
            Order::new(
                id.into(),
                user.into(),
                "AAPL".into(),
                side,
                price,
                quantity,
                None,
                1.0,
                1,
            )
            .unwrap()
        };
        book.place_order(order("s1", "alice", "SELL", 100, 5));
        book.place_order(order("s2", "bob", "SELL", 101, 5));
        let before = book.snapshot();

        let plan = book.plan_order(&order("b1", "carol", "BUY", 101, 8));

        assert_eq!(book.snapshot(), before);
        assert_eq!(plan.executions.len(), 4);
        assert_eq!(plan.remaining, 0);
        assert_eq!(plan.filled_resting_orders().collect::<Vec<_>>(), ["s1"]);
    }
}
