use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};

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
    pub fn best_bid(&self) -> Option<(Price, u32)> {
        self.buy_levels
            .first_key_value()
            .map(|(rev_price, level)| (rev_price.0, level.total_quantity()))
    }

    pub fn best_ask(&self) -> Option<(Price, u32)> {
        self.sell_levels
            .first_key_value()
            .map(|(price, level)| (*price, level.total_quantity()))
    }

    /// Aggregated resting quantity per price level, best price first, capped at `depth` levels
    /// per side. This is the L2 view: price points and their total size, no per-order identity.
    pub fn l2_snapshot(&self, depth: usize) -> (Vec<(Price, u32)>, Vec<(Price, u32)>) {
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

    fn match_order(&mut self, order: &mut Order) -> Vec<Execution> {
        let mut executions = Vec::new();

        // Snapshot the crossing prices before walking them. Self-trade prevention can leave a level
        // standing with orders still in it, so re-reading "the best level" each pass would spin
        // forever on a level made up entirely of the aggressor's own orders.
        // ponytail: one Vec allocation per aggressive order; replace with an in-place BTreeMap
        // cursor if the critical path ever needs the allocation back.
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

            self.match_at_level(order, price, &mut executions);
        }

        executions
    }

    /// Consumes as much of `order` as the resting orders at `price` allow, skipping any resting
    /// order owned by the aggressor. Returns when the level is exhausted, holds only the
    /// aggressor's own orders, or the aggressor is fully filled.
    fn match_at_level(&mut self, order: &mut Order, price: Price, executions: &mut Vec<Execution>) {
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

            // One match produces two fills: one for the buy side, one for the sell side.
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

    pub fn place_order(&mut self, mut order: Order) -> Vec<Execution> {
        let executions = self.match_order(&mut order);

        if order.leaves_qty > 0 {
            let price = order.price;
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
