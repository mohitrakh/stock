use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};

use super::price_level::PriceLevel;
use super::types::{Execution, Order, Price, Side};

#[derive(Debug)]
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
                    if best_ask_price > order.price {
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
                    if best_bid_price < order.price {
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
