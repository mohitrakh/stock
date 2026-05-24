use crate::types::{Execution, Order, Price, Side};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
};

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) order: Option<Order>,
    pub(crate) prev_idx: Option<usize>,
    pub(crate) next_idx: Option<usize>,
}

#[derive(Debug)]
pub struct PriceLevel {
    pub price: f64,
    pub(crate) nodes: Vec<Node>,
    head_idx: Option<usize>,
    tail_idx: Option<usize>,
    pub(crate) order_map: HashMap<String, usize>, // order_id -> index
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
