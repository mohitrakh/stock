use std::collections::HashMap;

use super::types::{Node, Order, Price};

#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Price,
    pub(crate) nodes: Vec<Node>,
    head_idx: Option<usize>,
    tail_idx: Option<usize>,
    pub(crate) order_map: HashMap<String, usize>, // order_id -> index
}

impl PriceLevel {
    pub fn new(price: Price) -> Self {
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
    pub fn total_quantity(&self) -> u64 {
        let mut total = 0u64;
        let mut current_idx = self.head_idx;
        while let Some(idx) = current_idx {
            if let Some(ref order) = self.nodes[idx].order {
                total = total
                    .checked_add(order.leaves_qty as u64)
                    .expect("price-level quantity exceeds u64");
            }
            current_idx = self.nodes[idx].next_idx;
        }
        total
    }
    /// The oldest resting order at this level not owned by `exclude_user`.
    ///
    /// Self-trade prevention walks past the aggressor's own orders instead of stopping at them,
    /// so a resting self-order can never hide a valid counterparty queued behind it.
    pub fn first_matchable_mut(&mut self, exclude_user: &str) -> Option<&mut Order> {
        let mut current = self.head_idx;

        while let Some(idx) = current {
            match &self.nodes[idx].order {
                Some(order) if order.user_id != exclude_user => break,
                _ => current = self.nodes[idx].next_idx,
            }
        }

        self.nodes[current?].order.as_mut()
    }
}
