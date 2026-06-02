use std::collections::HashMap;

use super::types::{Node, Order};

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
