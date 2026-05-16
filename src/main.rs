use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "BUY" => Ok(Side::Buy),
            "SELL" => Ok(Side::Sell),
            _ => Err("side must be BUY or SELL".to_string()),
        }
    }
}

#[derive(Debug)]
struct Node {
    order: Option<Order>,
    prev_idx: Option<usize>,
    next_idx: Option<usize>,
}

#[derive(Debug)]
pub struct PriceLevel {
    pub price: f64,
    nodes: Vec<Node>,
    head_idx: Option<usize>,
    tail_idx: Option<usize>,
    order_map: HashMap<String, usize>, // order_id -> index
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
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub quantity: u32,
    pub leaves_qty: u32,
    pub timestamp: f64,
    pub seq_num: u64,
}

impl Order {
    pub fn new(
        order_id: String,
        user_id: String,
        symbol: String,
        side: &str,
        price: f64,
        quantity: u32,
        leaves_qty: Option<u32>,
        timestamp: f64,
        seq_num: u64,
    ) -> Result<Self, String> {
        let side = Side::from_str(side)?;

        if price <= 0.0 || quantity == 0 {
            return Err("price and quantity must be positive".to_string());
        }

        let leaves_qty = leaves_qty.unwrap_or(quantity);

        Ok(Order {
            order_id,
            user_id,
            symbol,
            side,
            price,
            quantity,
            leaves_qty,
            timestamp,
            seq_num,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Execution {
    pub execution_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub symbol: String,
    pub price: f64,
    pub quantity: u32,
    pub timestamp: f64,
}
fn main() {
    let mut level = PriceLevel::new(100.0);

    // Append 5 orders (id1, id2, id3, id4, id5)
    for i in 1..=5 {
        let order = Order::new(
            format!("id{}", i),
            "user1".into(),
            "AAPL".into(),
            "BUY",
            100.0,
            10 * i,
            None,
            0.0,
            i as u64,
        )
        .unwrap();
        level.append(order);
    }

    // Check head
    assert_eq!(level.peek_front().unwrap().order_id, "id1");

    // Remove from middle
    let removed = level.remove("id3");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().order_id, "id3");

    // Pop all remaining in expected order
    assert_eq!(level.pop_front().unwrap().order_id, "id1");
    assert_eq!(level.pop_front().unwrap().order_id, "id2");
    assert_eq!(level.pop_front().unwrap().order_id, "id4");
    assert_eq!(level.pop_front().unwrap().order_id, "id5");
    assert!(level.is_empty());

    println!("All PriceLevel tests passed! ✅");
}
