#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Price(f64);

impl Price {
    pub fn new(val: f64) -> Self {
        assert!(!val.is_nan(), "Price cannot be NaN");
        Price(val)
    }
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl Eq for Price {}

impl PartialOrd for Price {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Price {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

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

#[derive(Debug)]
pub struct Node {
    pub order: Option<Order>,
    pub prev_idx: Option<usize>,
    pub next_idx: Option<usize>,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderState {
    New,
    PartialFill { leaves_qty: u32 },
    Filled,
    Canceled,
}
