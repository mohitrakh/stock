use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(u64);

impl Price {
    pub fn new(minor_units: u64) -> Result<Self, String> {
        if minor_units == 0 {
            return Err("price must be positive".to_string());
        }

        Ok(Self(minor_units))
    }

    pub const fn minor_units(self) -> u64 {
        self.0
    }

    pub const fn checked_notional(self, quantity: u64) -> Option<u64> {
        self.0.checked_mul(quantity)
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

#[derive(Debug)]
pub struct Node {
    pub order: Option<Order>,
    pub prev_idx: Option<usize>,
    pub next_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: Price,
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
        price: u64,
        quantity: u32,
        leaves_qty: Option<u32>,
        timestamp: f64,
        seq_num: u64,
    ) -> Result<Self, String> {
        let side = Side::from_str(side)?;

        if quantity == 0 {
            return Err("quantity must be positive".to_string());
        }

        let price = Price::new(price)?;
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
    pub price: Price,
    pub quantity: u32,
    pub timestamp: f64,
}

// Custom error type — no external crates needed
#[derive(Debug, PartialEq)]
pub enum RiskError {
    LimitExceeded {
        user_id: String,
        symbol: String,
        current_volume: u64,
        limit: u64,
    },
}

#[derive(Debug, PartialEq)]
pub enum WalletError {
    InsufficientFunds,
    Overflow,
}

#[derive(Debug)]
pub enum ExchangeCommand {
    PlaceOrder {
        order: Order,
        respond_to: oneshot::Sender<Result<String, String>>,
    },
    CancelOrder {
        order_id: String,
        user_id: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    Deposit {
        user_id: String,
        amount: u64,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
}
