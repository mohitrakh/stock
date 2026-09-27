use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
pub struct Node {
    pub order: Option<Order>,
    pub prev_idx: Option<usize>,
    pub next_idx: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// A user's cash position. `available` is what a new buy order can still reserve.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BalanceView {
    pub user_id: String,
    pub balance: u64,
    pub locked: u64,
    pub available: u64,
}

/// The lifecycle state of one order, as a client sees it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderView {
    pub order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: u64,
    pub quantity: u32,
    pub filled_quantity: u32,
    pub remaining_quantity: u32,
    pub status: String,
    pub creation_time: f64,
}

/// A user's holding in one symbol. `locked` is reserved behind resting sell orders; `available` is
/// what a new sell order can still reserve.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionView {
    pub symbol: String,
    pub quantity: u64,
    pub locked: u64,
    pub available: u64,
}

/// One fill, from the perspective of one party to it. `side` and `order_id` are that party's, so
/// the two records a single match produces differ between buyer and seller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionView {
    pub execution_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: u64,
    pub quantity: u32,
    pub timestamp: f64,
}

/// A user's daily trading cap in one symbol, and how much of it today's orders have used.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RiskLimitView {
    pub symbol: String,
    pub max_daily_quantity: u64,
    pub used_today: u64,
}

/// One aggregated price point of an L2 book: a price and the total size resting on it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct L2Level {
    pub price: u64,
    pub quantity: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderBookView {
    pub symbol: String,
    pub bids: Vec<L2Level>,
    pub asks: Vec<L2Level>,
}

/// Live gateway plumbing, not replayable business data — every variant carries a response
/// channel. The read variants are deliberately *not* mirrored by an `ExchangeInputEvent`: they
/// change no state, so recording them would pad the event log and slow every future replay
/// without changing a single outcome.
#[derive(Debug)]
pub enum ExchangeCommand {
    PlaceOrder {
        order: Order,
        respond_to: oneshot::Sender<Result<OrderView, String>>,
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
    DepositShares {
        user_id: String,
        symbol: String,
        quantity: u64,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    SetRiskLimit {
        user_id: String,
        symbol: String,
        max_daily_quantity: u64,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    GetExecutions {
        user_id: String,
        symbol: Option<String>,
        order_id: Option<String>,
        start_time: Option<f64>,
        end_time: Option<f64>,
        respond_to: oneshot::Sender<Vec<ExecutionView>>,
    },
    GetRiskLimit {
        user_id: String,
        symbol: String,
        respond_to: oneshot::Sender<RiskLimitView>,
    },
    GetBalance {
        user_id: String,
        respond_to: oneshot::Sender<BalanceView>,
    },
    GetPositions {
        user_id: String,
        respond_to: oneshot::Sender<Vec<PositionView>>,
    },
    GetOrder {
        order_id: String,
        user_id: String,
        respond_to: oneshot::Sender<Option<OrderView>>,
    },
}
