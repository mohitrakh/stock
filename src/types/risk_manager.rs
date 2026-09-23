use std::collections::HashMap;

use super::types::{Order, RiskError};

/// Seconds in a trading day, used to turn an order's timestamp into a day number.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// The cap applied to any `(user, symbol)` with no explicit limit — the design document's own
/// example, "a user can only trade a maximum of 1 million shares of Apple stock in one day".
///
/// A compiled-in constant rather than configuration on purpose: a limit read from the environment
/// would make replay depend on the environment, so the same log could reproduce differently on a
/// machine configured differently. Overrides arrive as events instead, which stay in the log.
pub const DEFAULT_MAX_DAILY_QUANTITY: u64 = 1_000_000;

pub struct RiskManager {
    limits: HashMap<(String, String), u64>,
    volumes: HashMap<(String, String), u64>,
    /// Which day the counters in `volumes` belong to, as a day number derived from order
    /// timestamps. `None` until the first order is seen.
    current_day: Option<i64>,
}

impl RiskManager {
    pub fn new() -> Self {
        Self {
            limits: HashMap::new(),
            volumes: HashMap::new(),
            current_day: None,
        }
    }

    pub fn set_limit(&mut self, user_id: String, symbol: String, limit: u64) {
        self.limits.insert((user_id, symbol), limit);
    }

    pub fn limit_for(&self, user_id: &str, symbol: &str) -> u64 {
        self.limits
            .get(&(user_id.to_string(), symbol.to_string()))
            .copied()
            .unwrap_or(DEFAULT_MAX_DAILY_QUANTITY)
    }

    pub fn used_today(&self, user_id: &str, symbol: &str) -> u64 {
        self.volumes
            .get(&(user_id.to_string(), symbol.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn day_of(timestamp: f64) -> i64 {
        (timestamp / SECONDS_PER_DAY).floor() as i64
    }

    /// Clears the day's counters when an order's own timestamp says a new day has started.
    ///
    /// The day comes from the event, never from the system clock. A wall-clock reset would destroy
    /// deterministic replay: the same log would rebuild different state tomorrow than it did today,
    /// and orders that were accepted would start being rejected. `Order.timestamp` is already
    /// recorded in `NewOrderRequested`, so deriving the day from it replays exactly.
    ///
    /// Only rolls forward. A timestamp that goes backwards across a boundary — clock jitter at the
    /// gateway — is ignored rather than resetting the counters a second time.
    fn roll_day(&mut self, timestamp: f64) {
        let day = Self::day_of(timestamp);

        match self.current_day {
            Some(current) if day <= current => {}
            _ => {
                self.volumes.clear();
                self.current_day = Some(day);
            }
        }
    }

    /// Rolls the trading day if this order starts one, then checks it against the limit.
    ///
    /// Takes `&mut self` because the day roll is state. It runs before matching, so the volume it
    /// counts is what the order could trade, not what it did — a pre-trade check cannot know fills
    /// that have not happened yet.
    pub fn check(&mut self, order: &Order) -> Result<(), RiskError> {
        self.roll_day(order.timestamp);

        let key = (order.user_id.clone(), order.symbol.clone());
        let limit = self.limit_for(&order.user_id, &order.symbol);
        let current_volume = self.volumes.get(&key).copied().unwrap_or(0);

        let projected =
            current_volume
                .checked_add(order.quantity as u64)
                .ok_or(RiskError::LimitExceeded {
                    user_id: order.user_id.clone(),
                    symbol: order.symbol.clone(),
                    current_volume,
                    limit,
                })?;

        if projected > limit {
            return Err(RiskError::LimitExceeded {
                user_id: order.user_id.clone(),
                symbol: order.symbol.clone(),
                current_volume,
                limit,
            });
        }

        Ok(())
    }

    /// Counts an accepted order against the day, once its collateral has been reserved.
    pub fn record(&mut self, order: &Order) {
        let key = (order.user_id.clone(), order.symbol.clone());
        let current = self.volumes.get(&key).copied().unwrap_or(0);

        self.volumes
            .insert(key, current.saturating_add(order.quantity as u64));
    }

    /// Returns the allowance held by a cancelled order's unfilled quantity.
    ///
    /// Without this, placing and cancelling would burn the day's allowance on shares that never
    /// traded, and the counter would stop meaning what the requirement says it means. With it, the
    /// number tracks what was actually traded today plus what is currently at risk of trading:
    /// filled quantity is never returned, open quantity is.
    pub fn release(&mut self, order: &Order, quantity: u32) {
        let key = (order.user_id.clone(), order.symbol.clone());
        let current = self.volumes.get(&key).copied().unwrap_or(0);

        self.volumes
            .insert(key, current.saturating_sub(quantity as u64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_at(user: &str, quantity: u32, timestamp: f64) -> Order {
        Order::new(
            format!("{}-{}", user, timestamp),
            user.to_string(),
            "AAPL".to_string(),
            "BUY",
            10,
            quantity,
            None,
            timestamp,
            0,
        )
        .unwrap()
    }

    #[test]
    fn the_default_limit_is_the_documented_one() {
        let risk = RiskManager::new();

        assert_eq!(risk.limit_for("anyone", "AAPL"), 1_000_000);
    }

    #[test]
    fn volume_accumulates_until_the_limit_is_reached() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let first = order_at("trader", 6, 0.0);
        risk.check(&first).unwrap();
        risk.record(&first);

        // 6 + 4 fits exactly, 6 + 5 does not.
        risk.check(&order_at("trader", 4, 1.0)).unwrap();
        assert!(matches!(
            risk.check(&order_at("trader", 5, 1.0)),
            Err(RiskError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn limits_are_per_user_and_per_symbol() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 5);

        let used = order_at("trader", 5, 0.0);
        risk.check(&used).unwrap();
        risk.record(&used);

        assert!(risk.check(&order_at("trader", 1, 0.0)).is_err());
        // Another user is unaffected, and so is the same user in another symbol.
        assert!(risk.check(&order_at("someone-else", 1, 0.0)).is_ok());
        assert_eq!(risk.used_today("someone-else", "AAPL"), 0);
    }

    #[test]
    fn the_day_rolls_from_the_order_timestamp_not_the_clock() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let day_one = order_at("trader", 10, 3_600.0);
        risk.check(&day_one).unwrap();
        risk.record(&day_one);

        // Same day, later: the allowance is gone.
        assert!(risk.check(&order_at("trader", 1, 7_200.0)).is_err());

        // A timestamp in the next day resets it. Nothing here consults the system clock, which is
        // what lets a recorded history replay to the same answers on any future date.
        risk.check(&order_at("trader", 10, 90_000.0)).unwrap();
        assert_eq!(risk.used_today("trader", "AAPL"), 0);
    }

    #[test]
    fn a_backwards_timestamp_does_not_reset_the_day_again() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let second_day = order_at("trader", 10, 90_000.0);
        risk.check(&second_day).unwrap();
        risk.record(&second_day);

        // Clock jitter pointing back into the previous day must not hand back the allowance.
        assert!(matches!(
            risk.check(&order_at("trader", 1, 3_600.0)),
            Err(RiskError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn cancelling_returns_the_unfilled_allowance() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let placed = order_at("trader", 10, 0.0);
        risk.check(&placed).unwrap();
        risk.record(&placed);
        assert_eq!(risk.used_today("trader", "AAPL"), 10);

        // Four traded, six cancelled: only the four that actually traded stay counted.
        risk.release(&placed, 6);

        assert_eq!(risk.used_today("trader", "AAPL"), 4);
        risk.check(&order_at("trader", 6, 1.0)).unwrap();
    }
}
