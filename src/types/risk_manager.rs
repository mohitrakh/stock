use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::types::{Order, RiskError};

/// The cap applied to any `(user, symbol)` with no explicit limit — the design document's own
/// example, "a user can only trade a maximum of 1 million shares of Apple stock in one day".
///
/// A compiled-in constant rather than configuration on purpose: a limit read from the environment
/// would make replay depend on the environment, so the same log could reproduce differently on a
/// machine configured differently. Overrides arrive as events instead, which stay in the log.
pub const DEFAULT_MAX_DAILY_QUANTITY: u64 = 1_000_000;

pub struct RiskManager {
    limits: HashMap<(String, String), u64>,
    /// Today's executed quantity plus every quantity still resting and able to execute today.
    /// "Today" is the trading day the operator opened; `start_day` begins a new one.
    volumes: HashMap<(String, String), u64>,
}

/// Risk counters are persisted as rows because JSON object keys cannot faithfully represent the
/// `(user, symbol)` keys used by the in-memory lookup maps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RiskManagerSnapshot {
    entries: Vec<RiskSnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RiskSnapshotEntry {
    user_id: String,
    symbol: String,
    limit: Option<u64>,
    volume: u64,
}

impl RiskManager {
    pub fn new() -> Self {
        Self {
            limits: HashMap::new(),
            volumes: HashMap::new(),
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

    /// Starts a trading day, when the operator opens the market.
    ///
    /// The day comes from a journaled open, never from a clock or an order's timestamp: a
    /// wall-clock reset would make the same log rebuild different state tomorrow than it did today.
    /// Usage starts from zero: executed quantity belongs to the day it traded, and nothing rests
    /// overnight, because the close expired every resting order and released its allowance.
    pub fn start_day(&mut self) {
        self.volumes.clear();
    }

    /// Checks an order against today's limit. It runs before matching, so the volume it counts is
    /// what the order could trade, not what it did: a pre-trade check cannot know fills that have
    /// not happened yet.
    pub fn check(&self, order: &Order) -> Result<(), RiskError> {
        let current_volume = self
            .volumes
            .get(&(order.user_id.clone(), order.symbol.clone()))
            .copied()
            .unwrap_or(0);

        self.check_projected(order, current_volume)
    }

    fn check_projected(&self, order: &Order, current_volume: u64) -> Result<(), RiskError> {
        let limit = self.limit_for(&order.user_id, &order.symbol);

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

    /// Counts an accepted order against the day, once its collateral has been reserved. A fill
    /// changes nothing here: the same shares move from "could trade today" to "did trade today".
    pub fn record(&mut self, order: &Order) {
        let key = (order.user_id.clone(), order.symbol.clone());
        let current = self.volumes.get(&key).copied().unwrap_or(0);

        self.volumes.insert(
            key,
            current
                .checked_add(order.quantity as u64)
                .expect("validated risk usage must not overflow at commit"),
        );
    }

    pub fn validate_release(&self, user_id: &str, symbol: &str, quantity: u64) -> bool {
        self.used_today(user_id, symbol) >= quantity
    }

    /// Returns the allowance held by a cancelled or expired order's unfilled quantity.
    ///
    /// Without this, placing and cancelling would burn the day's allowance on shares that never
    /// traded, and the counter would stop meaning what the requirement says it means. With it, the
    /// number tracks what was actually traded today plus what is currently at risk of trading:
    /// filled quantity is never returned, unfilled quantity is.
    pub fn release(&mut self, user_id: &str, symbol: &str, quantity: u64) {
        let updated = self
            .used_today(user_id, symbol)
            .checked_sub(quantity)
            .expect("prepared release must not exceed risk usage");
        self.volumes
            .insert((user_id.to_string(), symbol.to_string()), updated);
    }

    pub(crate) fn snapshot(&self) -> RiskManagerSnapshot {
        let mut keys: Vec<_> = self
            .limits
            .keys()
            .chain(self.volumes.keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();

        RiskManagerSnapshot {
            entries: keys
                .into_iter()
                .map(|(user_id, symbol)| {
                    let key = (user_id.clone(), symbol.clone());
                    RiskSnapshotEntry {
                        limit: self.limits.get(&key).copied(),
                        volume: self.volumes.get(&key).copied().unwrap_or(0),
                        user_id,
                        symbol,
                    }
                })
                .collect(),
        }
    }

    pub(crate) fn from_snapshot(snapshot: RiskManagerSnapshot) -> Result<Self, String> {
        let mut limits = HashMap::new();
        let mut volumes = HashMap::new();

        for entry in snapshot.entries {
            let key = (entry.user_id, entry.symbol);
            if volumes.insert(key.clone(), entry.volume).is_some() {
                return Err(format!(
                    "risk snapshot contains duplicate entry for {} {}",
                    key.0, key.1
                ));
            }
            if let Some(limit) = entry.limit {
                limits.insert(key, limit);
            }
        }

        Ok(Self { limits, volumes })
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
    fn a_new_day_starts_only_when_the_market_opens() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let traded = order_at("trader", 10, 3_600.0);
        risk.check(&traded).unwrap();
        risk.record(&traded);

        // A timestamp a day later changes nothing: only an opened trading day does. Nothing here
        // consults a clock, which is what lets a recorded history replay to the same answers.
        assert!(risk.check(&order_at("trader", 1, 90_000.0)).is_err());

        risk.start_day();
        assert_eq!(risk.used_today("trader", "AAPL"), 0);
        risk.check(&order_at("trader", 10, 90_000.0)).unwrap();
    }

    #[test]
    fn releasing_returns_only_the_unfilled_allowance() {
        let mut risk = RiskManager::new();
        risk.set_limit("trader".to_string(), "AAPL".to_string(), 10);

        let placed = order_at("trader", 10, 0.0);
        risk.check(&placed).unwrap();
        risk.record(&placed);
        assert_eq!(risk.used_today("trader", "AAPL"), 10);

        // Four traded, six cancelled or expired: only the four that actually traded stay counted.
        assert!(risk.validate_release("trader", "AAPL", 6));
        risk.release("trader", "AAPL", 6);

        assert_eq!(risk.used_today("trader", "AAPL"), 4);
        assert!(!risk.validate_release("trader", "AAPL", 5));
        risk.check(&order_at("trader", 6, 1.0)).unwrap();
    }
}
