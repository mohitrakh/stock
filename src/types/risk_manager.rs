use std::collections::HashMap;

use super::types::{Order, RiskError};

pub struct RiskManager {
    limits: HashMap<(String, String), u64>, // (user_id, symbol) -> max qty
    volumes: HashMap<(String, String), u64>, // (user_id, symbol) -> used qty today
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

    pub fn check_and_record(&mut self, order: &Order) -> Result<(), RiskError> {
        let key = (order.user_id.clone(), order.symbol.clone());

        // No limit set = unlimited, always pass
        let limit = match self.limits.get(&key) {
            Some(&l) => l,
            None => return Ok(()),
        };

        let current_volume = *self.volumes.get(&key).unwrap_or(&0);
        let incoming_qty = order.quantity as u64;

        if current_volume + incoming_qty > limit {
            return Err(RiskError::LimitExceeded {
                user_id: order.user_id.clone(),
                symbol: order.symbol.clone(),
                current_volume,
                limit,
            });
        }

        *self.volumes.entry(key).or_insert(0) += incoming_qty;
        Ok(())
    }
}
