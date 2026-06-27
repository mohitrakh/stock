use std::collections::HashMap;

use super::types::{Side, WalletError};

pub struct Wallet {
    balances: HashMap<String, u64>,
    locked: HashMap<String, u64>,
}

impl Wallet {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
            locked: HashMap::new(),
        }
    }

    pub fn deposit(&mut self, user_id: String, amount: u64) {
        *self.balances.entry(user_id).or_insert(0) += amount;
    }

    pub fn balance(&self, user_id: &str) -> u64 {
        self.balances.get(user_id).copied().unwrap_or(0)
    }

    pub fn locked(&self, user_id: &str) -> u64 {
        self.locked.get(user_id).copied().unwrap_or(0)
    }

    pub fn available(&self, user_id: &str) -> u64 {
        self.balance(user_id).saturating_sub(self.locked(user_id))
    }

    pub fn check_and_lock(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        quantity: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        let required = (price * quantity as f64) as u64;
        let balance = self.balances.get(user_id).copied().unwrap_or(0);
        let locked = self.locked.get(user_id).copied().unwrap_or(0);

        let available = balance.checked_sub(locked).unwrap_or(0);
        if available < required {
            return Err(WalletError::InsufficientFunds);
        }

        *self.locked.entry(user_id.to_string()).or_insert(0) += required;
        Ok(())
    }

    pub fn commit_buy_fill(
        &mut self,
        user_id: &str,
        limit_price: f64,
        execution_price: f64,
        qty_filled: u64,
    ) -> Result<(), WalletError> {
        let amount_spent = (execution_price * qty_filled as f64) as u64;
        let amount_reserved = (limit_price * qty_filled as f64) as u64;

        let balance = self
            .balances
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *balance = balance
            .checked_sub(amount_spent)
            .ok_or(WalletError::InsufficientFunds)?;

        let locked = self
            .locked
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *locked = locked
            .checked_sub(amount_reserved)
            .ok_or(WalletError::InsufficientFunds)?;

        Ok(())
    }

    // Called on fill: money is actually spent
    pub fn commit_fill(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        qty_filled: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        self.commit_buy_fill(user_id, price, price, qty_filled)
    }

    // Called on cancel: just release the lock, no balance change
    pub fn unlock_funds(
        &mut self,
        user_id: &str,
        side: &Side,
        price: f64,
        qty_unlocked: u64,
    ) -> Result<(), WalletError> {
        if matches!(side, Side::Sell) {
            return Ok(());
        }

        let amount = (price * qty_unlocked as f64) as u64;

        let locked = self
            .locked
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *locked = locked
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        Ok(())
    }
}
