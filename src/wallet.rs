use std::collections::HashMap;

use crate::types::Side;

#[derive(Debug, PartialEq)]
pub enum WalletError {
    InsufficientFunds,
    Overlfow,
}

pub struct Wallet {
    pub(crate) balances: HashMap<String, u64>,
    pub(crate) locked: HashMap<String, u64>,
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

        let amount = (price * qty_filled as f64) as u64;

        let balance = self
            .balances
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *balance = balance
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        let locked = self
            .locked
            .get_mut(user_id)
            .ok_or(WalletError::InsufficientFunds)?;
        *locked = locked
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;

        Ok(())
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
