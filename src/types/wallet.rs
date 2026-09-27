use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::types::{Price, WalletError};

pub struct Wallet {
    balances: HashMap<String, u64>,
    locked: HashMap<String, u64>,
}

/// Normalized on-disk form of the cash ledger. The runtime maps are intentionally kept private
/// and use a convenient lookup shape; snapshots use rows so JSON never has to encode map keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WalletSnapshot {
    pub(crate) entries: Vec<WalletSnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WalletSnapshotEntry {
    pub(crate) user_id: String,
    pub(crate) balance: u64,
    pub(crate) locked: u64,
}

impl Wallet {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
            locked: HashMap::new(),
        }
    }

    /// Credits cash. Reports overflow rather than wrapping — previously this was a bare `+=`, which
    /// would have panicked in debug and silently wrapped a balance to near zero in release.
    pub fn deposit(&mut self, user_id: String, amount: u64) -> Result<(), WalletError> {
        let current = self.balances.get(&user_id).copied().unwrap_or(0);
        let updated = current.checked_add(amount).ok_or(WalletError::Overflow)?;

        self.balances.insert(user_id, updated);

        Ok(())
    }

    pub fn validate_deposit(&self, user_id: &str, amount: u64) -> Result<(), WalletError> {
        self.balance(user_id)
            .checked_add(amount)
            .ok_or(WalletError::Overflow)
            .map(|_| ())
    }

    pub(crate) fn commit_deposit(&mut self, user_id: String, amount: u64) {
        let updated = self
            .balance(&user_id)
            .checked_add(amount)
            .expect("prepared wallet deposit must remain valid");
        self.balances.insert(user_id, updated);
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

    /// Reserves the cash a buy order would cost. Sell-side collateral is shares, not cash, and is
    /// handled by `Positions` — this used to take a `Side` and silently do nothing for a sell,
    /// which is precisely how unbacked sells got through.
    pub fn check_and_lock(
        &mut self,
        user_id: &str,
        price: Price,
        quantity: u64,
    ) -> Result<(), WalletError> {
        let required = price
            .checked_notional(quantity)
            .ok_or(WalletError::Overflow)?;
        let balance = self.balances.get(user_id).copied().unwrap_or(0);
        let locked = self.locked.get(user_id).copied().unwrap_or(0);

        let available = balance.checked_sub(locked).unwrap_or(0);
        if available < required {
            return Err(WalletError::InsufficientFunds);
        }

        let new_locked = locked.checked_add(required).ok_or(WalletError::Overflow)?;
        self.locked.insert(user_id.to_string(), new_locked);
        Ok(())
    }

    pub fn validate_lock(
        &self,
        user_id: &str,
        price: Price,
        quantity: u64,
    ) -> Result<(), WalletError> {
        let required = price
            .checked_notional(quantity)
            .ok_or(WalletError::Overflow)?;
        let balance = self.balance(user_id);
        let locked = self.locked(user_id);
        let available = balance.checked_sub(locked).unwrap_or(0);
        if available < required {
            return Err(WalletError::InsufficientFunds);
        }
        locked
            .checked_add(required)
            .ok_or(WalletError::Overflow)
            .map(|_| ())
    }

    pub(crate) fn commit_lock(&mut self, user_id: &str, price: Price, quantity: u64) {
        let required = price
            .checked_notional(quantity)
            .expect("prepared wallet lock must have a valid notional");
        let updated = self
            .locked(user_id)
            .checked_add(required)
            .expect("prepared wallet lock must remain valid");
        self.locked.insert(user_id.to_string(), updated);
    }

    pub fn commit_buy_fill(
        &mut self,
        user_id: &str,
        limit_price: Price,
        execution_price: Price,
        qty_filled: u64,
    ) -> Result<(), WalletError> {
        let amount_spent = execution_price
            .checked_notional(qty_filled)
            .ok_or(WalletError::Overflow)?;
        let amount_reserved = limit_price
            .checked_notional(qty_filled)
            .ok_or(WalletError::Overflow)?;

        let balance = self
            .balances
            .get(user_id)
            .copied()
            .ok_or(WalletError::InsufficientFunds)?;
        let locked = self
            .locked
            .get(user_id)
            .copied()
            .ok_or(WalletError::InsufficientFunds)?;

        let new_balance = balance
            .checked_sub(amount_spent)
            .ok_or(WalletError::InsufficientFunds)?;
        let new_locked = locked
            .checked_sub(amount_reserved)
            .ok_or(WalletError::InsufficientFunds)?;

        self.balances.insert(user_id.to_string(), new_balance);
        self.locked.insert(user_id.to_string(), new_locked);

        Ok(())
    }

    /// Called on cancel: releases a buy order's remaining reservation, no balance change.
    pub fn unlock_funds(
        &mut self,
        user_id: &str,
        price: Price,
        qty_unlocked: u64,
    ) -> Result<(), WalletError> {
        let amount = price
            .checked_notional(qty_unlocked)
            .ok_or(WalletError::Overflow)?;

        let locked = self
            .locked
            .get(user_id)
            .copied()
            .ok_or(WalletError::InsufficientFunds)?;
        let new_locked = locked
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)?;
        self.locked.insert(user_id.to_string(), new_locked);

        Ok(())
    }

    pub fn validate_unlock(
        &self,
        user_id: &str,
        price: Price,
        qty_unlocked: u64,
    ) -> Result<(), WalletError> {
        let amount = price
            .checked_notional(qty_unlocked)
            .ok_or(WalletError::Overflow)?;
        self.locked(user_id)
            .checked_sub(amount)
            .ok_or(WalletError::InsufficientFunds)
            .map(|_| ())
    }

    pub(crate) fn commit_unlock(&mut self, user_id: &str, price: Price, qty_unlocked: u64) {
        let amount = price
            .checked_notional(qty_unlocked)
            .expect("prepared wallet unlock must have a valid notional");
        let updated = self
            .locked(user_id)
            .checked_sub(amount)
            .expect("prepared wallet unlock must remain valid");
        self.locked.insert(user_id.to_string(), updated);
    }

    pub(crate) fn commit_settlement(&mut self, user_id: String, balance: u64, locked: u64) {
        self.balances.insert(user_id.clone(), balance);
        self.locked.insert(user_id, locked);
    }

    pub(crate) fn snapshot(&self) -> WalletSnapshot {
        let mut user_ids: Vec<_> = self
            .balances
            .keys()
            .chain(self.locked.keys())
            .cloned()
            .collect();
        user_ids.sort();
        user_ids.dedup();

        WalletSnapshot {
            entries: user_ids
                .into_iter()
                .map(|user_id| WalletSnapshotEntry {
                    balance: self.balance(&user_id),
                    locked: self.locked(&user_id),
                    user_id,
                })
                .collect(),
        }
    }

    pub(crate) fn from_snapshot(snapshot: WalletSnapshot) -> Result<Self, String> {
        let mut balances = HashMap::new();
        let mut locked = HashMap::new();

        for entry in snapshot.entries {
            if entry.locked > entry.balance {
                return Err(format!(
                    "wallet snapshot locks more cash than it holds for {}",
                    entry.user_id
                ));
            }
            if balances
                .insert(entry.user_id.clone(), entry.balance)
                .is_some()
            {
                return Err(format!(
                    "wallet snapshot contains duplicate user {}",
                    entry.user_id
                ));
            }
            locked.insert(entry.user_id, entry.locked);
        }

        Ok(Self { balances, locked })
    }
}
