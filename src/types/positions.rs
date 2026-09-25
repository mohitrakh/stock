use std::collections::HashMap;

/// Why a share reservation or delivery was refused. Mirrors `WalletError`, because a position
/// ledger is the same ledger shape as a cash ledger with shares as the unit.
#[derive(Debug, PartialEq)]
pub enum PositionError {
    InsufficientShares,
    Overflow,
}

/// Share holdings per `(user_id, symbol)`, with the same three-number shape the wallet uses for
/// cash: what you hold, what is reserved against resting sell orders, and what is left to sell.
///
/// This exists so a sell order has to be backed by shares the seller actually holds. Without it the
/// exchange credits a seller cash for shares that never existed, which creates money out of nothing
/// and means the books do not balance.
pub struct Positions {
    holdings: HashMap<(String, String), u64>,
    locked: HashMap<(String, String), u64>,
}

impl Positions {
    pub fn new() -> Self {
        Self {
            holdings: HashMap::new(),
            locked: HashMap::new(),
        }
    }

    fn key(user_id: &str, symbol: &str) -> (String, String) {
        (user_id.to_string(), symbol.to_string())
    }

    /// Adds shares: an external deposit, or the buyer's side of a fill.
    ///
    /// Unlike `Wallet::deposit` this reports overflow rather than panicking in debug and wrapping in
    /// release. The wallet's silent credit is a known gap; there was no reason to copy it here.
    pub fn credit(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        let key = Self::key(user_id, symbol);
        let current = self.holdings.get(&key).copied().unwrap_or(0);
        let updated = current
            .checked_add(quantity)
            .ok_or(PositionError::Overflow)?;

        self.holdings.insert(key, updated);

        Ok(())
    }

    pub fn validate_credit(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        self.holding(user_id, symbol)
            .checked_add(quantity)
            .ok_or(PositionError::Overflow)
            .map(|_| ())
    }

    pub(crate) fn commit_credit(&mut self, user_id: &str, symbol: &str, quantity: u64) {
        let key = Self::key(user_id, symbol);
        let updated = self
            .holding(user_id, symbol)
            .checked_add(quantity)
            .expect("prepared position credit must remain valid");
        self.holdings.insert(key, updated);
    }

    pub fn holding(&self, user_id: &str, symbol: &str) -> u64 {
        self.holdings
            .get(&Self::key(user_id, symbol))
            .copied()
            .unwrap_or(0)
    }

    pub fn locked(&self, user_id: &str, symbol: &str) -> u64 {
        self.locked
            .get(&Self::key(user_id, symbol))
            .copied()
            .unwrap_or(0)
    }

    pub fn available(&self, user_id: &str, symbol: &str) -> u64 {
        self.holding(user_id, symbol)
            .saturating_sub(self.locked(user_id, symbol))
    }

    /// Reserves shares behind a sell order, the mirror of locking cash behind a buy.
    pub fn check_and_lock(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        if self.available(user_id, symbol) < quantity {
            return Err(PositionError::InsufficientShares);
        }

        let key = Self::key(user_id, symbol);
        let locked = self.locked.get(&key).copied().unwrap_or(0);
        let updated = locked
            .checked_add(quantity)
            .ok_or(PositionError::Overflow)?;

        self.locked.insert(key, updated);

        Ok(())
    }

    pub fn validate_lock(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        if self.available(user_id, symbol) < quantity {
            return Err(PositionError::InsufficientShares);
        }
        self.locked(user_id, symbol)
            .checked_add(quantity)
            .ok_or(PositionError::Overflow)
            .map(|_| ())
    }

    pub(crate) fn commit_lock(&mut self, user_id: &str, symbol: &str, quantity: u64) {
        let key = Self::key(user_id, symbol);
        let updated = self
            .locked(user_id, symbol)
            .checked_add(quantity)
            .expect("prepared position lock must remain valid");
        self.locked.insert(key, updated);
    }

    /// Delivers shares on a fill: they leave the holding and their reservation is released
    /// together, so a seller can never deliver the same shares twice.
    pub fn commit_sell_fill(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        let key = Self::key(user_id, symbol);

        let holding = self.holdings.get(&key).copied().unwrap_or(0);
        let locked = self.locked.get(&key).copied().unwrap_or(0);

        let new_holding = holding
            .checked_sub(quantity)
            .ok_or(PositionError::InsufficientShares)?;
        let new_locked = locked
            .checked_sub(quantity)
            .ok_or(PositionError::InsufficientShares)?;

        self.holdings.insert(key.clone(), new_holding);
        self.locked.insert(key, new_locked);

        Ok(())
    }

    /// Releases the reservation on a cancelled sell order's remaining quantity. The shares were
    /// never spent, so only the lock moves.
    pub fn unlock(
        &mut self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        let key = Self::key(user_id, symbol);
        let locked = self.locked.get(&key).copied().unwrap_or(0);
        let updated = locked
            .checked_sub(quantity)
            .ok_or(PositionError::InsufficientShares)?;

        self.locked.insert(key, updated);

        Ok(())
    }

    pub fn validate_unlock(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: u64,
    ) -> Result<(), PositionError> {
        self.locked(user_id, symbol)
            .checked_sub(quantity)
            .ok_or(PositionError::InsufficientShares)
            .map(|_| ())
    }

    pub(crate) fn commit_unlock(&mut self, user_id: &str, symbol: &str, quantity: u64) {
        let key = Self::key(user_id, symbol);
        let updated = self
            .locked(user_id, symbol)
            .checked_sub(quantity)
            .expect("prepared position unlock must remain valid");
        self.locked.insert(key, updated);
    }

    pub(crate) fn commit_settlement(
        &mut self,
        user_id: String,
        symbol: String,
        holding: u64,
        locked: u64,
    ) {
        self.holdings
            .insert((user_id.clone(), symbol.clone()), holding);
        self.locked.insert((user_id, symbol), locked);
    }

    /// Every `(symbol, holding, locked)` the user has a record for, sorted by symbol so the
    /// response is stable across calls.
    pub fn holdings_for(&self, user_id: &str) -> Vec<(String, u64, u64)> {
        let mut rows: Vec<(String, u64, u64)> = self
            .holdings
            .iter()
            .filter(|((holder, _), _)| holder == user_id)
            .map(|((_, symbol), quantity)| {
                (symbol.clone(), *quantity, self.locked(user_id, symbol))
            })
            .collect();

        rows.sort_by(|left, right| left.0.cmp(&right.0));

        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sell_cannot_be_reserved_without_shares() {
        let mut positions = Positions::new();

        assert_eq!(
            positions.check_and_lock("seller", "AAPL", 1),
            Err(PositionError::InsufficientShares)
        );
    }

    #[test]
    fn reserving_shares_removes_them_from_available_but_not_from_holdings() {
        let mut positions = Positions::new();
        positions.credit("seller", "AAPL", 10).unwrap();

        positions.check_and_lock("seller", "AAPL", 6).unwrap();

        assert_eq!(positions.holding("seller", "AAPL"), 10);
        assert_eq!(positions.locked("seller", "AAPL"), 6);
        assert_eq!(positions.available("seller", "AAPL"), 4);

        // The remaining 4 can still be sold, the reserved 6 cannot be sold twice.
        assert_eq!(
            positions.check_and_lock("seller", "AAPL", 5),
            Err(PositionError::InsufficientShares)
        );
        positions.check_and_lock("seller", "AAPL", 4).unwrap();
    }

    #[test]
    fn a_fill_takes_the_shares_and_their_reservation_together() {
        let mut positions = Positions::new();
        positions.credit("seller", "AAPL", 10).unwrap();
        positions.check_and_lock("seller", "AAPL", 10).unwrap();

        positions.commit_sell_fill("seller", "AAPL", 4).unwrap();

        assert_eq!(positions.holding("seller", "AAPL"), 6);
        assert_eq!(positions.locked("seller", "AAPL"), 6);
        assert_eq!(positions.available("seller", "AAPL"), 0);
    }

    #[test]
    fn cancelling_returns_the_reservation_and_leaves_holdings_alone() {
        let mut positions = Positions::new();
        positions.credit("seller", "AAPL", 10).unwrap();
        positions.check_and_lock("seller", "AAPL", 10).unwrap();
        positions.commit_sell_fill("seller", "AAPL", 4).unwrap();

        positions.unlock("seller", "AAPL", 6).unwrap();

        assert_eq!(positions.holding("seller", "AAPL"), 6);
        assert_eq!(positions.locked("seller", "AAPL"), 0);
        assert_eq!(positions.available("seller", "AAPL"), 6);
    }

    #[test]
    fn holdings_are_tracked_per_symbol() {
        let mut positions = Positions::new();
        positions.credit("trader", "AAPL", 10).unwrap();
        positions.credit("trader", "MSFT", 3).unwrap();
        positions.check_and_lock("trader", "AAPL", 10).unwrap();

        // Reserving AAPL must not touch MSFT.
        assert_eq!(positions.available("trader", "AAPL"), 0);
        assert_eq!(positions.available("trader", "MSFT"), 3);

        assert_eq!(
            positions.holdings_for("trader"),
            vec![("AAPL".to_string(), 10, 10), ("MSFT".to_string(), 3, 0)]
        );
    }

    #[test]
    fn credit_reports_overflow_rather_than_wrapping() {
        let mut positions = Positions::new();
        positions.credit("whale", "AAPL", u64::MAX).unwrap();

        assert_eq!(
            positions.credit("whale", "AAPL", 1),
            Err(PositionError::Overflow)
        );
        assert_eq!(positions.holding("whale", "AAPL"), u64::MAX);
    }
}
