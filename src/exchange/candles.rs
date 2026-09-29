//! Deterministic one-minute candles derived from committed two-sided execution pairs.
//!
//! This module has no file, network, or exchange-core ownership. It is the pure projection that
//! the market-data process will later persist with its `ReaderCheckpoint` and expose over HTTP.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::committed_batch::{self, CommittedCommand, ExecutionPair, NewOrderOutcome};
use crate::types::{exchange_event::EventEnvelope, types::Execution};

pub(crate) const CANDLE_INTERVAL_SECONDS: u64 = 60;

/// One UTC one-minute OHLCV bucket. `open` and `close` follow committed event order, not wall
/// clock arrival order, so replay of the same journal always produces the same result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Candle {
    pub(crate) symbol: String,
    pub(crate) start_time: u64,
    pub(crate) open: u64,
    pub(crate) high: u64,
    pub(crate) low: u64,
    pub(crate) close: u64,
    pub(crate) volume: u64,
    pub(crate) trade_count: u64,
}

/// Derived candle state. It deliberately stores no execution-id de-duplication set: once wired
/// into the MDP, the projection and reader checkpoint must be persisted atomically, which is the
/// boundary that prevents a committed batch from being applied twice after restart.
#[derive(Debug, Clone, Default)]
pub(crate) struct CandleProjection {
    candles: BTreeMap<(String, u64), Candle>,
}

impl CandleProjection {
    pub(crate) fn from_candles(candles: Vec<Candle>) -> Result<Self, String> {
        let mut projection = Self::default();
        for candle in candles {
            if candle.symbol.is_empty()
                || candle.start_time % CANDLE_INTERVAL_SECONDS != 0
                || candle.open == 0
                || candle.high == 0
                || candle.low == 0
                || candle.close == 0
                || candle.low > candle.high
                || candle.open < candle.low
                || candle.open > candle.high
                || candle.close < candle.low
                || candle.close > candle.high
                || candle.volume == 0
                || candle.trade_count == 0
            {
                return Err("persisted candle is invalid".into());
            }
            let key = (candle.symbol.clone(), candle.start_time);
            if projection.candles.insert(key, candle).is_some() {
                return Err("persisted candle is duplicated".into());
            }
        }
        Ok(projection)
    }

    /// Applies only validated, committed trades, in place. A two-sided execution pair is one
    /// trade, so this reads its first execution record only after the shared batch decoder has
    /// checked the pair. An error can leave the projection partly updated; the market-data
    /// process then withdraws its whole view, so that state is never served or saved.
    pub(crate) fn apply_batch(&mut self, batch: &[EventEnvelope]) -> Result<(), String> {
        let command = committed_batch::decode(batch)?;
        if let CommittedCommand::NewOrder {
            outcome: NewOrderOutcome::Accepted { executions, .. },
            ..
        } = command
        {
            for pair in executions {
                self.apply_execution_pair(&pair)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn candles_for_symbol(&self, symbol: &str) -> Vec<Candle> {
        self.candles
            .range((symbol.to_string(), 0)..=(symbol.to_string(), u64::MAX))
            .map(|(_, candle)| candle.clone())
            .collect()
    }

    pub(crate) fn persisted_candles(&self) -> Vec<Candle> {
        self.candles.values().cloned().collect()
    }

    pub(crate) fn candles_in_range(
        &self,
        symbol: &str,
        start_time: u64,
        end_time: u64,
    ) -> Vec<Candle> {
        self.candles
            .range((symbol.to_string(), start_time)..=(symbol.to_string(), end_time))
            .map(|(_, candle)| candle.clone())
            .collect()
    }

    fn apply_execution_pair(&mut self, pair: &ExecutionPair) -> Result<(), String> {
        self.apply_trade(&pair.first)
    }

    fn apply_trade(&mut self, trade: &Execution) -> Result<(), String> {
        if trade.symbol.is_empty() || trade.quantity == 0 || trade.price.minor_units() == 0 {
            return Err("execution has an invalid symbol, price, or quantity".into());
        }
        let start_time = candle_start(trade.timestamp)?;
        let price = trade.price.minor_units();
        let quantity = u64::from(trade.quantity);
        let key = (trade.symbol.clone(), start_time);

        match self.candles.get_mut(&key) {
            Some(candle) => {
                candle.high = candle.high.max(price);
                candle.low = candle.low.min(price);
                candle.close = price;
                candle.volume = candle
                    .volume
                    .checked_add(quantity)
                    .ok_or("candle volume overflow")?;
                candle.trade_count = candle
                    .trade_count
                    .checked_add(1)
                    .ok_or("candle trade count overflow")?;
            }
            None => {
                self.candles.insert(
                    key,
                    Candle {
                        symbol: trade.symbol.clone(),
                        start_time,
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        volume: quantity,
                        trade_count: 1,
                    },
                );
            }
        }
        Ok(())
    }
}

fn candle_start(timestamp: f64) -> Result<u64, String> {
    if !timestamp.is_finite() || timestamp < 0.0 {
        return Err("execution timestamp must be a finite UTC epoch value".into());
    }
    let seconds = timestamp.floor();
    if seconds >= u64::MAX as f64 {
        return Err("execution timestamp is outside the candle range".into());
    }
    let seconds = seconds as u64;
    Ok(seconds - seconds % CANDLE_INTERVAL_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        exchange_event::{ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::{Order, Price},
    };

    fn order() -> Order {
        Order::new(
            "incoming".into(),
            "buyer".into(),
            "AAPL".into(),
            "BUY",
            110,
            10,
            None,
            1.0,
            1,
        )
        .unwrap()
    }

    fn execution(id: &str, price: u64, quantity: u32, timestamp: f64) -> Execution {
        Execution {
            execution_id: id.into(),
            buy_order_id: "incoming".into(),
            sell_order_id: "resting".into(),
            symbol: "AAPL".into(),
            price: Price::new(price).unwrap(),
            quantity,
            timestamp,
        }
    }

    fn accepted(executions: Vec<Execution>) -> Vec<EventEnvelope> {
        let order = order();
        let mut events = vec![ExchangeEvent::Input(
            ExchangeInputEvent::NewOrderRequested {
                order: order.clone(),
            },
        )];
        events.push(ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted {
            order_id: order.order_id,
            seq_num: 1,
        }));
        events.extend(executions.into_iter().map(|execution| {
            ExchangeEvent::Output(ExchangeOutputEvent::ExecutionCreated { execution })
        }));
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| EventEnvelope {
                seq_num: index as u64 + 1,
                event,
            })
            .collect()
    }

    #[test]
    fn one_execution_pair_creates_one_trade_in_its_minute_bucket() {
        let mut projection = CandleProjection::default();
        projection
            .apply_batch(&accepted(vec![
                execution("first-buy", 100, 2, 61.9),
                execution("first-sell", 100, 2, 61.9),
                execution("second-buy", 99, 3, 119.9),
                execution("second-sell", 99, 3, 119.9),
                execution("third-buy", 105, 1, 120.0),
                execution("third-sell", 105, 1, 120.0),
            ]))
            .unwrap();

        assert_eq!(
            projection.candles_for_symbol("AAPL"),
            vec![
                Candle {
                    symbol: "AAPL".into(),
                    start_time: 60,
                    open: 100,
                    high: 100,
                    low: 99,
                    close: 99,
                    volume: 5,
                    trade_count: 2,
                },
                Candle {
                    symbol: "AAPL".into(),
                    start_time: 120,
                    open: 105,
                    high: 105,
                    low: 105,
                    close: 105,
                    volume: 1,
                    trade_count: 1,
                },
            ]
        );
    }

    #[test]
    fn invalid_trade_timestamp_fails_the_batch() {
        let mut projection = CandleProjection::default();
        let batch = accepted(vec![
            execution("valid-buy", 100, 2, 60.0),
            execution("valid-sell", 100, 2, 60.0),
            execution("bad-buy", 101, 1, -1.0),
            execution("bad-sell", 101, 1, -1.0),
        ]);

        // The batch fails. It may have updated the first trade's candle already: the market-data
        // process withdraws the whole view on any error, so that state is never served or saved.
        assert!(projection.apply_batch(&batch).is_err());
    }
}
