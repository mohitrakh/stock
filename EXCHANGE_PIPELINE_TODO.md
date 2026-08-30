# Exchange Pipeline TODO

This is the short-term migration checklist. `PROJECT_DIRECTION.md` records project history; `stock-exchange-system-design.md` remains the long-term target.

## Completed Milestone - ExchangeCore Ownership Split

`ExchangeCore` now owns and coordinates `OrderManager`, `Sequencer`, and `MatchingEngine` without adding component threads or changing the HTTP command/event boundary.

```text
ExchangeRuntime
  -> ExchangeCore
       -> OrderManager
       -> Sequencer
       -> MatchingEngine
```

## Completed Implementation

- [x] Add `src/exchange/core.rs` and export it from `src/exchange/mod.rs`.
- [x] Add `ExchangeCore` with ownership of `OrderManager`, `Sequencer`, and `MatchingEngine`.
- [x] Separate order validation/registration from sequencing and matching.
- [x] Let `ExchangeCore` coordinate new-order processing and apply returned executions through `OrderManager`.
- [x] Move cancellation sequencing and matching-engine cancellation coordination into `ExchangeCore`.
- [x] Change `ExchangeRuntime` to call `ExchangeCore` instead of calling `OrderManager` directly.
- [x] Move lifecycle tests to the `ExchangeCore` boundary.
- [x] Add coverage proving rejected operations do not consume matching sequence numbers.
- [x] Run formatting and the full test suite.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

## Acceptance Criteria - Verified

- `OrderManager` no longer owns a `Sequencer` or `MatchingEngine`.
- `ExchangeCore` owns and coordinates the three logical trading components.
- HTTP controllers and `ExchangeCommand` did not require architectural changes.
- `ExchangeRuntime` still appends input and output events in sequence.
- Lifecycle, matching, wallet, cancellation, sequence, and runtime behavior remain covered.
- The core-ownership checkpoint passed 18 tests.

## Completed Milestone - Exact Minor-Unit Prices

The gateway and critical trading path now represent monetary prices as positive integer minor units. `Price(u64)` flows through orders, executions, order-book levels, matching quotes, wallet reservation, settlement, and cancellation.

### Completed Implementation

- [x] Change the HTTP order price from `f64` to integer minor units.
- [x] Replace the floating price wrapper with ordered `Price(u64)`.
- [x] Carry `Price` through orders, executions, price levels, order books, and matching quotes.
- [x] Replace float-to-integer wallet casts with checked exact notional calculations.
- [x] Reject notional overflow before locking funds or registering an order.
- [x] Add coverage for exact partial-fill settlement, cancellation unlocks, overflow rejection, and adjacent price levels.
- [x] Update the project journal and system documentation.

### Acceptance Criteria - Verified

- Monetary prices no longer use `f64`; only timestamps do.
- JSON order prices are integers in the same minor unit used by deposits and balances.
- Exact price comparisons determine price levels and crossing behavior.
- Wallet reservation, fill settlement, and cancellation unlocks use checked multiplication.
- `cargo test` passes 21 tests.

## Next Milestone

Not selected yet. Re-read the resulting code and discuss the next architectural goal before implementation.

Do not automatically begin market data, replay, persistence, sell-side positions, mmap, ring buffers, UDP, CPU pinning, multiple component threads, or per-symbol partitioning.
