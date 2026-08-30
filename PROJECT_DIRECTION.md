# Project Direction - Exchange Core and Exact Prices Complete

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. The current Rust code before making architecture decisions.

The repository is a learning stock exchange with an exchange-grade architecture target. Prefer small, tested changes that move toward deterministic, replayable, single-owner processing.

## Current Status

The project has a working HTTP-to-exchange boundary, an in-memory event-store-shaped runtime, a separated exchange-core pipeline, and exact integer price handling.

```text
Axum HTTP handler
  -> bounded Tokio mpsc command queue
  -> dedicated exchange worker thread
  -> ExchangeRuntime
       -> append sequenced input ExchangeEvent
       -> ExchangeCore
            -> OrderManager
                 -> RiskManager
                 -> Wallet
            -> Sequencer
            -> MatchingEngine
                 -> OrderBook
       -> append output ExchangeEvent
       -> reply through temporary oneshot channel
```

`ExchangeCommand` is live gateway plumbing and may contain `respond_to`. `ExchangeEvent` contains replayable business data and must remain free of HTTP response channels.

`ExchangeRuntime` owns the command receiver and ordered in-memory `Vec<EventEnvelope>`. `ExchangeCore` owns the deterministic trading components and coordinates their calls. All core operations still run on the one existing exchange-worker thread.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-08-30:

```text
cargo test
21 passed; 0 failed
```

The compiler reports existing dead-code warnings, but the test suite passes.

## Completed Milestones

### 1. Core order lifecycle correctness

The prototype covers duplicate rejection, risk and wallet ordering, buy-side locking, resting and filled states, execution-price settlement, cancellation ownership, cancellation unlocks, and wallet error propagation.

### 2. Single-owner exchange worker boundary

HTTP handlers send `ExchangeCommand` values through bounded Tokio `mpsc` to one dedicated worker. A Tokio `oneshot` carries the temporary live HTTP result back. HTTP handlers do not mutate exchange state directly.

### 3. In-memory event-store-shaped runtime

`ExchangeRuntime` converts live commands into replayable input events, wraps all events in monotonic `EventEnvelope` sequence numbers, processes requests, appends output events, and exposes a read-only event-log view for tests and future consumers.

The log is process memory only. It is not durable and is not visible through HTTP or terminal output by default.

### 4. Exchange core responsibility split

`ExchangeCore` now owns `OrderManager`, `Sequencer`, and `MatchingEngine`.

For new orders it coordinates:

```text
OrderManager validation and reservation
  -> Sequencer assignment
  -> OrderManager registration
  -> MatchingEngine processing
  -> OrderManager execution application and settlement
```

For cancellations it coordinates:

```text
OrderManager ownership/lifecycle validation
  -> Sequencer assignment
  -> MatchingEngine removal
  -> OrderManager unlock and canceled-state transition
```

`OrderManager` no longer owns the sequencer or matching engine. It owns order lifecycle state, risk, wallet operations, fill validation, and settlement.

The lifecycle tests now exercise `ExchangeCore`, and an additional test proves that rejected orders and unauthorized cancellations do not consume matching sequence numbers.

### 5. Exact minor-unit price representation

`Price` now wraps `u64` minor units and is used by orders, executions, price levels, order books, matching quotes, wallet reservation, settlement, and cancellation unlocks.

The gateway accepts integer JSON prices. No monetary value is converted through `f64`; timestamps remain `f64` because they are not money. Checked notional calculations reject overflow before wallet state changes.

Tests cover exact settlement through a partial fill and cancellation at `1025` minor units, rejection of an overflowing price-times-quantity calculation without mutation, and distinct adjacent book levels at `1025` and `1026`.

## Current Next Goal

No new implementation goal has been selected yet.

The exact-price milestone is complete. Before choosing the next task, discuss whether the next correctness goal should be replay, sell-side positions, wallet credit overflow handling, or another documented limitation.

Do not begin the next implementation milestone without that discussion.

## Known Prototype Limitations

- sell-side inventory/positions are not modeled
- one global minor-unit price scale is assumed; per-product currency and tick-size metadata are not modeled
- wallet balance credits do not yet return overflow errors
- the event log is in memory and disappears on restart
- replay does not yet rebuild core state from a stored log
- input and output variants share one `ExchangeEvent` enum
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- there is no market-data or reporting consumer
- internal matching failures after reservation do not yet have a rollback model

## What Not To Work On Yet

Do not start Crossbeam, ring buffers, mmap, CPU pinning, component threads, per-symbol workers, market data, reporting, FIX/SBE, UDP, replication, or hot/warm engines without first selecting and documenting a new milestone.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.
