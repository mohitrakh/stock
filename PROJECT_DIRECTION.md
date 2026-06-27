# Project Direction

This file exists so a new chat or future contributor can quickly recover the intended path for this project.

Before suggesting architecture changes, read this file and then read:

1. `stock-exchange-system-design.md`
2. `SYSTEM_DOCUMENTATION.md`
3. The current Rust code, especially:
   - `src/main.rs`
   - `src/state.rs`
   - `src/controllers/exchange_controller.rs`
   - `src/types/order_manager.rs`
   - `src/types/matching_engine.rs`
   - `src/types/order_book.rs`
   - `src/types/wallet.rs`
   - `src/types/risk_manager.rs`

The design target comes from `stock-exchange-system-design.md`. The code is still a learning/prototype implementation, so prefer small correctness steps over large infrastructure rewrites.

## Goal

Build a learning stock exchange system that gradually moves toward an exchange-style architecture:

```text
gateway
  -> order manager
  -> sequencer
  -> matching engine
  -> outbound executions/events
  -> market data/reporting/replay consumers
```

The immediate goal is not to build a production exchange. The immediate goal is to make the architecture and domain behavior correct enough that more serious pipeline pieces can be added later.

## Current Architecture

Axum is currently the HTTP gateway.

Current command flow:

```text
Axum HTTP handler
  -> bounded tokio::sync::mpsc command queue
  -> dedicated exchange worker thread
  -> OrderManager
       -> RiskManager
       -> Wallet
       -> Sequencer
       -> MatchingEngine
            -> OrderBook
```

Important current decision:

```text
HTTP handlers should not directly mutate OrderManager.
They should send commands to the exchange worker.
```

The exchange worker owns the core state. This is the first step toward a deterministic application loop.

Current implementation detail:

- `AppState` carries a `Sender<ExchangeCommand>`.
- `main.rs` creates a bounded Tokio `mpsc` queue.
- `main.rs` spawns one dedicated `std::thread` exchange worker.
- HTTP handlers attach a `oneshot` response channel to each command.
- The worker processes commands one at a time and sends one response back.

## Why Tokio mpsc Is Used Right Now

Tokio `mpsc` is a temporary bridge between:

```text
async Axum gateway
  -> synchronous exchange worker thread
```

It is acceptable for the current prototype because:

- Axum handlers are async.
- `tx.send(command).await` integrates cleanly with async request handling.
- The exchange core is still owned by one dedicated thread.
- The architecture boundary is more important right now than the final queue implementation.

This does not mean Tokio `mpsc` is the final low-latency exchange queue.

Later, after correctness and event modeling are solid, the queue may be replaced with:

- nonblocking Crossbeam usage
- fixed-size ring buffer
- mmap-backed event store
- shared-memory event log

Do not replace the queue yet unless there is a specific measured reason.

## Core Architecture Principle

Separate the system into two worlds:

```text
Gateway world:
  async I/O, HTTP, auth, request parsing, validation

Exchange core world:
  synchronous, deterministic, single-owner state mutation
```

The queue is the boundary between those worlds.

Inside Axum handlers:

- Avoid blocking waits.
- Avoid directly mutating exchange state.
- Send commands and await responses.

Inside the exchange core:

- Async is not required.
- Blocking loops are acceptable.
- Later CPU pinning can target dedicated OS threads.
- Only one owner should mutate order/risk/wallet/matching state.

## What Not To Work On Yet

Do not work on these yet:

- Crossbeam migration
- ring buffer implementation
- mmap event store
- CPU pinning
- per-symbol workers
- multiple binaries/projects
- hot/warm replication
- market data publisher
- reporting service
- FIX/SBE gateway

Those are valid future topics, but adding them now would hide current domain bugs behind more infrastructure.

## Current Next Goal

The next goal is:

```text
Make OrderManager the source of truth for order lifecycle.
```

Before adding more pipeline pieces, `OrderManager` must correctly handle:

- order acceptance
- order rejection
- sequence assignment
- resting orders
- immediate fills
- partial fills
- full fills
- cancels
- wallet lock/unlock
- wallet settlement
- ownership checks

## Why This Is Next

The design document emphasizes deterministic replay and event-sourced state transitions.

That is impossible if the core lifecycle is unclear.

Current code still has important weaknesses:

- `OrderManager::add_order` sends the order to `MatchingEngine` before registering it as accepted state.
- `record_fill` mutates wallet before fully validating the fill transition.
- wallet errors are ignored in a few places.
- sell-side inventory is not modeled.
- prices use `f64`, which is not appropriate for real money/tick logic.
- executions are returned as raw structs, but there is no first-class `ExchangeEvent` model yet.

The next work should fix lifecycle correctness before adding outbound event consumers.

## Immediate Implementation Plan

Work in this order.

### 1. Stabilize `OrderManager::add_order`

Desired shape:

```text
receive order
  -> reject duplicate order_id
  -> risk check
  -> wallet lock/reserve
  -> assign sequence number
  -> register order in OrderManager.orders
  -> send order to MatchingEngine
  -> apply fills
  -> update order states
  -> settle wallet
  -> return success/error
```

Why:

If the matching engine creates immediate executions, `OrderManager` must already know the incoming order so its state can be updated safely.

### 2. Stabilize `record_fill`

Desired shape:

```text
record_fill(order_id, filled_qty)
  -> find managed order
  -> verify order is not terminal
  -> verify filled_qty <= remaining_quantity
  -> commit wallet effect
  -> reduce remaining_quantity
  -> set state to PartiallyFilled or Filled
```

Why:

Do not mutate wallet before proving the fill is valid.

### 3. Stabilize `cancel_order`

Desired shape:

```text
cancel_order(order_id)
  -> verify order exists
  -> verify not Filled/Canceled
  -> cancel resting quantity in MatchingEngine
  -> unlock only remaining locked funds
  -> mark order Canceled
```

Why:

Cancel must be correct for partially filled orders and must not unlock funds that were already spent.

### 4. Add Focused Tests

Add tests before expanding architecture.

Minimum tests:

```text
buy order rests when there is no seller
sell order rests when there is no buyer
buy fully matches resting sell
buy partially matches resting sell
sell fully matches resting buy
cancel resting buy unlocks remaining funds
cancel by wrong user is rejected
insufficient funds rejects buy order
duplicate order_id is rejected
```

Do not try to test the entire HTTP stack first. Start with core unit tests around `OrderManager`, `MatchingEngine`, `OrderBook`, and `Wallet`.

## Next Goal After Lifecycle Correctness

After the lifecycle is correct, add explicit events.

Create an `ExchangeEvent` enum similar to:

```rust
pub enum ExchangeEvent {
    FundsDeposited { user_id: String, amount: u64 },
    OrderAccepted { order_id: String, seq_num: u64 },
    OrderRejected { order_id: String, reason: String },
    OrderCanceled { order_id: String, seq_num: u64 },
    ExecutionCreated { execution: Execution },
}
```

The goal is to make state changes observable:

```text
command in
  -> deterministic state transition
  -> events out
```

Events are required for:

- market data
- reporting
- replay
- audit/debugging
- warm replica processing later

## Later Roadmap

Only after lifecycle correctness and explicit events:

1. Add outbound event queue from the exchange worker.
2. Add a market data consumer that reads executions/events.
3. Add a reporting/audit consumer.
4. Add event persistence/replay.
5. Replace `f64` prices with integer tick/cents representation.
6. Add sell-side inventory/position tracking.
7. Revisit queue implementation.
8. Consider Crossbeam/ring buffer/mmap.
9. Consider CPU pinning.
10. Consider per-symbol partitioning.

## Important Mental Model

Do not think of the project as a normal CRUD backend.

Think of it as:

```text
commands enter the exchange
the exchange assigns order
the exchange mutates state deterministically
the exchange emits events
other systems consume those events
```

The current code should evolve toward that model gradually.

## Instruction For Future Chats

When starting a new chat, use this prompt:

```text
Please read PROJECT_DIRECTION.md, stock-exchange-system-design.md, and SYSTEM_DOCUMENTATION.md first.
Then inspect the current Rust code before suggesting changes.
Keep the project on the documented path.
Do not jump to Crossbeam/ring buffer/mmap/CPU pinning until OrderManager lifecycle correctness and ExchangeEvent are done.
```

If a future suggestion conflicts with this file, resolve the conflict deliberately instead of drifting.
