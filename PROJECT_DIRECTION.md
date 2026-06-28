# Project Direction - Event Store Runtime

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

## Current Status

The first lifecycle-correctness milestone is done. `OrderManager` now owns the core order lifecycle decisions for the current prototype: duplicate rejection, risk check before risk record, wallet lock before accepting buy orders, sequence assignment before matching, order registration before matching, fill validation before settlement mutation, execution-price settlement, cancel ownership validation, cancel unlock after matching-engine cancellation, and wallet error propagation.

The current core tests cover resting orders, full and partial fills, cancel unlocks, wrong-user cancel rejection, insufficient funds, duplicate order IDs, execution-price settlement, and risk/wallet ordering.

Latest verified status:

```text
cargo test
11 passed
```

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

The immediate goal is not to build a production exchange. The immediate goal is to move from "HTTP calls a worker function" toward an event-store-shaped exchange runtime that can later support market data, reporting, replay, and a more serious queue/log implementation.

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
Build a tiny in-memory event-store-shaped exchange runtime.
```

This is not just "add an event enum". The goal is to start the actual pipeline shape from the system design:

```text
API command
  -> sequenced input event
  -> exchange core processes event
  -> output events are appended
  -> later consumers read those events
```

The key idea is to separate runtime/API plumbing from replayable trading-domain events.

`ExchangeCommand` is still useful at the HTTP boundary because it carries `respond_to` channels for live requests. But `ExchangeCommand` should not become the event-store message because `respond_to` cannot be persisted or replayed. The trading-domain event should contain only business data, wrapped with a sequence number.

The next implementation should introduce a small exchange runtime module that owns:

- the command receiver
- the `OrderManager`
- an in-memory event log
- conversion from API commands into sequenced input events
- processing of input events through the core
- appending output events
- temporary HTTP responses through `oneshot`

This gives us the beginning of the real pipeline without prematurely adding mmap, ring buffers, SBE, separate services, or extra component threads.

## Completed Milestone: Lifecycle Correctness

The previous goal was:

```text
Make OrderManager the source of truth for order lifecycle.
```

`OrderManager` needed to correctly handle:

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

This milestone is now considered complete for the current prototype. Future bugs may still appear, but the known lifecycle issues from this phase have been addressed and covered with focused tests.

## Why The New Goal Is Next

The design document emphasizes deterministic replay and event-sourced state transitions.

Now that the core lifecycle is stable enough, the next blocker is architectural: the system still behaves mostly like a request/response worker, not an event-store-shaped exchange.

Current code still has important architecture limitations:

- sell-side inventory is not modeled.
- prices use `f64`, which is not appropriate for real money/tick logic.
- `ExchangeCommand` mixes API request plumbing with exchange input.
- `respond_to` channels cannot be persisted or replayed.
- executions are raw structs, not first-class output events.
- there is no event log for market data, reporting, audit, or replay to consume.
- the exchange worker loop still lives in `main.rs`.

The next work should create the smallest useful version of the event pipeline before adding market data or reporting consumers.

## Immediate Implementation Plan - Event Store Runtime

Work in this order.

### 1. Extract The Exchange Worker

Move `run_exchange_worker` out of `main.rs` into a dedicated module, for example:

```text
src/exchange_worker.rs
```

`main.rs` should start the gateway and the exchange runtime. It should not become the owner of exchange pipeline logic.

### 2. Define Replayable Exchange Events

Add trading-domain events that contain business data only.

Example shape:

```rust
pub enum ExchangeEvent {
    NewOrderRequested { order: Order },
    CancelOrderRequested { order_id: String, user_id: String },
    FundsDepositRequested { user_id: String, amount: u64 },
    FundsDeposited { user_id: String, amount: u64 },
    OrderAccepted { order_id: String, seq_num: u64 },
    OrderRejected { order_id: String, reason: String },
    OrderCanceled { order_id: String, seq_num: u64 },
    ExecutionCreated { execution: Execution },
}
```

Input events and output events can be split into separate enums later if the combined enum gets unclear.

### 3. Add An Event Envelope

Wrap every event with sequencing metadata.

```text
EventEnvelope
  -> seq_num
  -> event
```

This is the small in-memory version of the event-store entry shown in the system design.

### 4. Add An In-Memory Event Log

Start with a simple `Vec<EventEnvelope>` owned by the exchange runtime.

This is not the final low-latency design. It is a learning/prototype stand-in for the future mmap/ring-buffer/shared-memory event store.

### 5. Convert Commands Into Input Events

The HTTP layer can keep sending `ExchangeCommand` for now. Inside the exchange runtime:

```text
ExchangeCommand::PlaceOrder
  -> sequenced ExchangeEvent::NewOrderRequested

ExchangeCommand::CancelOrder
  -> sequenced ExchangeEvent::CancelOrderRequested

ExchangeCommand::Deposit
  -> sequenced ExchangeEvent::FundsDepositRequested
```

The `respond_to` channel stays with the API command. It is used only to answer the live HTTP request and is not written into the event log.

### 6. Process Events Through The Core

The exchange runtime should process the input event by calling `OrderManager`, then append output events.

Example:

```text
NewOrderRequested
  -> OrderManager::add_order
  -> OrderAccepted or OrderRejected
  -> ExecutionCreated events if matched
```

### 7. Add The First Consumer After The Log Exists

After events are being appended, add one simple consumer.

Do not start with a full market data publisher. Start with the smallest useful consumer, such as an audit/debug consumer or test-only reader that proves events can be read in sequence.

Market data and reporting should come after this because they need a clean event stream to consume.

## Later Roadmap

After the event-store-shaped runtime exists:

```text
1. Add a simple event consumer.
2. Add a market data consumer that reads executions/events.
3. Add a reporting/audit consumer.
4. Add event persistence/replay.
5. Replace f64 prices with integer tick/cents representation.
6. Add sell-side inventory/position tracking.
7. Revisit queue implementation.
8. Consider Crossbeam/ring buffer/mmap.
9. Consider CPU pinning.
10. Consider per-symbol partitioning.
```

## Important Mental Model

Do not think of the project as a normal CRUD backend.

Think of it as:

```text
commands enter the exchange
commands become sequenced input events
the exchange core processes events deterministically
the exchange appends output events
other systems consume the event log
```

The current code should evolve toward that model gradually.

## Instruction For Future Chats

When starting a new chat, use this prompt:

```text
Please read PROJECT_DIRECTION.md, stock-exchange-system-design.md, and SYSTEM_DOCUMENTATION.md first.
Then inspect the current Rust code before suggesting changes.
Keep the project on the documented path.
Do not jump to Crossbeam/ring buffer/mmap/CPU pinning until the in-memory event-store-shaped runtime exists.
```

If a future suggestion conflicts with this file, resolve the conflict deliberately instead of drifting.
