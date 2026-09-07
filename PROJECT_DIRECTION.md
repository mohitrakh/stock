# Project Direction - Durable Event Log Milestone Selected

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. The current Rust code before making architecture decisions.

The repository is a learning stock exchange with an exchange-grade architecture target. Prefer small, tested changes that move toward deterministic, replayable, single-owner processing.

## Current Status

The project has a working HTTP-to-exchange boundary, an in-memory event-store-shaped runtime, a separated exchange-core pipeline, exact integer price handling, and deterministic in-memory replay with live runtime recovery.

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

`replay_event_log` rebuilds a fresh core from recorded inputs and checks regenerated outputs against history. `ExchangeRuntime::from_event_log` uses that validated core and the supplied log to resume live processing. Application startup still creates an empty runtime; recovery currently requires a caller to supply the in-memory log.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-09-06:

```text
cargo fmt -- --check
cargo test
30 passed; 0 failed
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

### 6. Deterministic in-memory replay

`ExchangeInputEvent` and `ExchangeOutputEvent` distinguish requests from outcomes inside the common `ExchangeEvent` wrapper. Failed cancellations now produce `CancelRejected`, so their outcomes are recorded as well as returned to the caller.

The shared `process_input_event` function applies one input to a core and returns its live result and output events without writing the log. Live processing records these events; replay uses the same function to rebuild state and compare outputs.

`replay_event_log` requires contiguous envelope sequence numbers starting at 1. It applies only input events to a fresh `ExchangeCore`, checks the exact values and order of regenerated outputs, and returns the core only after validating the complete supplied history. Sequence mismatches, missing outputs, unexpected outputs, and output mismatches return `ReplayError`.

`ExchangeRuntime::from_event_log` retains the validated log and resumes its event sequence after the last envelope. The rebuilt core also retains its reconstructed matching sequence. A test recovers an eight-event deposit/partial-fill history, then processes a live cancellation at event sequences 9 and 10 with matching sequence 3.

Tests cover equal outputs from equal input sequences, replayed matching state and sequence continuation, all four replay error categories, and continued live processing after recovery. This completes the in-memory replay milestone; durable storage and restart recovery are still pending.

## Current Milestone - Durable Event Log and Startup Recovery

The next milestone is to make exchange history survive a process restart.

Today, `ExchangeRuntime` keeps `EventEnvelope` values only in a `Vec`. The existing replay code can rebuild and validate an exchange when a complete log is supplied, but normal startup has no saved log to supply. Stopping the application therefore loses deposits, orders, executions, wallet state, order books, and sequence progress.

The target flow for this milestone is:

```text
live command
  -> process through the existing single-owner ExchangeRuntime and ExchangeCore
  -> save the input and generated outputs as one complete durable record
  -> confirm the record has reached durable storage
  -> reply to the HTTP request

application startup
  -> read durable records from storage
  -> rebuild EventEnvelope history
  -> validate and replay it through replay_event_log
  -> start ExchangeRuntime from the recovered history
  -> accept new commands and continue both sequence counters
```

This remains a correctness-first learning milestone. Start with a simple append-only file store behind a small storage boundary. Do not put PostgreSQL calls into `ExchangeCore`, add component threads, or replace the current command/event boundary. `ExchangeCore` must continue to process trading state synchronously in memory on the exchange-worker thread.

### Required behavior

- Events must have a stable serialized representation that can be written to a file and read back into the existing Rust event types.
- One processed command and all of its generated output events must be recoverable as one complete unit. A crash must not leave an input that replay treats as valid while its required outputs are absent.
- The runtime must not send a successful HTTP reply until the corresponding durable record has been written and synchronized.
- A storage failure must be returned or cause the exchange worker to stop accepting work. The runtime must not continue after its in-memory state has moved ahead of durable history.
- Startup must fail clearly when stored history is incomplete, corrupt, out of sequence, or fails deterministic replay. It must not silently discard that history and start an empty exchange.
- An empty or missing log may start a new exchange. A valid existing log must rebuild the exchange and continue event-log and matching sequence numbers.

### Milestone acceptance example

Deposit buyer funds, place orders that create a partial fill, stop the application, and start it again from the same event file. After restart, cancel the resting quantity successfully and verify that balances, locked funds, order state, order-book state, recorded history, and both sequence counters continue correctly.

### Decisions and risks to keep visible

Durability creates a failure point after the core has changed in memory but before the file write is confirmed. If that write fails, continuing to trade would make memory disagree with recoverable history. For this milestone, the safe behavior is to fail closed and stop accepting commands; transactional rollback inside `ExchangeCore` is a separate correctness problem.

HTTP retry safety is related but is not provided by restart recovery alone. The server may durably accept an order and then lose the HTTP response. Because the controller currently creates a new order ID for each request, a client retry could submit a second order. Request identity and idempotent retries should be discussed as a later milestone or explicit extension, not silently claimed as part of durable recovery.

Wallet credit overflow and internal failures after partial state changes remain real correctness limitations. Persistence would preserve those results rather than repair them. Keep them visible, but they do not block the first small storage step.

Full event-schema migration is outside this learning milestone. We still need serialization, but we do not promise that every old development log will survive future Rust type or error-message changes. During development, an intentionally incompatible format change may require starting with a fresh event file. Exact replay comparison remains unchanged for files produced by the current format.

### Implementation rule

Implement exactly one small, testable step at a time. Explain and agree on each step before coding, verify it, then update this journal with the completed checkpoint. Do not begin with startup wiring or file I/O until the event serialization boundary has been discussed.

### Verified Checkpoints

#### 1. Event serialization boundary

`Price`, `Side`, `Order`, `Execution`, `ExchangeInputEvent`, `ExchangeOutputEvent`, `ExchangeEvent`, and `EventEnvelope` now implement Serde serialization and deserialization. `ExchangeCommand` remains outside the serialized boundary because it is live gateway plumbing and contains the temporary response channel.

The durable record boundary is one complete `Vec<EventEnvelope>` containing an input and all outputs generated for that input. JSON is the initial human-readable storage representation. Explicit `direction`, `kind`, `event`, and `data` fields distinguish the outer input/output wrapper from the concrete business event.

A round-trip test serializes a complete deposit input/output batch to bytes, deserializes it, and proves that the recovered envelopes exactly equal the originals. No file I/O, runtime persistence, or startup recovery is implemented yet.

Verified on 2026-09-06:

```text
cargo fmt -- --check
cargo test
30 passed; 0 failed
```

## Known Prototype Limitations

- sell-side inventory/positions are not modeled
- one global minor-unit price scale is assumed; per-product currency and tick-size metadata are not modeled
- wallet balance credits do not yet return overflow errors
- the event log is in memory and disappears on restart
- durable file storage is not implemented; serialization currently exists only as a tested in-memory byte round trip
- application startup uses `ExchangeRuntime::new`; it does not load history or invoke recovery
- replay requires the full history from an empty core; snapshots and replay from a partial history are not supported
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- there is no market-data or reporting consumer
- internal matching failures after reservation do not yet have a rollback model

## What Not To Work On Yet

During the durable-log milestone, do not switch to Crossbeam, ring buffers, mmap, CPU pinning, component threads, per-symbol workers, market data, reporting, FIX/SBE, UDP, replication, or hot/warm engines. A simple append-only file is the current learning step toward the target event-store architecture.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.
