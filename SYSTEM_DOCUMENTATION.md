# Stock Trading System - Current Implementation Documentation

This document describes the code that exists now. `stock-exchange-system-design.md` describes the long-term target, while `PROJECT_DIRECTION.md` records completed milestones and the next task.

---

## 🏗️ System Architecture Overview

Axum acts as the gateway. It sends live `ExchangeCommand` values to one dedicated worker. `ExchangeRuntime` converts those commands into replayable events, records them in memory, invokes `ExchangeCore`, records output events, and returns live results through `oneshot` channels.

```mermaid
graph TD
    HTTP[Axum HTTP handlers] --> CMD[ExchangeCommand queue]
    CMD --> RT[ExchangeRuntime]
    RT --> LOG[In-memory EventEnvelope log]
    RT --> CORE[ExchangeCore]
    CORE --> OM[OrderManager]
    CORE --> SEQ[Sequencer]
    CORE --> ME[MatchingEngine]
    OM --> RM[RiskManager]
    OM --> W[Wallet]
    ME --> OB[OrderBook (per Symbol)]
    OB --> PL[PriceLevel]
    PL --> N[Node (Doubly Linked List)]
    N --> O[Order]
```

### Current Flow of an Order Placement

1. The HTTP handler creates `ExchangeCommand::PlaceOrder` with a temporary reply channel.
2. `ExchangeRuntime` appends `NewOrderRequested` to its in-memory event log.
3. `ExchangeCore` asks `OrderManager` to check duplicates, risk, and wallet funds.
4. `ExchangeCore` obtains the next matching sequence from `Sequencer`.
5. `ExchangeCore` registers the sequenced order with `OrderManager` before calling `MatchingEngine`.
6. `ExchangeCore` gives returned executions to `OrderManager` for lifecycle updates and wallet settlement.
7. `ExchangeRuntime` appends `OrderAccepted` or `OrderRejected`, followed by any `ExecutionCreated` events.
8. The runtime sends the live result back through the command's `oneshot` channel.

All of these core calls run synchronously on the existing single exchange-worker thread. Logical component separation did not introduce internal channels or component threads.

## Exchange Core (`src/exchange/core.rs`)

`ExchangeCore` is the synchronous coordinator for the critical trading path. It owns `OrderManager`, `Sequencer`, and `MatchingEngine`.

For a new order, it asks `OrderManager` to validate and reserve the order, assigns the matching sequence, registers the order, calls `MatchingEngine`, and gives the returned executions back to `OrderManager`.

For a cancellation, it validates ownership and lifecycle state, assigns the matching sequence, removes the order from `MatchingEngine`, and then tells `OrderManager` to unlock funds and mark the order canceled.

`AddOrderOutcome` belongs to this layer because it combines results from lifecycle management, sequencing, and matching.

## Event Runtime (`src/exchange/runtime.rs`)

`ExchangeRuntime` owns the command receiver, `ExchangeCore`, in-memory event log, and the next event-log sequence number.

`ExchangeCommand` belongs to the live HTTP boundary because it contains `respond_to`; it is not replayable. `ExchangeEvent` contains business data only, and `EventEnvelope` adds a monotonic `seq_num`.

The current event variants cover deposits, new orders, cancellations, accepted/rejected outcomes, successful deposits, cancellations, and created executions. The log is currently process memory only: it is neither durable nor exposed through HTTP.

---

## 🗃️ 1. Core Types (`src/types/types.rs`)

This module defines the basic data structures, enums, and primitives used throughout the matching engine and risk/wallet sub-systems.

### `Price` (Struct)
A positive exact price represented as integer minor units. The prototype uses one shared unit for prices, deposits, balances, locks, and settlement. For example, `1025` represents `$10.25` when the configured minor unit is one cent.
*   **Fields:**
    *   `0` (`u64`): Exact minor-unit value.
*   **Methods:**
    *   `new(minor_units: u64) -> Result<Price, String>`
        *   Rejects zero and constructs a positive price.
    *   `minor_units(self) -> u64`
        *   Returns the exact integer value used by the gateway and wallet.
    *   `checked_notional(self, quantity: u64) -> Option<u64>`
        *   Computes price times quantity without overflow.

### `Side` (Enum)
Represents the trade direction of an order.
*   **Variants:**
    *   `Buy`: Represents a bid order.
    *   `Sell`: Represents an ask order.
*   **Methods:**
    *   `from_str(s: &str) -> Result<Side, String>`
        *   Converts `"BUY"` or `"SELL"` string slices into the corresponding enum variant.

### `Node` (Struct)
A node within the doubly-linked list used inside a price level queue.
*   **Fields:**
    *   `order` (`Option<Order>`): The resting order stored in this node.
    *   `prev_idx` (`Option<usize>`): The index of the previous node in the allocation vector.
    *   `next_idx` (`Option<usize>`): The index of the next node in the allocation vector.

### `Order` (Struct)
Represents a trading order containing placement specs, volume requirements, and sequencing information.
*   **Fields:**
    *   `order_id` (`String`): Globally unique identifier for the order.
    *   `user_id` (`String`): Identifier of the user placing the order.
    *   `symbol` (`String`): Asset ticker symbol (e.g., AAPL).
    *   `side` (`Side`): The buy or sell trade direction.
    *   `price` (`Price`): Exact limit price in minor units.
    *   `quantity` (`u32`): Initial requested order quantity.
    *   `leaves_qty` (`u32`): Remaining unfilled quantity.
    *   `timestamp` (`f64`): System epoch timestamp when the order was created.
    *   `seq_num` (`u64`): The unique sequence number assigned to this action.
*   **Methods:**
    *   `new(...) -> Result<Order, String>`
        *   Accepts an integer minor-unit price, validates positive price and quantity, and parses the side string.

### `Execution` (Struct)
Represents a match event between a buyer and a seller.
*   **Fields:**
    *   `execution_id` (`String`): Unique execution ID.
    *   `buy_order_id` (`String`): The matching buy order ID.
    *   `sell_order_id` (`String`): The matching sell order ID.
    *   `symbol` (`String`): The ticker symbol traded.
    *   `price` (`Price`): Exact execution price copied from the resting order.
    *   `quantity` (`u32`): The quantity filled.
    *   `timestamp` (`f64`): The time when matching occurred.

---

## 📊 2. Price Level Queue (`src/types/price_level.rs`)

Stores and manages resting orders at a single price point. It uses a vector-backed doubly-linked list (`Vec<Node>`) to support fast updates.

### `PriceLevel` (Struct)
*   **Fields:**
    *   `price` (`Price`): Exact price value of this level.
    *   `nodes` (`Vec<Node>`): The list containing the order nodes.
    *   `head_idx` (`Option<usize>`): Index pointing to the front of the queue (oldest order).
    *   `tail_idx` (`Option<usize>`): Index pointing to the back of the queue (newest order).
    *   `order_map` (`HashMap<String, usize>`): Maps an order ID to its index in `nodes` for $O(1)$ lookups.
*   **Methods:**
    *   `new(price: Price) -> PriceLevel`
        *   Creates an empty price level.
    *   `append(&mut self, order: Order)`
        *   Appends a new order to the tail of the queue ($O(1)$ time-priority tracking).
    *   `remove(&mut self, order_id: &str) -> Option<Order>`
        *   Removes an order anywhere in the queue by updating the linked list node pointers ($O(1)$ cancel).
    *   `peek_front(&self) -> Option<&Order>`
        *   Returns a reference to the order at the front of the queue without removing it.
    *   `pop_front(&mut self) -> Option<Order>`
        *   Removes and returns the oldest order (front of queue).
    *   `is_empty(&self) -> bool`
        *   Checks if the queue contains any active orders.
    *   `total_quantity(&self) -> u32`
        *   Traverses the active queue and returns the sum of `leaves_qty` of all resting orders.
    *   `peek_front_mut(&mut self) -> Option<&mut Order>`
        *   Returns a mutable reference to the order at the front of the queue (e.g., to adjust remaining quantities).

---

## 📖 3. Order Book (`src/types/order_book.rs`)

Maintains two separate sides (bid and ask) for a single symbol using self-balancing trees (`BTreeMap`) sorted by price.

### `OrderBook` (Struct)
*   **Fields:**
    *   `symbol` (`String`): The ticker symbol.
    *   `buy_levels` (`BTreeMap<Reverse<Price>, PriceLevel>`): Buy orders sorted by price descending (highest bid first).
    *   `sell_levels` (`BTreeMap<Price, PriceLevel>`): Sell orders sorted by price ascending (lowest ask first).
    *   `order_map` (`HashMap<String, (Price, Side)>`): Maps an active order ID to its price and side for $O(1)$ routing.
    *   `exec_counter` (`u64`): Monotonic counter used to generate unique trade execution IDs.
*   **Methods:**
    *   `new(symbol: String) -> OrderBook`
        *   Initializes a clean, empty order book.
    *   `best_bid(&self) -> Option<(Price, u32)>`
        *   Returns the highest bid price and its total depth/quantity.
    *   `best_ask(&self) -> Option<(Price, u32)>`
        *   Returns the lowest ask price and its total depth/quantity.
    *   `cancel_order(&mut self, order_id: &str) -> Option<Order>`
        *   Locates, removes, and returns the order. Removes the price level map entry if it becomes empty.
    *   `match_order(&mut self, order: &mut Order) -> Vec<Execution>`
        *   Matches an incoming order against resting opposite-side orders. Loops through price levels and generates matches until the order is filled or price cross bounds are broken. Handles self-trade prevention (skips matches if user IDs are the same).
    *   `place_order(&mut self, mut order: Order) -> Vec<Execution>`
        *   Attempts to match the incoming order. If there is a remaining quantity, appends it as a resting order in the book.
    *   `is_resting(&self, order_id: &str) -> bool`
        *   Checks if the order ID is currently resting in the book's map.

---

## ⚙️ 4. Matching Engine (`src/types/matching_engine.rs`)

Routes incoming orders and cancellations to the appropriate `OrderBook` and enforces sequence consistency.

### `MatchingEngine` (Struct)
*   **Fields:**
    *   `order_books` (`HashMap<String, OrderBook>`): Maps ticker symbols to their respective order books.
    *   `order_location` (`HashMap<String, String>`): Maps order IDs to their symbol to optimize cancellation lookups.
    *   `last_seq` (`u64`): The last processed sequence number to guard against out-of-order execution.
*   **Methods:**
    *   `new() -> MatchingEngine`
        *   Creates a new matching engine instance.
    *   `process_order(&mut self, order: Order) -> Result<Vec<Execution>, String>`
        *   Validates the sequence number, obtains/creates the symbol's book, places/matches the order, updates `last_seq`, and tracks the location if it becomes a resting order.
    *   `best_bid_ask(&self, symbol: &str) -> Option<((Price, u32), (Price, u32))>`
        *   Retrieves the current best bid and ask (prices and quantities) for a given symbol.
    *   `cancel_order(&mut self, order_id: &str, cancel_seq: u64) -> Result<Option<Order>, String>`
        *   Enforces sequence order, routes the cancel request to the correct order book, updates tracking maps, and updates `last_seq`.
    *   `is_resting(&self, order_id: &str) -> bool`
        *   Returns true if the order ID exists in the resting order tracking index.
    *   `get_order_leaves(&self, order_id: &str) -> Option<u32>`
        *   Retrieves the remaining unfilled quantity (`leaves_qty`) of a resting order.

---

## 🛡️ 5. Risk Manager (`src/types/risk_manager.rs`)

Validates if trading activity stays within allowed constraints to prevent over-exposure.

### `RiskManager` (Struct)
*   **Fields:**
    *   `limits` (`HashMap<(String, String), u64>`): Maps `(user_id, symbol)` to the maximum quantity they can trade/order.
    *   `volumes` (`HashMap<(String, String), u64>`): Maps `(user_id, symbol)` to the total cumulative quantity they have submitted.
*   **Methods:**
    *   `new() -> RiskManager`
        *   Creates a new risk manager.
    *   `set_limit(&mut self, user_id: String, symbol: String, limit: u64)`
        *   Configures or updates a volume limit constraint.
    *   `check(&self, order: &Order) -> Result<(), RiskError>`
        *   Validates the order without mutating recorded volume.
    *   `record(&mut self, order: &Order)`
        *   Records volume only after the wallet check succeeds.
    *   `check_and_record(&mut self, order: &Order) -> Result<(), RiskError>`
        *   Older combined convenience method; the current order path deliberately calls `check` and `record` separately.

---

## 💳 6. Wallet Ledger (`src/types/wallet.rs`)

Tracks capital, manages buy-side order locks (collateral), and settles balances during executions.

### `Wallet` (Struct)
*   **Fields:**
    *   `balances` (`HashMap<String, u64>`): Maps user IDs to total cash balances.
    *   `locked` (`HashMap<String, u64>`): Maps user IDs to locked/escrowed cash balances.
*   **Methods:**
    *   `new() -> Wallet`
        *   Initializes an empty wallet.
    *   `deposit(&mut self, user_id: String, amount: u64)`
        *   Credits cash directly to a user's balance.
    *   `check_and_lock(&mut self, user_id: &str, side: &Side, price: Price, quantity: u64) -> Result<(), WalletError>`
        *   For buy orders, computes the exact notional with checked multiplication and lock-reserves it. Sell orders pass through without locking cash.
    *   `commit_buy_fill(&mut self, user_id: &str, limit_price: Price, execution_price: Price, qty_filled: u64) -> Result<(), WalletError>`
        *   Atomically settles a buyer at the exact execution price and releases any excess lock caused by price improvement.
    *   `unlock_funds(&mut self, user_id: &str, side: &Side, price: Price, qty_unlocked: u64) -> Result<(), WalletError>`
        *   Releases the exact remaining lock during cancellation. Notional overflow is returned as `WalletError::Overflow`.

---

## 🎛️ 7. Order Manager (`src/types/order_manager.rs`)

Owns order lifecycle state, risk checks, wallet reservation, cancellation completion, fill validation, and settlement. It does not own sequencing or order books.

### `OrderState` (Enum)
*   **Variants:**
    *   `New`: Fresh order.
    *   `PartiallyFilled`: Order has matched some shares, but has remaining leaves.
    *   `Filled`: Order is completely filled.
    *   `Canceled`: Remaining leaves quantity cancelled.

### `ManagedOrder` (Struct)
*   **Fields:**
    *   `order` (`Order`): The core order.
    *   `state` (`OrderState`): Current order state.
    *   `remaining_quantity` (`u32`): Outstanding quantity to match.

### `OrderManager` (Struct)
*   **Fields:**
    *   `orders` (`HashMap<String, ManagedOrder>`): Stores all historical and active orders.
    *   `risk_manager` (`RiskManager`): Manages risk checks.
    *   `wallet` (`Wallet`): Manages cash balances.
    *   `execution_callbacks` (`Vec<Box<dyn Fn(Execution)>>`): List of subscriber callbacks triggered upon trade match.
*   **Methods:**
    *   `new() -> OrderManager`
        *   Creates a fresh lifecycle manager, risk manager, and wallet.
    *   `prepare_order(&mut self, order: Order) -> Result<Order, OrderManagerError>`
        *   Rejects duplicates, runs risk checks, locks wallet funds, and records accepted risk volume before sequencing.
    *   `register_order(&mut self, order: Order)`
        *   Stores the sequenced order before matching so immediate executions can update both orders.
    *   `apply_executions(&mut self, executions: &[Execution]) -> Result<(), OrderManagerError>`
        *   Validates fills, settles wallets, updates order states, and triggers execution callbacks.
    *   `validate_cancel_for_user(&self, order_id: &str, user_id: &str) -> Result<(), OrderManagerError>`
        *   Validates order existence, ownership, and non-terminal state before cancellation sequencing.
    *   `complete_cancel(&mut self, order_id: &str) -> Result<(), OrderManagerError>`
        *   Unlocks remaining funds and records the canceled state after matching-engine removal succeeds.
    *   `record_fill(&mut self, order_id: &str, filled_qty: u32) -> Result<(), OrderManagerError>`
        *   Internal helper. Updates remaining quantity, transitions lifecycle states, and processes wallet adjustments.
    *   `get_state(&self, order_id: &str) -> Option<OrderState>`
        *   Inspects the lifecycle state of an order.
    *   `subscribe<F>(&mut self, callback: F)`
        *   Subscribes listener closures to receive execution notices.

---

## ⏱️ 8. Sequencer (`src/sequencer.rs`)

Generates monotonic sequence numbers to serialize instructions.

### `Sequencer` (Struct)
*   **Fields:**
    *   `next_seq` (`u64`): The next sequence number to assign.
*   **Methods:**
    *   `new(start_seq: u64) -> Sequencer`
        *   Creates a sequencer starting from the specified number.
    *   `next(&mut self) -> u64`
        *   Returns the current sequence number and increments the counter by 1.

---

## 🗄️ 9. Database & State (`src/db.rs` & `src/state.rs`)

Simple connection utilities for PostgreSQL backing.

### `connect_db` (Function in `src/db.rs`)
*   `connect_db() -> PgPool`
    *   Reads `DATABASE_URL` from the environment, sets up a connection pool, and returns it.

### `AppState` (Struct in `src/state.rs`)
*   **Fields:**
    *   `db` (`PgPool`): The shared SQLx connection pool shared across web routes.
    *   `tx` (`Sender<ExchangeCommand>`): Bounded command-queue sender used by HTTP handlers to reach the single exchange worker.

---

## Current Verification

As of 2026-08-30, `cargo test` passes 18 tests. These cover the exchange-core lifecycle, matching, wallet settlement, cancellation, rejected-operation sequence behavior, and sequential consumption of the runtime event log.
