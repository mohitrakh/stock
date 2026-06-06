# Stock Trading System - Core Engine Documentation

This document provides a simple, structured explanation of the core types, components, fields, and methods that drive the stock trading system. It excludes user models and API controller/routing details to focus entirely on the core execution, risk, and matching logic.

---

## 🏗️ System Architecture Overview

The system is organized hierarchically, with the `OrderManager` acts as the central orchestrator that coordinates sequence generation, risk validation, capital/funds validation, and order matching:

```mermaid
graph TD
    OM[OrderManager] --> SEQ[Sequencer]
    OM --> RM[RiskManager]
    OM --> W[Wallet]
    OM --> ME[MatchingEngine]
    ME --> OB[OrderBook (per Symbol)]
    OB --> PL[PriceLevel]
    PL --> N[Node (Doubly Linked List)]
    N --> O[Order]
```

### Flow of an Order Placement
1. **Sequence Generation**: `OrderManager` obtains a unique, monotonic sequence number from the `Sequencer` for serialization.
2. **Risk Check**: `RiskManager` verifies if the user's cumulative quantity limit for that asset is exceeded.
3. **Wallet Check**: `Wallet` ensures the buyer has enough available (unlocked) funds and locks the required escrow.
4. **Matching**: `MatchingEngine` delegates the order to the correct `OrderBook` where it is matched against resting orders.
5. **Settlement**: If fills occur, trade cash is moved in the `Wallet` from the buyer to the seller, and remaining quantities are updated.

---

## 🗃️ 1. Core Types (`src/types/types.rs`)

This module defines the basic data structures, enums, and primitives used throughout the matching engine and risk/wallet sub-systems.

### `Price` (Struct)
A safe wrapper around `f64` representing order prices, ensuring prices are valid and ordering is deterministic.
*   **Fields:**
    *   `0` (`f64`): The raw price value.
*   **Methods:**
    *   `new(val: f64) -> Price`
        *   Instantiates a new price and asserts that the price is not NaN.
    *   `as_f64(self) -> f64`
        *   Unwraps and returns the underlying `f64` value.

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
    *   `price` (`f64`): Limit price for the order.
    *   `quantity` (`u32`): Initial requested order quantity.
    *   `leaves_qty` (`u32`): Remaining unfilled quantity.
    *   `timestamp` (`f64`): System epoch timestamp when the order was created.
    *   `seq_num` (`u64`): The unique sequence number assigned to this action.
*   **Methods:**
    *   `new(...) -> Result<Order, String>`
        *   Validates and constructs an order. Ensures price and quantity are positive and parses the side string.

### `Execution` (Struct)
Represents a match event between a buyer and a seller.
*   **Fields:**
    *   `execution_id` (`String`): Unique execution ID.
    *   `buy_order_id` (`String`): The matching buy order ID.
    *   `sell_order_id` (`String`): The matching sell order ID.
    *   `symbol` (`String`): The ticker symbol traded.
    *   `price` (`f64`): The execution price.
    *   `quantity` (`u32`): The quantity filled.
    *   `timestamp` (`f64`): The time when matching occurred.

---

## 📊 2. Price Level Queue (`src/types/price_level.rs`)

Stores and manages resting orders at a single price point. It uses a vector-backed doubly-linked list (`Vec<Node>`) to support fast updates.

### `PriceLevel` (Struct)
*   **Fields:**
    *   `price` (`f64`): The price value of this level.
    *   `nodes` (`Vec<Node>`): The list containing the order nodes.
    *   `head_idx` (`Option<usize>`): Index pointing to the front of the queue (oldest order).
    *   `tail_idx` (`Option<usize>`): Index pointing to the back of the queue (newest order).
    *   `order_map` (`HashMap<String, usize>`): Maps an order ID to its index in `nodes` for $O(1)$ lookups.
*   **Methods:**
    *   `new(price: f64) -> PriceLevel`
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
    *   `best_bid(&self) -> Option<(f64, u32)>`
        *   Returns the highest bid price and its total depth/quantity.
    *   `best_ask(&self) -> Option<(f64, u32)>`
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
    *   `best_bid_ask(&self, symbol: &str) -> Option<((f64, u32), (f64, u32))>`
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
    *   `check_and_record(&mut self, order: &Order) -> Result<(), RiskError>`
        *   Validates if the new order quantity would exceed the user's limit. If valid, records the volume increment.

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
    *   `check_and_lock(&mut self, user_id: &str, side: &Side, price: f64, quantity: u64) -> Result<(), WalletError>`
        *   For buy orders, verifies if the user has enough unlocked funds and lock-reserves the maximum cost. Sell orders pass through without locking cash.
    *   `commit_fill(&mut self, user_id: &str, side: &Side, price: f64, qty_filled: u64) -> Result<(), WalletError>`
        *   Debits both the active total balance and the locked balance of a buyer when a fill is reported.
    *   `unlock_funds(&mut self, user_id: &str, side: &Side, price: f64, qty_unlocked: u64) -> Result<(), WalletError>`
        *   Releases locked cash back into the user's available balance (used during order cancellations).

---

## 🎛️ 7. Order Manager Orchestrator (`src/types/order_manager.rs`)

Integrates risk, wallet ledger, sequencer, and the matching engine, tracking the lifecycle status of all orders.

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
    *   `engine` (`MatchingEngine`): Coordinates order matching.
    *   `sequencer` (`Sequencer`): Monotonic sequence generator.
    *   `execution_callbacks` (`Vec<Box<dyn Fn(Execution)>>`): List of subscriber callbacks triggered upon trade match.
*   **Methods:**
    *   `new() -> OrderManager`
        *   Creates an initialized orchestrator with fresh components.
    *   `add_order(&mut self, mut order: Order) -> Result<(), OrderManagerError>`
        *   Orchestrates placement: checks risk, locks wallet funds, assigns sequence number, passes to matching engine, records fills, settles cash transfers to sellers, and triggers execution callbacks.
    *   `cancel_order(&mut self, order_id: &str) -> Result<(), OrderManagerError>`
        *   Orchestrates cancel: verifies state is active, requests engine cancellation, releases locked wallet funds, and updates order state.
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
