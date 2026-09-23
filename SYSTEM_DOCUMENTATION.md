# Stock Trading System - Current Implementation Documentation

This document describes the code that exists now. `stock-exchange-system-design.md` describes the long-term target, while `PROJECT_DIRECTION.md` records completed milestones and the next task.

---

## 🏗️ System Architecture Overview

Axum acts as the gateway. It sends live `ExchangeCommand` values to one dedicated worker. `ExchangeRuntime` converts those commands into replayable events, records them in memory, invokes `ExchangeCore`, records output events, and returns live results through `oneshot` channels.

Every processed command is written to an append-only event log file before the client is answered, and application startup rebuilds the exchange from that file through deterministic replay. `main` calls `recover_runtime`, which opens the log, replays it, and refuses to start on any history it cannot trust.

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

### Read Path

`ExchangeCommand::GetBalance`, `GetOrder`, and `GetOrderBook` travel the same bounded queue to the same worker, because the worker owns the state a read has to touch. `handle_command` answers them directly from `ExchangeCore` and returns through the command's `oneshot`.

They are **not** mirrored by an `ExchangeInputEvent` and never reach the event log. A read mutates nothing, so recording it would make every future replay longer without changing any outcome. `queries_do_not_append_to_the_event_log` enforces this.

HTTP surface:

| Method | Path | Auth |
|---|---|---|
| `POST` | `/exchange/deposit` | required |
| `POST` | `/exchange/orders` | required, returns an `OrderView` |
| `POST` | `/exchange/orders/cancel` | required |
| `POST` | `/exchange/shares/deposit` | required |
| `GET` | `/exchange/balance` | required |
| `GET` | `/exchange/positions` | required |
| `GET` | `/exchange/executions?symbol=&order_id=&start_time=&end_time=` | required, own fills only; all filters optional |
| `POST` | `/exchange/risk/limits` | required, sets the caller's own daily cap |
| `GET` | `/exchange/risk/limits?symbol=` | required |
| `GET` | `/exchange/orders/{order_id}` | required, owner only (404 otherwise) |
| `GET` | `/exchange/orderbook/{symbol}?depth=N` | public; depth defaults to 10, capped at 50 |

## Exchange Core (`src/exchange/core.rs`)

`ExchangeCore` is the synchronous coordinator for the critical trading path. It owns `OrderManager`, `Sequencer`, and `MatchingEngine`.

For a new order, it asks `OrderManager` to validate and reserve the order, assigns the matching sequence, registers the order, calls `MatchingEngine`, and gives the returned executions back to `OrderManager`.

For a cancellation, it validates ownership and lifecycle state, assigns the matching sequence, removes the order from `MatchingEngine`, and then tells `OrderManager` to unlock funds and mark the order canceled.

`AddOrderOutcome` belongs to this layer because it combines results from lifecycle management, sequencing, and matching.

## Event Runtime (`src/exchange/runtime.rs`)

`ExchangeRuntime` owns the command receiver, `ExchangeCore`, in-memory event log, and the next event-log sequence number.

`ExchangeCommand` belongs to the live HTTP boundary because it contains `respond_to`; it is not replayable. `ExchangeEvent` contains business data only, and `EventEnvelope` adds a monotonic `seq_num`.

### Input and Output Events

`ExchangeInputEvent` represents deposit, new-order, and cancellation requests. `ExchangeOutputEvent` represents successful deposits, order acceptance or rejection, successful or rejected cancellations, and created executions. `ExchangeEvent::Input` and `ExchangeEvent::Output` wrap these types in one ordered log. Events, envelopes, and replay-relevant order/execution values support `PartialEq` for comparison.

`process_input_event` takes a mutable core and one input event. It returns `ProcessedInput`, containing the live result and generated output events, without writing the runtime log. Live processing appends the input, calls this shared processor, appends its outputs, and sends the result through the response channel.

### Replay and Recovery

`replay_event_log(&[EventEnvelope])` first checks that envelope sequences are contiguous starting at 1. It then creates a fresh `ExchangeCore` and processes each recorded input through `process_input_event`. Generated outputs must exactly match the following recorded outputs in both value and order. Recorded outputs are checked, not applied to the core a second time.

`ReplayError` distinguishes `EventSequenceMismatch`, `MissingOutput`, `UnexpectedOutput`, and `OutputMismatch`. The rebuilt core is returned only after the entire supplied log passes validation; a failed replay does not return its partially rebuilt core.

`ExchangeRuntime::from_event_log(rx, event_log)` uses that core, retains the supplied log, and sets the next event-log sequence to the last envelope's sequence plus one. An empty log produces a fresh core with next event sequence 1. Matching-input sequencing is reconstructed by replaying core operations and remains distinct from event-log sequencing.

Recovery requires the full history from an empty core, including deposits and orders that affect later inputs. Snapshots and replay from a partial history are not implemented.

## Event Store (`src/exchange/event_store.rs`)

`EventStore` is the only code that touches the log file. `ExchangeCore` never sees it.

On-disk format: an 8-byte magic header `EXCHLOG1` once, then one framed record per processed command — `[len: u32 LE][crc32: u32 LE][JSON payload]`, where the payload is the `Vec<EventEnvelope>` holding that command's input event and every output event it produced. Writing them as one record is what guarantees a crash can never leave an input on disk without its outputs.

`EventStore::open(path)` creates the file if absent, validates the magic, decodes every record, truncates a torn tail left by a crash, and returns the store together with the recovered envelopes. `append(&[EventEnvelope])` frames one batch, writes it with a single `write_all`, and calls `sync_all`; it returns only once the bytes are durable.

Recovery distinguishes damage a crash can cause from damage it cannot. A record that runs past the end of the file is a torn tail: decoding stops there, the file is truncated, and the history before it is accepted. A record whose bytes are all present but whose checksum or JSON fails is corruption: `open` returns `EventStoreError::Corrupt` and the exchange refuses to start.

### Durable-before-visible

`ExchangeRuntime::record_and_process_input_event` processes the input, numbers the input and its outputs as one batch, appends that batch to the store, and only then extends its in-memory log and returns the result. A store failure is fatal: the waiting client receives an "exchange halted" error, `run()` exits its loop, and every later request fails fast with "exchange worker is unavailable". The core has already applied the command in memory and there is no rollback, so continuing would let memory and durable history disagree.

### Startup

`recover_runtime(rx, path)` runs on the main thread before the listener binds. It opens the store, replays the recovered history through `replay_event_log`, and returns an `ExchangeRuntime` that continues both sequence counters and keeps writing to the same store. A `StartupError` — corrupt file, wrong magic, non-contiguous sequence, or outputs that do not match what replay regenerates — prints the reason and exits with status 1. The path comes from `EVENT_LOG_PATH`, default `exchange-events.log`.

### Client order ids

`POST /exchange/orders` accepts an optional `client_order_id` (trimmed, 1–64 characters). When present it becomes the order id, so a retried request collides with `OrderManager`'s existing duplicate check and returns 409 instead of opening a second order. When absent the server mints a uuid.

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
    *   `first_matchable_mut(&mut self, exclude_user: &str) -> Option<&mut Order>`
        *   Walks from the head and returns the oldest resting order **not** owned by `exclude_user`. This is what makes self-trade prevention a skip rather than a stop: a self-order at the front of the queue can no longer hide a valid counterparty behind it.

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
    *   `l2_snapshot(&self, depth: usize) -> (Vec<(Price, u32)>, Vec<(Price, u32)>)`
        *   Aggregated resting quantity per price level, best price first, capped at `depth` levels per side.
    *   `match_order(&mut self, order: &mut Order) -> Vec<Execution>`
        *   Matches an incoming order against resting opposite-side orders. Snapshots the crossing price levels first — `range(..=price)` on the asks, `range(..=Reverse(price))` on the bids, both yielding best price first — then walks them in order. The snapshot matters: self-trade prevention can leave a level standing, so re-reading "the best level" each pass would spin forever on a level holding only the aggressor's own orders.
    *   `match_at_level(&mut self, order, price, executions)`
        *   Consumes as much of the aggressor as one price level allows, using `first_matchable_mut` so the aggressor's own resting orders are skipped instead of blocking the match. Emits two `Execution` records per match, one for each side.
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
    *   `l2_snapshot(&self, symbol: &str, depth: usize) -> Option<OrderBookView>`
        *   L2 depth for one symbol as a serializable view, or `None` when no book has been opened for that symbol yet. Backs `GET /exchange/orderbook/{symbol}`.
    *   `cancel_order(&mut self, order_id: &str, cancel_seq: u64) -> Result<Option<Order>, String>`
        *   Enforces sequence order, routes the cancel request to the correct order book, updates tracking maps, and updates `last_seq`.
    *   `is_resting(&self, order_id: &str) -> bool`
        *   Returns true if the order ID exists in the resting order tracking index.
    *   `get_order_leaves(&self, order_id: &str) -> Option<u32>`
        *   Retrieves the remaining unfilled quantity (`leaves_qty`) of a resting order.

---

## 🛡️ 5. Risk Manager (`src/types/risk_manager.rs`)

Validates if trading activity stays within allowed constraints to prevent over-exposure.

The cap counts **shares at submission**, not notional and not fills: the check runs before matching, so it cannot know fills that have not happened yet. Counting submissions also prevents a user submitting unlimited orders and exceeding the cap as they fill.

### `RiskManager` (Struct)
*   **Fields:**
    *   `limits` (`HashMap<(String, String), u64>`): Maps `(user_id, symbol)` to the daily cap. Absent means `DEFAULT_MAX_DAILY_QUANTITY`.
    *   `volumes` (`HashMap<(String, String), u64>`): Quantity counted against the current day.
    *   `current_day` (`Option<i64>`): Which day `volumes` belongs to, as a day number derived from order timestamps.
*   **Constant:**
    *   `DEFAULT_MAX_DAILY_QUANTITY` (`u64` = 1,000,000): the design document's own figure, applied when no explicit limit exists. Compiled in rather than configured, because a limit read from the environment would make replay depend on the environment.
*   **Methods:**
    *   `set_limit(&mut self, user_id: String, symbol: String, limit: u64)`
        *   Configures a cap. Reached only through the `RiskLimitSetRequested` event, so limits live in the log and replay exactly.
    *   `limit_for(&self, user_id, symbol) -> u64` / `used_today(&self, user_id, symbol) -> u64`
    *   `check(&mut self, order: &Order) -> Result<(), RiskError>`
        *   Rolls the trading day if this order starts one, then validates against the cap with checked arithmetic. Takes `&mut self` because the day roll is state.
    *   `record(&mut self, order: &Order)`
        *   Counts an accepted order against the day, after its collateral is reserved.
    *   `release(&mut self, order: &Order, quantity: u32)`
        *   Returns a cancelled order's unfilled allowance. Filled quantity is never returned, so the counter means "traded today, plus currently at risk of trading".

### The trading day, and why it is not the clock

`roll_day` clears the counters when an order's own timestamp crosses into a new day:

```rust
fn day_of(timestamp: f64) -> i64 { (timestamp / SECONDS_PER_DAY).floor() as i64 }
```

Resetting from `SystemTime::now()` would break deterministic replay outright — the same log would rebuild different state tomorrow, orders that were accepted would start being rejected, and startup would fail with an `OutputMismatch`. `Order.timestamp` is already recorded in `NewOrderRequested`, so deriving the day from it replays exactly. The roll only moves forward, so gateway clock jitter cannot hand an allowance back twice.

"Day" is a UTC 86,400-second bucket: no market hours, weekends, or holidays.

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

## 📦 6b. Positions (`src/types/positions.rs`)

Share holdings per `(user_id, symbol)`, with the same three numbers the wallet keeps for cash: what is held, what is reserved behind resting sell orders, and what remains available to sell.

This is what makes a sell order backed. Before it existed, `Wallet::check_and_lock` returned `Ok(())` for any sell and `apply_execution` credited the seller cash regardless, so a user could sell shares they did not own and be paid for them — cash was created from nothing on every such trade.

### `Positions` (Struct)
*   **Fields:**
    *   `holdings` (`HashMap<(String, String), u64>`): `(user_id, symbol)` to shares held.
    *   `locked` (`HashMap<(String, String), u64>`): shares reserved behind resting sell orders.
*   **Methods:**
    *   `credit(&mut self, user_id, symbol, quantity) -> Result<(), PositionError>`
        *   Adds shares: an external deposit, or the buyer's side of a fill. Reports overflow rather than wrapping, unlike `Wallet::deposit`.
    *   `check_and_lock(&mut self, user_id, symbol, quantity) -> Result<(), PositionError>`
        *   Reserves shares behind a sell order, the mirror of locking cash behind a buy.
    *   `commit_sell_fill(&mut self, user_id, symbol, quantity) -> Result<(), PositionError>`
        *   Delivers shares on a fill; holding and reservation drop together so the same shares cannot be delivered twice.
    *   `unlock(&mut self, user_id, symbol, quantity) -> Result<(), PositionError>`
        *   Releases a cancelled sell order's remaining reservation. The shares were never spent, so only the lock moves.
    *   `holding` / `locked` / `available` (`-> u64`)
    *   `holdings_for(&self, user_id) -> Vec<(String, u64, u64)>`
        *   Every `(symbol, holding, locked)` the user has, sorted by symbol. Backs `GET /exchange/positions`.

### Settlement

`OrderManager::apply_execution` moves four legs per match — the buyer's cash out, the seller's cash in, the seller's shares out, the buyer's shares in. Cash paid equals cash received and shares delivered equals shares received, so neither total changes. Shares move before cash so that a failure, were one ever possible, happens before any money has.

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
    *   `order_view(&self, order_id: &str, user_id: &str) -> Option<OrderView>`
        *   The client-facing view of one order: quantity, filled, remaining, status, price, creation time. Returns `None` when the order does not exist **or belongs to someone else** — a non-owner gets the same answer as a missing order, so order ids cannot be probed for existence.
    *   `balance_view(&self, user_id: &str) -> BalanceView`
        *   Balance, locked, and available cash for one user.
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

As of 2026-09-16, `cargo fmt -- --check` passes and `cargo test` passes 70 tests with no failures. Position coverage includes rejection of unbacked and oversized sells, reservation and release behaviour, resale of shares received in a fill, and `a_fill_creates_no_cash_and_no_shares`, which asserts that total cash and total shares are unchanged across a partial fill. Event-store coverage includes the CRC-32 check vector, write/read symmetry across reopen, torn-tail truncation, refusal of a flipped byte and of a foreign file, a real write failure reported rather than swallowed, a three-run restart that verifies balances, locks, order state, book, and both sequence counters, and refusal of a well-formed record whose recorded outcome does not replay. The restart flow was additionally exercised live across two hard kills, a torn log, and a corrupted log; see `docs/tasks/02-durable-event-log.md`. These cover the exchange-core lifecycle, matching, exact minor-unit prices, wallet settlement, cancellation, rejected-operation sequence behavior, and sequential consumption of the runtime event log.

Replay coverage includes deterministic output generation, all four replay error categories, reconstruction of matching state with sequence continuation, and live processing after runtime recovery. The recovery test rebuilds an eight-event deposit/partial-fill history and verifies a live cancellation at event sequences 9 and 10 with matching sequence 3.

Read-path coverage includes self-trade prevention filling the next user rather than stopping, the book never being left crossed against a matchable counterparty, a level of only the aggressor's own orders not blocking a worse level, queries leaving the event log untouched, ownership isolation on order reads, exact JSON shapes for all three views, and axum route construction. The HTTP surface was additionally exercised against a running server; see `docs/tasks/01-observable-exchange.md`.
