# Project Direction - Next Milestone Not Yet Selected

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. `docs/tasks/` for the per-task write-ups: what each task changed, how, and why.
5. The current Rust code before making architecture decisions.

The repository is a learning stock exchange with an exchange-grade architecture target. Prefer small, tested changes that move toward deterministic, replayable, single-owner processing.

## Current Status

The project has a working HTTP-to-exchange boundary, a durable append-only event log with startup recovery, a separated exchange-core pipeline, exact integer price handling, deterministic replay, collateral on both sides of a trade (cash for buys, shares for sells), client-supplied order ids for retry safety, and a read surface that lets a client observe balances, positions, order state, and L2 market data.

```text
Axum HTTP handler
  -> bounded Tokio mpsc command queue
  -> dedicated exchange worker thread
  -> ExchangeRuntime
       -> ExchangeCore
            -> OrderManager
                 -> RiskManager (daily cap; day derived from the order's own timestamp)
                 -> Wallet      (cash: locks a buy's notional)
                 -> Positions   (shares: locks a sell's quantity)
            -> Sequencer
            -> MatchingEngine
                 -> OrderBook
       -> number the input and every output it produced as one batch
       -> EventStore::append   (one framed record, write_all + sync_all; fatal on failure)
       -> extend the in-memory event log
       -> reply through temporary oneshot channel

Axum HTTP handler (reads)
  -> same bounded Tokio mpsc command queue
  -> same exchange worker thread
  -> ExchangeRuntime
       -> ExchangeCore read method (no event appended, nothing written)
       -> reply through temporary oneshot channel

application startup (main thread, before the listener binds)
  -> EventStore::open      (magic header, length + CRC-32 framing, torn-tail truncation)
  -> replay_event_log      (contiguous sequence, deterministic outputs)
  -> ExchangeRuntime::from_store
  -> exit 1 on any history that cannot be trusted; never a silent empty start
```

`ExchangeCommand` is live gateway plumbing and may contain `respond_to`. `ExchangeEvent` contains replayable business data and must remain free of HTTP response channels.

`ExchangeRuntime` owns the command receiver, the `EventStore`, and the ordered in-memory `Vec<EventEnvelope>` that mirrors it. `ExchangeCore` owns the deterministic trading components and coordinates their calls; it never touches the file. All core operations still run on the one existing exchange-worker thread.

`replay_event_log` rebuilds a fresh core from recorded inputs and checks regenerated outputs against history. `recover_runtime` opens the event log file, replays it, and returns a runtime that continues both sequence counters and keeps writing to the same file. `main` calls it before binding the listener and exits with a clear message on any history that cannot be trusted. The path comes from `EVENT_LOG_PATH`, default `exchange-events.log`.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-09-16:

```text
cargo fmt -- --check
cargo test
70 passed; 0 failed
```

The compiler reports four dead-code warnings, all pre-existing: accessors superseded by the view methods, `set_limit` awaiting the risk milestone, and constructors only tests call. The replay machinery is now live from `main`. The restart flow was additionally verified against a running server across two hard kills, a torn log, and a corrupted log.

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

### 7. Observable exchange and the book-crossing fix

Full write-up: `docs/tasks/01-observable-exchange.md`.

This milestone was inserted ahead of durable storage. An audit found that every component built since milestone 3 was unreachable from the running binary — replay, recovery, best bid/ask, order state, balance lookup, and risk limits all existed only to satisfy tests. The deployed exchange was three POST endpoints whose order response was a bare uuid string, so a client could place an order and never learn whether it filled. Durable storage could not have been honestly verified from a system with nothing readable in it.

Three read endpoints now exist: `GET /exchange/balance`, `GET /exchange/orders/{order_id}` (owner only; non-owners get 404 so order ids cannot be probed), and the public `GET /exchange/orderbook/{symbol}?depth=N`. `POST /exchange/orders` returns a JSON `OrderView` with `filled_quantity`, `remaining_quantity`, and `status` instead of a uuid string.

Reads reach the single-owner worker through new `ExchangeCommand` query variants and are answered straight from `ExchangeCore`. They deliberately have no `ExchangeInputEvent` counterpart and never touch the event log: they mutate nothing, so recording them would lengthen every future replay without changing an outcome. A test asserts the log length is unchanged across all three queries.

Self-trade prevention was broken and is fixed. It used `break`, which stopped matching entirely instead of skipping the aggressor's own resting order — a self-order at the head of a price level hid every valid counterparty behind it, and the aggressor rested into a crossed book (bid 100 against ask 100, permanently). `PriceLevel::first_matchable_mut` now walks past the aggressor's own orders, and `match_order` snapshots the crossing price levels up front so a level that cannot be consumed is skipped rather than retried forever. Skip-based prevention still permits a user to cross against themselves; removing that requires engine-generated cancellations and is deferred.

Also fixed: cancelling an unknown order returned 400, because the handler compared against the literal `"Order not found"` while the runtime formats errors as `OrderNotFound("...")`. It returns 404 now.

Verified with 38 passing tests plus a live HTTP run against a disposable database: the Alice/Bob self-trade case fills correctly and leaves the book uncrossed, balances move exactly through a fill, locks release on cancel, and every error path returns the intended status code.

### 8. Durable event log and startup recovery

Full write-up: `docs/tasks/02-durable-event-log.md`.

Exchange history now survives a process restart. Every requirement listed when this milestone was selected is met, and the acceptance example — deposit, partial fill, stop, restart from the same file, cancel the resting quantity, verify balances, locks, order state, book, and both sequence counters — was run both as a unit test and live against a hard-killed server.

The event serialization checkpoint (Serde on every event type, one `Vec<EventEnvelope>` per command as the record boundary, JSON as the payload) was the first step and stands unchanged. On top of it, `src/exchange/event_store.rs` adds the file: an 8-byte `EXCHLOG1` magic header once, then one framed record per processed command — `[len: u32 LE][crc32: u32 LE][JSON payload]`. The framing came out of research into how production write-ahead logs are built and answers this milestone's hardest requirement directly: bare JSON lines cannot tell a crash's half-written tail apart from real corruption, while length plus checksum make the damage a crash can cause *identifiable*. A single file rather than segments was chosen because directory `fsync` is a no-op on Windows, where this project is developed.

`EventStore::open` validates the magic, decodes every record, and truncates a torn tail so the next append starts from clean history. It treats the two failure shapes differently on purpose: a record that runs past end-of-file is a torn write and is dropped whole; a record whose bytes are all present but whose checksum or JSON fails is corruption and refuses startup. Recovery repairs only the one kind of damage a crash can actually cause. `append` writes a batch with one `write_all` and returns only after `sync_all`.

`ExchangeRuntime` is now durable-before-visible: it processes the input, numbers the input and all its outputs as one batch, appends the batch, and only then extends its in-memory log and replies. A store failure is fatal, as this milestone specified: the waiting client gets an "exchange halted" error, the worker loop exits, and every later request fails fast rather than receiving a false success. `recover_runtime` runs on the main thread before the listener binds, so untrusted history produces a process that refuses to start rather than a dead worker behind a live server.

The retry hazard this milestone's notes flagged — an order durably accepted while its HTTP reply is lost — was folded in rather than deferred, because durability is what makes it real. `POST /exchange/orders` accepts an optional `client_order_id` (the FIX `ClOrdID` idea); a duplicate collides with `OrderManager`'s existing duplicate check and returns 409, and exactly one order exists. The dedup logic already existed; only the server-minted uuid had been hiding it.

Deferred with reasons recorded in the write-up: snapshots (LMAX replays a full trading day in under a minute; this exchange is nowhere near that), group commit (one worker, one command at a time — nothing to batch), streaming the file at startup, and rollback inside `ExchangeCore` on a failed write.

Verified with 46 passing tests and a live session: eight events recovered after a hard kill with every value identical, fourteen after a second kill, a torn log recovered with the last command dropped whole, and a corrupted log refused with `checksum mismatch in the record at byte 8` and exit status 1.

### 9. Sell-side positions

Full write-up: `docs/tasks/03-sell-side-positions.md`.

The exchange used to create money out of nothing. `Wallet::check_and_lock` returned `Ok(())` immediately for a sell, there was no share ledger for it to check against, and `apply_execution` credited the seller cash unconditionally — so anyone could sell any quantity of any symbol without owning a single share and be paid for it. Total cash rose after every such trade.

`src/types/positions.rs` adds `Positions`: holdings and reservations per `(user_id, symbol)`, the same three-number shape the wallet uses for cash. A sell now reserves shares at placement exactly as a buy reserves cash, a fill delivers them, and a cancel releases the unfilled remainder. Settlement in `apply_execution` became four legs — buyer's cash out, seller's cash in, seller's shares out, buyer's shares in — so a trade moves value between two parties instead of manufacturing it. `a_fill_creates_no_cash_and_no_shares` asserts both totals are unchanged across a partial fill; on the old code cash was 60 higher afterwards on exactly that sequence.

Shares enter through a new `SharesDepositRequested` input event and `POST /exchange/shares/deposit`, mirroring the cash deposit, and `GET /exchange/positions` reads them back. `Wallet::check_and_lock` and `unlock_funds` lost their now-dead `Side` parameter — that `if sell { return Ok(()) }` branch *was* the bug, hiding inside a function whose name promised a check — and the already-unreachable `commit_fill` was deleted.

Twenty existing tests failed the moment the check landed, every one because it sold shares nobody owned. The suite had been quietly asserting that unbacked selling works; a green run had never proved otherwise because nothing had asked.

This milestone is an addition to the specification, taken knowingly: the design doc never mentions positions, inventory or holdings, and its wallet requirement is only ever about cash. The reasoning is that over-selling is the same requirement as over-spending pointed at the other side of the trade, and that a daily volume cap on a system that mints money would be a speed limit on a car with no brakes.

Old logs containing an accepted unbacked sell now refuse to start, which is correct and was verified with a hand-crafted legacy log: every checksum in it passes, and only deterministic replay catches the disagreement (`OutputMismatch`, expected `OrderRejected`, recorded `OrderAccepted`). This is the fresh-log rule meeting its first real case. The file magic was deliberately not bumped, because a log with no sells still replays perfectly.

Verified with 57 passing tests and a live session: a naked sell rejected with nothing minted, a backed sell accepted and the same shares refused a second time, cash and shares both conserved across a real fill (10,000 → 10,000 and 10 → 10), and positions plus reservations intact after a hard kill and restart.

### 10. Risk limits, executions, and ledger cleanup

Full write-up: `docs/tasks/04-risk-limits-and-executions.md`.

This milestone closes the last unmet functional requirements in the target design, plus the small correctness gaps that had accumulated in the limitations list.

**Risk limits are enforced.** `RiskManager` previously rejected nothing: `set_limit` was never called on the live path, so `check` always returned `Ok` — the same built-but-unreachable pattern caught in milestone 7, on a requirement the design states twice and frames as regulatory.

The architecturally interesting part was the day boundary. Clearing daily counters from `SystemTime::now()` would have destroyed the event log built in milestone 8: the same history would rebuild different state tomorrow, orders that were accepted would start being rejected, and the exchange would refuse to start on its own log. The day is therefore derived from `Order.timestamp`, which is already stamped by the gateway and already recorded in `NewOrderRequested`. Nothing in the risk path reads the clock, and the roll only ever moves forward so that gateway jitter cannot refund an allowance.

The same trap applies to the limits themselves, so limits are events — `RiskLimitSetRequested` / `RiskLimitSet`, with `POST /exchange/risk/limits` — rather than configuration, which would have made replay depend on the environment. The fallback when nobody has set one is a compiled-in `DEFAULT_MAX_DAILY_QUANTITY` of 1,000,000, the design's own figure, so the documented cap applies out of the box.

Two ambiguities in the source were resolved and recorded: the cap counts **shares at submission** (line 30 is the requirements interview and outranks the later "$1M a day" aside; a pre-trade check cannot count fills that have not happened), and **cancelling returns the unfilled allowance**, so the counter means "traded today plus currently at risk of trading" rather than burning the day on orders that never traded.

**`GET /exchange/executions`** is the last read the design specifies, with optional `symbol`, `order_id`, `start_time` and `end_time` filters. One match produces two rows, one per party, differing in `order_id` and `side` — which side you were on is a property of the viewer, not the trade. The index is built by `OrderManager` during settlement rather than projected from the event log: scanning history per request is O(history), and a real projection needs a subscriber component, which belongs to the market-data milestone rather than being smuggled in under an endpoint. A pleasant consequence is that execution history rebuilds itself on restart for free, because settlement runs again during replay.

**Three deferred gaps closed.** `Wallet::deposit` was a bare `+=` that would panic in debug and wrap a balance to near zero in release; it now uses `checked_add` and returns `Result`, matching `Positions::credit`, which added `FundsDepositRejected` to the output events. `apply_executions` silently ignored an odd trailing execution, meaning a fill could quietly never settle; that is now an error. And 42 `unused_must_use` warnings introduced by the new fallible signatures were resolved rather than left to drown a real one.

Verified with 70 passing tests and a live session: the default cap applies unconfigured, a custom cap of 10 admits 6 then 4 and refuses 5 in between, cancelling hands the allowance back, other symbols are untouched, both parties see their own side of a fill, every filter narrows correctly, and after a hard kill the event-set limit (10, 6 used) and both parties' fills all came back.

## 11. Failure-Safe Atomic Exchange Commands

Full write-up: `docs/tasks/05-failure-safe-atomic-commands.md`.

This milestone fixes the command boundary that previously allowed a late settlement or cancellation failure to leave partially changed authoritative state. New commands now use a prepare/commit protocol: the complete transition is calculated against cloned affected books and read-only ledger validation, all business rules are checked, the input and output batch is appended and synchronized, and only then is the prepared plan committed. The commit phase is infallible for a plan that passed preparation.

Order, cancellation, settlement, wallet, positions, risk usage, execution indexes, order-book state, and both sequence domains are covered by the same transition. A failed append leaves the live core, event log, callbacks, and sequence counters unchanged. Execution callbacks are published only after the durable commit. Internal faults are separated from ordinary client rejections; they halt the worker and are surfaced as unavailable responses. The HTTP health endpoint also returns 503 once the worker has stopped.

The regression suite now covers seller-credit overflow, buyer-position overflow, a later-fill failure, cancellation failure, append failure, callback ordering, and deterministic replay. `cargo fmt -- --check` and `cargo test` pass 76 tests. Full clippy remains a repository-wide cleanup task because it reports older public compatibility APIs and existing style lints in addition to the new code.

This milestone deliberately does not start market data, reporting, hot-warm replication, mmap, ring buffers, snapshots, or performance work. Those components must consume committed events after this boundary rather than observe tentative state.

The prior implementation description below is retained as historical context; the prepare/commit path is now the live path.

The current order and cancellation paths are not atomic. A command can change some authoritative state and then fail during a later operation. For a new order, the current path can reserve collateral and risk, consume a matching sequence, register the order, mutate the book, and then settle executions. Settlement changes the seller's shares, buyer's shares, buyer's cash, seller's cash, execution indexes, and order states one operation at a time. If a later balance, position, or invariant check fails, the earlier changes remain.

This is a real correctness failure, not only a theoretical edge case. For example, a seller whose cash balance is already at the maximum can sell successfully through the first settlement legs, then fail when the exchange credits the seller. The order is reported as rejected even though shares, cash, order state, book state, reservations, and sequencing may already have changed. A multi-fill order can apply earlier fills before a later fill fails. Cancellation has the same shape because the order is removed from the matching engine before collateral release and lifecycle completion finish.

The runtime currently converts every core error into an ordinary `OrderRejected` or `CancelRejected` output. That incorrectly treats internal arithmetic or invariant failures as client mistakes. It also means replay can reproduce the same partial mutation and the same rejection. Deterministic replay proves that the same result is repeated; it does not prove that the result is correct.

The milestone must establish this command boundary:

```text
prepare the complete command without mutating authoritative state
    -> calculate every fill and cumulative ledger/risk/order/book change
    -> validate the complete transition
    -> durably append the input and output event batch
    -> apply the prepared transition through an infallible commit
    -> publish committed events and reply to the client
```

Preparation must cover the entire command, including all fills, cash, shares, risk usage, collateral, order lifecycle, matching-book changes, execution records, and both journal and matching sequence effects. The implementation should use an explicit prepared plan or state delta. Cloning the entire exchange for every command is not the target design because it would scale with all state and would undermine the critical path.

The error boundary is part of the milestone:

- An expected business rejection leaves all trading state and the matching sequence unchanged. Its input and rejection result may be persisted.
- An internal arithmetic, settlement, matching, or invariant fault is never persisted as an ordinary rejection. It stops further command processing, publishes nothing, and exposes the exchange as unavailable.
- An event-store append or synchronization failure does not acknowledge a commit. The worker stops, and a restart recovers from the last durable history.
- Notifications and future subscribers run only after the durable commit boundary, never during tentative settlement.

The first implementation checkpoint is a regression test that forces a late settlement failure and proves that balances, positions, orders, reservations, the book, and the matching sequence remain unchanged. The milestone is complete only when tests also cover buyer-position overflow, a failure in a later fill, cancellation failure after book removal, storage failure before commit, no notification before durability, typed internal-fault handling, and unavailable/503 behavior after worker failure. `cargo fmt -- --check` and `cargo test` must pass before this journal is marked complete.

Market data remains part of the long-term system-design target, but it is removed from the current milestone. A market-data publisher needs a trustworthy committed event stream before it can safely build live L2 data and candlesticks. Reporting needs the same committed truth for compliance records. Hot-warm needs the same deterministic, failure-safe state transitions before a second engine can safely replay them. These are later consumers of committed events, not substitutes for fixing the command boundary first.

## Known Prototype Limitations

- the risk-limit endpoint sets the caller's own cap, so a trader can raise their own limit; a real exchange would make this a compliance action
- the daily cap counts shares, not notional; "day" is a UTC 86,400-second bucket with no market hours, weekends or holidays
- risk limits are per `(user, symbol)`; there is no cross-symbol or portfolio-level risk
- execution queries are served from an index built during settlement, not projected from the event store
- short selling is not supported at all: a sell must be fully backed by shares held, with no borrow model
- settlement is instant at match; there is no T+1/T+2 settlement cycle or pending-position concept
- share deposits let anyone credit themselves any quantity, exactly as cash deposits do; both are placeholders for real custody
- there is no product/instrument registry, so a share or risk-limit request accepts any non-empty symbol string
- self-trade prevention skips the aggressor's own orders but cannot stop a user crossing against themselves; that needs engine-generated cancellations
- `Order.leaves_qty` and `ManagedOrder.remaining_quantity` are separate sources of truth for the same number
- one global minor-unit price scale is assumed; per-product currency and tick-size metadata are not modeled
- wallet balance credits are checked for overflow before commit
- the event log is one file that grows without bound; there are no snapshots, so replay is always from the beginning and startup reads the whole file into memory
- one `fsync` per command; group commit is the marked upgrade path
- a store write failure halts the worker before the prepared change is committed
- a `client_order_id` is unique forever, never reusable after its order is terminal as FIX permits
- replay requires the full history from an empty core; snapshots and replay from a partial history are not supported
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- there is no market-data or reporting consumer
- internal matching and settlement failures are rejected before commit and halt the worker if they cannot be represented as a business rejection

## What Not To Work On Yet

Until the next milestone is selected and discussed, do not start on snapshots, group commit, Crossbeam, ring buffers, mmap, CPU pinning, component threads, per-symbol workers, market data, reporting, FIX/SBE, UDP, replication, or hot/warm engines. The single append-only file is the event store for now, and it is what the target design's downstream consumers will read from when their turn comes.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.

Every completed task produces two documents, not just code:

1. A new numbered write-up in `docs/tasks/` covering what the task was, how it was done, and the reasoning behind each decision — including the options that were rejected and why. Write it so it can be read cold, months later, without the conversation that produced it.
2. An update to this journal recording the milestone.

Do not claim a milestone here before `cargo fmt -- --check` and `cargo test` both pass. Prefer checking behavior against a running server over trusting unit tests alone; a passing test on unreachable code is what this project has already been caught doing once.
