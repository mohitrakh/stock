# Project Direction - Same-host Warm Replica v1 Complete

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. `docs/tasks/` for the per-task write-ups: what each task changed, how, and why.
5. The current Rust code before making architecture decisions.

The repository is a learning stock exchange with an exchange-grade architecture target. Prefer small, tested changes that move toward deterministic, replayable, single-owner processing.

## Current Status

The project has a working HTTP-to-exchange boundary, atomic prepare/commit processing, a durable append-only event log, journal-bound core snapshots with suffix replay, and a bounded mmap stream with independent readers and durable catch-up. MDP v1 reconstructs public L2 books independently. Reporter v1 is the second independent subscriber: it consumes the same complete committed batches and atomically projects durable order lifecycle and one-row-per-trade history into PostgreSQL. Its database/checkpoint transaction and restart behavior are verified with an isolated PostgreSQL acceptance test. Same-host Warm Replica v1 independently follows committed batches into a read-only deterministic core and supports a manual, writer-fenced hand-off. Neither subscriber nor the warm follower is on the trading path.

```text
Axum HTTP handler
  -> bounded Tokio mpsc command queue
  -> dedicated exchange worker thread
  -> ExchangeRuntime
       -> prepare the complete transition through ExchangeCore (no live mutation)
            -> OrderManager
                 -> RiskManager (daily cap; day derived from the order's own timestamp)
                 -> Wallet      (cash: locks a buy's notional)
                 -> Positions   (shares: locks a sell's quantity)
            -> Sequencer
            -> MatchingEngine
                 -> OrderBook
       -> number the input and every output it produced as one batch
       -> EventStore::append_record (one framed record, write_all + sync_all)
       -> commit the prepared core transition
       -> extend the in-memory event log
       -> publish complete batch and committed watermark through mmap
       -> notify local execution callbacks
       -> periodically replace the journal-bound core snapshot
       -> reply through temporary oneshot channel

Axum HTTP handler (reads)
  -> same bounded Tokio mpsc command queue
  -> same exchange worker thread
  -> ExchangeRuntime
       -> ExchangeCore read method (no event appended, nothing written)
       -> reply through temporary oneshot channel

application startup (main thread, before the listener binds)
  -> validate journal-bound core snapshot when present
  -> EventStore::open_suffix + deterministic suffix replay when valid
       otherwise EventStore::open + full deterministic replay
  -> initialize mmap watermark from the validated recovered journal
  -> write a fresh snapshot after normal recovery
  -> exit 1 on any history that cannot be trusted; never a silent empty start

independent MDP process
  -> StreamReader over the same journal and mmap stream
  -> atomically persist open-order projection plus reader checkpoint
  -> serve GET /marketdata/orderbook/{symbol} on 127.0.0.1:4001 by default
  -> return 503 after any terminal follower error; trading remains independent

same-host warm replica process
  -> StreamReader over the same journal and mmap stream
  -> replay complete batches into a read-only ReplicaCore before binding 127.0.0.1:4003
  -> GET /health and GET /status report follower availability only
  -> POST /promote returns 409 while the primary owns the journal writer lock
  -> after a successful local fence, fully recover and replay the journal before primary startup
```

`ExchangeCommand` is live gateway plumbing and may contain `respond_to`. `ExchangeEvent` contains replayable business data and must remain free of HTTP response channels.

`ExchangeRuntime` owns the command receiver, `EventStore`, `StreamWriter`, a journal-bound snapshot schedule, and the ordered in-memory event history. After snapshot recovery that vector contains only the replayed suffix; the complete history remains in the journal. `ExchangeCore` owns the deterministic trading components; it never touches the files. All core operations still run on one exchange-worker thread. `ReplicaCore` is a separate read-only follower core: it has no writer, queue, callbacks, database, or customer routes. Readers and the warm follower are separate consumers, not additional owners of exchange state.

`replay_event_log` rebuilds a fresh core from recorded inputs and checks regenerated outputs against history. `ReplicaCore` uses the same complete-batch comparison before it advances its applied checkpoint. Production startup calls `recover_runtime_with_stream_and_snapshot` before binding the listener. A valid versioned, checksummed snapshot is tied to the journal device/inode and a complete-batch byte boundary; it restores the core and replays only the later suffix. A missing, corrupt, inconsistent, or journal-mismatched snapshot is preserved and falls back to full replay. `EVENT_LOG_PATH` defaults to `exchange-events.log`; `EVENT_STREAM_PATH` defaults to that path plus `.mmap`; `EVENT_SNAPSHOT_PATH` defaults to that path plus `.snapshot`; and `EVENT_SNAPSHOT_INTERVAL` defaults to 10,000 commands. The journal is authoritative; the mmap file is a disposable delivery cache and the snapshot is only a recovery checkpoint. Warm promotion intentionally takes the writer lock with `EventStore::open_existing` and fully replays the recovered journal rather than promoting mmap-derived state.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-09-27:

```text
cargo fmt -- --check
cargo test --locked --offline
118 unit tests + 4 executable integration tests passed; 0 failed
```

Verification includes separate OS-process readers, forced writer kills after append and during publication, reader checkpoint resume, the probe executable without a database, and the MDP executable catching up from the journal, serving HTTP, restarting from state, following live publication, and failing closed after a follower error. A manual run with isolated PostgreSQL also drove the real exchange and MDP through rest, partial fill, cancellation, MDP restart, and resumed live publication. Reporter qualification injects a failing checkpoint write after lifecycle/trade writes have begun and proves the whole transaction rolls back; it then proves journal-to-mmap catch-up, multi-fill/cancel/reject projection, and a post-commit Reporter restart without duplicate rows. Snapshot tests prove FIFO core restoration, suffix replay and both sequence continuations, corrupt-snapshot fallback without deleting the artifact, snapshot replacement atomicity, journal identity binding, torn suffix repair, and no snapshot advance after a failed append. These are local correctness tests, not throughput measurements or a machine-power-loss test. Existing compatibility APIs and repository-wide clippy warnings remain.

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

At this milestone, the shared `process_input_event` function applied inputs and generated outputs for both live processing and replay. Milestone 11 replaced it with `prepare_input_event` plus explicit commit, preserving the shared deterministic logic while moving mutation after durable append.

`replay_event_log` requires contiguous envelope sequence numbers starting at 1. It applies only input events to a fresh `ExchangeCore`, checks the exact values and order of regenerated outputs, and returns the core only after validating the complete supplied history. Sequence mismatches, missing outputs, unexpected outputs, and output mismatches return `ReplayError`.

`ExchangeRuntime::from_event_log` retains the validated log and resumes its event sequence after the last envelope. The rebuilt core also retains its reconstructed matching sequence. A test recovers an eight-event deposit/partial-fill history, then processes a live cancellation at event sequences 9 and 10 with matching sequence 3.

Tests cover equal outputs from equal input sequences, replayed matching state and sequence continuation, all four replay error categories, and continued live processing after recovery. This completes the in-memory replay milestone; durable storage and restart recovery are still pending.

### 7. Observable exchange and the book-crossing fix

Full write-up: `docs/tasks/01-observable-exchange.md`.

This milestone was inserted ahead of durable storage. An audit found that every component built since milestone 3 was unreachable from the running binary — replay, recovery, best bid/ask, order state, balance lookup, and risk limits all existed only to satisfy tests. The deployed exchange was three POST endpoints whose order response was a bare uuid string, so a client could place an order and never learn whether it filled. Durable storage could not have been honestly verified from a system with nothing readable in it.

At this milestone, three read endpoints existed: `GET /exchange/balance`, `GET /exchange/orders/{order_id}` (owner only; non-owners get 404 so order ids cannot be probed), and the public `GET /exchange/orderbook/{symbol}?depth=N`. MDP v1 later removed the exchange-owned L2 route and replaced it with `/marketdata/orderbook/{symbol}` on the independent subscriber. `POST /exchange/orders` returns a JSON `OrderView` with `filled_quantity`, `remaining_quantity`, and `status` instead of a uuid string.

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

### Historical problem and acceptance criteria (resolved in milestone 11)

The previous order and cancellation paths were not atomic. They could reserve collateral and risk, consume a matching sequence, register an order, mutate the book, and then fail during settlement, leaving earlier changes in place. The current prepare/commit path resolves that failure.

This was a real correctness failure. A seller at the maximum cash balance could fail during credit after earlier settlement legs had changed state. Multi-fill orders and cancellation had similar partial-mutation risks. Regression tests now verify that these failures leave authoritative state unchanged.

The old runtime converted every core error into an ordinary rejection. Replay could reproduce the same partial mutation, so determinism alone did not establish correctness. Internal faults now halt processing instead of being recorded as client mistakes.

Milestone 11 established this command boundary:

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

The acceptance tests covered late settlement failure, buyer-position overflow, a later-fill failure, cancellation failure, storage failure before commit, no notification before durability, internal-fault handling, and worker unavailability. Those checks remain part of the suite.

Market data was deferred while this command boundary was repaired. The following mmap milestone supplies committed delivery; business consumers remain separate work.

## 12. Committed mmap Event Stream

Full write-up: `docs/tasks/06-mmap-committed-event-stream.md`.

The selected task was to connect the existing durable, atomic engine to independent same-host readers before building market data. It is now implemented end to end. Production startup creates a fixed-size 4 MiB cache, and every successful durable/core commit publishes the exact framed batch and its journal byte/sequence watermark. Input and output envelopes remain together, including business rejections. HTTP queries never enter this stream.

Each `StreamReader` has its own checkpoint. Current batches come from mmap; overwritten or oversized batches come from the read-only durable journal, bounded by the published watermark. Catch-up and live reading use the same cursor, so the handoff does not skip or redeliver batches within a reader session. A saved checkpoint is tied to the journal's device/inode and validated against whole-command boundaries. Consumer state and checkpoint must be saved together for transactional processing; the probe's stdout and checkpoint are not one transaction.

The writer owns an exclusive lifetime journal lock. mmap copies use short shared/exclusive file locks, an atomic publication marker, and checksums. Readers release their lock before deserializing or doing consumer work. This is a correctness-first, synchronized implementation, not the target lock-free ring buffer. A paused reader inside its copy can delay the writer; no low-latency claim is made.

A crash after durable append but before publication is recovered by replay and publishing the recovered journal watermark. A failed durable append publishes nothing and commits no live state. A publication failure happens after durable commit and halts the worker; the client's outcome is uncertain until recovery. Interrupted copies are refused rather than consumed as complete data.

Run the diagnostic subscriber independently, without PostgreSQL:

```sh
cargo run -- --event-probe exchange-events.log exchange-events.log.mmap /tmp/stock-reader.json --once
```

It prints one JSON array per complete command. Omit `--once` to follow live data; omit the checkpoint path to start at the beginning. This is trusted internal account/order data, not a public market-data feed.

At completion of the mmap milestone, the first business subscriber had not been selected. The following milestone instead closes a correctness bug found during that review.

## 13. Overnight Risk Accounting

Full write-up: `docs/tasks/07-overnight-risk-accounting.md`.

Daily risk usage now means "quantity traded today plus quantity still open and able to trade today." The risk manager keeps open exposure separately from current-day usage. Accepted orders increase both; fills move quantity out of open exposure without reducing the day's usage; cancellations release only the unfilled open quantity. On a day rollover, old executions expire but overnight resting orders remain counted.

This closes a confirmed bypass. Previously, rollover cleared the entire counter even while old orders remained executable. Cancelling one of those orders could then subtract its remainder from the new day's usage and create additional allowance. A cap of 10 could therefore coexist with 20 resting shares and later report zero usage.

Fill and cancellation risk changes are now validated during preparation and applied during the existing infallible commit. No event format changed: replay reconstructs the corrected state from accepted orders, executions, and cancellations. An old journal containing an order accepted only because of the previous bypass can refuse startup with an output mismatch; preserve that journal and handle the incompatibility explicitly.

Verified on 2026-09-26: an overnight order remains counted on day two; four filled shares plus six resting shares report usage 10; cancelling the six leaves usage 4; an order for seven is rejected and an order for six is accepted. The result survives two durable recoveries. `cargo fmt -- --check` passes and `cargo test --locked --offline` passes 95 unit tests plus 2 executable integration tests.

## 14. Market Data Publisher v1

**Status: completed on 2026-09-26.** Full write-up: `docs/tasks/08-market-data-publisher-v1.md`.

The committed mmap stream was built so independent business components could consume durable exchange history without reading mutable core state. The first such component is now the Market Data Publisher. MDP v1 consumes committed batches, independently reconstructs L2 order books, persists its derived state with its checkpoint, and exposes that data from a market-data-owned HTTP surface.

```text
single-owner exchange worker
  -> authoritative durable journal
  -> bounded committed mmap stream
  -> independent MDP process using StreamReader
       -> private per-order projection state
       -> public per-symbol L2 price levels
       -> market-data read surface
```

The MDP is a subscriber, not another owner of exchange state. It does not call `ExchangeCore`, send an L2 query through `ExchangeCommand`, share the matching engine's in-memory books, or delay order acceptance while it processes data. If the MDP stops, trading continues. When it returns, it catches up from the journal through the existing `StreamReader` path and then resumes live mmap consumption.

### Event-to-projection contract

`StreamReader` returns one complete command batch at a time. MDP v1 interprets that batch as a unit:

- `NewOrderRequested` carries the full candidate order. The projection changes only when the same batch contains `OrderAccepted`; `OrderRejected` changes nothing.
- An accepted order's `ExecutionCreated` outputs reduce the corresponding buy and sell order quantities. Any remaining quantity from the accepted incoming order becomes a resting order at its limit price.
- The matching engine emits two execution records for one match, one for each party. MDP v1 validates the two-sided pair and applies it once. It neither doubles the quantity nor silently accepts a malformed pair.
- `CancelOrderRequested` changes the projection only when followed by `OrderCanceled`; `CancelRejected` changes nothing.
- Deposit, share-deposit, and risk-limit batches do not affect public market data.
- Public output must contain market fields only. User ids, balances, positions, risk limits, and raw internal events must never be exposed by the market-data endpoint.

The existing event schema is sufficient for this projection because the input and all outputs are framed together as one command. No authoritative event type or journal format changed.

### Projection state and recovery

The MDP owns its own order map, remaining quantities, and aggregated bid/ask levels. Price-level totals and the shared public L2 response use checked `u64` quantities. Core aggregation was widened as the correctness oracle and can now represent several valid `u32` orders at one price.

A reader checkpoint is not sufficient by itself: resuming after the checkpoint with an empty projection would silently omit earlier orders. The MDP therefore persists its open orders and `ReaderCheckpoint` in one versioned state file. It saves through a synchronized temporary file and atomic rename after every batch, before replacing the served snapshot. A present but corrupt or mismatched state file refuses startup. Missing state explicitly rebuilds from sequence 1.

The projection is derived and disposable. The journal remains the source of truth. Deleting the projection must be recoverable by replaying committed batches, while deleting or replacing the journal is not a market-data recovery procedure.

### Completed behavior and evidence

The implementation and tests demonstrate the following:

- A separate OS process starts from an empty projection, consumes the journal/mmap stream through `StreamReader`, catches up, and follows new committed batches.
- It reconstructs correct L2 state for resting orders, immediate fills, partial fills, multiple fills in one command, successful cancellations, rejected orders, rejected cancellations, and the existing self-trade-prevention behavior.
- Each two-sided execution pair is applied once, while both affected order quantities are updated correctly.
- A slow reader survives mmap-window overwrite by reading the missing committed records from the journal and returning to live mmap delivery without a gap.
- After process death, projection state and checkpoint resume together without dropping or applying a command twice. Starting without saved state performs a complete rebuild from sequence 1.
- The MDP refuses corrupt batches, sequence gaps, invalid execution pairs, incompatible checkpoints, and projection/checkpoint mismatches instead of publishing a plausible but incorrect book.
- Public L2 output uses overflow-safe aggregate quantities and contains no private account data.
- The public L2 read path is owned by the market-data component and does not enqueue a read against the single-owner exchange worker.
- Core-oracle tests compare the independent projection with `ExchangeCore::l2_snapshot`; runtime and executable tests cover real stream publication, journal catch-up, MDP restart, exchange stream restart, live following, and HTTP output.
- The executable starts without database configuration. Its old exchange-owned L2 route is gone; `GET /marketdata/orderbook/{symbol}?depth=N` and the MDP `/health` route are owned by the subscriber process.
- `cargo fmt -- --check`, `cargo test --locked --offline`, clippy, and the task write-up complete the milestone checks.

### Explicitly outside MDP v1

Do not expand this milestone into candlesticks, historical analytics storage, reporting, FIX/SBE, UDP or multicast distribution, paid depth tiers, hot-warm matching, cross-host replication, lock-free ring buffers, CPU pinning, group commit, snapshots of the authoritative exchange core, or per-symbol exchange workers. Those build on a proven subscriber boundary and need separate correctness and failure models.

Candlesticks were specifically deferred from MDP v1 because they introduce interval, timestamp, exchange-calendar, and retention policy. Reporting was deferred from MDP v1 because database effects require their own idempotency and checkpoint transaction; it is now the selected separate milestone below. Hot-warm failover remains deferred because it additionally requires cross-host replication, leader/fencing rules, promotion behavior, and explicit RPO/RTO tests.

## Selected Next Milestone: Reporter v1

**Status: completed on 2026-09-26.** Full write-up: `docs/tasks/09-reporter-v1.md`.

Reporter v1 will be the second independent business subscriber to the committed event pipeline. MDP answers, "What does the public order book look like now?" Reporter answers, "What happened to each order and trade over time?" A fully filled or canceled order disappears from the live book, but its history must remain available for order history, trade confirmation, reconciliation, investigation, and later compliance/reporting work.

```text
authoritative journal + bounded mmap delivery cache
  -> StreamReader
       -> MDP v1
            -> current open-order projection
            -> current public L2 book
       -> Reporter v1
            -> durable order lifecycle rows
            -> durable trade rows
            -> reporter checkpoint
```

The reporter is not part of matching and must never be placed on the trading critical path. It does not query or mutate `ExchangeCore`, and matching does not wait for PostgreSQL. If the reporter or its database is stopped, trading and MDP continue. When the reporter returns, it resumes from its saved checkpoint or catches up from the authoritative journal through `StreamReader`, then follows current mmap publication.

### Reporter v1 output

The first reporting projection is intentionally narrow:

- One durable order-lifecycle record per submitted order, including ownership, symbol, side, limit price, original quantity, filled quantity, remaining quantity, current status, and acceptance, rejection, or cancellation outcome.
- One durable trade record per actual match, including symbol, price, quantity, both order ids, both execution ids, and the committed event identity used to make the row unique.
- One durable reporter checkpoint tied to the journal identity and the next complete command batch to consume.

The matching engine currently emits two adjacent `ExecutionCreated` events for one match, one for each party. The reporter must validate that pair and store one trade, not double the traded quantity. A stable committed envelope sequence can identify the trade while both execution ids remain available for audit. Exact table names and indexes belong in the task design, but the order, trade, and checkpoint responsibilities are required.

Reporter v1 does not need a public reporting API. The database projection and a small health/readiness surface are sufficient for this milestone; integration tests can inspect PostgreSQL directly. Customer statements, tax documents, downloads, settlement, dashboards, candles, and historical market-data APIs are later work.

### Shared committed-batch interpretation

MDP currently contains the first strict interpretation of committed command batches: accepted versus rejected orders, successful versus rejected cancellations, and two-sided execution pairs. Reporter needs the same structural understanding. Before adding a second independent copy of those rules, extract a small shared decoder that converts a raw committed batch into a validated committed-command representation.

The shared layer should validate batch structure and execution-pair agreement only. MDP keeps book-specific checks such as resting-order presence and price-level changes. Reporter keeps database and lifecycle-specific checks. This avoids two subscribers quietly disagreeing about what one committed batch means, without changing the durable `ExchangeEvent` schema or journal format.

### Transaction and recovery rule

The important new correctness problem is coordinating an external database effect with the stream checkpoint. For each complete command batch, Reporter v1 must use one PostgreSQL transaction:

```text
begin SQL transaction
  -> validate and apply order/trade projection changes
  -> save the reporter's new ReaderCheckpoint
commit SQL transaction
```

If the process dies before the SQL commit, neither the reporting rows nor the checkpoint advances, so the complete batch is retried. If the commit succeeds, both advance together. Unique keys and lifecycle checks must make a duplicate or inconsistent replay visible rather than silently creating a second trade. This provides atomic application inside the reporter database; it is not a claim of universal exactly-once delivery to arbitrary external systems.

A missing reporter checkpoint means an explicit rebuild from journal sequence 1 into an empty reporter projection. A present but invalid checkpoint, journal identity mismatch, incomplete command boundary, or inconsistent reporting state must refuse startup. Reporter database failures are terminal for that reporter process but must not affect exchange availability.

### Why this follows MDP

The completed mmap stream was built for multiple independent consumers, and MDP proved the first consumer can catch up, persist its state with its checkpoint, restart, and remain outside matching. Reporter v1 reuses that proven boundary while testing the next distinct failure model: a subscriber whose durable projection lives in PostgreSQL and whose projection writes and checkpoint must commit together.

This should come before candles because candle work first needs interval, timestamp, exchange-calendar, late-event, and retention decisions. It should come before hot-warm failover because failover additionally needs cross-host replication, leader election, fencing, promotion behavior, and explicit RPO/RTO targets. Lock-free mmap and critical-path tuning should follow measured latency evidence rather than replace the current correctness-first transport speculatively.

### Reporter v1 completion criteria

Reporter v1 is complete only when all of the following are demonstrated:

- It runs as a separate OS process and consumes complete committed batches through `StreamReader`.
- Accepted, rejected, resting, partially filled, fully filled, multi-fill, successfully canceled, and rejected-cancel orders produce the expected lifecycle records.
- Each valid two-sided execution pair produces exactly one trade record and updates both affected orders correctly.
- Projection changes and the new `ReaderCheckpoint` commit in the same PostgreSQL transaction.
- Injected failures before and after SQL commit prove that restart creates neither missing nor duplicate order/trade effects.
- Missing state rebuilds from sequence 1; corrupt, structurally inconsistent, or journal-mismatched state refuses startup without silently clearing existing data.
- A reporter can catch up from journal records outside the mmap window, return to live mmap delivery, survive reporter restart, and survive exchange stream restart with the same journal.
- Reporter health becomes ready only after initial catch-up and becomes unavailable after a terminal stream or database failure.
- Stopping the reporter or PostgreSQL does not stop order entry, matching, journal commits, mmap publication, or MDP service.
- Integration tests use an isolated PostgreSQL database and compare representative report rows with the exchange core's known order and execution results.
- The existing durable event schema, journal format, single-owner core, and trading critical path remain unchanged.
- `cargo fmt -- --check`, `cargo test --locked --offline`, clippy, an executable exchange-plus-reporter recovery run, `docs/tasks/09-reporter-v1.md`, and the related documentation updates are complete.

Before implementation begins, preserve the completed mmap and MDP working tree as a reviewed repository checkpoint so the new milestone is not mixed with uncommitted prior work.

## 15. Reporter v1 Recovery Qualification

Full write-up: `docs/tasks/10-reporter-recovery-qualification.md`.

Reporter v1's critical database boundary is now exercised by a real Reporter process against a freshly initialized isolated PostgreSQL instance. The acceptance test applies the versioned migration itself, then installs a database trigger that makes the checkpoint write fail after Reporter has started writing lifecycle and trade rows. The reporter exits before becoming ready, and the test proves that orders, trades, and the checkpoint are all absent: PostgreSQL rolled back the complete batch.

After removing the trigger, the same process catches up older batches from the journal, consumes the final batch from mmap, and produces filled, partially filled/canceled, rejected, and rejected-cancel outcomes plus exactly one trade for each valid execution pair. The test kills that ready Reporter and starts a new one on the same journal and database. The saved checkpoint resumes at sequence 17 and the row counts remain four orders and two trades, with no duplicate effects.

This verifies Reporter transaction atomicity for an injected database failure and post-commit process restart. It does not claim universal exactly-once delivery to arbitrary external systems, machine-power-loss testing, throughput, a public reporting API, or automatic database migration in production. The migration remains an explicit operator action outside the test.

## 16. Authoritative Core Snapshots and Suffix Replay

Full write-up: `docs/tasks/11-authoritative-core-snapshots.md`.

The exchange now checkpoints a normalized, versioned, CRC-protected representation of the committed `ExchangeCore` state. It records both sequence domains and the durable journal's device/inode plus its exact complete-batch byte boundary. Books retain price/time FIFO, while linked-node lookup structures are rebuilt and verified on load.

Snapshot publication happens only after the journal append, core commit, mmap publication, and local callbacks. It writes a private temporary file, synchronizes it, atomically renames it, then synchronizes the parent directory. A replacement failure keeps the earlier snapshot. A failed journal append cannot create a snapshot for an uncommitted command.

On a normal start, a valid snapshot restores the core and `EventStore::open_suffix` validates/replays only later records. The runtime retains that suffix in memory, but the complete journal stays authoritative for MDP, Reporter, and future readers. A missing snapshot starts with full replay. A corrupt, malformed, or journal-mismatched snapshot is preserved, reported, and causes full replay; it is not silently overwritten during that run.

`EVENT_SNAPSHOT_PATH` defaults to `EVENT_LOG_PATH + .snapshot`; `EVENT_SNAPSHOT_INTERVAL` defaults to 10,000 processed commands and must be positive. Startup writes a fresh checkpoint after a safe normal recovery. There is no journal compaction, truncation, or reader-retention change.

Verified on 2026-09-27: `cargo fmt -- --check`; `cargo test --locked --offline` passes 118 unit tests and 4 executable integration tests. The new tests cover normalized core state, FIFO and sequence continuation, snapshot-plus-suffix recovery, corrupt artifact preservation and full fallback, replacement failure, journal identity, complete suffix boundaries, torn suffix repair, and failed append safety.

## 17. Candlestick Publisher v1

Full write-up: `docs/tasks/12-candlestick-publisher-v1.md`.

MDP now derives one-minute UTC OHLCV candles from the same complete committed batches that feed
its public L2 book. One adjacent two-sided execution pair is one trade, never two. The bucket uses
the recorded execution timestamp, floored to its UTC minute; this preserves deterministic replay
without introducing a clock read into matching or subscriber processing.

The MDP's version-2 state file holds open orders, all derived candle buckets, and one shared
reader checkpoint. Each batch is applied to candidate L2 and candle projections, then the whole
state is synchronized and atomically replaced before either in-memory view advances. A restart
therefore cannot advance the checkpoint without the corresponding candle update. Missing state
replays from the journal; malformed or version-1 state fails closed and must be rebuilt from the
authoritative journal.

`GET /marketdata/candles?symbol=&start_time=&end_time=` returns ascending one-minute candles for
an inclusive epoch-second range. All three parameters are required, `start_time` must not exceed
`end_time`, and a valid range with no trades returns an empty array. The endpoint shares MDP's
availability behavior: a terminal stream, projection, or persistence error returns 503.

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
- the event log is one file that grows without bound; snapshots reduce exchange-core replay work but do not compact or retain less journal history
- one `fsync` per command; group commit is the marked upgrade path
- a store write failure halts the worker before the prepared change is committed
- a `client_order_id` is unique forever, never reusable after its order is terminal as FIX permits
- snapshots rely on the journal file identity and committed boundary; an invalid snapshot falls back to full replay and is retained for diagnosis
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- MDP state rewrites and synchronizes the complete JSON open-order and candle projection after every command; this is correctness-first and not a high-throughput persistence design
- candle state retains every one-minute bucket without a retention limit, rollups, or external historical store
- there is no public reporting API or trade-tape service
- mmap is a same-host Unix transport using cooperative file locks, JSON, and a bounded window; it is not lock-free, cross-server replication, or an ingress transport
- the journal still grows without bound; a snapshot-recovered exchange retains only its suffix in memory, while reader checkpoint validation still scans history on reader restart
- mmap is not the durable recovery source; never modify, truncate, replace, or unlink mapped files while processes use them
- consumer crashes require checkpoint/state coordination; arbitrary downstream effects are not exactly-once
- internal matching and settlement failures are rejected before commit and halt the worker if they cannot be represented as a business rejection

## What Not To Work On Yet

No next milestone is selected. Do not expand the completed candle work into tax or customer statements, settlement, broad historical-market-data APIs, journal compaction, group commit, Crossbeam, lock-free ring buffers, CPU pinning, new trading-component threads, per-symbol workers, FIX/SBE, UDP, replication, or hot-warm engines. The committed mmap reader remains the input for all independent subscribers; inbound commands remain on the existing bounded Tokio queue.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.

Every completed task produces two documents, not just code:

1. A new numbered write-up in `docs/tasks/` covering what the task was, how it was done, and the reasoning behind each decision — including the options that were rejected and why. Write it so it can be read cold, months later, without the conversation that produced it.
2. An update to this journal recording the milestone.

Do not claim a milestone here before `cargo fmt -- --check` and `cargo test` both pass. Prefer checking behavior against a running server over trusting unit tests alone; a passing test on unreachable code is what this project has already been caught doing once.
