# Project Direction - Milestone 23 (Two Machines) In Progress

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. `docs/tasks/` for the per-task write-ups: what each task changed, how, and why.
5. The current Rust code before making architecture decisions.

The repository is a learning stock exchange with an exchange-grade architecture target. Prefer small, tested changes that move toward deterministic, replayable, single-owner processing.

## Current Status

The project has a working HTTP-to-exchange boundary, atomic prepare/commit processing, a durable append-only event log, journal-bound core snapshots with suffix replay, and a bounded mmap stream with independent readers and durable catch-up. MDP v1 reconstructs public L2 books independently. Reporter v1 is the second independent subscriber: it consumes the same complete committed batches and atomically projects durable order lifecycle and one-row-per-trade history into PostgreSQL. Its database/checkpoint transactions and restart behavior are verified with isolated PostgreSQL acceptance tests. Same-host Warm Replica v1 independently follows committed batches into a read-only deterministic core and supports a manual, writer-fenced hand-off. Neither subscriber nor the warm follower is on the trading path. Since milestone 21 both subscribers keep up: the MDP applies batches in place and saves once a second, and the reporter commits up to 1,000 batches per PostgreSQL transaction and records rejected submissions and refused cancellations apart from order lifecycles. Critical-path performance v1 made the exchange measurable (`--bench`) and about 100× faster on disk: the worker group-commits every queued command behind one journal sync, and matching plans fills against the live book instead of copying it.

```text
Axum HTTP handler
  -> bounded Tokio mpsc command queue
  -> dedicated exchange worker thread
  -> ExchangeRuntime: take every queued command (up to 1,024) as one group
       for each command, in queue order:
       -> prepare the complete transition through ExchangeCore (no live mutation)
            -> OrderManager
                 -> RiskManager (daily cap; day derived from the order's own timestamp)
                 -> Wallet      (cash: locks a buy's notional)
                 -> Positions   (shares: locks a sell's quantity)
            -> Sequencer
            -> MatchingEngine
                 -> OrderBook::plan_order (read-only; the live book is never copied)
       -> number the input and every output it produced as one batch; encode its record
       -> commit the prepared transition in memory (the next command sees it)
       -> hold the reply
     then, once per group:
       -> EventStore::append_record (all records, one write_all + ONE sync_all)
       -> publish each complete batch and the committed watermark through mmap
       -> notify local execution callbacks
       -> release every held reply
       (no snapshots while trading: the warm replica writes them)

Axum HTTP handler (reads)
  -> same bounded Tokio mpsc command queue
  -> same exchange worker thread, inside the same group
  -> ExchangeRuntime
       -> ExchangeCore read method (no event appended, nothing written)
       -> reply held until the group's sync, so a read never shows unsynced state

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
  -> StreamReader in journal-only mode (mmap supplies only the committed watermark)
  -> replay complete batches into a read-only ReplicaCore before binding 127.0.0.1:4003
  -> every EVENT_SNAPSHOT_INTERVAL commands, write the journal-bound core snapshot
     at exactly its applied checkpoint
  -> GET /health and GET /status report follower availability only
  -> POST /promote returns 409 while the primary owns the journal writer lock
  -> after the fence, verify journal identity before any read or repair,
     then fully recover and replay the journal before primary startup
```

`ExchangeCommand` is live gateway plumbing and may contain `respond_to`. `ExchangeEvent` contains replayable business data and must remain free of HTTP response channels.

`ExchangeRuntime` owns the command receiver, `EventStore`, and `StreamWriter`. It writes one journal-bound snapshot at startup and none while trading; the warm replica writes the periodic ones. It keeps no event history in production; the complete history is the journal, and an in-memory copy exists only in test builds. Group commit commits each command in memory before its group's sync, so after a failed sync the live core is ahead of the disk: the worker halts, that core is never used again, and recovery from the journal is the only way back. `ExchangeCore` owns the deterministic trading components; it never touches the files. All core operations still run on one exchange-worker thread. `ReplicaCore` is a separate read-only follower core: it has no writer, queue, callbacks, database, or customer routes. Readers and the warm follower are separate consumers, not additional owners of exchange state.

`replay_event_log` rebuilds a fresh core from recorded inputs and checks regenerated outputs against history. `ReplicaCore` uses the same complete-batch comparison before it advances its applied checkpoint. Production startup calls `recover_runtime_with_stream_and_snapshot` before binding the listener. A valid versioned, checksummed snapshot is tied to the journal device/inode and a complete-batch byte boundary; it restores the core and replays only the later suffix. A missing, corrupt, inconsistent, or journal-mismatched snapshot is preserved and falls back to full replay. `EVENT_LOG_PATH` defaults to `exchange-events.log`; `EVENT_STREAM_PATH` defaults to that path plus `.mmap`; `EVENT_SNAPSHOT_PATH` defaults to that path plus `.snapshot`; and `EVENT_SNAPSHOT_INTERVAL` (the warm replica's snapshot interval) defaults to 10,000 commands. The journal is authoritative; the mmap file is a disposable delivery cache and the snapshot is only a recovery checkpoint. Warm promotion takes the writer lock with `EventStore::open_existing_matching`, which proves the locked file is the journal the warm followed before reading or repairing it, then fully replays the recovered journal rather than promoting mmap-derived state.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-10-05, on Linux (the office Ubuntu machine; the crate uses Unix-only APIs and does not build on Windows):

```text
cargo fmt -- --check
cargo test --locked
164 unit tests + the executable integration tests passed; 0 failed
3 opt-in Reporter acceptance tests ignored by default (need REPORTER_TEST_DATABASE_URL);
  all passed against PostgreSQL 16 when run with it
```

Measured with `--bench` (Docker Desktop VM, release build): about 37,000–39,000 orders/s on disk at
maximum rate over 20,000 orders, 43,000 in memory; p99 about 20–30 ms at 1,000 orders/s. With
snapshots every 10,000 commands, 200,000 orders run at about 21,900 orders/s now that the warm
replica writes them, up from about 8,300 when the trading thread did. See
`docs/tasks/14-critical-path-performance-v1.md` and `docs/tasks/15-snapshots-by-the-warm-replica.md`.

On the office Ubuntu machine, milestone 22's trading days keep the state to one day. Five days of
200,000 orders end with 263 MB of exchange memory, an 85 MB largest snapshot and a 2.4 s restart.
The milestone 21 binary, on the same 1,000,000 orders, ends with 1,234 MB, 422 MB and 11.7 s.
Throughput is unchanged (40,000 to 45,000 orders/s with no warm replica). See
`docs/performance/07-trading-days-bound-the-state.md`.

Warm Replica v1 was additionally verified by a live run of the real primary and warm executables against PostgreSQL: a refused promotion while the primary ran, live following, a `SIGKILL` of the primary, a successful promotion, identical balances, positions, and order state on the promoted primary, a new trade there, and one contiguous journal across the hand-off.

Verification includes separate OS-process readers, forced writer kills after append and during publication, reader checkpoint resume, the probe executable without a database, and the MDP executable catching up from the journal, serving HTTP, restarting from state, following live publication, and failing closed after a follower error. A manual run with isolated PostgreSQL also drove the real exchange and MDP through rest, partial fill, cancellation, MDP restart, and resumed live publication. Reporter qualification injects a failing checkpoint write after lifecycle/trade writes have begun and proves the whole transaction rolls back; it then proves journal-to-mmap catch-up, multi-fill/cancel/reject projection (including a retried order id, a reused rejected id, an intruder's cancellation, and two symbols sharing execution ids), and a post-commit Reporter restart without duplicate rows. A second test fails the group after a full 1,000-batch group and proves only that group rolls back. Snapshot tests prove FIFO core restoration, suffix replay and both sequence continuations, corrupt-snapshot fallback without deleting the artifact, snapshot replacement atomicity, journal identity binding, torn suffix repair, and no snapshot advance after a failed append. These are local correctness tests, not throughput measurements or a machine-power-loss test. Existing compatibility APIs and repository-wide clippy warnings remain.

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

## 18. Same-host Warm Replica v1

Full write-up: `docs/tasks/13-warm-replica-v1.md`.

A second process can now follow the exchange and take over from it. Started with `--warm-replica`, it owns no database connection, customer routes, journal writer, or mmap writer. It replays every complete committed batch into a read-only `ReplicaCore` through `replay_committed_batch`, the same prepare/compare/commit path recovery uses, so it cannot accept an output the primary would not have produced. It keeps an *applied* checkpoint separate from its reader's physical cursor and advances it only after a batch has been compared and committed; a replay failure therefore stops the follower at the last good batch and can never become a promotion boundary. A valid primary snapshot is an optional starting point. The loopback-only management listener, `127.0.0.1:4003` by default, serves `GET /health`, `GET /status`, and `POST /promote`.

Promotion is manual and writer-fenced. `POST /promote` returns `409` while the primary still holds the journal's exclusive writer lock, and the warm keeps following with nothing changed. Once the lock is free, `EventStore::open_existing_matching` takes it, compares the file's device/inode with the followed journal before reading or repairing anything, and recovers the journal. `promote_replica_with_stream_and_snapshot` then discards the warm core and rebuilds from the entire writer-locked journal — mmap-derived state is never promoted. The new primary republishes the stream watermark, attaches the snapshot schedule, connects PostgreSQL, and binds the customer listener. `202` means the old writer is fenced, not that port 4000 is ready.

The milestone was finished in two sessions. The first built and tested the follower, fencing, and promotion, but its hand-off compared journal identity only *after* recovery, and recovery truncates a torn tail. A stricter opener existed with a passing test, but was never called, never formatted, and never documented — a passing test on unreachable code again. Completing the milestone exposed the checkpoint's identity, switched promotion to the stricter opener, and deleted the unchecked one. A regression test written first failed on the old code: a journal swapped in at the followed path, with a torn record, was cut back to its bare 8-byte header before promotion noticed it was the wrong file. It now passes with the file untouched.

Verified on 2026-09-29 on Linux: `cargo fmt -- --check` is clean and `cargo test --locked` passes 131 unit tests and 6 executable integration tests. A live run of the real executables against PostgreSQL proved the whole hand-off: `409` while the primary ran, the warm following new commands without writing, a `SIGKILL` of the primary, `202`, the promoted process serving port 4000 with identical balances, positions, and order state, a new trade on the promoted primary, and journal sequences 1–18 contiguous across the hand-off.

This is v1 of the target design's hot-warm engine, not the whole of it. There is no heartbeat, automatic failover, leader election, second host, reliable-UDP or Raft replication, network-partition handling, or RTO/RPO measurement. Promotion replays the whole journal rather than reusing the caught-up warm core, so its duration grows with history.

## 19. Critical-Path Performance v1

Full write-up: `docs/tasks/14-critical-path-performance-v1.md`, with one file per optimization in `docs/performance/`.

The first measurement of the exchange, and the fixes it pointed at. `--bench` (`src/exchange/bench.rs`) drives the real worker, journal, mmap stream and snapshot schedule with a deterministic open-loop workload, records latency from each order's *intended* send time in an HdrHistogram (the answer to coordinated omission), and reports orders per sync and memory. The baseline was 384 orders/s on disk, with about 99% of every order spent waiting for its own `fsync`, and per-order cost that grew with book depth (90 orders/s at 10,000 resting orders even with free syncs).

Three optimizations followed, each measured on its own:

1. **Group commit** (`docs/performance/01-group-commit.md`). The worker stages every queued command (prepared and committed in memory, in queue order), writes all their records with one `write_all` and one `sync_all`, and only then publishes, runs callbacks and releases replies, reads included. Natural batching, with no timer: groups of 1 when quiet and up to 1,024 when busy. Disk throughput went from 384 to 18,713 orders/s, and at a fixed 1,000/s p99 went from 5.1 s to 22 ms. The failure rule changed: after a failed sync the in-memory core is never used again; the worker halts, and a process restart recovers from the journal. Durable-before-visible is unchanged.
2. **Planned matching** (`docs/performance/02-match-without-copying-the-book.md`). `MatchingEngine::prepare_order` used to clone the whole symbol book to match on a throwaway copy; cancel did too. `OrderBook::plan_order` now works out fills read-only against the live book, and `apply_plan` applies them at commit. The old matcher is kept as a test oracle, and a 20,000-step differential test proves identical executions, books and indexes. Throughput at 10,000 resting orders went from 107 to 52,465 orders/s; 200,000 orders in memory went from 3,309 to 43,438 orders/s.
3. **No history in RAM** (`docs/performance/03-no-history-in-ram.md`). The runtime's never-read `event_log` vector (600–700 bytes per order, forever) now exists only in test builds. Peak memory at 1,000,000 orders fell from 1.92 GB to 1.32 GB, and throughput from 4,477 to 30,865 orders/s, because the VM had been reclaiming memory at the cost of most of its CPU.

The benchmark also found the next bottleneck: the periodic core snapshot serializes the entire, ever-growing exchange state on the trading thread, which holds the optimized exchange to about 8,300 orders/s. The remaining per-order CPU is spread over preparation (~9 µs), JSON encoding (~7 µs), commit (~6 µs) and per-record mmap publication (~4 µs), and on disk the worker idles during each group's sync.

Verified on 2026-09-29: `cargo fmt -- --check` is clean, `cargo test --locked` passes 136 unit tests and the integration tests, and the release build's warning count is unchanged at 13.

## 20. Snapshots Written by the Warm Replica

Full write-up: `docs/tasks/15-snapshots-by-the-warm-replica.md`; measurements in `docs/performance/04-snapshots-off-the-trading-thread.md`.

The primary's trading thread no longer writes periodic core snapshots. It used to stop trading every 10,000 commands to serialize its entire, ever-growing state (82 MB at 200,000 orders, up to about 1.5 s per snapshot). Now it writes one snapshot at startup, and the warm replica writes the rest. After each replayed and checked batch the warm replica counts a command, and every `EVENT_SNAPSHOT_INTERVAL` commands it writes the journal-bound snapshot at exactly its applied checkpoint, with state and position captured together on the one follower thread. The warm replica now reads every batch from the durable journal (`StreamReader::journal_only`), so the state it snapshots is built exactly as journal recovery would build it, never from the mmap cache. An invalid snapshot found at warm start is preserved, and that warm replica writes none. Primary and warm replica can both replace the file; each uses a unique temporary file and an atomic rename, so any snapshot found is complete and valid, only possibly older.

Measured on disk with snapshots every 10,000 commands: 200,000 orders went from about 8,300 to about 21,900 orders/s (2.6×; the primary alone with no snapshot writer does 26,084). At 5,000 orders/s p90 went from 221–291 ms to 34–44 ms and the worst latency from 0.7–1.04 s to 170–442 ms, close to the no-snapshot level. Restarting from the warm replica's snapshot took 3.9 s against 25.2 s for a full replay. The cost moved rather than disappeared: at full speed the warm replica spent 17.6 s writing 21 snapshots, and caught up 14.5 s after the benchmark ended. Bounding the state (a trading-day boundary) is the fix for that.

Verified on 2026-09-29: `cargo fmt` is clean, `cargo test --locked` passes 138 unit tests and the integration tests, and release warnings are unchanged at 13.

## 21. Subscribers Keep Up

**Status: completed on 2026-09-30.** Full write-up: `docs/tasks/16-subscribers-keep-up.md`; one file per optimization in `docs/performance/` (`05`, `06`).

Goal: make the two subscribers follow the exchange at its own speed, and fix the reporter's recorded bugs. Parts: (1) market data applies in place and saves once a second — **complete**; (2) the reporter commits many batches per PostgreSQL transaction — **complete**; (3) reporter bug fixes: a reused order id halting it, a rejected cancellation overwriting the owner's row, execution ids that repeat across symbols (found by this milestone's benchmark), plus two problems found by the independent review — **complete**.

**Part 1, complete (2026-09-29).** Write-up: `docs/performance/05-market-data-keeps-up.md`. The MDP used to copy both projections, serialize its whole state, and fsync twice for every command: about 39 commands/s. It now applies each batch in place under one lock holding `Option<View>` (`None` = unavailable, set before the lock is released if a batch fails, so a half-applied view is never served), and saves the view with the checkpoint of its last applied batch at most once a second (counted from the end of the previous save), plus once at the end of catch-up. A crash replays at most about a second of journal onto the last saved pair. A guard withdraws the view if the follower thread ends for any reason. Measured on a 200,000-order journal: catch-up from 39 to about 56,000 commands/s (3.6 s instead of about 86 minutes); live, it stayed within a second of the exchange at 5,000 orders/s and at the exchange's maximum (about 34,500 orders/s). `cargo test` passes 140 unit tests and the market-data executable tests.

**Part 2, complete (2026-09-29).** Write-up: `docs/performance/06-reporter-batched-transactions.md`. The reporter used one PostgreSQL transaction (and so one WAL fsync) per command, and three statements per trade: about 365 commands/s. `apply_available`, used for both catch-up and live following, now applies up to 1,000 batches per transaction and commits them with the checkpoint just after the last applied batch; a group also ends when the reporter is caught up, and any error rolls back the whole group and stops the reporter. Each trade is now one statement: a data-modifying CTE fills both orders and inserts the trade only if both fills applied. Measured on a 200,000-order journal: 365 → 1,050 commands/s with group commit, → 1,578 with one round trip per trade (4.3×). Live, it keeps up at 1,000 orders/s; at 5,000 orders/s it falls behind and drains a 10-second burst in about 21 s. Per-row PostgreSQL work (indexes, foreign keys, row versions) now dominates, so set-based multi-row writes or `COPY` are the next lever. The PostgreSQL acceptance test passes.

**Part 3, complete (2026-09-30).** Write-up: `docs/tasks/16-subscribers-keep-up.md`.
- (A) A rejected submission, such as a client's retry of an accepted id or an id rejected before, is a row in `rejected_orders`, keyed by its input's journal sequence. It no longer collides with the accepted order's key and halts the reporter.
- (B) A refused cancellation is a row in `rejected_cancellations` with the requester. It never touches the owner's order row, and a successful cancellation must come from the owner.
- (C) Execution ids are unique per symbol, because each book numbers its own. The 10-symbol benchmark journal, which stopped the old reporter at event 255, now completes.
- (D, found by the independent review) Client-supplied order ids and symbols are checked at the gateway: 1–64 bytes, no control characters. A multi-kilobyte cancel id would otherwise have stopped the reporter on every restart, because PostgreSQL cannot index it.
- (E, found by the review) The reporter's `/health` goes to 503 however its follower thread ends.

The migration `20260930000000_reporter_rejections.sql` runs in one transaction. It empties the report so the reporter rebuilds it from sequence 1, and adds a required `report_version` column so a reporter from before it cannot save a checkpoint. Measured on the office Ubuntu machine, the fixed reporter catches up at about 2,100 commands/s on both the 1-symbol and the 10-symbol journal. The Part 2 code does 1,400–1,900 there, depending on database state, so the fixes cost nothing. `cargo test` passes 142 unit tests and the integration tests; both PostgreSQL acceptance tests pass.

## 22. Trading Day

**Status: completed on 2026-10-05** (selected on 2026-10-01; Part 1 complete on 2026-10-02; Parts 2 to 4 on 2026-10-05). Measurement: `docs/performance/07-trading-days-bound-the-state.md`. Write-up: `docs/tasks/17-trading-day.md`. Decisions confirmed by the owner:
- a loopback operator port opens and closes the market;
- every order still resting at the close expires (day orders only);
- the previous day is cleared from memory at the next open;
- earlier days are read from the reporter's database only.

### Goal

Give the exchange a trading day (open, trade, close) and bound its memory to one day. The design asks for normal trading hours only. Today the exchange accepts orders at any time and keeps every order and fill forever, so its memory, snapshots, restart time and the warm replica's snapshot cost grow without limit. A snapshot was already 82 MB at 200,000 orders. At the design's 1 billion orders a day, memory would run out within hours. LMAX snapshots nightly and replays only the day's journal; this milestone gives the exchange the same shape.

### Behaviour

1. **Sessions are journaled commands, never clock reads.** New inputs `MarketOpenRequested { trading_day }` and `MarketCloseRequested` produce `MarketOpened { trading_day }`, `MarketClosed { trading_day }`, or `SessionRejected { reason }`. The worker processes them like any other command, so replay, the warm replica and every subscriber see exactly the same day boundaries. This is the reason risk limits are events: a decision taken from the clock would replay differently.
   - `trading_day` is a calendar date chosen by the operator (`2026-10-01`), and each must be later than the last.
   - Opening an open market or closing a closed one is rejected.
   - A new journal starts closed.
2. **While the market is closed, new orders are rejected** with a journaled `OrderRejected` ("market closed"). Deposits, share deposits and risk-limit changes are accepted at any time. A cancellation while closed finds nothing resting and is rejected as today.
3. **At the close, every resting order expires.** For each one:
   - its unfilled collateral is released: cash at the limit price for a buy, shares for a sell;
   - its risk exposure is released;
   - its state becomes `Expired`, and it leaves the book;
   - it consumes a matching sequence, as a cancellation does, and the close's journal record carries one `OrderExpired { order_id, seq_num }` for it.

   One record holds every expiry. A close that would exceed the 64 MiB record limit (about 450,000 resting orders) is rejected before anything changes; closing symbol by symbol would be the later fix. *(Changed in Part 2 after the independent review: a refused close could leave the market stuck open, so the books now hold at most 200,000 resting orders and the close always fits; see Progress.)*
4. **The next open clears the previous day.** Finished orders (filled, canceled, expired) and the per-user fills index leave memory. Balances, positions, risk limits, and each symbol's book with its execution counter stay, so execution ids never repeat. Daily risk usage restarts from zero, because nothing rests overnight.
5. **Ids per trading day.** A client order id must be unique within a trading day (the FIX tag 11 rule), and can be reused on a later day. `GET /exchange/orders/{id}` and `GET /exchange/executions` answer for the current or just-closed day; earlier days are in the reporter's PostgreSQL tables.
6. **The risk day is the trading day.** The order-timestamp day (`RiskManager::roll_day`) and milestone 13's overnight carry-over are removed.

### Components

- **Core** (`core.rs`, `order_manager.rs`, `matching_engine.rs`, `order_book.rs`, `risk_manager.rs`): session state in `ExchangeCore`; a prepared close (expiry plan for every resting order) and its infallible commit; an open that clears the previous day and resets risk; `OrderState::Expired`.
- **Runtime** (`runtime.rs`, `types.rs`):
  - new `ExchangeCommand` variants to open, close and read the session;
  - the closed-market check sits in `prepare_input_event`, the one entry point shared by live processing, replay and the warm replica. Core unit tests that call `ExchangeCore` directly are unaffected.
- **Operator port** (`main.rs`): a loopback-only listener, `EXCHANGE_OPERATOR_ADDR` (default `127.0.0.1:4004`), with:
  - `POST /session/open` with `{"trading_day":"2026-10-01"}`;
  - `POST /session/close`;
  - `GET /session`.

  It uses the same trust model as the warm replica's management port. A promoted warm replica serves it too, because it uses the same startup path.
- **Snapshot**: format version 2 adds the session; a version-1 snapshot is refused and falls back to full replay. The warm replica also writes a snapshot just after each open, when the state is smallest.
- **Shared decoder** (`committed_batch.rs`): `MarketOpened { trading_day }`, and `MarketClosed { trading_day, expired }` listing the expired order ids.
- **MDP**: removes expired orders from its book at the close; candles are unchanged.
- **Reporter** (third migration, which again empties the report for a rebuild):
  - order rows gain `trading_day`, keyed by `(trading_day, order_id)`;
  - status `expired` is added;
  - the checkpoint row stores the current trading day, which the reporter learns from the journal.
- **Benchmark**: opens the market before sending orders; `--days N` runs N trading days (open, orders, close).

### Compatibility

Existing journals do not replay: their orders were accepted with no session open, so replay reports an `OutputMismatch`. Start a new journal, as milestones 9 and 13 required. Market-data state files and the report are rebuilt from the new journal. Remove the old snapshot file (`EVENT_SNAPSHOT_PATH`) and the market-data state file with the old journal: a snapshot bound to another journal, or of an older format, is preserved for diagnosis and turns snapshot writing off, and a market-data checkpoint for another journal refuses startup.

### Parts

Each part goes through code, tests, measurement where relevant, its docs, and an update to this journal.

1. Sessions: commands, operator port, closed-market rejection, the risk day from sessions, snapshot version 2.
2. Expiry at the close, through the decoder, the MDP and the reporter.
3. Clearing the previous day at the next open: ids per day, reporter keys and migration, the warm replica's snapshot at the open.
4. Measurement. Compare the milestone 21 binary on the same order volume with no days against the new binary over several days: memory, snapshot size and write time, warm-replica lag, and restart time per day. Write `docs/performance/07-*.md` and `docs/tasks/17-trading-day.md`.

### Completion criteria

- Orders before the first open and after a close are rejected and journaled; deposits work at any time.
- After a close every book is empty, and balances, locks, positions and risk usage equal what cancelling each resting order would have given.
- After the next open the previous day's orders and fills are gone from memory, the same client order id is accepted again, and no execution id repeats.
- Replay, snapshot recovery and warm-replica following across open, close and open rebuild identical state; a promoted warm replica serves the operator port.
- The MDP's book is empty after a close. The reporter records expired orders and per-day keys, and restarts without duplicates.
- The multi-day benchmark shows memory, snapshot size and restart time staying flat across days.
- `cargo fmt -- --check`, `cargo test --locked`, both PostgreSQL acceptance tests and an independent review all pass.

Not in this milestone: opening or closing auctions, a holiday calendar or automatic schedule, good-till-cancel orders, market orders, journal files per day or archival, and a reporting or history API.

### Progress

**Part 1, complete (2026-10-02): opening and closing the market.**
- **Journaled session.** `MarketOpenRequested { trading_day }` and `MarketCloseRequested` produce `MarketOpened`, `MarketClosed` or `SessionRejected` (`AlreadyOpen`, `NotAfterLastTradingDay`, `AlreadyClosed`). The core's `Session` starts closed with no day; days only move forward.
- **Closed-market refusal.** While closed, `prepare_input_event` refuses new orders as `OrderRejected { reason: "MarketClosed" }`, which the customer API answers with 409. That function is shared by live trading, replay and the warm replica. Deposits, risk limits and cancellations are accepted at any time.
- **Risk day.** The daily cap now restarts at each open (`RiskManager::start_day`) instead of following order timestamps; resting quantity still carries into the new day until Part 2 expires it.
- **Operator port.** `127.0.0.1:4004` (`EXCHANGE_OPERATOR_ADDR`, loopback only) serves `POST /session/open`, `POST /session/close` and `GET /session`.
- **Snapshots and benchmark.** Snapshots are format version 2 and carry the session. The benchmark opens a fixed day first.

Verified on Linux: `cargo fmt -- --check`, and `cargo test --locked` with 147 unit tests plus the integration tests. A live run of the real binary covered:
- refusal before the open;
- trading while open;
- refusal after the close;
- refused re-opens;
- after `SIGKILL` and restart, the session recovered from the journal;
- a new day.

The benchmark is unchanged at about 46,000 orders/s on the office Ubuntu machine.

**Part 2, complete (2026-10-05): expiry at the close.**
- **Day orders.** At the close every resting order expires: its unfilled cash or shares and its risk allowance are released exactly as a cancellation would release them, its state becomes `Expired`, and it leaves the book. Each expiry consumes a matching sequence. Orders expire oldest first, by the matching sequence that accepted them, because the books' hash-map order differs between processes.
- **One record per close, and a cap that keeps it possible.** The close's record holds `MarketClosed { trading_day }` and then one `OrderExpired { order_id, seq_num }` per resting order. The books hold at most 200,000 resting orders across all symbols: an order that would rest beyond that is refused as `OrderRejected { reason: "BookFull" }` (409), while orders that trade without resting are never refused. With the longest ids the gateway allows, every byte escaped in JSON, such a close takes about 50 MB of the 64 MiB limit. As a safety net for orders that skipped the gateway's id check, a close whose record would still be too large is refused as `SessionRejected { reason: "TooManyRestingOrders(n)" }` and changes nothing; the check counts every envelope sequence at its widest, so replay always reaches the same decision. The first version had only that refusal; the independent review showed it could leave the market stuck open for good, since only owners can cancel orders, and the owner chose the cap.
- **Risk.** Nothing rests overnight, so milestone 13's open-exposure counter and its fill-time bookkeeping are removed. Usage grows on acceptance, stays on a fill, shrinks by the unfilled quantity on a cancellation or expiry, and restarts from zero at each open.
- **Subscribers.** The shared decoder yields `MarketClosed { trading_day, expired }` and checks consecutive sequences and unique orders. The MDP removes the expired orders and fails closed if any order is left. The reporter marks them `expired` with their `expiry_sequence` in one statement per close; the migration `20261005000000_reporter_expiry.sql` empties the report for a rebuild and moves `report_version` to 3.
- **Snapshots** are format version 3 and refuse resting orders while the market is closed.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 153 unit tests plus the integration tests, and both PostgreSQL acceptance tests. In release, a book of 200,000 resting orders with worst-case 64-byte ids refused the next resting order and closed in one 50.1 MB record (53.0 MB at the widest sequences), prepared in 0.47 s. The independent review found no correctness bug in the expiry path; its other findings were fixed (a size test that could not fail, an expiry order tie-break, a stale comment), except the reporter's "nothing still rests after a close" check, which moves to Part 3 because until rows carry their trading day it would scan the whole report at every close. A live run of the real exchange, MDP and reporter covered a fill and three resting orders (one partly filled), the close, released locks, a refused cancel of an expired order (400), an empty MDP book, `expired` report rows with sequences 5 to 7, a `SIGKILL` restart, and a next day starting with zero risk usage.

**Part 3, complete (2026-10-05): the next open clears the previous day.**
- **One day in memory.** The open drops the previous day's finished orders and the per-user fills index. Balances, positions, risk limits, and each symbol's (empty) book with its execution counter stay, so execution ids never repeat. Between a close and the next open, the day just closed can still be read.
- **Ids per trading day.** A client order id must be unique within a trading day and returns on a later day. `GET /exchange/orders/{id}` and `GET /exchange/executions` answer for the current or just-closed day; earlier days are in the reporter's tables.
- **Snapshots** are format version 4: a version-3 snapshot could hold orders that replay under the new rule would have cleared. The warm replica also writes a snapshot right after each open, when the state is smallest, so a restart replays only the current day.
- **Reporter.** The decoder yields `MarketOpened { trading_day }`. The reporter follows the day from the journal and keeps it in its checkpoint row. Orders are keyed by `(trading_day, order_id)`, trades carry the day of the two orders they fill, and rejected orders and cancellations record the day the journal was in (NULL before the first open), so a refusal can be matched to its day's order. After a close the reporter checks that nothing from that day still rests in the report. The migration `20261005100000_reporter_trading_days.sql` empties the report for a rebuild and moves `report_version` to 4. Part 2's migration is kept as its own step so that each part can be committed on its own, which makes this a fourth migration rather than the planned single third one.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 157 unit tests plus the integration tests, and all three PostgreSQL acceptance tests: the lifecycle journal now runs two days with an id reused on the second, and a new test shows that a close leaving an order resting in the report stops the reporter. A live run of the real exchange, MDP, reporter and warm replica over two days covered: a same-day id reuse refused (409); the closed day readable until the next open; after the open, the old id 404 and no fills, with balances carried over; both ids accepted again and trading with execution id `exec_2`; per-day report rows and trades, with the checkpoint on the new day; one warm snapshot right after each open (the last 653 bytes, at the second open, despite an interval of 1,000); and a `SIGKILL` restart from it. The independent review found no high-severity bug, and its findings were fixed: rejected orders and cancellations now record their day, the open simply drops every order instead of claiming a safety net it was not, the client-order-id comment and API advice now say ids are per day, a test now snapshots before an open, and stale docs and two operational notes (removing an old snapshot with an old journal, and empty books kept per symbol) were updated.

**Part 4, complete (2026-10-05): measurement.** Write-up: `docs/performance/07-trading-days-bound-the-state.md`; raw output `docs/performance/results/results-m22.txt`. The benchmark gained `--days N`: each day opens, runs `--orders` measured orders, and closes, printing its own throughput, close time and memory. On the office Ubuntu machine, the same 1,000,000 orders with a warm replica beside the exchange, milestone 21 in one run against milestone 22 as five days of 200,000:
- exchange memory at the end 1,234 MB against 263 MB, and after each close 256 to 263 MB on every day;
- warm replica peak memory 2,121 MB against 506 MB;
- largest snapshot 422 MB against 85 MB, back to 0.7 MB right after each open; slowest snapshot write 10.5 s against 1.9 s, and snapshot writing in total 375 s against 81 s;
- warm replica caught up 365 s against 78 s after the benchmark;
- restart at the end of the run 11.7 s against 2.4 s, the same 2.4 s as after one day; early in a day (snapshot of 667 KB right after the open) 49 ms.

Each close took 250 to 380 ms and expired about 41,700 orders. Throughput without a warm replica is unchanged within noise (40,000 to 45,000 orders/s on both binaries). The warm replica still falls behind at maximum rate, but its snapshot work now grows with the run's length rather than with history.

**Milestone 22 complete (2026-10-05).** Every completion criterion above holds:
- orders are refused before the first open and after a close;
- a close leaves the ledgers exactly where cancellations would;
- the next open clears the day, and its client order ids return;
- replay, snapshot recovery and the warm replica agree across days;
- the MDP and the reporter follow the expiries and the per-day keys;
- memory, snapshot size and restart time stay flat across days.

Both independent reviews' findings were fixed (parts 2 and 3).

## 23. Two Machines

**Status: selected on 2026-10-05; Part 1 complete on 2026-10-05.** Write-ups: `docs/tasks/18-one-order-cannot-stop-the-exchange.md` for Part 1, then `docs/tasks/19-two-machines.md`. Decisions confirmed by the owner:
- the primary waits for the replica: a command is answered, and becomes visible to anyone, only once it is on both machines' disks;
- if the replica cannot be reached, the primary pauses until an operator either promotes the replica or tells the primary to run alone;
- the machines talk over TCP;
- for this milestone the second machine is a second container on the office Ubuntu machine, with its own volume and network address. It shares the CPU and the disk, and every measurement must say so.

### Goal

Survive the loss of a machine without losing an acknowledged command, and resume trading on the other machine in seconds. The design asks for 99.99% availability, a recovery point of zero ("data loss is not acceptable") and a recovery time of seconds. When the milestone was selected:
- one disk holds the only journal, so losing that machine loses every balance, position and order;
- promotion replays the whole journal, so it takes longer every day: a full replay of 200,000 orders took 25.2 s in milestone 20;
- the journal is identified by its file's device and inode, which a copy on another machine does not share, so every snapshot and subscriber checkpoint would be refused there;
- a reader that restarts (the warm replica, the MDP, the reporter) re-reads the journal from its first record, whatever its checkpoint says;
- one client order could stop the exchange, and a second machine would stop on the same order. The design names this risk: bugs can bring down the primary and the backup alike.

### Behaviour

1. **No command can stop the exchange (Part 1).**
   - An order that would trade against more than 10,000 resting orders is refused while it is prepared, as a journaled `OrderRejected { reason: "TooManyFills" }` (409). Its record therefore always fits the 64 MiB limit, and replay reaches the same decision. Before, such an order made a record too large to write, and the worker halted. The order was never journaled, so a restart worked, and the client could send it again.
   - A deposit is refused if it would take the exchange's total cash, or a symbol's total shares, past `u64::MAX`. Fills only move cash and shares between users, so no fill can then overflow a balance or a holding. Before, one deposit of `u64::MAX` and a one-share trade halted the worker the same way. The independent review of Part 1 found this.
   - The exchange and the warm replica refuse to start without `JWT_SECRET`. Before, the token check fell back to the key `"secret"` when it was unset.
2. **The journal names itself (Part 2).** A new journal starts with a random journal id in its header. Snapshots, reader checkpoints, the stream header, the MDP state and the reporter checkpoint bind to that id instead of the device and inode, so a byte-identical copy on another machine is the same journal.
3. **Restarts cost the day, not history (Part 2).**
   - A reader checks its checkpoint by reading the record there, not every record before it.
   - Promotion reuses the warm replica's core, which it built from the journal and checked output by output, and replays only the records after it. Nothing on the promotion path grows with history.
   - The warm replica's lag stays bounded at the exchange's full rate. Today it grows through the day because of its snapshots, and a promotion would have to replay that backlog first.
4. **Every acknowledged command is on both machines (Part 3).**
   - The primary sends each group's records to the replica over TCP while it syncs its own journal.
   - The replica checks them, appends exactly those bytes to its own journal, syncs, and confirms.
   - The primary publishes, runs callbacks and replies only once both syncs are done. Nothing that exists on one machine only is ever visible to a client or a subscriber.
   - A replica that starts behind first catches up from the primary's journal.
5. **Pause, never guess (Part 3).** If the replica stops confirming, the primary stops committing: commands wait in its queue and nothing is acknowledged. The operator port shows the pause and offers "run alone": the primary continues without a replica, and the recovery point of zero no longer holds until a replica has caught up again. The operator must never both promote the replica and tell the primary to run alone.
6. **Fenced promotion on the second machine (Part 4).**
   - Promotion is manual.
   - Each primary term has an epoch number, journaled at its start. The promoted replica raises the epoch and refuses the old primary. The old primary can no longer get a confirmation, so it can no longer acknowledge anything.
   - When the old machine returns, it rejoins as the replica. It drops the end of its journal that was never confirmed, and so was never acknowledged or published, then follows the new primary.
7. **Subscribers continue (Part 4).** The reporter, whose checkpoint is in PostgreSQL, resumes against the new primary's journal without a rebuild, because it is the same journal. The MDP starts on the new primary's machine like any MDP: from its state file when one is there, otherwise from the journal.

### Parts

1. No command can stop the exchange: the fill cap and the deposit totals; `JWT_SECRET` required at startup.
2. On one machine: the journal id, checkpoint checks that skip history, promotion from the warm replica's core, and a warm replica whose lag stays bounded. Measure promotion and restart times before and after on a multi-day journal.
3. Replication over TCP with the primary waiting for the replica; the pause and "run alone".
4. Epoch-fenced promotion on the second machine; the old primary rejoining as the replica; subscribers continuing.
5. Measurement and failure tests:
   - throughput and p99 with and without replication;
   - killing the primary under load loses no acknowledged command;
   - recovery time, from the promote request to the first accepted order;
   - the replica killed, the network cut, and the old primary returning with an unconfirmed tail.

### Completion criteria

- An order that would take more than 10,000 resting orders is refused and journaled. The largest order still accepted fits in one record with the gateway's longest ids. No deposit can take the exchange's total cash or a symbol's total shares past `u64::MAX`. The exchange and the warm replica refuse to start without `JWT_SECRET`.
- Promotion time and reader restart time no longer grow with the journal, measured on a multi-day journal against the current code.
- With replication on, killing the primary at any moment under load loses no acknowledged command over repeated runs, and the replica's journal is a byte-identical prefix of the primary's.
- With the replica unreachable, the primary acknowledges nothing until the operator acts.
- After a promotion, the old primary acknowledges nothing. When it returns, it drops its unconfirmed tail and follows the new primary.
- The reporter continues after a failover from its checkpoint, without a rebuild.
- Throughput, latency and recovery time with replication are measured and written up.
- `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests and an independent review of each part all pass.

Not in this milestone:
- automatic failover, or promotion on missed heartbeats;
- leader election or Raft, and more than one replica;
- reliable UDP or multicast, and a second data center;
- replicating PostgreSQL: users and the report stay on one database;
- an authenticated or encrypted replication link: private network only;
- journal files per day or archiving;
- a pipelined journal sync.

### Progress

**Part 1, complete (2026-10-05): no command can stop the exchange.** Write-up: `docs/tasks/18-one-order-cannot-stop-the-exchange.md`.
- **The fill cap.**
  - One new order may trade against at most 10,000 resting orders (`MAX_FILLS_PER_ORDER`). Beyond that it is refused while it is prepared, as a journaled `OrderRejected { reason: "TooManyFills" }` (409), and replay reaches the same decision.
  - Planning first walks the resting orders the order would trade with, by reference only. It builds fills and executions only within the cap, so a refused sweep costs one walk of at most 10,001 orders.
  - At the widest values one trade takes 1,376 bytes of the record, so 10,000 take 13.8 MB of the 64 MiB limit.
- **The deposit totals.**
  - A deposit is refused (`Overflow`) if it would take the exchange's total cash, or a symbol's total shares, past `u64::MAX`.
  - Fills only move cash and shares between users, so no fill can overflow a balance or holding.
  - The totals are running sums: deposits add to them, fills leave them alone, and snapshots rebuild them and are refused beyond them.
  - Found by the independent review: one deposit of `u64::MAX` and a one-share trade halted the worker.
  - Not fixed: nothing withdraws, so one client can deposit up to the limit and refuse every later deposit. Operator-only deposits (the gateway milestone) remove this.
- **No fallback key.** The exchange and the warm replica refuse to start without a non-empty `JWT_SECRET`, the warm replica before it follows anything. The token check never falls back to `"secret"` any more.
- **Compatibility.** A journal holding an accepted order with more than 10,000 fills, or deposits beyond the totals, no longer replays; none of this project's journals has either. A primary and a warm replica must run the same version.

Verified on Linux: `cargo fmt -- --check`, and `cargo test --locked` with 164 unit tests plus the integration tests, including a new process test of both refusals to start. Live runs of the real binary:
- **The sweep.** 55,000 resting sells with the gateway's longest ids, then one buy that sweeps them:
  - the milestone 22 binary halted ("record exceeds size limit"), recovered on restart with the book intact, and halted again on the same order;
  - the new binary refused it with 409 in 0.02 s, then filled an order taking exactly 10,000 (a 12.4 MB record) and kept trading.
- **The overflow.** A deposits `u64::MAX` and sells one share; B deposits 1 and buys it:
  - the milestone 22 binary halted ("wallet balance invalid");
  - the new binary refused B's deposit, so the buy was an ordinary insufficient-funds rejection; a share deposit beyond the AAPL total was refused too.

The independent review's other findings were fixed:
- the write-up was missing from `.gitignore`'s exceptions;
- a size test's final check could never fail;
- a test name overstated what it checks;
- a refused sweep built every execution before being refused;
- three wording errors in the write-up.

Its re-check of the fixes confirmed the conservation argument and the two-step planner. It found that the first version of the deposit check scanned every holding of every symbol on each share deposit, which any client could make slow; the totals are now running sums. It also found the deposit limit that one client can use up, recorded above as not fixed.

## Known Prototype Limitations

- the risk-limit endpoint sets the caller's own cap, so a trader can raise their own limit; a real exchange would make this a compliance action
- the daily cap counts shares, not notional; the trading day is opened and closed by an operator command, with no automatic schedule, market-hours clock, weekends or holidays
- risk limits are per `(user, symbol)`; there is no cross-symbol or portfolio-level risk
- execution queries are served from an index built during settlement, not projected from the event store
- short selling is not supported at all: a sell must be fully backed by shares held, with no borrow model
- settlement is instant at match; there is no T+1/T+2 settlement cycle or pending-position concept
- share deposits let anyone credit themselves any quantity, exactly as cash deposits do; both are placeholders for real custody
- there is no product/instrument registry, so orders, share deposits and risk limits accept any symbol of 1-64 bytes without control characters; every symbol ever traded keeps an empty book (with its execution counter) in memory and in every snapshot, so a client can grow the state without bound by using new symbols
- self-trade prevention skips the aggressor's own orders but cannot stop a user crossing against themselves; that needs engine-generated cancellations
- `Order.leaves_qty` and `ManagedOrder.remaining_quantity` are separate sources of truth for the same number
- one global minor-unit price scale is assumed; per-product currency and tick-size metadata are not modeled
- a deposit is refused if it would take the exchange's total cash, or a symbol's total shares, past `u64::MAX`, so no fill can overflow a balance or holding; nothing withdraws, so one client can deposit up to that limit and refuse every later deposit by anyone, until deposits become operator actions
- the event log is one file that grows without bound; snapshots reduce exchange-core replay work but do not compact or retain less journal history; startup reads the part of the journal it replays (all of it without a usable snapshot or on warm promotion, the suffix with one) into memory in one piece rather than streaming it
- order records and the per-user execution index hold one trading day; the open replaces both maps, releasing their memory, while the matching engine's order-location map and risk usage keep the capacity of the largest day, and a price level's node slots are freed only when the level empties, which the close guarantees once a day
- the books hold at most 200,000 resting orders across all symbols, so that the close's one journal record always fits; beyond that an order that would rest is refused (409 `BookFull`) until orders trade, are cancelled, or expire at the close, and one user can fill the book, as there is no per-user share; there is no close spread across several records
- one order may trade against at most 10,000 resting orders (`TooManyFills`, 409), so that its record always fits the 64 MiB limit; a client that wants more must split its order
- one journal `sync_all` per group of queued commands; the worker waits during it (no pipelined journaler thread yet), and p99 cannot beat the disk's own sync latency
- the core snapshot serializes the whole exchange state, which since milestone 22 holds one trading day: at 200,000 orders a day it grows to about 85 MB and 2 s by the close, and falls to under 1 MB at the next open. It runs on the warm replica (every interval and right after each open), which still falls behind at full load because it snapshots every 10,000 commands, and only at primary startup on the primary
- without a running warm replica no periodic snapshots are written; a restart then replays everything since the primary's last startup snapshot
- the warm replica's snapshot writes share the host's disk with the journal's syncs
- after a failed journal sync the in-memory core is ahead of the disk; it is never used again, the worker halts, and the process must be restarted to recover from the journal
- a `client_order_id` is unique within its trading day, including after its order finished; earlier days' orders are only in the reporter's tables, and there is no reporting API to read them
- snapshots rely on the journal file identity and committed boundary; an invalid snapshot falls back to full replay and is retained for diagnosis
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- MDP saves its complete JSON open-order and candle state at most once a second; a crash replays up to about a second of journal, and each save still grows with candle history
- candle state retains every one-minute bucket without a retention limit, rollups, or external historical store
- there is no public reporting API or trade-tape service
- mmap is a same-host Unix transport using cooperative file locks, JSON, and a bounded window; it is not lock-free, cross-server replication, or an ingress transport
- the journal still grows without bound; reader checkpoint validation still scans history on reader restart
- mmap is not the durable recovery source; never modify, truncate, replace, or unlink mapped files while processes use them
- consumer crashes require checkpoint/state coordination; arbitrary downstream effects are not exactly-once
- internal matching and settlement failures are rejected before commit and halt the worker if they cannot be represented as a business rejection
- warm promotion is manual and same-host: no heartbeat, automatic failover, leader election, second machine, or measured RTO/RPO
- promotion replays the entire journal instead of reusing the caught-up warm core, so its duration grows with history
- the warm replica's management API is unauthenticated and loopback-only; `202` from `/promote` means the old writer is fenced, not that the customer listener is ready
- the crate uses Unix-only APIs (advisory file locks, device/inode journal identity, mmap) and builds and tests on Linux only
- the reporter applies about 1,600 commands/s: it keeps up at 1,000 orders/s but falls behind a sustained faster exchange and catches up afterwards; set-based writes or `COPY` are the next lever

## What Not To Work On Yet

Milestone 23, two machines, is selected; its specification is the section above. Do not grow it into automatic failover, promotion on missed heartbeats, leader election, more than one replica, reliable UDP, or Raft: each needs its own failure model, and a three-machine quorum is the recorded follow-up after it. After milestone 23, the recorded candidates in order are:
1. the gateway and trust boundary: risk limits and deposits set by the operator only, a product list so that only listed symbols trade, and per-user rate limiting;
2. set-based reporter writes or `COPY`;
3. pushed updates (WebSocket) of a user's own fills and the L2 book;
4. latency: a binary record format instead of JSON, a pipelined journal sync, and CPU pinning.

The same restraint applies to tax or customer statements, settlement, broad historical-market-data APIs, journal compaction, Crossbeam, lock-free ring buffers, new trading-component threads, per-symbol workers, and FIX/SBE. The committed mmap reader remains the input for all independent subscribers and for the warm follower; inbound commands remain on the existing bounded Tokio queue.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

The crate uses Unix-only APIs and does not compile on Windows. Build and test on Linux — the Ubuntu machine, or a `rust` container with the repository mounted (from Git Bash, set `MSYS_NO_PATHCONV=1` so container paths are not rewritten). The customer, operator, and warm-replica listeners bind loopback only, so a live failover run needs the primary, the warm replica, and the HTTP client in one network namespace. A new journal starts with the market closed: open a trading day on the operator port (`POST 127.0.0.1:4004/session/open`) before placing orders.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.

Every completed task produces two documents, not just code:

1. A new numbered write-up in `docs/tasks/` covering what the task was, how it was done, and the reasoning behind each decision — including the options that were rejected and why. Write it so it can be read cold, months later, without the conversation that produced it.
2. An update to this journal recording the milestone.

Do not claim a milestone here before `cargo fmt -- --check` and `cargo test` both pass. Prefer checking behavior against a running server over trusting unit tests alone; a passing test on unreachable code is what this project has already been caught doing once.
