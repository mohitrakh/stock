# Project Direction - Milestone 26 (Pushed Updates) Planned, Awaiting Owner Review

This is the canonical project journal and direction file. Read it first when returning to the project, then read:

1. `stock-exchange-system-design.md` for the target architecture.
2. `EXCHANGE_PIPELINE_TODO.md` for the current milestone state.
3. `SYSTEM_DOCUMENTATION.md` for the code that exists now.
4. `docs/tasks/` for the per-task write-ups of milestones 7 to 23: what each task changed, how, and why.
5. `docs/milestones/` for milestone 24 onwards: one folder per milestone holding its theory, implementation, measurements and decisions (see "Documentation for this milestone" under milestone 24).
6. The current Rust code before making architecture decisions.

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
       -> EventStore::write_records (all records, one write_all); tell the journaler
       -> go on to the next group (at most 2,048 commands wait for the disk)
     journaler thread (milestone 25 part 2), in a loop:
       -> hand the replication link everything written (it ships while this disk syncs)
       -> ONE sync_all, covering everything written so far; wait for the replica's confirmation
     worker, whenever a sync ends, for every group the disk now holds, in queue order:
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
  -> right after each open, and whenever the journal has grown by EVENT_SNAPSHOT_GROWTH
     times the last snapshot's size, write the journal-bound core snapshot
     at exactly its applied checkpoint
  -> GET /health and GET /status report follower availability only
  -> POST /promote returns 409 while the primary owns the journal writer lock
  -> after the fence, verify it is the file the replica read, with the same journal id,
     before any read or repair; read only the journal after the applied checkpoint and
     replay it into the replica's own core, which becomes the primary's
```

`ExchangeCommand` is live gateway plumbing and may contain `respond_to`. `ExchangeEvent` contains replayable business data and must remain free of HTTP response channels.

`ExchangeRuntime` owns the command receiver, `EventStore`, and `StreamWriter`. It writes one journal-bound snapshot at startup and none while trading; the warm replica writes the periodic ones. It keeps no event history in production; the complete history is the journal, and an in-memory copy exists only in test builds. Group commit commits each command in memory before its group's sync, so after a failed sync the live core is ahead of the disk: the worker halts, that core is never used again, and recovery from the journal is the only way back. `ExchangeCore` owns the deterministic trading components; it never touches the files. All core operations still run on one exchange-worker thread. `ReplicaCore` is a separate read-only follower core: it has no writer, queue, callbacks, database, or customer routes. Readers and the warm follower are separate consumers, not additional owners of exchange state.

`replay_event_log` rebuilds a fresh core from recorded inputs and checks regenerated outputs against history. `ReplicaCore` uses the same complete-batch comparison before it advances its applied checkpoint. Production startup calls `recover_runtime_with_stream_and_snapshot` before binding the listener. A valid versioned, checksummed snapshot is tied to the journal's id and a complete-batch byte boundary; it restores the core and replays only the later suffix. A missing, corrupt, inconsistent, or journal-mismatched snapshot is preserved and falls back to full replay. `EVENT_LOG_PATH` defaults to `exchange-events.log`; `EVENT_STREAM_PATH` defaults to that path plus `.mmap`; `EVENT_SNAPSHOT_PATH` defaults to that path plus `.snapshot`; and `EVENT_SNAPSHOT_GROWTH` (how much the journal grows between the warm replica's snapshots, as a multiple of the last snapshot's size) defaults to 4. The journal is authoritative; the mmap file is a disposable delivery cache and the snapshot is only a recovery checkpoint. Warm promotion takes the writer lock with `EventStore::open_suffix`, which proves the locked file is the one the warm replica read, with the expected journal id and at least what it applied, before reading or repairing anything. It then reads only the journal after the applied checkpoint, and the new primary replays that into the warm replica's own core, which was built from the journal, never from mmap-derived state.

Order and execution prices use `Price(u64)` minor units throughout the critical path. The HTTP order request also accepts an integer minor-unit price; for a cent-based scale, `1025` means `$10.25`. Wallet notionals use checked integer multiplication.

Latest verified status on 2026-10-09, on Linux (the office Ubuntu machine; the crate uses Unix-only APIs and does not build on Windows):

```text
cargo fmt -- --check
cargo test --locked
209 unit tests + the executable integration tests passed; 0 failed
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
Since milestone 23 part 3 a promotion takes over the warm replica's own core: 58 ms on that
journal instead of 25.8 s, and 0.5 s from the end of a maximum-rate run instead of 103 s.
Throughput is unchanged (40,000 to 45,000 orders/s with no warm replica). See
`docs/performance/07-trading-days-bound-the-state.md`.

Since milestone 23, with replication on (`REPLICATION_LISTEN_ADDR`, `stock --replica`), every
acknowledged command is on both machines' disks unless the operator runs the primary alone, and an
operator can promote the second machine, fenced by epochs. With
customers trading, killing the primary ten times lost none of 83,186 acknowledged orders, and the
promote request to the first accepted order took 26 to 74 ms (two containers sharing one machine's
2 cores and SSD). Replicated, the maximum rate is 34,000 to 36,000 orders/s. See
`docs/performance/10-synchronous-replication.md` and `docs/performance/11-failover-under-load.md`.

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

**Status: completed on 2026-10-09** (selected on 2026-10-05; Part 1 complete on 2026-10-05; Parts 2 and 3 on 2026-10-06; Parts 4 and 5 on 2026-10-07; Part 6 on 2026-10-09). Measurement of the whole: `docs/performance/11-failover-under-load.md`. Write-ups: `docs/tasks/18-one-order-cannot-stop-the-exchange.md` for Part 1, then `docs/tasks/19-two-machines.md`. Part 2 as first specified was split in two on 2026-10-06, as recommended: the journal id and restarts in Part 2, promotion and the warm replica's lag in Part 3. Decisions confirmed by the owner:
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
3. **Restarts cost the day, not history.**
   - A reader checks its checkpoint by reading the record there, not every record before it (Part 2).
   - Promotion reuses the warm replica's core, which it built from the journal and checked output by output, and replays only the records after it. Nothing on the promotion path grows with history (Part 3).
   - The warm replica's lag stays bounded at the exchange's full rate (Part 3). Today it grows through the day because of its snapshots, and a promotion would have to replay that backlog first.
4. **Every acknowledged command is on both machines (Part 4).**
   - The primary sends each group's records to the replica over TCP while it syncs its own journal.
   - The replica checks them, appends exactly those bytes to its own journal, syncs, and confirms.
   - The primary publishes, runs callbacks and replies only once both syncs are done. Nothing that exists on one machine only is ever visible to a client or a subscriber.
   - A replica that starts behind first catches up from the primary's journal.
5. **Pause, never guess (Part 4).** If the replica stops confirming, the primary stops committing: commands wait in its queue and nothing is acknowledged. The operator port shows the pause and offers "run alone": the primary continues without a replica, and the recovery point of zero no longer holds until a replica has caught up again. The operator must never both promote the replica and tell the primary to run alone.
6. **Fenced promotion on the second machine (Part 5).**
   - Promotion is manual.
   - Each primary term has an epoch number, journaled at its start. The promoted replica raises the epoch and refuses the old primary. The old primary can no longer get a confirmation, so it can no longer acknowledge anything.
   - When the old machine returns, it rejoins as the replica. It drops the end of its journal that was never confirmed, and so was never acknowledged or published, then follows the new primary.
7. **Subscribers continue (Part 5).** The reporter, whose checkpoint is in PostgreSQL, resumes against the new primary's journal without a rebuild, because it is the same journal. The MDP starts on the new primary's machine like any MDP: from its state file when one is there, otherwise from the journal.

### Parts

1. No command can stop the exchange: the fill cap and the deposit totals; `JWT_SECRET` required at startup.
2. On one machine: the journal id, and checkpoint checks that skip history. Measure restart times before and after on a multi-day journal.
3. On one machine: promotion from the warm replica's core, and a warm replica whose lag stays bounded. Measure promotion time and the warm replica's lag before and after.
4. Replication over TCP with the primary waiting for the replica; the pause and "run alone".
5. Epoch-fenced promotion on the second machine; the old primary rejoining as the replica; subscribers continuing.
6. Measurement and failure tests:
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

**Part 2, complete (2026-10-06): a journal that names itself.** Write-up: `docs/tasks/19-two-machines.md`; measurement: `docs/performance/08-restarts-without-rereading-history.md`.
- **The journal id.** A new journal's header is `EXCHLOG2` plus 16 random bytes, chosen once. These name the journal by that id instead of its file's device and inode:
  - the stream header (`EXCHBUS2`);
  - reader checkpoints, and with them the market-data state (version 3) and the probe's checkpoint;
  - snapshot boundaries (version 5);
  - the reporter's checkpoint row (migration `20261006000000_reporter_journal_id.sql`, `report_version` 5);
  - the promotion fence and suffix recovery, which compare the id under the writer lock before reading on.

  A byte-identical copy at another path is the same journal; an `EXCHLOG1` journal is refused with its reason.
- **Checkpoints checked where they point.** A reader no longer re-reads the journal from its start: the record at the checkpoint must be complete, pass its checksum and begin with the expected sequence, or the checkpoint must sit at the committed end.
- **An older copy of the journal is refused.** The id cannot tell an older copy of the same journal from the journal itself. So the primary refuses to start when its stream already published more than the recovered journal holds, and promotion refuses a journal whose complete records end before what the warm replica applied. The independent review found this gap; device and inode used to refuse a file renamed into place.
- **Compatibility.** Start a new journal. Remove the old stream, snapshot, market-data state and probe checkpoint files, and apply the fifth reporter migration, which empties the report for a rebuild.

Verified on Linux: `cargo fmt -- --check`, and `cargo test --locked` with 172 unit tests plus the integration tests, and all three PostgreSQL acceptance tests. Measured on the same five-day workload (788 MB, 1,000,000 orders):
- a market-data restart from its checkpoint went from 9,654 ms to 9 ms;
- a warm replica starting from a 667 KB snapshot went from 9,620 ms to 68 ms;
- full builds and promotion (18.9 s) are unchanged, and promotion is Part 3.

The same files copied to another directory started as the same journal: the warm replica in 67 ms, market data in 9 ms. The milestone 22 binary refused its own copied files and did not start.

A first-day copy of a two-day journal, put back in place, was refused. Renamed into place, it was refused by promotion ("shorter than the 930 bytes already applied from it"); copied over the followed file, it made the running warm replica stop following ("committed history regressed"). Either way the primary refused to start ("the journal ends before what its stream already published").

Re-checking the fixes, the reviewer confirmed that the new stream check refuses no legitimate restart and that no test still passes vacuously. Its smaller findings, all fixed:
- promotion now compares the length of the complete records, not the raw file, so an older copy with a torn tail reaching past what was applied is refused too, before anything is cut;
- the stream refusal also names a disk that lost synced writes, and the operator notes say what starting anyway means;
- the known limitation now includes a restored journal without a stream file;
- a new test proves promotion passes the applied length, and the stream test checks each of its two conditions alone.

The independent review found the older-copy gap above, and a reporter acceptance test that had stopped testing anything, because a rewritten journal got a new id. Both are fixed; that test and the other two that expect the reporter to stop now check its error output for the reason. Its smaller findings were fixed too:
- an old snapshot is refused for its version;
- new tests cover mid-journal checkpoints and a real version 2 market-data state;
- the docs no longer claim the normal restart re-reads the older journal.

**Part 3, complete (2026-10-06): promotion from the warm replica's own state.** Write-up: `docs/tasks/19-two-machines.md`; measurement: `docs/performance/09-promotion-from-the-warm-replica.md`.
- **Promotion reuses the warm replica's core.** It takes the writer lock, reads only the journal after the warm replica's applied checkpoint (`EventStore::open_suffix`), and the new primary replays that into the core (`promote_replica`) with every output checked. It writes no startup snapshot. Before anything is read or repaired, the path must still name the file the warm replica read (`NotTheFollowedFile`, even for a byte-identical copy), with the same id, reaching both what the stream published and what the warm replica applied (`ShorterThanApplied`). The core includes the snapshot the warm replica started from, which promotion trusts as a restart does. Part 2's whole-journal promotion opener is gone.
- **Snapshots by journal growth.** Right after each open, and whenever the journal has grown by `EVENT_SNAPSHOT_GROWTH` (default 4) times the last snapshot's size, instead of every 10,000 commands. The snapshot work is proportional to the journal applied. While snapshots are being written, a restart replays at most about four snapshot sizes past the last one, plus whatever the warm replica had not applied yet.
- **A reader behind reads committed records straight from the journal**, consulting the stream only when it reaches the last end it validated: two system calls per record instead of six. Every reader catches up faster.
- **The CRC-32 is the `crc` crate's table-driven one** (already a dependency of sqlx): 7.8 times faster, same values.
- **Compatibility.** No file format changed. `EVENT_SNAPSHOT_GROWTH` replaces `EVENT_SNAPSHOT_INTERVAL`. A promotion refuses a journal path that names a different file from the one the warm replica read; start a new warm replica on it instead.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 179 unit tests plus the integration tests, and all three PostgreSQL acceptance tests. Measured on the five-day workload (788 MB, 1,000,000 orders) with a warm replica beside the benchmark, all sharing the office machine's 2 cores:
- promotion of a caught-up warm replica went from 25,816 ms to 58 ms; requested the moment a maximum-rate run stopped, from 103,255 ms to 502 ms until the customer port answered;
- at maximum rate, the warm replica caught up 1.7 s after trading stopped instead of 85 s; its lag now swings within each day between 0 and about 250,000 events instead of growing for the whole run. In one of two runs the lows rose by about 50,000 events a day, so on this machine the maximum rate is at its limit;
- at 20,000 orders/s its lag stayed at a median of 0 and at most 99,000 events, where before it grew to 2.19 million;
- snapshot time over the five days fell from 87.2 s to 11.0 s, and a warm replica's full replay from 29.1 s to 19.2 s;
- a primary restart at the end of the fifth day took 2.9 s (2.4 s in milestone 22).

The independent review found nothing of high severity. Its findings, all fixed:
- the test that the warm replica never builds its core from the stream's cache had stopped testing it, because a reader behind its validated end now reads the journal anyway; the warm replica now catches up before the differing record is published;
- a failing snapshot was retried after every command when the warm replica had started without one; failed attempts now back off by the growth factor;
- an older copy written over the followed file passed promotion when the warm replica lagged; promotion now also requires the stream's published end;
- the reasoning now says that promotion trusts the warm replica's starting snapshot; the CRC test covers a vector longer than 16 bytes; the id check on the promotion path has its own test; and the descriptions of the stream cache and a few numbers were corrected.

It also found an older gap, recorded below: a primary that dies while publishing leaves the warm replica unable to be promoted.

Its re-check confirmed the fixes and that the published-end check never refuses a legitimate promotion. It found that the published end was read under a stream lock that waits, so a primary frozen mid-publication could have made `/promote` hang instead of answering 409; it is now read without waiting, from any header with a valid checksum, as the primary itself trusts it. The back-off on a journal path that names another journal now has a test, and the restart bound is qualified everywhere: it holds while snapshots are being written, plus whatever the warm replica had not applied.

**Part 4, complete (2026-10-07): synchronous replication.** Write-up: `docs/tasks/19-two-machines.md`; measurement: `docs/performance/10-synchronous-replication.md`.
- **The replica.** `stock --replica PRIMARY_ADDR JOURNAL STREAM` dials the primary's `REPLICATION_LISTEN_ADDR` and keeps a byte-identical copy of the journal, and a stream of its own for a warm replica on that machine. It checks every record (framing, checksum, sequence), appends exactly those bytes, syncs, and confirms. It publishes only up to the commit point, the end both disks hold, keeps no record bytes in memory, and stops if its own disk fails.
- **The primary waits for it.** Each group is shipped while the primary syncs its own disk, and published and answered only once the replica confirms it. A restarted replicated primary publishes only what its stream had already published, and serves nothing until the replica holds the rest.
- **The handshake.** The replica's committed end is on both disks, so the primary's journal must reach it at a command boundary with the same next sequence, or the replica is refused. Beyond it, the replica keeps its bytes only if the primary's are identical (CRC-32, read from the files a megabyte at a time); otherwise it cuts back to its committed end, since the primary never synced what it lacks there. A first version compared only positions and sequences: a test showed that a restarted primary can write a different record of the same length in place of one it lost.
- **Pause, never guess.** Waiting for the replica never times out. `/health` answers 503, the operator port shows the mode (`GET /replication`), and `POST /replication/run-alone` lets the primary continue alone until a replica holds everything it synced.
- **A silent link is dropped.** The primary sends a frame at least every second and the replica answers each one; either side drops a link silent for 10 s, and the replica dials again.
- **Compatibility.** No file format changed. Without `REPLICATION_LISTEN_ADDR` the primary behaves as before, except that recovery now always syncs the journal it recovered.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 197 unit tests plus the integration tests, and all three PostgreSQL acceptance tests. Live, with a primary and a replica in two containers on the office machine, each with its own volume:
- the primary paused without a replica (503, an open that got no answer) and applied the open when the replica connected;
- it paused again when the replica was killed, ran alone on request, and turned synchronous again when the replica returned;
- killed and restarted, it was followed again from where the replica was;
- the replica, restarted without its stream file, kept its whole journal;
- with the network cut, both sides dropped the silent link after 10 s, and the replica was back 11 s after the cut.

After every step the two journals were byte-identical. Measured on the same machine, where both containers share 2 cores and one SSD:
- at the maximum rate, 32,372 and 34,511 orders/s replicated, against 44,610 and 43,968 alone;
- at 5,000 orders/s, a p50 of 14.7 and 12.3 ms replicated, against 7.4 and 7.5 ms alone;
- with the replica's files in memory, no difference beyond the noise: on this machine the whole cost is the replica's sync waiting for the same disk;
- after every replicated run the two journals were byte-identical.

The independent review found nothing that lets a command be answered or published before both disks hold it in normal operation. Its findings, all fixed:
- a dead link went unnoticed, so after a power loss or a silent network cut the replica never dialed again: heartbeats and a 10 s timeout on both sides now, and a 5 s limit on each dial;
- a restarted primary trusted records only its page cache held: recovery now always syncs, which closes the same gap without replication;
- the replica went on after its own disk failed: it now stops;
- its committed end, from a stream that is never synced, could be about 30 s old after a power loss: its stream's header is now synced within about two seconds of moving;
- running alone could stay on for good under load: it now ends once the replica holds everything the primary synced;
- a stream that would not open was never retried;
- a restart holding records back wrote its startup snapshot past what it published;
- smaller points about the hello, frame limits, a slow peer, a directory sync and when a promotion reads its replication address.

Two tests could pass without reaching their path, and the new or fixed tests each fail when their fix is removed. A separate check of the replica's memory fix found three of the same problems, and three wording errors in these documents.

Re-checking the fixes, the reviewer confirmed all twelve and found the new tests sound. It found that a warm replica could still lose its own snapshot for good: one beyond what the stream had published, after a power loss left the stream behind or while a restarted primary held records back, was treated as invalid, and the warm replica then wrote no snapshots for the rest of its life. This could happen without replication too. Such a snapshot, at a command boundary of the journal, now counts as ahead, not invalid: the warm replica rebuilds from sequence 1 and writes no snapshot behind it. The replica now reuses its hello until its copy changes instead of re-reading the copy before every dial, a tight test bound was loosened, and the docs' test lists, a count and a sync number were corrected. A final check of those two fixes found both first versions wrong: the cached hello survived a cut, so a replica refilled to the same length could get a record it had just confirmed cut, and the warm replica wrote its first snapshot behind the one it had found. Both are fixed, each with a test that fails without its fix.

**Part 5, complete (2026-10-07): epoch-fenced promotion on the second machine.** Write-up: `docs/tasks/19-two-machines.md`. Decisions confirmed by the owner: the epochs are compared in the replication handshake, and promotion on the second machine is Part 3's: stop the replica process, then promote the warm replica there.
- **Terms.** Each promotion journals `TermStarted { epoch }`, one epoch after the highest its copy has ever indexed, before anything else the new primary writes; the first primary's term is epoch 0 and has no record. It changes no business state, so snapshots keep their format.
- **A term index beside every journal copy** (`<journal>.terms`): each term's epoch, first sequence and offset, saved before the record it lists, checked against the journal when loaded, and rebuilt by one pass over the journal when missing or wrong.
- **Epochs in the handshake.** The primary refuses a replica that has seen a later term (this primary was replaced), and a replica from an earlier term whose committed records go past the start of the next term here (it acknowledged commands after the promotion: a split). The replica refuses a primary from an earlier term than one it holds. A replica from an earlier term is otherwise welcome, and drops what it never acknowledged.
- **Promotion and rejoin.** On the replica's machine the operator stops the replica, whose writer lock makes `/promote` answer 409 until then, and promotes the warm replica; the new primary takes the next epoch before it listens. The old primary rejoins with `stock --replica NEW_PRIMARY JOURNAL STREAM` on its own journal and stream file. A promotion that stops before its term is journaled leaves the term begun in the index: a plain restart on that journal journals it, and a new promotion takes the epoch after it. A primary running alone syncs its stream's end before it answers, so that the split check can read what it acknowledged.
- **Subscribers.** The reporter resumes on the new primary's machine from its PostgreSQL checkpoint, since it is the same journal, and now prints where it resumes. The market-data process starts there like any market-data process.
- **Compatibility.** A new `term_started` event, and the epoch in the handshake frames: a primary and its replica must run the same version.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 207 unit tests plus the integration tests, and all three PostgreSQL acceptance tests. Live, with machine A (primary and reporter) and machine B (replica and warm replica) in two containers with their own volumes:
- A acknowledged 57 commands, then A and its reporter were killed;
- on B, `/promote` answered 409 while the replica ran; after the replica was stopped, the warm replica was promoted into term 1 and, run alone, served 375 ms after the replica stopped;
- B held every acknowledged command, and a reporter started on B resumed at sequence 115, where A's reporter had saved its checkpoint;
- A, restarted as a primary, answered nothing: nothing would confirm it;
- A, restarted as B's replica, cut the 247 bytes of the command it had written but never acknowledged, followed B in term 1, and the journals became byte-identical.

The independent review found nothing that lets an old primary be confirmed after a promotion. Its findings, fixed:
- a promotion that stopped before journaling its term forgot it, and after a power loss its replica, which had synced the term start, was refused for good; a term begun is now journaled by the next plain restart of a primary on that journal;
- the split check read a stream end a primary never syncs, so commands acknowledged alone could be cut after a power loss instead of the rejoin being refused; a primary running alone now syncs it before answering, and the old primary must rejoin with its own stream file;
- the index now sits beside the journal's real path, and a term start out of order stops the replica;
- four missing tests, and several claims in these documents. Not fixed: an index that misses a later term is not noticed, since only an older index put in its place could do that.

Its re-check confirmed the fixes. It found that the first version of the first fix let a promotion take an epoch that, on a replica's copy, may be another primary's term start indexed and never written; a promotion then took the epoch after every term its index held, and only a plain restart journals a term begun. Two tests proved less than they claimed and now check what they say, and a few claims were corrected.

A last independent check of the final changes found a gap left in that fix. A replica's index stops listing a term start it never wrote: when the replica restarts, when a load drops an entry past the journal's end, or when a promotion fails between its two saves. A promotion of that copy could then take the other primary's epoch, and equal epochs skip the split check. The index now keeps the highest epoch it has ever listed, which no cut or drop lowers, and a promotion takes the one after it; the handshake still compares the epochs the journal holds. A promotion also no longer runs a plain restart's check for a term begun. With the final build the live failover passed again (B serving 426 ms after its replica stopped, the journals byte-identical), and removing the fencing's checks and every round's fixes one at a time makes the covering test fail each time: eighteen removals, one check removed under two tests. Not covered by a test: a primary running alone syncing its stream's end, which only a power loss would show.

**Part 6, complete (2026-10-09): measurement and failure tests.** Write-up: `docs/tasks/19-two-machines.md`; measurement: `docs/performance/11-failover-under-load.md`; raw output `docs/performance/results/results-m23.txt`. Decision confirmed by the owner: reproduce the warm replica's stop on an interrupted publication, then fix it.
- **The warm replica waits out an interrupted publication.** A stream writer killed between clearing the ready marker and setting it again used to make the warm replica exit, so the machine had nothing to promote; on the second machine that writer is the replica process, which the runbook kills right before the promotion. Now that error alone (`PublicationInterrupted`, a type of its own in `event_stream.rs`) means wait: nothing is applied or snapshotted, `/status` reports `stream_interrupted`, a restarted writer lets it follow again, and a promotion works as before, since it fences the journal's writer lock and reads the rest from the journal. A warm replica *started* on an interrupted stream still refuses to start.
- **Live failure tests with customers trading:** `~/stock-scripts/m23p6-load.py`, 16 HTTP users in a container sharing the exchange's network namespace, logging every acknowledgement as it arrives and afterwards checking each on the surviving exchange; `m23p6-failover.sh`, `m23p6-faults.sh`, `m23p6-gap.sh`, `m23p6-bench.sh` and `m23p6-mutate.sh` on the office machine.
- **Compatibility.** No file format changed; `/status` on the warm replica has a new field.

Verified on Linux: `cargo fmt -- --check`, `cargo test --locked` with 209 unit tests plus the integration tests, and all three PostgreSQL acceptance tests. The new tests failed before the fix (the reproduction), and each of six removals of the fix fails the test that covers it. Live, two containers sharing the office machine's 2 cores and one SSD:
- the primary killed after 2.2 to 6.7 s of load, ten times, five of them 0.3 s after its network was cut: every one of 83,186 acknowledged orders was on B, B's journal was a byte-identical prefix of A's, the reporter resumed on B at A's checkpoint, and A rejoined as B's replica, cutting its unconfirmed tail (5.7 to 7.1 KB in three runs) to identical journals;
- from the promote request to the first accepted order, 26 to 74 ms, median 42 ms, timed by a client on B (Part 5's 375 to 697 ms included the script's polling);
- the replica killed under load: nothing acknowledged for the 3.4 s until the operator ran the primary alone, `/health` 503; the replica's network cut: nothing acknowledged for the 12.6 s until it came back, the silent link dropped after 10.6 s. Synchronous again about 1 s after each return; 27,308 acknowledged orders, all on the primary, identical journals;
- B's replica killed 40 times under load with the Part 5 and the Part 6 binary: the warm replica never met an interrupted publication (the window is tiny; the tests create that state directly), and the promotion after each series kept every acknowledged order (49,555 and 65,342);
- `--bench`: 45,000 to 56,000 orders/s alone and 34,000 to 36,000 replicated at the maximum rate; p50 7 to 8 ms alone and 11 to 12 ms replicated at 5,000 orders/s; no difference beyond the noise with the replica's files in memory. Journals byte-identical after every replicated run.

The independent review found the code correct. Its findings: the documents still called the warm replica unpromotable after an interrupted publication (updated with this part); a warm replica started on an interrupted stream still exits (recorded among the limitations; it fails closed); and `/status` reported `false` until the follower's first loop even when the catch-up had found the stream interrupted (it now starts from the warm replica's state).

**Milestone 23 complete (2026-10-09).** Every completion criterion above holds:
- an order that would take more than 10,000 resting orders is refused and journaled, the largest order still accepted fits in one record with the gateway's longest ids, no deposit can overflow the totals, and the exchange and the warm replica refuse to start without `JWT_SECRET` (Part 1);
- promotion and reader restart times no longer grow with the journal, measured on the five-day journal (Parts 2 and 3);
- with replication on, killing the primary under load lost no acknowledged command over ten runs and two more promotions, and the replica's journal was a byte-identical prefix of the primary's each time (Part 6);
- with the replica unreachable, killed or cut off, the primary acknowledged nothing until the operator acted or the replica returned (Parts 4 and 6);
- after a promotion the old primary acknowledges nothing, and when it returns it drops its unconfirmed tail and follows the new primary (Parts 5 and 6);
- the reporter continues after a failover from its checkpoint, without a rebuild (Parts 5 and 6);
- throughput, latency and recovery time with replication are measured and written up (`docs/performance/10-synchronous-replication.md`, `11-failover-under-load.md`);
- `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests and an independent review of each part pass.

Not shown: a power loss, and two machines with their own disks and a real network. Every live figure is two containers on one machine.

## 24. Gateway and Trust Boundary

**Status: closed on 2026-10-14 after Parts 1 and 2 (both complete on 2026-10-12). Parts 3 to 6 were dropped by the owner.** This is a learning project, and custody, KYC and protection against abusive customers are not goals. Unlimited deposits, self-set risk limits, invented symbols and floods are therefore accepted, and remain in Known Prototype Limitations. Parts 1 and 2 stay: Part 1's tools and baseline are the starting point of milestone 25, and Part 2 fixed a performance fault (bcrypt on the threads that serve orders). The decision and its reasoning are in `docs/milestones/24-gateway/decisions.md`. The plan below is kept as it was written, as the record of what was considered.

The owner selected it after a review of the code at commit `27ce277` on `dev-rakh`. Its documents are in their own folder, `docs/milestones/24-gateway/` (see "Documentation for this milestone" below). All seven decisions marked *proposed* below were confirmed by the owner on 2026-10-12 as recommended (`docs/milestones/24-gateway/decisions.md`). Measurements run on the office Ubuntu machine over SSH, as milestones 22 and 23 did.

### Why this milestone

Milestones 8 to 23 made every acknowledged command survive a crash, a torn write, and the loss of a whole machine. The customer API, however, lets any customer change that state in ways no exchange allows, or block every other customer. Checked in the code on 2026-10-12:

- **Anyone can create money and shares.** Registration is open (`POST /users/register`). A registered user can credit themselves any amount of cash (`POST /exchange/deposit`, `exchange_controller::deposit`) and any number of shares in any symbol (`POST /exchange/shares/deposit`). Milestone 9 made trades conserve cash and shares, but nothing guards the deposits themselves. The journal already lists both deposits as "placeholders for real custody".
- **Anyone can raise their own risk limit.** `POST /exchange/risk/limits` sets the caller's own daily cap. The design calls risk checks a regulatory requirement ("the exchange is a regulated facility, so we need to make sure it runs risk checks"). A check that the checked user can switch off does not meet it.
- **One customer can block every other customer:**
  - Deposit until the exchange's total cash, or a symbol's total shares, reaches `u64::MAX`. Milestone 23 part 1 added that guard so that no fill can overflow. After it, every later deposit by anyone is refused, and nothing withdraws.
  - Rest 200,000 orders (`MAX_RESTING_ORDERS`). After that, every order that would rest is refused with `BookFull`, for everyone, until orders trade, are cancelled, or expire at the close.
  - Name new symbols. Each symbol ever used keeps an empty book and its execution counter in memory and in every snapshot, so the state grows without limit.
- **Logins and registrations can stall order entry.** `login` calls `bcrypt::verify`, and `register` calls `bcrypt::hash`, both at `DEFAULT_COST` (12). Each call runs 2^12 = 4,096 rounds of bcrypt's expensive key setup. Both run directly inside an `async fn`, so the call occupies a Tokio worker thread until it finishes. `#[tokio::main]` starts one worker thread per logical CPU it sees: 4 on the office i3-7100 (2 cores, 4 threads). Inside a container that count depends on the container's CPU limits, and Part 1 records it. Those same threads parse and route every order request. A cost-12 bcrypt typically takes a few hundred milliseconds on a desktop CPU. That is an estimate, and Part 1 measures it on the office machine. So about four simultaneous registrations, which need no account at all, could hold every worker thread for that long.
- **Login reveals which emails have accounts.** It answers "user not found" for an unknown email and "invalid password" for a known one. It also skips bcrypt for an unknown email, so the response time leaks the same fact.
- **Smaller gateway faults:**
  - `GET /users/` returns a hard-coded list of two invented users. It is a leftover placeholder.
  - The JWT check calls `std::env::var("JWT_SECRET")` on every request instead of reading the key once at startup.
  - Authentication failures at login are answered as validation errors.

The milestone 23 journal had already put "the gateway and trust boundary" first among the candidates. This review confirms that order.

### Why not latency first

This reasoning was given to the owner on 2026-10-12, who had expected latency to be next. It is recorded here with every number and its source so it can be checked later. `00-selection.md` in the milestone folder expands it.

**What the design asks.** The design asks for round-trip latency "at the millisecond level, with a particular focus on the 99th percentile". It measures from the moment an order enters the exchange until its execution returns.

**What was measured.** The figures are from `docs/performance/results/results-m23.txt`, two runs per configuration. The machine is the office Ubuntu machine: two containers sharing its 2 cores and one SATA SSD. Each run is `--bench` at a fixed 5,000 orders/s:

| Configuration | p50 | p99 | p99.9 |
|---|---|---|---|
| One machine (alone) | 7.9 / 7.2 ms | 30.4 / 16.7 ms | 65.0 / 27.6 ms |
| Replicated | 11.4 / 12.4 ms | 28.8 / 49.2 ms | 40.9 / 88.9 ms |

So the median is a few milliseconds, and the p99 is tens of milliseconds. The gap to the design is real, but it is about one order of magnitude, not several.

**Where the CPU time per order goes.** These figures are from `docs/tasks/14-critical-path-performance-v1.md`, "Where the time goes now". They came from an instrumented build in the Docker Desktop VM, at the maximum rate, on disk:

| Step | Time per order |
|---|---|
| prepare (risk, wallet, match plan, settlement checks) | 8.8 µs |
| build envelopes and JSON-encode the record | 6.9 µs |
| commit to the core | 5.7 µs |
| journal write and sync, shared across the group | 6.0 µs |
| mmap publish and callbacks | 3.5 µs |
| release replies | 0.6 µs |

That is about 31.5 µs of the worker's time per order. These are microseconds, not CPU cycles. About 25.5 µs of it is CPU work. The 6.0 µs sync row is not CPU work: it is the worker idling while the disk syncs, shared across a group of about 1,000 orders. The instrumentation slowed that build to about 36,000 orders/s, so the proportions are more reliable than the sums.

**What each recorded latency item would buy:**
- **A binary record format** would remove most of the 6.9 µs of JSON, about a quarter of the CPU per order. That raises the maximum rate, which is CPU-bound. At 5,000 orders/s the worker is mostly idle, waiting for the disk. Saving about 7 µs from a median of about 7,500 µs changes the median by about 0.1%.
- **A pipelined journal sync** lets the worker prepare group N+1 while group N syncs. At the maximum rate the worker sits idle for about 6 ms during each group's sync (`docs/tasks/14`). Overlapping that wait raises throughput and shortens queues under load. At light load it cannot go below one sync, plus one round trip to the replica when replicated. It also lets the in-memory core run further ahead of the disk, which changes the failure rule recorded for milestone 19.
- **CPU pinning** removes scheduler jitter of the order of microseconds, on machines that have spare cores isolated for the pinned thread. The office machine has 2 physical cores. They are shared by the primary, the replica, the warm replica, the MDP, the reporter, PostgreSQL and the load generator. Pinning the worker there would starve the others, so it is not useful on this hardware.

**Part of the median is unexplained.** In run 1 at 5,000 orders/s, the bench made 8,267 syncs in 20.009 s. That is 413 syncs per second, so one group cycle takes 1 / 413 ≈ 2.42 ms on average and holds 12.1 orders. A simple model of the wait:
- an order first waits for the group in progress to finish, about half a cycle: ≈ 1.2 ms;
- then its own group's cycle: ≈ 2.4 ms;
- plus the generator's timer error, at most 1 ms and about 0.5 ms on average (`docs/performance/00-benchmark-harness.md`).

That totals about 4.1 ms, against a measured median of 7.9 ms: about 3.8 ms unexplained. Run 2 had 7,896 syncs in 20.006 s: 395 per second, a cycle of 2.53 ms, a model of about 4.3 ms, and a measured median of 7.2 ms. That leaves about 2.9 ms unexplained. So about 3–4 ms of the median is not accounted for.

*Corrected after Part 1 (2026-10-12):* the same in-process run on the same machine three days later gave a median of 3.6–3.8 ms and about 0.9 ms unexplained, with no change to the trading path. So the machine's state, not the code, moved the median by a factor of two. See `docs/milestones/24-gateway/00-selection.md`, "Correction after Part 1". This is a crude model, not a measurement. Cycles vary in length, and a long cycle collects more orders. Only a build that timestamps each stage can say where the rest goes.

**What has never been measured:**
- **The HTTP path.** `--bench` deliberately skips HTTP, JSON request parsing and JWT (`docs/performance/00-benchmark-harness.md`, "What it does not measure"). The design measures from the gateway, so the latency customers actually see is unknown.
- **The office SSD's raw sync time.** The only raw figure is from the Docker Desktop VM's virtual disk: 1.8 ms at p50 and 22 ms at p99 for a 600-byte write plus `sync_all`.
- **Real hardware.** Every replicated figure is two containers on one machine with one SSD. There is no run on two machines with their own disks.

**The floor is the disk.** On every `fsync`, a consumer SATA SSD without power-loss protection must make the data durable. For a drive without protected cache, that means writing it to flash. Drives with power-loss protection (most server NVMe drives) can acknowledge the flush from cache that a capacitor protects. Their sync then commonly takes tens of microseconds instead of milliseconds. This is general knowledge about drive classes, not a measurement made here.

**Conclusion.** Latency work now would shave microseconds while the milliseconds sit in the disk. The measurements it needs (the HTTP path, a timed breakdown of each stage, the raw sync of the real disk) do not exist yet. This milestone records the HTTP path and the raw sync as its Part 1 baseline, and the latency milestone can reuse them.

### Goal

- Only the operator can create cash, shares, products and risk limits.
- Only listed products trade.
- No single customer can deny service to the others through the customer API.
- The HTTP path is measured end to end, before and after.

### Behaviour

1. **A product list, journaled.**
   - New input `ProductListRequested { symbol }` produces `ProductListed { symbol }` or `ProductListingRejected { symbol, reason }`. Reasons: `AlreadyListed`, or a symbol that fails the gateway's identifier rule.
   - The list is journaled for the same reason as sessions and risk limits. A list read from configuration would make the same journal rebuild different state on a differently configured machine.
   - Operator port: `POST /products` with `{"symbol":"AAPL"}`, and `GET /products`. Customer port: `GET /exchange/products`.
   - A symbol's book is created when it is listed, not on first use.
   - The listing check sits in `prepare_input_event`, the entry point shared by live processing, replay and the warm replica. It refuses three things for an unlisted symbol, each with the reason `UnknownSymbol`:
     - a new order: `OrderRejected` (409, as for other state-dependent refusals such as `BookFull` and `MarketClosed`);
     - a share deposit: `SharesDepositRejected`;
     - a risk limit: a new output, `RiskLimitRejected { user_id, symbol, reason }`, because a risk limit can be refused for the first time.
   - *Proposed:* no delisting in this milestone, and products carry only a symbol (no tick size or lot size). See decision 1.
2. **Deposits and risk limits by the operator only.**
   - Remove the customer routes `POST /exchange/deposit`, `POST /exchange/shares/deposit` and `POST /exchange/risk/limits`.
   - Add operator routes: `POST /accounts/{user_id}/cash`, `POST /accounts/{user_id}/shares` and `POST /accounts/{user_id}/risk-limits`.
   - Customers keep the reads: `GET /exchange/balance`, `/exchange/positions` and `/exchange/risk/limits`.
   - The input events already carry the `user_id`, so this part changes no event and no journal format. Only the operator port can issue them now. The operator port remains loopback-only and unauthenticated, the same trust model as the session commands.
   - A customer can no longer use up the deposit totals. The operator still can, which is acceptable for a trusted operator.
   - *Proposed:* the operator handler checks that the user exists in PostgreSQL before it enqueues the command, so that a mistyped user id cannot receive money. The check is not journaled and is not on the trading path. See decision 2.
3. **A per-user share of the book, journaled.**
   - Each user may have at most `MAX_RESTING_ORDERS_PER_USER` resting orders. *Proposed:* 2,000, which is 1% of the 200,000-order book. It is a compiled-in constant, like `MAX_RESTING_ORDERS`.
   - An order that would rest beyond it is refused as `OrderRejected { reason: "UserBookShareExceeded" }` (409). The decision is made in preparation, so replay reaches the same decision.
   - With deposits by the operator only, a new account has no collateral and can rest nothing. So filling the book with many accounts needs the operator to fund each of them.
   - Changing the constant later changes replay decisions, like the other compiled-in limits: start a new journal when it changes.
4. **Rate limiting per user, in the gateway, not journaled.**
   - Each authenticated user gets a token bucket in the gateway, checked before the command queue.
   - *Proposed:* writes (orders and cancellations) at 100 per second with a burst of 200. Reads in a separate bucket. Limits per IP address on the unauthenticated `register` and `login`.
   - A request over its limit gets `429 Too Many Requests` with `Retry-After`. It never reaches the queue.
   - It is not journaled, and it must not be. The decision depends on arrival times, which replay cannot reproduce. A request refused before the queue changes no state, so replay needs nothing from it.
   - The map of buckets is bounded: idle entries are evicted.
   - *Proposed:* a hand-written token bucket of a few dozen lines rather than a new dependency such as `governor`. See decision 5.
5. **Login and registration hardening.**
   - Run `bcrypt::hash` and `bcrypt::verify` on Tokio's blocking pool (`spawn_blocking`), so the worker threads that serve orders never run them. *Proposed:* also cap the bcrypt calls in flight with a semaphore, so that bcrypt cannot occupy both cores. See decision 7.
   - Answer an unknown email and a wrong password the same way: 401 with "invalid email or password". For an unknown email, verify against a fixed dummy hash, so both answers take the same time.
   - Remove the placeholder `GET /users/`.
   - Read `JWT_SECRET` once at startup into the application state.

### Parts

Each part goes through code, tests, measurement where relevant, its documents in the milestone folder, and an update to this journal.

1. **Baseline, before any change, on the milestone 23 binary.**
   - Build an HTTP load generator. It is open loop: each order is timed from its intended send time, which avoids coordinated omission. Latencies go in an HdrHistogram. It drives the real customer port through Axum, JSON and JWT. See decision 6.
   - Measure order round trips over HTTP at 1,000 and 5,000 orders/s, alone and replicated. Compare them with in-process `--bench` at the same rates: the difference is the gateway's cost.
   - Measure one cost-12 bcrypt on the office machine.
   - Measure order latency while N simultaneous registrations and logins run, for several N: the stall.
   - Measure the office SSD's raw `write` + `sync_all` latency for a 600-byte record, as `docs/performance/00` did on the VM. This gives the later latency milestone its floor.
2. **Login and registration hardening (behaviour 5).** Measure the stall again against the Part 1 baseline.
3. **The product list (behaviour 1).**
   - Snapshot version 6.
   - The shared decoder yields `ProductListed`.
   - The MDP serves books only for listed symbols.
   - The reporter accepts the new events. *Proposed:* no new reporter table (decision 3).
   - The benchmark lists its symbols before setup.
4. **Deposits and risk limits by the operator (behaviour 2).** The benchmark is unaffected, because it sends commands in-process. The live scripts in `~/stock-scripts` move to the operator port.
5. **The per-user book share and rate limiting (behaviours 3 and 4).** Measure the limiter's cost on the HTTP path. Show that a user over the limit does not move other users' latency.
6. **Final measurement and abuse tests.**
   - Run Part 1's measurements again on the final binary.
   - Reproduce every abuse in "Why this milestone" live on the milestone 23 binary. Show the new binary refuses each one, as milestone 23 part 1 did with the sweep and the overflow.

**Why this order:**
- The baseline must be recorded before anything changes; it cannot be recovered afterwards.
- Login hardening is the smallest change, and it is measured directly against that baseline.
- The product list comes before operator share deposits, because a share deposit needs a listed symbol.
- Rate limiting comes last, because it needs the final set of routes.

### Components

- **Core** (`core.rs`, `order_manager.rs`, `matching_engine.rs`):
  - the set of listed products, with books created at listing;
  - a resting-order count per user, rebuilt and validated when a snapshot loads, like the other indexes.
- **Events** (`exchange_event.rs`): `ProductListRequested`, `ProductListed`, `ProductListingRejected` and `RiskLimitRejected`.
- **Runtime** (`runtime.rs`, `types.rs`): `ExchangeCommand` variants for listing and reading products, and the listing check in `prepare_input_event`.
- **Snapshot** (`snapshot.rs`): version 6, holding the product list.
- **Shared decoder** (`committed_batch.rs`), **MDP** (`market_data.rs`), **reporter** (`reporter.rs`): accept the new events.
- **Operator port** (`operator.rs`): the product and account routes.
- **Customer gateway** (`routes/`, `controllers/`, `middleware/`):
  - remove the write routes for deposits and limits;
  - add the rate limiter and its 429 answer;
  - read the key once;
  - run bcrypt off the worker threads;
  - give login one uniform answer;
  - delete `GET /users/`.
- **Benchmark** (`bench.rs`): list products first. A new HTTP load generator, if decision 6 puts it in the crate.

### Compatibility

The product list changes replay. An old journal holds orders on symbols that were never listed, and a new binary would reject them, so replay stops with an `OutputMismatch`. Start a new journal. Remove the old snapshot, the stream file, the market-data state and the probe checkpoint with it. If the reporter's schema changes, add a migration that empties the report for a rebuild. The primary, the replica and the warm replica must run the same version.

### Completion criteria

- No customer route can create cash, shares, products or risk limits. Those routes no longer exist.
- The operator can do each of these through the operator port. Replay, snapshots, the warm replica and replication carry every one of them.
- Orders, share deposits and risk limits on an unlisted symbol are refused and journaled, and replay reaches the same decision. Memory and snapshots no longer grow with symbols that customers invent.
- No user can rest more than `MAX_RESTING_ORDERS_PER_USER` orders, and replay agrees.
- A user over the rate limit gets 429, nothing is journaled for it, and other users' latency does not change beyond the noise (measured).
- Simultaneous registrations and logins do not move order p99 beyond the noise (measured against the Part 1 stall).
- An unknown email and a wrong password get the same status and the same body.
- HTTP round-trip latency is measured at the start and the end, beside the in-process bench, on the office machine. The raw sync latency of the office SSD is recorded.
- Every abuse listed in "Why this milestone" is reproduced live on the milestone 23 binary and refused by the new one.
- `cargo fmt -- --check`, `cargo test --locked` and the PostgreSQL acceptance tests pass, and an independent review of each part is done.
- The milestone folder is complete, as "Documentation for this milestone" requires.

### Not in this milestone

- withdrawals;
- delisting;
- tick size and lot size, unless decision 1 adds them;
- operator authentication and an audit of which operator did what (the operator port stays loopback-only and unauthenticated);
- user roles, KYC, or account approval in PostgreSQL;
- TLS;
- pushed updates (WebSocket);
- latency work: a binary format, a pipelined sync, CPU pinning;
- web-application-firewall or captcha defences beyond simple limits per IP address;
- short selling;
- self-trade prevention by engine-generated cancellation.

### Decisions for the owner

Each decision is confirmed before the part that needs it starts, and is recorded with its date in `decisions.md` in the milestone folder.

1. **Product fields.** Recommended: symbol only. This milestone is about trust, and a tick size changes price validation and the benchmark's workload. Alternative: symbol, tick size and lot size now.
2. **Unknown user ids on the operator port.** Recommended: check the `users` table before enqueueing. Alternative: accept any id that passes the identifier rule.
3. **The reporter and listings.** Recommended: no table; the reporter accepts the events and ignores them. Alternative: a `products` table.
4. **The limits.** `MAX_RESTING_ORDERS_PER_USER` (proposed 2,000). The rates and bursts (proposed 100 writes per second with a burst of 200, reads separate, and limits per IP address for registration and login).
5. **Rate limiter.** Recommended: hand-written token bucket, because it needs no new dependency and is worth learning. Alternative: the `governor` crate (GCRA).
6. **HTTP load generator.** Recommended: Rust, inside the crate. The current client is `~/stock-scripts/m23p6-load.py`, which lives outside the repository. A Python client on the same 2-core machine is unlikely to keep accurate open-loop timing at 5,000 orders/s, so it would measure itself. Alternative: extend the Python script.
7. **Cap on bcrypt calls in flight.** Recommended: yes, with a semaphore. Alternative: rely on the blocking pool alone.

### Documentation for this milestone

The owner's requirement, given on 2026-10-12: learning is the main goal of this project. Everything about this milestone is written down in depth, in its own folder, so that it can be studied cold months later. Nothing is compromised for brevity. Nothing stays only in chat, in a commit message, or in a reviewer's notes. Later milestones use the same layout unless the owner says otherwise.

Folder: `docs/milestones/24-gateway/`. Before the first commit, check that git tracks it (`git check-ignore -v`), because `.gitignore` uses exceptions for some documents.

- **`README.md`.** Reading order, the status of each part, and a short summary of every file.
- **`00-selection.md`.** Why this milestone and not latency. It expands the two sections above: every number with its source file, the machine and setup it was measured on, and the arithmetic of every derived figure. An example is the 413 syncs per second, the 2.42 ms cycle and the 4.1 ms model.
- **`01-theory.md`.** All the background needed to read the code without prior context, with worked examples:
  - trust boundaries, and where each check belongs: in the gateway, or in the deterministic core;
  - authentication versus authorization;
  - which decisions are journaled and which must never be, and why replay forces that split;
  - reference data and listing in real exchanges: security definitions, tick and lot sizes;
  - rate-limiting algorithms: fixed window, sliding window, token bucket, leaky bucket and GCRA, with their trade-offs;
  - how real exchanges throttle: message-rate limits and order-to-trade ratios;
  - password hashing: what bcrypt's cost factor means, the arithmetic of the work factor, and why it is slow on purpose;
  - async runtimes: Tokio's worker model, why a CPU-bound call inside an `async fn` stalls unrelated requests, and `spawn_blocking`;
  - user enumeration and timing side channels;
  - open-loop load testing, coordinated omission, and HdrHistogram;
  - `fsync`, drive write caches and power-loss protection.
- **One file per part** (`part-1-baseline.md`, `part-2-login.md`, and so on). Each covers:
  - what changed, file by file and function by function;
  - why each choice was made;
  - every option rejected, and why;
  - every test added, and what it proves;
  - every live run, with its exact commands;
  - compatibility.
- **`performance/`.** One file per measurement. Each covers:
  - the question it answers;
  - the method: machine, CPU, disk, OS, containers, build, exact command lines, workload, rates and number of runs;
  - tables of results;
  - the arithmetic of every derived number, shown step by step;
  - the noise and error bounds, such as the generator's ±1 ms timer;
  - what the numbers prove and what they do not.

  Raw output goes in `performance/results/` as `.txt` files. `.gitignore` ignores `*.log`, so a raw result saved as `.log` would silently not be committed.
- **`decisions.md`.** Every owner decision, with its date and the options that were shown.
- **`glossary.md`.** Every term the other files use: GCRA, coordinated omission, power-loss protection, epoch, and so on.

### Progress

**Part 1, complete (2026-10-12): the baseline, before any change.** Write-up: `docs/milestones/24-gateway/part-1-baseline.md`; measurements in its `performance/01` to `05`, with raw output and the scripts that produced it.
- **New tools, no change to the exchange's behaviour:**
  - `stock --http-bench`: an open-loop HTTP load generator. It sends the same orders as `--bench` and adds `--logins`, `--deadline` and a `/health` control.
  - `stock --sync-probe` and `stock --bcrypt-probe`.
  - The exchange prints its Tokio worker count at startup.
  - The scripts now live in the repository.
- **The disk.** One durable 600-byte append on the office SSD takes 0.80–0.90 ms at p50, 1.31–1.39 ms on average and 5.3–5.5 ms at p99. Almost all of that is the `sync_all`. On tmpfs the same append takes 1.6 µs.
- **bcrypt.** One call at cost 12 takes 229 ms. The whole machine maxes out at about 15 per second.
- **HTTP's share.** HTTP adds 0.3–0.9 ms to an order's p50. At 5,000 orders/s it adds 49% to the p99 alone and 71% replicated.
- **The login stall.** One login loop pushes the order p99 from about 14 ms to 0.1–0.2 s, and four push the p50 to 0.15–0.19 s. With sixteen, connections already open are still served slowly, but new connections, to the operator port too, get no answer for tens of seconds. In one of three runs with eight, a few orders were never answered, so the logins never stopped, and new connections went unserved for the 17 minutes until the run was stopped.
- **Enumeration.** A login reveals whether an email has an account, by its body and by a 230 ms timing gap.
- **A correction.** The same in-process bench ran twice as fast as on 2026-10-09, with no change to the trading path. The "3–4 ms unexplained" in "Why not latency first" did not recur, and only same-day comparisons are trustworthy on this machine.

Verified on the office machine: `cargo fmt -- --check`; `cargo test --locked` with 211 unit tests plus the integration tests. An independent review found no code bug. It found ten errors in the documents, all fixed and listed in the write-up. The most important: the "17 minutes with no order journaled" was a misread timestamp. The orders had been journaled, a few were never answered, and so the login loops never stopped.

**Part 2, complete (2026-10-12): login and registration hardening.** Write-up: `docs/milestones/24-gateway/part-2-login.md`; measurements in its `performance/06` and `07`.
- **bcrypt off the worker threads.** It runs on Tokio's blocking pool, at most `max(1, CPUs / 2)` calls at a time (2 on the office machine). The permit moves into the blocking closure, so a disconnecting client cannot free it while its bcrypt still runs.
- **One answer for every failed login.** `401 invalid email or password`, after exactly one full bcrypt computation on every path. When the email is unknown, it verifies against a dummy hash computed at the accounts' cost before the port opens.
- **Smaller fixes.** The JWT key is read once. `GET /users` is gone.
- **Measured the same hour against the Part 1 binary:**
  - with 4 or 16 clients logging in back to back, orders keep a p50 of 2.8–4.0 ms and a p99 of 10–19 ms, the same as with no logins (Part 1: 0.21–0.58 s and 0.74–2.64 s);
  - at C = 16, no connection is refused and nothing goes unanswered (Part 1: 33–1,415 failed connections, 14 unanswered, its container at 383–390% CPU);
  - logins are capped at 8.1–8.3 per second and wait instead, with the p50 Little's law predicts (1.9 s at C = 16);
  - known and unknown emails now get the same 401 in the same time (230.5 against 229.9 ms on average; Part 1: 230.3 against 0.59 ms).
- **The measuring tool had a bug.** Its login loops retried failed logins without yielding, which made the generator spin at 366% CPU in one run. They now back off 10 ms. The C = 16 numbers above were re-measured with the fix, and they confirm Part 1's starvation was the exchange's own.
- **Not fixed:**
  - registration still answers `user already exists`;
  - a failed login now costs a full bcrypt, so fake logins can crowd out real ones until Part 5's per-IP limits.

Verified on the office machine: `cargo fmt -- --check`; `cargo test --locked` with 213 unit tests plus the integration tests. Three mutations, each removing one fix, each make the covering test fail. An independent review found no gateway bug, and its findings were all fixed:
- an unreadable stored hash answered `400`, revealing the account. It now answers `401`, is logged, and costs one bcrypt, like every other path;
- the first correction of Part 1's C = 16 rows wrongly blamed a spinning generator; those runs' own CPU figures rule it out.

Rules for every file:
- every claim that carries a number gives its source: measured in this milestone (which file), an earlier document (file and section), or an outside reference;
- every derived figure shows its arithmetic;
- an estimate is labelled as an estimate;
- the unit is always explicit: µs of CPU time, not "CPU cycles".

## 25. Latency

**Status: selected and planned on 2026-10-14; the owner accepted the plan and decisions 1–4 as recommended. Parts 1 and 2 complete on 2026-10-14; Part 3 dropped by the owner on 2026-10-19; Part 4 complete and the milestone closed on 2026-10-19.** Progress is recorded at the end of this section. Its documents will go in `docs/milestones/25-latency/`, with the same layout and depth rules as milestone 24 (see milestone 24, "Documentation for this milestone"). The decisions marked *proposed* need the owner's confirmation before the part that uses them.

### Why this milestone

- The owner's own interest.
- The design asks for round-trip latency "at the millisecond level, with a particular focus on the 99th percentile".
- Milestone 24 part 1 built the measurement base this milestone needs:
  - `--http-bench` drives real HTTP traffic, with a `/health` control;
  - `--sync-probe` measures the disk alone;
  - the scripts run on the office machine.

What is known, measured on the office machine on 2026-10-12 (`docs/milestones/24-gateway/performance/`):
- **The disk.** One durable 600-byte append takes 0.80–0.90 ms at p50, 1.31–1.39 ms on average and 5.3–5.5 ms at p99. Almost all of it is `sync_all`; on tmpfs it takes 1.6 µs.
- **At 5,000 orders/s, alone, in-process:** p50 3.6–3.8 ms and p99 about 19 ms. The group cycle is 1.46–1.65 ms, and the flush is about 80–90% of it.
- **Through HTTP**, the p50 is 0.3–0.9 ms higher. The p99 is 49% higher alone and 71% higher replicated, likely from CPU contention (not measured).
- **A simple model leaves about 0.9 ms of the median unexplained** at 5,000/s. On 2026-10-09 the same model left 3–4 ms, which did not recur.
- **The CPU work per order** is about 25.5 µs in the Docker Desktop VM (`docs/tasks/14`). JSON encoding is about 6.9 µs of it.
- **The machine is shared and noisy.** Its speed differed by a factor of two between two days, so every comparison must be made on the same day, interleaved, over several runs.

The current group commit (`ExchangeRuntime::run`, `handle_group`, `flush` in `runtime.rs`):
1. The worker takes every queued command, up to 1,024.
2. It prepares and commits each one in memory.
3. It writes all their records with one `write_all` and one `sync_all`. When replicated, it ships them to the replica while it syncs, and waits for the replica's confirmation.
4. It publishes each batch to the mmap stream and runs the callbacks.
5. It releases every held reply.
6. Only then does it take the next group.

During step 3 the worker does nothing: on the office SSD that is about 1.3 ms on average and 5 ms at p99, per group.

### Goal

Know, stage by stage, where an order's time goes, then shorten the stages that dominate. The rule is never to optimize a stage that has not been measured. Durable-before-visible must hold throughout: no reply, read, publication or callback may reveal a command before the disk holds it, or both disks when replicated.

### Parts

1. **Where the time goes.**
   - **What to time.** Each order's progress through every stage:
     - the HTTP handler receives it;
     - it enters the command queue;
     - the worker dequeues it;
     - prepare ends;
     - its record is encoded;
     - it is committed in memory;
     - its group's `write_all` ends;
     - its group's `sync_all` ends;
     - the replica confirms (when replicated);
     - it is published to the mmap stream;
     - its reply is sent;
     - its HTTP response is written.

     Also, per group: its size, its sync time, and its confirmation time.
   - **How it is reported.** Percentiles and means per stage, so that the sum of the stage means can be compared with the measured end-to-end mean. Means add up; medians do not (milestone 25 theory, §2). *(Corrected 2026-10-14: this first said medians.)*
   - **What it should answer.** Where the unexplained part of the median goes, how much is queueing (waiting for the group in progress), and what share of the p99 is the disk.
   - *Proposed:* the timing is compiled in only with a cargo feature (`stage-timing`). The normal build is then unchanged and pays nothing, and the cost of the timing itself can be measured by comparing the two builds. See decision 1.
2. **A pipelined journal sync.**
   - **The change.** A second thread does the `sync_all` (and the replica's confirmation) of group N, while the worker stages group N+1. The replies, reads, publication and callbacks of group N are released only when group N is durable, and always in group order.
   - **Expected effect.** Higher maximum throughput (the worker no longer idles during a sync), and shorter queues, so a lower p99, under load. The p50 at light load cannot fall below one sync, plus one round trip when replicated.
   - **Pipeline depth.** *Proposed:* depth 2: one group syncing while one is staged; the worker waits if both slots are full. See decision 2.
   - **The failure rule.** It is milestone 19's rule extended: after a failed sync, the in-memory core may be up to two groups ahead of the disk. The worker halts, neither group is answered, and only a restart that recovers from the journal goes on. The tests of milestone 19's group commit are reworked for two groups in flight.
3. **A binary record format, only if Part 1 shows JSON encoding matters.**
   - **Expected effect.** It replaces JSON in the journal record, about 6.9 µs of CPU per order, which mainly raises the maximum rate.
   - **What it changes.**
     - The journal format, with a new magic (`EXCHLOG3`): start a new journal.
     - Every reader of records: recovery, the replica's checks, the mmap stream, the shared decoder, the MDP, the reporter and the probe.
   - **The format.** *Proposed:* decided at this part, from what Part 1 measures. The options are a hand-written fixed layout, or a crate such as `postcard` or `bincode`. See decision 3.
   - If Part 1 shows encoding is a small share of what dominates, this part is dropped and the reason recorded.
4. **Final measurement.**
   - **What is compared:**
     - the milestone 24 part 2 binary against the final binary;
     - on the same day, interleaved;
     - in-process and HTTP;
     - alone and replicated;
     - at 1,000 and 5,000 orders/s, and at the maximum rate in-process.

     *Proposed:* three runs per configuration instead of two (decision 4).
   - **tmpfs runs** separate the software's share from the disk's.
   - **CPU of each container** is sampled during the HTTP runs, to settle milestone 24's open question of CPU contention in the tail.

### Decisions for the owner

1. **How the stage timing is built in.** Recommended: a cargo feature, off by default. Alternative: always on, which has a measurable cost on every order.
2. **The pipeline depth.** Recommended: 2 (one group syncing, one staging). Alternative: deeper. That needs more memory held ahead of the disk, a harder failure rule, and gains little when one sync is already shared by many commands.
3. **The binary format** (only if Part 3 goes ahead). Hand-written, or a crate. Decided after Part 1.
4. **Runs per configuration.** Recommended: 3. Alternative: 2, as in milestone 24, which proved too few to settle the tails.

### Completion criteria

- A stage-by-stage breakdown of an order exists for in-process and HTTP, alone and replicated, at 1,000 and 5,000 orders/s. The sum of its stage means accounts for the measured end-to-end mean, and any remainder is stated. *(Corrected 2026-10-14: this first said medians.)*
- **Pipelined sync behaviour:**
  - durable-before-visible holds: tests prove that no reply, read, publication or callback of group N happens before group N, and every group before it, is durable;
  - a failed sync with a second group in flight answers neither group, halts the worker, and recovers from the journal;
  - replication still never acknowledges a command one machine lacks.
- **Pipelined sync results:** the maximum rate and the p99 at 5,000 orders/s are measured against the milestone 24 part 2 binary, on the same day, with three runs each. Either the improvement is beyond the spread between runs, or the reason it is not is explained.
- **The binary format:** either built and measured, or dropped with Part 1's numbers as the reason.
- `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests, and an independent review of each part all pass.
- `docs/milestones/25-latency/` is complete, written in depth.

### Not in this milestone

- CPU pinning: the office machine has 2 cores shared with about 30 containers;
- lock-free ring buffers or Crossbeam in place of the command queue;
- new threads for trading components, other than the one sync thread;
- per-symbol workers;
- mmap ingress;
- FIX or SBE;
- new hardware. A server SSD with power-loss protection would lower the floor by about two orders of magnitude; that is recorded as the largest lever, but it is outside the software.

### Progress

**Part 1, complete (2026-10-14): where the time goes.** Write-up: `docs/milestones/25-latency/part-1-stage-timing.md`; measurements in its `performance/01` and `02`.
- **The timing itself.** A cargo feature, `stage-timing`, times 16 stages of an order:
  - from the HTTP handler, through the queue, the in-group waits, prepare, encode and commit;
  - then the group's write, sync, confirm, publish and release;
  - to the reply.

  The worker records into thread-local histograms and merges once per group. The report is printed by `--bench`, and served on the operator port (`GET /stages`, `POST /stages/reset`). `--http-bench` reads it. The normal build is unchanged: `PlaceOrder` is now built through one constructor, and the un-replicated flush calls `write_records` + `sync` instead of their wrapper. Both benchmarks now print the end-to-end mean.
- **What the timing costs.** Nothing measurable at 5,000 orders/s, and about 3% of throughput at the maximum rate. Measuring that exposed a run-order effect: the second run of a back-to-back pair was about 9.5% faster at the maximum rate. Comparisons must alternate their order.
- **The stage means add up to the end-to-end mean.** Not covered by any stage:
  - 0.9–1.2 ms in-process, and 1.5–2.8 ms outside the server on HTTP: the generator's own lateness and the client side, 10–24% of what a benchmark reports;
  - 0.34–1.09 ms of reply wake-up on HTTP.
- **At 1,000 and 5,000 orders/s the disk dominates.** `Queue` + `Sync` + `Confirm` are 42–69% of the latency. The order's own CPU (prepare, encode, commit) is 33–91 µs, 0.2–1.3%. `Encode` alone is 0.05–0.33%.
- **At the maximum rate one group's cycle is 22.2 ms alone and 29.0 ms replicated:**
  - staging, 13.0 ms;
  - sync, 6.0 ms (replicated: 8.3 ms, plus a 4.4 ms confirm);
  - publish, 2.4 ms;
  - write and release, 0.8 ms.

  That gives about 45,800 and 34,900 orders/s, which agrees with what was measured.
- **Projections, not measurements:**
  - a pipelined sync could raise the maximum rate by about 37–66% alone and about 79% replicated, but would gain at most about 8% of latency at 5,000 orders/s;
  - a binary format could gain at most about 15% of maximum throughput and nothing measurable in latency.
- **One disturbed run.** HTTP replicated at 5,000/s, run 1, had a disk stall (syncs up to 73.5 ms) and is flagged.
- **Verified.** Tests pass: 213 in the normal build and 216 in the feature build. A mutation check, run alone with its own cargo target volume, fails the test for each of three removed recordings. An independent review found no code defect. Its documentation findings were all fixed; they are listed in the part's write-up. The most important was the undisclosed disturbed run. A final check of the fixes found the plan's acceptance criterion still saying "medians" and a broken table, both fixed.

**Owner, 2026-10-14:** "go ahead with next steps of our current milestone", taken as confirming Part 2. Part 3 was not answered.

**Part 2, complete (2026-10-14): a pipelined journal sync.** Write-up: `docs/milestones/25-latency/part-2-pipelined-sync.md`; measurement in its `performance/03-pipelined-sync.md`.
- **The change.** A journaler thread syncs the journal, and with replication ships to the replica and waits for its confirmation. The worker writes each group and goes on to stage the next. Each sync covers everything written before it began, so groups written during one sync share the next (one sync per group would make moderate load slower than no pipeline). At most 2,048 commands (two full groups) wait for the disk; then the worker waits too. A group is published, its callbacks run and its replies (reads included) are sent only once the disk holds it, in queue order. The worker waits on the queue and the journaler at once, through a current-thread Tokio runtime used only to wait.
- **Failure rules.** A failed sync answers everything in flight unavailable, publishes nothing, halts, and the journaler never syncs again. A staging fault or a failed write drains the groups before it, then halts. A failed publication fails its group and all later ones; running alone, the whole pass. Tested through the worker loop: the failed sync with a group in flight (answers, nothing published, halt), and the staging fault behind a group in flight (drain, then halt). A failed write is tested only without a group in flight; draining before that halt, the journaler never syncing again, and the publication rule's multi-group and running-alone cases are checked by reading the code only.
- **A bug the measurement found.** The first version shipped every group to the replica as soon as it was written. The replica syncs once per frame, so at moderate load it did about ten times the primary's syncs and fell seconds behind: p50 2.2 s at 1,000/s replicated, and HTTP orders unanswered. All tests had passed. Fixed by shipping once per sync from the journaler, with a test that guards it.
- **Results** (office machine, 3 interleaved runs each, alternating order):
  - maximum rate +41.5% alone (45,975 → 65,057 orders/s) and +36.7% replicated (35,297 → 48,256);
  - mean latency at 5,000/s down 10–28%, beyond the spread between runs in three of four configurations;
  - at 1,000/s, no change beyond the spread.

  Alone, the worker's cycle (16.12 ms per group) predicts 62,800 orders/s; the timed build measured 60,899. Replicated, the shared SSD's sync plus confirmation (18.56 ms) became the limit. Latency fell more than Part 1 projected (at most about 8%), because orders no longer wait for the rest of a large group to be staged.
- **Part 1's open question closed.** Measured per order (the new `WaitForSync` stage), the wait for the disk accounts for `Worker` to within 1–3%, against 12–25% not covered when only per-group stages existed. That is consistent with Part 1's size-bias explanation.
- **Verified:**
  - `cargo fmt`; 218 tests (normal build) and 221 (feature build); the 3 PostgreSQL acceptance tests;
  - a mutation check of 6 mutations, each failing its test;
  - two rounds of independent review: the reviews found no way to show anything before it is durable, or out of order; three code and test fixes were made (a test made deterministic, the publication-failure answers, running alone after a failed publication), plus fixes to the summary awk and the documents.

**Part 3, dropped (owner, 2026-10-19):** "drop part 3, push and start part 4". A binary record format would gain nothing measurable in latency (`Encode` is 0.05–0.33% of it) and at most about 23% of the maximum rate alone (an estimate; `Encode` is about 19% of the worker's 15.9 µs per command), for a journal format change every reader must follow. Reasons in `docs/milestones/25-latency/decisions.md`.

**Part 4, complete (2026-10-19): the final measurement.** `docs/milestones/25-latency/performance/04-final-measurement.md`. The milestone 24 part 2 binary against the final binary (`67fb6bc`), 84 interleaved runs on the office machine, plus two follow-ups (stage-timing builds; a balanced 6-run order check):
- **Maximum rate:** replicated +77.2% (28,127 → 49,842 orders/s, beyond the spread); alone +17.0% (43,916 → 51,367), within the spread (milestone 24 alone ranged 36,756–48,597). The cycle model gives +28.3% for that day's stages (sync 4.47 ms, shorter than on 2026-10-14, so less to hide); the timed builds measured +25.8%.
- **p99 at 5,000/s:** lower in all four configurations, by 6–49%; HTTP replicated 55.7 → 28.7 ms, beyond the spread.
- **Median at moderate load:** within the spread, both directions, except in-process replicated at 1,000/s (−16%, beyond it). At 5,000/s in-process replicated, three runs said +50%, but the pipeline ran second in two of three pairs; 6 balanced runs gave +14.6%, within the spread. The journaler keeps the sync path busy about 98% of the time (against about 78%), and its median doubled in the runs where its syncs were longer (3.2–3.4 ms against 2.1 ms). Why those syncs were longer is unproven (the shared SSD is the likely cause, an inference).
- **tmpfs (no disk):** alone the pipeline costs 124 µs at p50 and gains nothing; replicated it still raises the maximum rate by 21%, by inference by hiding the replica's round trip. On the SSD, replicated, milestone 25 reaches 81% of its tmpfs rate against milestone 24's 56%.
- **CPU:** in every HTTP configuration the host was busy, on average, 1.8–2.3 of its 4 hardware threads (2 cores); the exchange at most about one thread. The HTTP tail is not average CPU saturation (sub-second bursts and core sharing not ruled out); milestone 24's open question is answered as far as one-second samples can.

**Milestone 25 closed on 2026-10-19.** Follow-ups recorded, not planned: a probe timing `sync_all` with and without a concurrent appender; runs on two machines; an option to delay the journaler's next sync to protect the median at moderate load.

## 26. Pushed Updates

**Status: planned on 2026-10-19; awaiting the owner's review of the plan and decisions 1–6. No code until then.** Progress will be recorded at the end of this section. Documents go in `docs/milestones/26-pushed-updates/`, with the same layout and depth rules as milestones 24 and 25 (the repository's `CLAUDE.md`).

### Why this milestone

- **A functional requirement is unmet.** The design says a client can "receive matched trades in real-time" and "view the real-time order book" (`stock-exchange-system-design.md`, functional requirements). Today a client must poll `GET /marketdata/orderbook/{symbol}` (the MDP, port 4001) and `GET /exchange/executions` (port 4000).
- **The design places it off the trading path.** The market data publisher "rebuilds the order book and candlestick charts, then publishes to subscribers", and the market data and reporting flows are not on the critical path. The MDP already follows the committed mmap stream and holds every L2 book. It lacks only a way to push.
- **It is new learning ground:**
  - fan-out to many clients;
  - a snapshot followed by sequenced deltas;
  - gap detection and resynchronization;
  - backpressure and slow consumers;
  - authentication on long-lived connections.

  Milestones 24 and 25 were about one order's path. This one is about one event reaching many readers.
- **The cost is outside the exchange.** Both feeds read the committed stream, as the MDP and the reporter do, so the worker, the journal and replication are unchanged. Durable before visible holds by construction: the stream publishes only what the journal synced, and with replication only what the replica confirmed.

What is known now:
- **The MDP keeps up, as far as measured.** It applied about 56,000 commands/s on catch-up and stayed within a second of the exchange live at up to about 34,500 orders/s (`docs/performance/05-market-data-keeps-up.md`). Since milestone 25 the exchange's maximum rate is about 50,000–65,000 orders/s on the office machine. Whether the MDP keeps up at that rate, with deltas to compute, is not measured: Part 3 measures it.
- **Public data is already separated from private.** The MDP's projection holds no user ids, by rule. Milestone 14 says "user ids ... must never be exposed by the market-data endpoint", and a test checks that its state file contains none.
- **Fills carry no user ids.** `ExecutionCreated` names only the buy and sell order ids. The incoming order's user is in the same batch (`NewOrderRequested`); the resting order's user is not. So a private feed must keep its own `order_id -> user_id` map, built from accepted orders, as the core and the reporter do.
- **No WebSocket code exists.** Axum's `ws` feature is off; turning it on would add `tokio-tungstenite`.
- **Authentication reads only the `Authorization` header** (`AuthUser`). Browsers cannot set that header on a WebSocket handshake.

### Goal

Push every change a client is entitled to see, as soon as it is durable, without polling and without touching the trading path:
- **public:** the L2 book of each subscribed symbol, as a snapshot followed by deltas with sequence numbers, so a client always knows whether its copy is complete;
- **private:** each user's own fills and order status changes (accepted, partially filled, filled, canceled, expired, rejected), to that user only.

A client that falls behind is told so and resynchronizes; it never silently holds a wrong book. The exchange's throughput and latency must be unchanged with the feeds running: the mean must differ by less than 10%, or the difference must be explained.

### Parts

1. **The public L2 feed, in the MDP.**
   - **The change.** A WebSocket endpoint on the MDP's port: `GET /marketdata/ws`.
     - The client subscribes with `{"type":"subscribe","channel":"book","symbol":"AAPL"}`.
     - It gets a `snapshot`: every level, with two sequences:
       - `seq`, the stream point the snapshot reflects;
       - `last`, the sequence of that symbol's latest delta (0 if none).
     - It then gets a `delta` for every committed batch that changes that book. A delta carries:
       - the changed levels with their new quantities (0 removes a level);
       - `seq`, the batch's last envelope sequence;
       - `prev`, the sequence of the symbol's previous delta.
     - **The gap rule, in this order.** First, a delta whose `seq` is at or before the snapshot's `seq` is already in the snapshot, and is dropped. Then a delta is in order when its `prev` equals the last channel sequence the client knows: the snapshot's `last` at first, then each applied delta's `seq`. Any other `prev` is a gap, and the client resubscribes.
     - **`last` outlives the book.** The MDP removes a book when it empties, at the latest at every close. So each symbol's last delta sequence is kept per symbol, outside the book map. Otherwise the delta after a book emptied would carry `prev` 0, and every subscribed client would see a false gap.
     - **Symbols.** The MDP has no symbol registry, and it removes a book when the book empties, so after every close every book is gone. A subscription to any valid symbol identifier is therefore accepted, and an empty or not-yet-traded book gives an empty snapshot. Only an invalid identifier is refused.
     - Also in scope: unsubscribe, several symbols on one connection, and ping/pong.
   - **How.**
     - The follower thread already applies each batch under the view's write lock. It will record which levels the batch touched. It sends one delta per changed book to that symbol's fan-out channel **while it still holds the write lock**. A send only places the message in a queue; it never waits for a client.
     - A subscriber takes its snapshot and joins the channel under the same lock, so no delta can fall between the two.
     - When the follower halts, the view is withdrawn (`WithdrawOnExit`). Every feed connection is then closed with code 1011, not left open and silent.
   - **Slow consumers.** Each connection has a bounded queue. A client that falls behind is disconnected with a reason (decision 3): when its queue overflows, or when one write to its socket does not finish within a deadline. A client that stops reading fills its TCP buffers. Its task then waits in the write, never reads the queue again, and would never notice the overflow, so the write deadline is required. The client resubscribes and gets a fresh snapshot.
   - **Proof.**
     - An oracle test, in-process: a recorded history replayed through the projection and the feed. A client applying snapshot plus deltas must equal the projection's own view and the core's `l2_snapshot` at every sequence. The REST view cannot be the oracle per sequence: it carries no sequence, is limited to 50 levels, and answers 404 for an empty book.
     - Live, at quiescence: the client's book equals the REST view, within 50 levels, with 404 meaning empty.
     - Tests for:
       - gaps, by a test that withholds one delta;
       - resubscription;
       - the queue overflow and the write deadline;
       - malformed requests and invalid symbol identifiers;
       - an empty book before its first order and after the close;
       - the follower halting, which closes every connection;
       - a book that one user's orders leave locked or crossed (bid ≥ ask), which the feed must publish as it is.
2. **The private feed, in its own process.**
   - **The change.** A new process, `stock --user-feed JOURNAL STREAM STATE_FILE [LISTEN_ADDR]` (default `127.0.0.1:4006`), follows the committed stream like the MDP.
     - It keeps every open order's owner: `order_id -> user_id, symbol, side, remaining`.
     - It saves that map with its checkpoint in its own state file (mode 0600).
     - It pushes to each connected user, each message with the stream sequence:
       - `fill`: execution id, order id, symbol, side, price, quantity, and the order's new remaining quantity. The execution id is the one the REST `GET /exchange/executions` returns for the same trade: the first execution of the pair. Execution ids count per symbol book, so `(symbol, execution_id)` is the unique key;
       - `order`: status changes.
     - **Who receives what.** Order ids are chosen by the client and shared across all users within a day, so the owner map must be consulted only for orders it accepted:
       - an accepted order, its fills, its cancellation, and its expiry at the close go to its owner, from the map. For a fill, the incoming order's owner is in the same batch, and the resting order's owner comes from the map;
       - a rejected new order goes to the user named in that input, and never touches the map. The map may already hold another user's live order with the same id;
       - a refused cancellation goes to its requester, named in the input. The requester may not be the order's owner, and the owner is not told;
       - a fill always involves two different users. The engine skips the aggressor's own resting orders, so no trade has the same user on both sides.
     - The map empties at the close, when every resting order expires, so order ids reused on the next day are new entries.
   - **Authentication.** The first message must be `{"type":"auth","token":"<JWT>"}`, within a deadline. The token is verified with the same secret and rules as the HTTP gateway (decision 4). The connection closes when the token expires.
   - **Catching up after a disconnect.** The feed does not replay history. On connect it says which sequence it is at. Anything earlier is read once through the existing REST endpoints (`GET /exchange/executions`, `GET /exchange/orders/{id}`). Decision 5.
   - **Proof:**
     - both parties of a fill are told, each with their own order id and side;
     - a user never receives another user's events: an isolation test with several users trading against each other;
     - an order id that collides with another user's live order: the rejection goes only to the submitter;
     - a cancellation of another user's order: the refusal goes only to the requester;
     - cancellations, expiries at the close, and rejections;
     - restart from the saved state;
     - authentication failures, token expiry, and the deadline.
3. **Measurement.**
   - **A feed client generator, `--feed-bench`.** It runs many WebSocket clients (1, 100, 1,000), subscribed to books and private feeds, while `--http-bench` drives orders. For each message it records the time from the order's intended send to the message's arrival, by wall clock on one host, in an HdrHistogram.
   - **What is measured:**
     - push latency at 1,000 and 5,000 orders/s, alone and replicated;
     - how push latency grows with the number of clients;
     - each feed process's CPU, with milestone 25's cgroup sampler;
     - whether the MDP still keeps up at the exchange's maximum rate, now that it also computes deltas;
     - the exchange's throughput and latency with the feeds running against without, interleaved, in six runs each, with the order balanced. Milestone 25 found that three runs in a 2-to-1 order can mislead. This is the completion criterion.
   - **Slow consumers under load:** a client that stops reading is disconnected, and the other clients are not delayed.
4. **Final review and close.**
   - The documents are complete.
   - Every test and measurement is recorded with its command and result.
   - The independent reviews are done.

### Decisions for the owner

1. **The transport.**
   - Recommended: WebSocket, through axum's `ws` feature, which adds `tokio-tungstenite` and its small dependencies. One connection carries several subscriptions, in both directions, with ping/pong.
   - Alternative: Server-Sent Events. They are built into axum today, with no new dependency. They are one-way only, with one stream per connection, and come with automatic reconnection and a `Last-Event-ID` header to resume.
2. **Where each feed runs.**
   - Recommended: the public feed in the MDP, which already has the books and no user data; the private feed in a new process, so that user ids never enter the public process.
   - Alternative: the private feed in the primary (port 4000), where authentication already lives. Not recommended: a slow-consumer or memory problem would then share the trading process.
3. **Slow consumers.**
   - Recommended: a bounded queue per connection, and a deadline on each socket write. When the queue is full, or a write misses its deadline, disconnect with a reason. The client then resynchronizes: a fresh snapshot, or REST for private history.
   - Alternatives:
     - drop old messages: this breaks the deltas silently;
     - conflate: send a fresh snapshot instead of the queued deltas. There is no disconnect, but it needs more code, and it does not apply to private events.
4. **Authentication on the private feed.**
   - Recommended: the JWT in the first message, with a deadline, and the connection closing when the token expires.
   - Alternatives:
     - the token in the URL query: simple, but it ends up in logs and history;
     - the token in the `Sec-WebSocket-Protocol` header: it works in browsers, but it is unusual;
     - a cookie, which browsers send on the handshake, with a check of the `Origin` header: the gateway issues bearer tokens only, not cookies, so this would need a login change.
5. **Resume after a disconnect.**
   - Recommended: no replay. The feed reports its sequence, and the client fills the gap once through REST.
   - The limits of the REST catch-up, which the client must accept:
     - REST holds one trading day. A disconnect across a close and the next open loses the previous day's fills and expiries, and no API serves them;
     - REST replies carry no stream sequence, and the primary's REST is usually ahead of the feed, so the client must deduplicate, on `(symbol, execution_id)` for fills;
     - `GET /exchange/orders/{id}` needs the order id in advance.
   - Alternative: a replay buffer of the last N events per user, so that a quick reconnect resumes with no REST call. It needs more state, and it is a second source of truth for history.
6. **Book depth.**
   - Recommended: full-depth L2 snapshot and deltas. The client keeps the whole book and shows the depth it wants.
   - Alternative: only the top N levels, as a fresh top-N snapshot on every change. It is simpler for the client and needs no special case when a level leaves the top N, but costs more bytes per change.

### Completion criteria

- **Public feed:**
  - a client applying snapshot plus deltas equals the projection's view and the core's `l2_snapshot` at every sequence of a replayed history, and the REST view (within 50 levels) live at quiescence;
  - gaps are detectable from the sequences;
  - a slow client is disconnected with a reason and can resubscribe;
  - no user id ever appears in a public message (tested).
- **Private feed:**
  - both parties of every fill are told, each with their own order and side;
  - every order status change reaches its owner;
  - no user receives another user's event (tested with several users, a colliding order id, and a cancellation of another user's order);
  - authentication, the deadline and token expiry are enforced;
  - a restart resumes from the saved state.
- **Durable before visible:** no message is sent for anything the committed stream has not published. Both feeds read only the committed stream. A test holds a sync, as in milestone 25, and sees no message before it is released.
- **Measured, on the same day, interleaved, six runs each with the order balanced:**
  - push latency against the number of clients;
  - the feeds' CPU, and whether the MDP keeps up at the maximum rate;
  - the exchange's throughput, and its mean and p99 at 5,000 orders/s, with the feeds running against without. The mean must differ by less than 10%, or the difference must be explained. "Within the spread" alone is too weak: milestone 25 saw the maximum rate vary by up to 50% between runs.
- `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests, and an independent review of each part all pass.
- `docs/milestones/26-pushed-updates/` is complete, written in depth.

### Not in this milestone

- multicast over reliable UDP (the design's way to deliver market data to everyone at once, fairly) and FIX over SBE (the design's protocol for the external domain);
- L3 (order-by-order) data, paid depth tiers, candles over WebSocket;
- serving the feeds beyond loopback, TLS, and rate or connection limits against abuse (not goals of this project, owner 2026-10-14);
- a replay service for historical data, and a public trade tape;
- several feed servers;
- random subscriber ordering for fairness (the design: "assign a random order when a subscriber connects"), recorded as a possible later step;
- a browser user interface.

### Progress

Planned on 2026-10-19. Nothing built yet.

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
- the event log is one file that grows without bound; snapshots reduce exchange-core replay work but do not compact or retain less journal history; startup reads the part of the journal it replays (all of it without a usable snapshot, the suffix after the snapshot with one, and after the warm replica's position at promotion) into memory in one piece rather than streaming it
- order records and the per-user execution index hold one trading day; the open replaces both maps, releasing their memory, while the matching engine's order-location map and risk usage keep the capacity of the largest day, and a price level's node slots are freed only when the level empties, which the close guarantees once a day
- the books hold at most 200,000 resting orders across all symbols, so that the close's one journal record always fits; beyond that an order that would rest is refused (409 `BookFull`) until orders trade, are cancelled, or expire at the close, and one user can fill the book, as there is no per-user share; there is no close spread across several records
- one order may trade against at most 10,000 resting orders (`TooManyFills`, 409), so that its record always fits the 64 MiB limit; a client that wants more must split its order
- the journal is synced by one journaler thread, one `sync_all` at a time, covering everything written; an order still waits for the rest of the sync in progress and then its own, so p99 cannot beat the disk's own sync latency. With replication it also waits for the replica's sync and one round trip; on the office machine the two syncs share one SSD, which now limits the replicated maximum rate. The replica syncs once per frame it receives and does not coalesce frames itself; the primary must ship once per sync (milestone 25 part 2). The journaler starts the next sync as soon as anything is written, so in-process replicated at 5,000 orders/s the sync path was about 98% busy (timed builds, milestone 25 part 4). When syncs lengthen, every order then probably waits for about one and a half of them (an inference), and the median can rise while the tail falls; over 6 balanced runs the median effect was within the spread
- replication has one replica, and a new connection replaces the current one. Its link is neither authenticated nor encrypted, and its listener is the only one meant to be reached from another machine: use a private network. Waiting for the replica never times out: a primary without its replica waits until the operator acts (only a link silent for 10 s is dropped, so that the replica dials again). Running alone is not remembered across a restart, and a replica slower than the sustained load cannot end it until the load drops. The operator must never both let the primary run alone and promote the replica: nothing prevents it. The epochs detect it when the old primary tries to rejoin, provided it rejoins with its own stream file, but its commands acknowledged alone are then on its machine only. Started without replication after a promotion, the old primary would serve at once without even syncing its stream's end, so after a power loss its rejoin could cut those commands instead of being refused
- the replica publishes a record one round trip after confirming it, when the next commit point arrives; its stream holds no records, so its readers read every record from the journal
- the replica's committed end is what its stream published, synced within about two seconds of it moving. If the primary's disk loses writes it reported synced, or the primary starts on an older copy of its journal, the replica cannot tell the acknowledged records it lost, at most about two seconds' worth after a power loss, from records the primary never synced, and cuts them. Only a third machine could tell them apart
- the core snapshot serializes the whole exchange state, which since milestone 22 holds one trading day: at 200,000 orders a day it grows to about 85 MB and 2 s by the close, and falls to under 1 MB at the next open. It runs on the warm replica, right after each open and whenever the journal has grown by four times the last snapshot's size, and only at primary startup on the primary. The warm replica writes it on its follower thread, so a promotion request waits for a snapshot in progress (up to about 2.5 s at the end of a day), and at the maximum rate on the office machine, where it shares two cores with the primary, its lag rises within a day by up to about 250,000 events before it falls back
- without a running warm replica no periodic snapshots are written, including after a promotion until a new warm replica starts; a restart then replays everything since the last snapshot. As long as snapshots are being written, a restart replays at most about four snapshot sizes of journal, never more than the day: 2.9 s measured at the end of the fifth day on the office machine, about 4 s at worst
- the warm replica's snapshot writes share the host's disk with the journal's syncs
- after a failed journal sync the in-memory core is ahead of the disk; it is never used again, the worker halts, and the process must be restarted to recover from the journal
- a `client_order_id` is unique within its trading day, including after its order finished; earlier days' orders are only in the reporter's tables, and there is no reporting API to read them
- snapshots rely on the journal id and committed boundary; an invalid snapshot falls back to full replay and is retained for diagnosis. A journal copied by hand that kept its id but diverged before a snapshot or checkpoint would not be detected; the replication design never publishes a record that is not on both machines, and Part 5's epochs handle divergence after a failover
- event-log sequencing and matching-input sequencing remain distinct concepts
- live HTTP replies still use `oneshot`
- MDP saves its complete JSON open-order and candle state at most once a second; a crash replays up to about a second of journal, and each save still grows with candle history
- candle state retains every one-minute bucket without a retention limit, rollups, or external historical store
- there is no public reporting API or trade-tape service
- mmap is a same-host Unix transport using cooperative file locks, JSON, and a bounded window; it is not lock-free, cross-server replication, or an ingress transport
- the journal still grows without bound; a reader restart checks only the record at its checkpoint, and the primary's normal restart validates only the records after its snapshot (milestone 23 part 2), so damage in the older part of the journal is found only by what reads it again: a full replay with no usable snapshot, or a reader starting from the beginning. Promotion reads only the journal after the warm replica's position. Nothing scrubs the journal in the background
- restoring an older copy of the journal is unsupported. The stream's watermark and the warm replica's applied length catch it, but not when the copy comes back without a stream file, with an unreadable stream header, or with its own old stream file: that primary starts trading, and readers whose checkpoints lie beyond it are refused only until the new history grows past them. To go back, start a new journal
- mmap is not the durable recovery source; never modify, truncate, replace, or unlink mapped files while processes use them
- consumer crashes require checkpoint/state coordination; arbitrary downstream effects are not exactly-once
- internal matching and settlement failures are rejected before commit and halt the worker if they cannot be represented as a business rejection
- promotion is manual: no heartbeat, automatic failover or leader election. On the replica's machine the operator stops the replica, then promotes the warm replica there; the old primary must then rejoin as a replica, never run alone. Epochs refuse it if it ran alone anyway, but cannot stop it from doing so while it does not know about the promotion. A primary started directly on the replica's copy is no promotion: it begins no term of its own, so the epochs cannot fence the old primary
- promotion takes over only the file the warm replica followed: a journal replaced at its path, even by a byte-identical copy, needs a new warm replica on it first. It does not re-read the journal before the warm replica's position, so it would not notice that part being rewritten in place; never modify the journal
- the warm replica opens its control port only after its first catch-up, so a promotion cannot be requested before that
- a promotion binds the customer, operator and replication ports only after it has fenced the old primary: a port already in use then leaves no primary until the operator starts one
- if the stream's writer (the primary, or the replica process on the second machine) dies while publishing, leaving the ready marker cleared, a running warm replica waits where it is and can still be promoted (milestone 23 part 6), but a warm replica *started* then refuses to start, and the market-data process and the reporter stop; restarting the writer repairs the stream from the journal
- the warm replica's management API is unauthenticated and loopback-only; `202` from `/promote` means the old writer is fenced, not that the customer listener is ready
- registration answers `user already exists` for a taken email, so it reveals which emails have accounts. Login no longer does (milestone 24 part 2). Fixing registration needs email verification, which the project does not have
- every login and registration costs one bcrypt at cost 12 (about 229 ms of CPU on the office machine), at most `max(1, CPUs / 2)` at a time. That is about 8 per second on the office machine, and nothing limits who uses that capacity: a flood of fake logins makes real users' logins wait, though orders are unaffected. Rate limits were planned in milestone 24 part 5 and dropped by the owner
- the crate uses Unix-only APIs (advisory file locks, positioned reads, mmap) and builds and tests on Linux only
- the reporter applies about 1,600 commands/s: it keeps up at 1,000 orders/s but falls behind a sustained faster exchange and catches up afterwards; set-based writes or `COPY` are the next lever

## What Not To Work On Yet

Milestone 24 closed on 2026-10-14 after Part 2; its Parts 3 to 6 (product list, operator-only deposits, rate limits) were dropped by the owner and are not to be restarted unless the owner asks. Milestone 25, latency, closed on 2026-10-19 (Parts 1, 2 and 4; Part 3 dropped). Milestone 26, pushed updates, is planned and awaits the owner's review of its plan; do not start any other candidate while it is open. Do not grow two machines into automatic failover, promotion on missed heartbeats, leader election, more than one replica, reliable UDP, or Raft: each needs its own failure model, and a three-machine quorum is the recorded follow-up after it.

After milestone 26, the remaining candidate (the owner selects):
1. **Set-based reporter writes or `COPY`.** The reporter applies about 1,600 commands/s against the exchange's 35,000+. It is off the trading path and catches up afterwards, so this matters only at sustained volume.

Custody, KYC, operator-only deposits, rate limits and other protection against abusive customers are not goals of this learning project (owner, 2026-10-14).

The same restraint applies to tax or customer statements, settlement, broad historical-market-data APIs, journal compaction, Crossbeam, lock-free ring buffers, new trading-component threads, per-symbol workers, and FIX/SBE. The committed mmap reader remains the input for all independent subscribers and for the warm follower; inbound commands remain on the existing bounded Tokio queue.

## Rule For Future Sessions

Start by reading this file and `EXCHANGE_PIPELINE_TODO.md`. Verify the code and test result before trusting old milestone notes.

The crate uses Unix-only APIs and does not compile on Windows. Build and test on Linux — the Ubuntu machine, or a `rust` container with the repository mounted (from Git Bash, set `MSYS_NO_PATHCONV=1` so container paths are not rewritten). The customer, operator, and warm-replica listeners bind loopback only, so a live failover run needs the primary, the warm replica, and the HTTP client in one network namespace. The replication listener (`REPLICATION_LISTEN_ADDR`) is the exception: the replica on the other machine must reach it. A new journal starts with the market closed: open a trading day on the operator port (`POST 127.0.0.1:4004/session/open`) before placing orders.

Discuss architecture before implementation. If a suggestion conflicts with the target design or changes the command/event boundary, stop and explain the tradeoff. Update this journal whenever a milestone is completed so the next session does not repeat old work.

Every completed task produces two documents, not just code:

1. A new numbered write-up covering what the task was, how it was done, and the reasoning behind each decision — including the options that were rejected and why. Write it so it can be read cold, months later, without the conversation that produced it. Up to milestone 23 these went in `docs/tasks/`. From milestone 24 on, each milestone has its own folder, `docs/milestones/NN-name/`, with the layout and rules given under milestone 24's "Documentation for this milestone": a README, the selection reasoning, the theory, one file per part, a `performance/` folder with raw results, the decisions, and a glossary.
2. An update to this journal recording the milestone.

Learning is the main goal of this project. Write in depth, never in summary. Every number gives its source and its unit, and every derived figure shows its arithmetic. Every estimate says it is an estimate. An analysis given only in conversation (for example, "a binary format saves about 7 µs per order") must also be written into the milestone folder, with its source and its reasoning.

Do not claim a milestone here before `cargo fmt -- --check` and `cargo test` both pass. Prefer checking behavior against a running server over trusting unit tests alone; a passing test on unreachable code is what this project has already been caught doing once.
