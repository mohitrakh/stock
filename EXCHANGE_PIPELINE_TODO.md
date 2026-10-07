# Exchange Pipeline TODO

This is the short-term migration checklist. `PROJECT_DIRECTION.md` records project history; `stock-exchange-system-design.md` remains the long-term target.

## Completed Milestone - ExchangeCore Ownership Split

`ExchangeCore` now owns and coordinates `OrderManager`, `Sequencer`, and `MatchingEngine` without adding component threads or changing the HTTP command/event boundary.

```text
ExchangeRuntime
  -> ExchangeCore
       -> OrderManager
       -> Sequencer
       -> MatchingEngine
```

## Completed Implementation

- [x] Add `src/exchange/core.rs` and export it from `src/exchange/mod.rs`.
- [x] Add `ExchangeCore` with ownership of `OrderManager`, `Sequencer`, and `MatchingEngine`.
- [x] Separate order validation/registration from sequencing and matching.
- [x] Let `ExchangeCore` coordinate new-order processing and apply returned executions through `OrderManager`.
- [x] Move cancellation sequencing and matching-engine cancellation coordination into `ExchangeCore`.
- [x] Change `ExchangeRuntime` to call `ExchangeCore` instead of calling `OrderManager` directly.
- [x] Move lifecycle tests to the `ExchangeCore` boundary.
- [x] Add coverage proving rejected operations do not consume matching sequence numbers.
- [x] Run formatting and the full test suite.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

## Acceptance Criteria - Verified

- `OrderManager` no longer owns a `Sequencer` or `MatchingEngine`.
- `ExchangeCore` owns and coordinates the three logical trading components.
- HTTP controllers and `ExchangeCommand` did not require architectural changes.
- `ExchangeRuntime` still appends input and output events in sequence.
- Lifecycle, matching, wallet, cancellation, sequence, and runtime behavior remain covered.
- The core-ownership checkpoint passed 18 tests.

## Completed Milestone - Exact Minor-Unit Prices

The gateway and critical trading path now represent monetary prices as positive integer minor units. `Price(u64)` flows through orders, executions, order-book levels, matching quotes, wallet reservation, settlement, and cancellation.

### Completed Implementation

- [x] Change the HTTP order price from `f64` to integer minor units.
- [x] Replace the floating price wrapper with ordered `Price(u64)`.
- [x] Carry `Price` through orders, executions, price levels, order books, and matching quotes.
- [x] Replace float-to-integer wallet casts with checked exact notional calculations.
- [x] Reject notional overflow before locking funds or registering an order.
- [x] Add coverage for exact partial-fill settlement, cancellation unlocks, overflow rejection, and adjacent price levels.
- [x] Update the project journal and system documentation.

### Acceptance Criteria - Verified

- Monetary prices no longer use `f64`; only timestamps do.
- JSON order prices are integers in the same minor unit used by deposits and balances.
- Exact price comparisons determine price levels and crossing behavior.
- Wallet reservation, fill settlement, and cancellation unlocks use checked multiplication.
- `cargo test` passes 21 tests.

## Completed Milestone - Deterministic In-Memory Replay

Inputs and outputs share an ordered in-memory log but have separate event types. Live processing and replay use the same input-processing function, and a validated log can rebuild a runtime that continues processing live commands.

### Completed Implementation

- [x] Separate `ExchangeInputEvent` and `ExchangeOutputEvent` inside `ExchangeEvent`.
- [x] Record cancellation rejection outcomes.
- [x] Separate core input processing from runtime event-log mutation.
- [x] Add equality comparisons and verify deterministic output generation.
- [x] Add `replay_event_log` to validate sequences and compare regenerated outputs.
- [x] Reject sequence mismatches, missing outputs, unexpected outputs, and output mismatches.
- [x] Add `ExchangeRuntime::from_event_log` to resume live processing after replay.
- [x] Test replayed matching state and continuation of both sequence counters.
- [x] Update the project journal and current implementation documentation.

### Acceptance Criteria - Verified

- Replay applies only inputs to a fresh core and checks recorded outputs without applying them again.
- Event-log sequence numbers must be contiguous and start at 1.
- The rebuilt core is returned only after the complete supplied log passes validation.
- A recovered runtime retains its history and can process a new live cancellation.
- The recovery test continues an eight-event history at event sequences 9 and 10 with matching sequence 3.
- On 2026-09-06, `cargo fmt -- --check` passes and `cargo test` passes 29 tests.

This milestone uses an in-memory `Vec<EventEnvelope>`. Serialization, durable storage, snapshots, and loading history during application startup remain unimplemented.

## Completed Milestone - Observable Exchange

Full write-up: `docs/tasks/01-observable-exchange.md`.

Inserted ahead of durable storage after an audit found every component built since the runtime milestone was unreachable from the running binary, and that a client could place an order and never learn whether it filled. A crossed-book bug in self-trade prevention was found and fixed in the same pass.

### Completed Implementation

- [x] Add `GetBalance`, `GetOrder`, and `GetOrderBook` variants to `ExchangeCommand`.
- [x] Answer them directly from `ExchangeCore`, with no `ExchangeInputEvent` and no event-log write.
- [x] Add `order_view` / `balance_view` to `OrderManager` and `l2_snapshot` to `OrderBook` and `MatchingEngine`.
- [x] Return a JSON `OrderView` from `POST /exchange/orders` instead of a bare uuid string.
- [x] Add `GET /exchange/balance`, `GET /exchange/orders/{order_id}`, `GET /exchange/orderbook/{symbol}`.
- [x] Replace the self-trade `break` with a skip, via `PriceLevel::first_matchable_mut`.
- [x] Snapshot crossing price levels in `match_order` so a skipped level cannot spin the loop.
- [x] Map a cancel of an unknown order to 404 instead of 400.
- [x] Run formatting, the full test suite, and a live HTTP session against a running server.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

### Acceptance Criteria - Verified

- A client can read its balance, its own order state, and L2 market depth over HTTP.
- Queries never append to the event log; a test asserts the log length across all three.
- A non-owner reading another user's order id receives 404, not 403.
- A self-order at the head of a price level no longer hides a valid counterparty behind it.
- The aggressor never rests into a book crossed against a matchable counterparty.
- `cargo fmt -- --check` passes and `cargo test` passes 38 tests.

The exchange-owned L2 command and route in this historical milestone were later removed by MDP v1. Public depth now belongs to the independent market-data process.

## Completed Milestone - Durable Event Log and Startup Recovery

Full write-up: `docs/tasks/02-durable-event-log.md`.

Exchange history now survives a process restart. Each processed command is written to an append-only file as one framed record (length, CRC-32, JSON payload) holding the input and every output it produced, synchronized to disk before the client is answered. Startup replays the file through the existing deterministic replay and refuses to start on anything it cannot trust.

### Completed Implementation

- [x] Add `src/exchange/event_store.rs`: magic header, length + CRC-32 framing, `open` with torn-tail truncation, `append` with `sync_all`.
- [x] Write the input and its outputs as one record so a crash can never leave an input without its outputs.
- [x] Make `ExchangeRuntime` write the record before advancing its in-memory log or replying.
- [x] Fail closed: a store error answers the waiting client with a halt message and stops the worker.
- [x] Add `recover_runtime` and call it from `main` before the listener binds; exit 1 on any untrusted history.
- [x] Accept an optional `client_order_id` and return 409 on a duplicate, so a retried request cannot open a second order.
- [x] Read `EVENT_LOG_PATH` from the environment; ignore the log file in git.
- [x] Run formatting, the full test suite, and a live session covering two hard kills, a torn log, and a corrupted log.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

### Acceptance Criteria - Verified

- Deposit, partial fill, hard kill, restart: balances, locks, order state, order-book state, and both sequence counters continue exactly (event sequences 9 and 10, matching sequence 3).
- A record torn by a crash is discarded whole and the exchange starts on the history before it.
- A checksum mismatch or a record that fails deterministic replay refuses startup with a clear message; the exchange never silently starts empty.
- A write failure halts the worker rather than acknowledging a command that is not durable.
- The same `client_order_id` submitted twice produces exactly one order.
- `cargo fmt -- --check` passes and `cargo test` passes 46 tests.

## Completed Milestone - Sell-Side Positions

Full write-up: `docs/tasks/03-sell-side-positions.md`.

The exchange no longer creates money out of nothing. A sell must be backed by shares the seller holds, reserved at placement the same way a buy reserves cash, and a fill transfers both cash and shares so neither total changes.

### Completed Implementation

- [x] Add `src/types/positions.rs`: holdings and reservations per `(user_id, symbol)`, mirroring the wallet's shape.
- [x] Branch collateral in `prepare_order` — cash for a buy, shares for a sell — and add `OrderManagerError::PositionRejected`.
- [x] Make `apply_execution` a four-legged settlement so cash and shares are both conserved.
- [x] Release share reservations in `complete_cancel` for the unfilled remainder.
- [x] Add `SharesDepositRequested` / `SharesDeposited` / `SharesDepositRejected` events and `POST /exchange/shares/deposit`.
- [x] Add `GET /exchange/positions` so the fix is observable from outside the process.
- [x] Drop the now-dead `Side` parameter from `Wallet::check_and_lock` and `unlock_funds`; delete the unreachable `commit_fill`.
- [x] Update the twenty existing tests that had been selling shares nobody owned.
- [x] Run formatting, the full test suite, and a live session including a restart and a legacy log.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

### Acceptance Criteria - Verified

- A sell with no shares, or more shares than held, is rejected and credits nobody.
- Reserved shares cannot be sold twice; cancelling returns them to available.
- Cash and shares are both conserved across a partial fill — the invariant that would have caught this bug originally.
- Shares received in a fill can then be sold.
- Positions and their reservations survive a hard kill and restart.
- A pre-position-ledger log containing an accepted unbacked sell refuses to start with an `OutputMismatch`, despite valid framing and checksums.
- `cargo fmt -- --check` passes and `cargo test` passes 57 tests.

## Completed Milestone - Risk Limits, Executions, and Ledger Cleanup

Full write-up: `docs/tasks/04-risk-limits-and-executions.md`.

Closes the last unmet functional requirements in the target design, plus the deferred correctness gaps.

### Completed Implementation

- [x] Derive the trading day from `Order.timestamp` rather than the system clock, so daily counters replay deterministically; roll forward only.
- [x] Make limits events (`RiskLimitSetRequested` / `RiskLimitSet`) rather than configuration, with `POST /exchange/risk/limits`.
- [x] Apply a compiled-in `DEFAULT_MAX_DAILY_QUANTITY` of 1,000,000 when no limit is set.
- [x] Release the unfilled allowance in `complete_cancel`, alongside cash and shares.
- [x] Replace unchecked volume arithmetic with `checked_add`; delete the unused `check_and_record`.
- [x] Add `GET /exchange/risk/limits` and `GET /exchange/executions` with optional symbol, order, and time filters.
- [x] Index executions per party during settlement, so both sides see their own side and order id.
- [x] Make `Wallet::deposit` overflow-checked and fallible; add `FundsDepositRejected`.
- [x] Turn `apply_executions`' silently-dropped odd execution into an error.
- [x] Resolve the 42 `unused_must_use` warnings the new fallible signatures introduced.
- [x] Run formatting, the full test suite, and a live session including a restart.
- [x] Update `PROJECT_DIRECTION.md` and `SYSTEM_DOCUMENTATION.md`.

### Acceptance Criteria - Verified

- The documented 1,000,000 cap applies without anyone configuring it.
- A cap of 10 admits 6, refuses 5, then admits 4 exactly; rejection is pre-trade, with no reservation taken and no sequence number consumed.
- Cancelling returns the unfilled allowance; other symbols and other users are unaffected.
- The day rolls from the order's own timestamp, and a backwards timestamp does not reset it again.
- One match yields two execution rows, each carrying its own party's side and order id; every filter narrows correctly; nobody sees another user's fills.
- After a hard kill, the event-set limit and both parties' fills are rebuilt by replay.
- `cargo fmt -- --check` passes and `cargo test` passes 70 tests.

## Completed Milestone - Failure-Safe Atomic Exchange Commands

Full write-up: `docs/tasks/05-failure-safe-atomic-commands.md`.

### Completed Implementation

- [x] Prepare complete order and cancellation transitions without mutating authoritative state.
- [x] Validate all fills, ledger deltas, reservations, risk usage, lifecycle changes, books, and sequence effects before commit.
- [x] Append and synchronize the full input/output batch before applying the prepared plan.
- [x] Publish execution callbacks only after the durable commit.
- [x] Separate business rejections from internal faults and halt on internal or storage failures.
- [x] Return unavailable responses and a 503 health result after the worker stops.
- [x] Remove the old mutating settlement and matching paths so there is one live command path.
- [x] Add regression coverage for late settlement failure, later-fill failure, cancellation failure, append failure, and callback ordering.
- [x] Run formatting and the full test suite.

### Acceptance Criteria - Verified

- A failed late settlement leaves balances, positions, reservations, orders, books, execution indexes, and sequence counters unchanged.
- A failed append leaves prepared state uncommitted and publishes no callback.
- Normal business rejections remain durable outputs and do not consume matching sequence numbers.
- Internal faults stop further processing and are exposed as unavailable rather than ordinary validation errors.
- `cargo fmt -- --check` passes and `cargo test` passes 76 tests.

## Completed Milestone - Committed mmap Event Stream

Full write-up: `docs/tasks/06-mmap-committed-event-stream.md`.

- [x] Keep the durable journal authoritative and the exchange core single-owner.
- [x] Serialize each input/output batch once; synchronize it, commit core state, then publish it.
- [x] Use a fixed-size mmap window with a committed byte/sequence watermark.
- [x] Provide independent readers with their own sequence and byte positions.
- [x] Catch up from the read-only journal after window overwrite, oversize batches, or restart.
- [x] Reject corrupt/incomplete records, sequence gaps, duplicates, mismatched journals, and invalid checkpoints.
- [x] Recover the durable-but-not-yet-published suffix after writer restart.
- [x] Enforce one journal writer and fail closed on publication errors.
- [x] Wire production startup and add a separate CLI probe with optional checkpoint and live following.
- [x] Test multiple readers, separate processes, SIGKILL boundaries, append/publication failures, and executable checkpoint resume.
- [x] Update architecture, task, and project documentation.

Verified on 2026-09-26: `cargo fmt -- --check`; `cargo test` passes 92 unit tests and 2 executable integration tests. This milestone supplies a synchronized same-host transport, not lock-free latency, machine-crash durability of mmap, market data, reporting, or replication. Existing clippy warnings remain repository-wide work.

## Completed Milestone - Overnight Risk Accounting

Full write-up: `docs/tasks/07-overnight-risk-accounting.md`.

- [x] Keep remaining quantities from overnight orders in the next day's risk usage.
- [x] Track open exposure separately from current-day total usage.
- [x] Move fills out of open exposure without refunding the current day's allowance.
- [x] Release only the unfilled open quantity on cancellation.
- [x] Validate fill and cancellation risk changes during prepare-before-commit.
- [x] Verify the corrected state through durable replay and continued processing.

Verified on 2026-09-26: `cargo fmt -- --check`; `cargo test --locked --offline` passes 95 unit tests and 2 executable integration tests.

## Completed Milestone - Market Data Publisher v1

Full write-up: `docs/tasks/08-market-data-publisher-v1.md`.

- [x] Run the MDP as a separate process before database or authentication initialization.
- [x] Consume only complete committed batches through `StreamReader`.
- [x] Maintain a private open-order projection and checked `u64` L2 aggregates.
- [x] Validate accepted orders, two-sided execution pairs, resting-order fills, cancellations, and all quantity changes.
- [x] Apply each batch to a candidate projection and save it before replacing the served snapshot.
- [x] Persist versioned projection state and its `ReaderCheckpoint` through synchronized temporary-file replacement.
- [x] Rebuild from journal sequence 1 when state is missing and refuse present invalid or mismatched state.
- [x] Catch up before binding, follow live mmap publication, and fail health/L2 closed after terminal follower errors.
- [x] Move the public L2 route out of the exchange worker to `GET /marketdata/orderbook/{symbol}?depth=N`.
- [x] Remove `ExchangeCommand::GetOrderBook` while keeping core L2 snapshots as a correctness oracle.
- [x] Verify journal fallback, live following, MDP restart, exchange stream restart, state corruption, and database-free executable startup.

Verified on 2026-09-26: `cargo fmt -- --check`; `cargo test --locked --offline` passes 108 unit tests and 4 executable integration tests. Clippy passes with existing repository warnings. A manual isolated-database run drove the real exchange and MDP through rest, partial fill, cancellation, MDP restart, and resumed live publication.

## Completed Milestone - Reporter v1

Full write-up: `docs/tasks/09-reporter-v1.md`.

- [x] Extract a shared committed-batch decoder used by both MDP and Reporter.
- [x] Run Reporter as a separate `StreamReader` process with a dedicated health endpoint.
- [x] Project accepted/rejected orders, fills, cancellations, and one trade per execution pair into PostgreSQL.
- [x] Commit each projection change and advanced reader checkpoint in one PostgreSQL transaction.
- [x] Add a versioned migration, database-free regression coverage, and an opt-in isolated-PostgreSQL executable test.

Verified on 2026-09-26: `cargo fmt -- --check`; `cargo test --locked --offline` passes 109 unit tests and 4 executable integration tests, plus one ignored opt-in Reporter test. The Reporter executable test passed against an isolated local PostgreSQL instance after applying migrations.

## Completed Milestone - Reporter v1 Recovery Qualification

Full write-up: `docs/tasks/10-reporter-recovery-qualification.md`.

- [x] Make the isolated Reporter test reset and apply its own versioned migration.
- [x] Inject a checkpoint-write failure after lifecycle/trade writes begin and prove the SQL transaction rolls back all three effects.
- [x] Verify journal catch-up, mmap handoff, multi-fill trades, cancellations, rejected orders, and rejected cancellations in the real Reporter process.
- [x] Kill a ready Reporter, restart it against the same journal/database, and prove its checkpoint prevents duplicate order or trade rows.

Verified on 2026-09-27: `cargo fmt -- --check`; `cargo test --locked --offline` passes 109 unit tests and 4 executable integration tests. `REPORTER_TEST_DATABASE_URL=... cargo test --locked --offline --test reporter -- --ignored --nocapture` passed against a freshly initialized local PostgreSQL 18 instance.

## Completed Milestone - Authoritative Core Snapshots and Suffix Replay

Full write-up: `docs/tasks/11-authoritative-core-snapshots.md`.

- [x] Serialize all deterministic core state in a normalized, versioned, checksummed snapshot.
- [x] Bind each checkpoint to the exact journal device/inode, complete-record byte offset, next event sequence, and next matching sequence.
- [x] Rebuild FIFO books and transient indexes on load, then validate ledger collateral, book/order agreement, and sequence domains.
- [x] Publish snapshots through temporary-file sync, atomic rename, and parent-directory sync without touching the journal.
- [x] Recover from a valid checkpoint by validating and replaying only its journal suffix.
- [x] Preserve corrupt, malformed, or journal-mismatched checkpoints and safely fall back to full deterministic replay.
- [x] Verify suffix boundaries/torn tails, replacement failure, journal identity, both sequence continuations, and no snapshot advance after failed append.

Verified on 2026-09-27: `cargo fmt -- --check`; `cargo test --locked --offline` passes 118 unit tests and 4 executable integration tests. This is a recovery accelerator only: the journal remains complete and authoritative, with no compaction or subscriber-retention change.

## Completed Milestone - Candlestick Publisher v1

Full write-up: `docs/tasks/12-candlestick-publisher-v1.md`.

- [x] Extend the independent MDP with a deterministic one-minute UTC OHLCV projection from complete committed execution pairs.
- [x] Count each validated two-sided execution pair as one trade, using its first execution record.
- [x] Persist L2 orders, all candles, and the shared reader checkpoint in one versioned state replacement.
- [x] Rebuild both views from journal history when state is absent; fail closed on corrupt, invalid, or incompatible state.
- [x] Add `GET /marketdata/candles?symbol=&start_time=&end_time=` with required inclusive epoch-second bounds.
- [x] Verify range validation, journal catch-up, restart without duplicate candle volume, live mmap following, and existing MDP fail-closed behavior.

The bucket timestamp is the recorded execution timestamp floored to a UTC minute. Candles are retained without a limit in v1; resolution rollups, retention, and external historical storage remain separate architecture decisions.

## Completed Milestone - Same-host Warm Replica v1

Full write-up: `docs/tasks/13-warm-replica-v1.md`.

A manual, writer-fenced warm standby: a separate process follows committed batches into a read-only core and, once the primary has stopped, takes the journal writer lock and becomes the primary.

- [x] Add `ReplicaCore`, a read-only follower core with no journal writer, stream writer, command queue, callbacks, database, or customer routes.
- [x] Share one `replay_committed_batch` path between full recovery, snapshot-suffix replay, and the follower.
- [x] Start the warm from a valid primary snapshot when present, otherwise from sequence 1, and catch up before binding.
- [x] Keep an applied checkpoint separate from the reader's cursor, advancing it only after a batch is compared and committed.
- [x] Serve loopback-only `GET /health`, `GET /status`, and `POST /promote`, on `127.0.0.1:4003` by default.
- [x] Refuse promotion with `409` while the primary holds the journal writer lock, leaving the follower intact.
- [x] Compare journal identity under the lock and before any read or torn-tail repair (`open_existing_matching`), and delete the unchecked opener.
- [x] Rebuild the promoted primary from the entire writer-locked journal, never from mmap-derived state.
- [x] Wire `--warm-replica` in `main` through `promote_replica_with_stream_and_snapshot` into the normal primary startup.
- [x] Run formatting, the full suite, and a live failover of the real executables against PostgreSQL.
- [x] Update `PROJECT_DIRECTION.md`, `SYSTEM_DOCUMENTATION.md`, `DEFERRED_ITEMS.md`, and the task write-up.

### Acceptance Criteria - Verified

- A warm caught up from the journal matches the primary core exactly and writes neither the journal nor the stream.
- A batch whose recorded output disagrees with replay leaves the core and the applied checkpoint at the last good batch.
- A valid but different mmap cache cannot become primary state; promotion rebuilds from the journal.
- A durable batch hidden from mmap by a publication failure is recovered by promotion, and both sequences continue.
- Promotion while another process holds the journal returns `409`, keeps the warm healthy, and changes no journal or stream bytes.
- A journal swapped in at the followed path is refused with its torn tail unrepaired. This regression test failed on the pre-fix code, which truncated the replacement to its header.
- Live: `409` while the primary ran, following, `SIGKILL` of the primary, `202`, identical balances, positions, and order state on the promoted primary, a new trade there, and journal sequences 1–18 contiguous across the hand-off.

Verified on 2026-09-29 on Linux: `cargo fmt -- --check` is clean; `cargo test --locked` passes 131 unit tests and 6 executable integration tests, with the opt-in Reporter acceptance test ignored. This is manual same-host fencing, not automatic failover, cross-host replication, or a measured RTO/RPO.

## Completed Milestone - Critical-Path Performance v1

Full write-up: `docs/tasks/14-critical-path-performance-v1.md`; one file per optimization in `docs/performance/`.

- [x] Add `--bench`: an in-process, deterministic, open-loop load generator over the production worker, journal, mmap stream and snapshot schedule, with HdrHistogram latency from each order's intended send time.
- [x] Record the baseline before changing anything: 384 orders/s on disk, 2,796 in memory, 90 at 10,000 resting orders.
- [x] Group commit: stage every queued command in memory, one journal write and one sync per group, then publish, run callbacks and release replies (reads included).
- [x] Keep durable-before-visible; replace "a failed append leaves the core unchanged" with "after a failed sync the core is never used again" and rewrite the four affected tests (three now check recovery from the journal; the callback test checks that nothing reached history or callbacks).
- [x] Plan matching read-only against the live book (`plan_order`) and apply the plan at commit (`apply_plan`); stop cloning the book on order and cancel.
- [x] Prove the planned matcher identical to the old one with a 20,000-step differential test.
- [x] Make the never-read in-memory event history test-only.
- [x] Measure each optimization separately and write one document per optimization plus the task write-up.

### Acceptance Criteria - Verified

- Disk throughput 384 -> about 37,000-39,000 orders/s; at a fixed 1,000/s, p99 5.1 s -> 22-33 ms.
- Per-order cost independent of book depth: about 49,000-55,000 orders/s at 0, 1,000 and 10,000 resting orders (was 3,510 / 869 / 90).
- Memory no longer grows by 600-700 bytes per order from retained history; 1M orders peak 1.92 GB -> 1.32 GB.
- In memory the exchange meets the design's average of 43,000 orders/s; with production snapshots it reaches about 8,300 (next bottleneck, recorded).
- `cargo fmt -- --check` clean; `cargo test --locked` passes 136 unit tests and the integration tests; release warnings unchanged at 13.

## Completed Milestone - Snapshots Written by the Warm Replica

Full write-up: `docs/tasks/15-snapshots-by-the-warm-replica.md`; measurements in `docs/performance/04-snapshots-off-the-trading-thread.md`.

- [x] Remove the periodic snapshot schedule from the primary runtime; keep one snapshot at startup.
- [x] Give the warm replica a snapshot writer: every `EVENT_SNAPSHOT_INTERVAL` commands, write the journal-bound snapshot at exactly its applied checkpoint.
- [x] Make the warm replica read every batch from the journal (`StreamReader::journal_only`).
- [x] Preserve an invalid snapshot found at warm start and write none.
- [x] Remove the benchmark's `--snapshot-every` switch; measure snapshot cost with a warm replica beside the benchmark.
- [x] Measure before and after, and write the task and optimization documents.

### Acceptance Criteria - Verified

- The primary writes no snapshot while trading (test: the snapshot file is unchanged after five commands).
- A restart loads the warm replica's snapshot and replays only the suffix, matching the live core.
- Throughput with snapshots every 10,000 commands: about 8,300 -> about 21,900 orders/s; at 5,000 orders/s the worst latency went from 0.7-1.04 s to 170-442 ms.
- `cargo test --locked` passes 138 unit tests and the integration tests; release warnings unchanged at 13.

## Completed Milestone - Subscribers Keep Up

Full write-up: `docs/tasks/16-subscribers-keep-up.md`; measurements in `docs/performance/05-market-data-keeps-up.md` and `docs/performance/06-reporter-batched-transactions.md`.

- [x] MDP: serve one `RwLock<Option<View>>` (book, candles, applied checkpoint); apply each batch in place; withdraw the whole view (`None`) before releasing the lock if a batch fails.
- [x] MDP: save the view with its applied checkpoint at most once a second, counted from the end of the previous save, plus once at the end of catch-up; never on an error path.
- [x] MDP: withdraw the view when the follower thread ends for any reason; remove the candle projection's internal copy.
- [x] Reporter: apply up to 1,000 batches per PostgreSQL transaction, committed with the checkpoint just after the last applied batch; end a group when caught up; one code path for catch-up and following.
- [x] Reporter: record each trade in one statement (data-modifying CTE).
- [x] Reporter: record rejected submissions in `rejected_orders` and refused cancellations (with the requester) in `rejected_cancellations`, keyed by the input's journal sequence; keep only accepted orders in `reported_orders`; require the owner on a successful cancellation.
- [x] Reporter: make execution ids unique per symbol; add the `20260930000000_reporter_rejections.sql` migration.
- [x] Measure each optimization separately and write one document per optimization plus the task write-up.

### Acceptance Criteria - Verified

- MDP catch-up about 39 -> about 56,000 commands/s; live, within a second of the exchange at 5,000 orders/s and at its maximum (about 34,500 orders/s).
- Reporter catch-up about 365 -> 1,050 (group commit) -> 1,578 commands/s (one round trip per trade); live, it keeps up at 1,000 orders/s.
- A failed MDP batch is never served or saved; a restart from an older save replays to an identical view.
- A failure in a reporter group rolls back only that group; the previous 1,000-batch group and its checkpoint stay committed, and a restart applies the rest exactly once.
- A retried or reused order id, an intruder's cancellation, a cancellation of an already-canceled order, and two symbols with the same execution ids are all recorded correctly, without halting the reporter or changing an owner's row.
- On the office Ubuntu machine the fixed reporter catches up at about 2,100 commands/s on both the 1-symbol and the 10-symbol journal; the 10-symbol journal used to stop it at event 255.
- Client-supplied order ids and symbols are limited to 1-64 bytes with no control characters at the gateway, so no journaled value can stop the reporter.
- `cargo fmt -- --check` clean; `cargo test --locked` passes 142 unit tests and the integration tests; both PostgreSQL acceptance tests pass.

## Completed Milestone - Trading Day

Milestone 22, complete on 2026-10-05. The specification is in `PROJECT_DIRECTION.md` ("22. Trading Day"); the write-up is `docs/tasks/17-trading-day.md`, and the measurement `docs/performance/07-trading-days-bound-the-state.md`.

- [x] Part 1 (2026-10-02): session commands (`MarketOpenRequested` / `MarketCloseRequested`), the loopback operator port, closed-market rejection in `prepare_input_event`, the risk day from sessions, snapshot version 2.
- [x] Part 2 (2026-10-05): expiry of every resting order at the close (`OrderExpired`, collateral and risk released), through the shared decoder, the MDP and the reporter; at most 200,000 resting orders (`BookFull`), so the close always fits one record.
- [x] Part 3 (2026-10-05): clearing the previous day at the next open; client order ids unique per trading day; reporter keys `(trading_day, order_id)` and a fourth migration; a warm-replica snapshot after each open.
- [x] Part 4 (2026-10-05): multi-day benchmark (`--bench --days N`) against the milestone 21 binary on the same 1,000,000 orders; memory, snapshot size and restart time stay flat across days (`docs/performance/07-trading-days-bound-the-state.md`).
- [x] `PROJECT_DIRECTION.md` updated after every part; independent reviews of parts 2 and 3 with every finding fixed; `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests.

## Next Milestone

Selected on 2026-10-05: milestone 23, Two Machines. The specification, with the owner's decisions, is in `PROJECT_DIRECTION.md` ("23. Two Machines"). Write-ups: `docs/tasks/18-one-order-cannot-stop-the-exchange.md` (Part 1), then `docs/tasks/19-two-machines.md`.

- [x] Part 1 (2026-10-05): refuse an order that would trade against more than 10,000 resting orders (`TooManyFills`) and a deposit that would take the exchange's total cash or a symbol's total shares past `u64::MAX`, so no command can stop the worker; refuse to start without `JWT_SECRET`. Live: the milestone 22 binary halted on a 55,000-order sweep and on a fill crediting a `u64::MAX` balance; the new one refused both as ordinary rejections. Independent review done, findings fixed.
- [x] Part 2 (2026-10-06): a journal id in the header (`EXCHLOG2`) instead of device/inode, used by the stream, checkpoints, snapshots (version 5), market-data state (version 3) and the reporter (fifth migration); checkpoint checks that read only the record at the checkpoint. Restart from a checkpoint on the five-day journal: market data 9,654 → 9 ms, warm replica 9,620 → 68 ms (`docs/performance/08-restarts-without-rereading-history.md`).
- [x] Part 3 (2026-10-06): promotion from the warm replica's core, reading only the journal after its applied checkpoint and refusing a path that names another file; snapshots by journal growth (`EVENT_SNAPSHOT_GROWTH`, default 4); readers read committed records straight from the journal; a table-driven CRC-32. Promotion on the five-day journal 25,816 → 58 ms; from the end of a maximum-rate run 103 → 0.5 s; warm replica catch-up after it 85 → 1.7 s (`docs/performance/09-promotion-from-the-warm-replica.md`).
- [ ] Part 4: replication over TCP; the primary replies and publishes only after the replica confirms; the pause and "run alone".
- [ ] Part 5: epoch-fenced promotion on the second machine; the old primary rejoins as the replica; the reporter continues from its checkpoint.
- [ ] Part 6: measurement and failure tests.
- [ ] `PROJECT_DIRECTION.md` updated after every part; an independent review of each part; `cargo fmt -- --check`, `cargo test --locked`, the PostgreSQL acceptance tests.
