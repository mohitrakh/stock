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

## Next Milestone

No next milestone is selected. Discuss the next architecture step before implementation. Candles, journal compaction, group commit, lock-free transport, UDP/multicast, CPU pinning, hot-warm replication, and per-symbol partitioning remain separate milestones.
