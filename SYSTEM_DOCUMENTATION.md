# Stock Exchange - Current Implementation

Verified against this checkout on 2026-10-05. This file describes implemented behavior. `stock-exchange-system-design.md` is the target architecture; `PROJECT_DIRECTION.md` is the milestone journal.

## Ownership and command flow

Axum handlers send `ExchangeCommand` values through a bounded Tokio queue (10,000 commands) to one dedicated exchange-worker thread. Commands may carry a temporary `oneshot` reply channel. Persisted `ExchangeEvent` values contain only replayable business data.

`ExchangeRuntime` owns the receiver, core, durable event store, mmap stream writer, optional core-snapshot schedule, and next journal sequence. It keeps no event history in production: the durable journal is the complete history, and the in-memory `event_log` exists only in test builds. `ExchangeCore` owns the order manager, matching engine, and matching sequencer. The core does not access files, databases, queues, or HTTP.

The worker processes commands in groups (group commit). It blocks for one command, then takes every command already queued, up to 1,024:

```text
for each command in the group, in queue order:
  write:  prepare and validate the entire transition without changing live state
          -> number input and outputs as one batch and encode its framed record
          -> commit the prepared transition in memory (later commands in the group see it)
  read:   answer from the core at its place in the queue
  either: hold the reply
then once for the group:
  -> write every record with one write_all, then ONE sync_all
  -> publish each complete batch and the committed watermark through mmap
  -> notify local execution callbacks
  -> release every held reply
  (no core snapshot is written while trading; the warm replica writes them)
```

Nothing outside the worker can observe a command before its group is synced: not a reply, a read, an mmap batch, a callback or a snapshot. A quiet exchange forms groups of one; a busy one shares each sync between up to 1,024 commands.

Preparation includes every fill and cumulative settlement effect. Business rejections produce a durable input/rejection batch without changing trading state or consuming matching sequence. Internal faults halt processing; they are not recorded as ordinary client rejections.

An append or sync error answers every command in the group `exchange unavailable`, publishes nothing, runs no callbacks, and halts the worker. The in-memory core is then ahead of the disk, so it is never used again; recovery from the journal decides which complete records survived. An internal fault while staging answers that command `unavailable`, still syncs and answers the valid commands staged before it, drops the commands queued after it (the gateway reports 503), and halts. A publication error occurs after durable/core commit: the writer also stops, but the command can appear on recovery. An unavailable response is therefore not proof that a command did not happen.

The worker closes its receiver on exit. `AppState.exchange_available` becomes false, and `/health` returns 503. While the worker is available, health returns 200. This flag is not a full readiness/latency monitor.

## Core components

| Component | Ownership and current role |
|---|---|
| `ExchangeCore` | Coordinates read-only preparation and infallible installation of validated plans. |
| `OrderManager` | Owns lifecycle records, cash wallet, share positions, risk usage, execution indexes, and local execution callbacks. |
| `MatchingEngine` | Owns symbol books and order-to-symbol lookup. Prepares matching with a read-only `OrderBook::plan_order` against the live book and applies the plan at commit; prepares cancellation with a read-only presence check and removes at commit. Nothing is copied. |
| `OrderBook` | Holds bid/ask price levels and order-node lookup; matches price/time priority while skipping self trades. |
| `PriceLevel` / `Node` | Maintain FIFO order at a price through indexed linked nodes. |
| `Sequencer` | Supplies a candidate matching sequence with `peek`; `commit` advances it only when the prepared order/cancellation commits. |

The production path uses `prepare_add_order` / `commit_add_order` and `prepare_cancel_order` / `commit_cancel_order`. It does not use the former mutating `prepare_order -> apply_executions -> complete_cancel` path.

The order manager's `prepare_new_order` validates execution pairs, cumulative fills, projected cash/shares, collateral, risk, lifecycle changes, and execution views. `prepare_cancel_plan` validates ownership, state, and collateral release. Their commit methods install the prepared values. Preparation reads the live books; a match plan lists the resting orders to reduce or remove, the executions, and the remainder to rest, so its cost follows the orders the new order reaches, not the size of the book.

## Prices, collateral, risk, and fills

`Price(u64)` stores positive integer minor units. The API also uses integer minor-unit prices: with cents, 1025 represents $10.25. Checked arithmetic protects notionals and ledger credits. One shared price/currency scale is assumed; product tick-size and currency metadata are not implemented.

A buy reserves its limit-price notional. A sell reserves shares. Settlement transfers cash from buyer to seller and shares from seller to buyer, releases price improvement for buys, and adjusts both orders' remaining quantity. The whole command is validated before any settlement is committed. Cancellation, and expiry at the close, release the unfilled collateral and risk allowance.

`RiskManager` tracks limits and usage by user/symbol. The default daily cap is 1,000,000 shares. Usage means quantity traded today plus quantity still resting. Accepted submissions increase usage; a fill leaves it unchanged, because the shares move from "could trade today" to "did trade today"; cancellation and expiry return only the unfilled quantity. "Today" is the trading day: usage restarts from zero when the operator opens the market, since the close expired everything still resting (see "Trading sessions"), never from a clock or an order timestamp. Preparation validates cancellation and expiry releases before commit. Limit changes are journaled events. There are no market calendars or portfolio-wide limits, and the current API allows a trader to set their own cap.

Orders have new, partially-filled, filled, canceled, or expired lifecycle states. Executions record each party's side and are indexed for account queries. The two execution-side records for a match must not later be counted as two distinct market trades by a market-data consumer.

`client_order_id` is optional. Every client-supplied identifier (a client order id, a symbol, the order id of a cancellation) is trimmed and must be 1-64 bytes with no control characters, checked at the gateway: identifiers become journal fields and reporting index keys, and PostgreSQL can neither store a NUL byte nor index a multi-kilobyte value. A cancellation naming an impossible id answers 404 without being journaled. A provided value becomes the order id; an id already used in the current trading day is rejected instead of opening another order, and it can be used again on a later day. A client that lost a reply should check with `GET /exchange/orders/{order_id}` before the next open rather than retry across a close: after the open a retry opens a new order. Generated ids use UUIDs. This is order-specific duplicate protection, not general idempotency for deposits, cancellations, or all HTTP commands.

## Trading sessions

The market opens and closes by journaled commands, never by reading a clock, so replay reproduces every trading day exactly. `MarketOpenRequested { trading_day }` (a calendar date the operator chooses) produces `MarketOpened { trading_day }`. `MarketCloseRequested` produces `MarketClosed { trading_day }`, followed in the same record by one `OrderExpired { order_id, seq_num }` for every order still resting. A change the session does not allow produces `SessionRejected { reason }` and changes nothing:
- `AlreadyOpen`: opening an open market;
- `NotAfterLastTradingDay(day)`: a day that is not later than the last one;
- `AlreadyClosed`: closing a closed market;
- `TooManyRestingOrders(n)`: the close's record, with its `n` expiries, would exceed the 64 MiB record limit. The resting-order cap below prevents this for orders that passed the gateway's id check; the refusal is a safety net for any other entry point.

A new journal starts closed, with no trading day.

While the market is closed, `prepare_input_event` (shared by live processing, replay and the warm replica) refuses every new order as `OrderRejected { reason: "MarketClosed" }`, and the customer API answers 409. Deposits, share deposits, risk-limit changes and cancellations are accepted at any time.

Every order is a day order. At the close, each resting order releases its unfilled cash or shares and its risk allowance exactly as its cancellation would, becomes `expired`, and leaves the book; each expiry consumes a matching sequence, oldest accepted order first. Preparation sums the releases per user and checks them against what is reserved, so the commit cannot fail half way. The size check serializes the close with every envelope sequence at its widest, so replay reaches the same decision wherever the close lands in the journal. Opening the next day restarts daily risk usage from zero and clears the previous day: its finished orders and the per-user fills index leave memory, so a client order id is unique within its trading day only, and `GET /exchange/orders/{order_id}` and `GET /exchange/executions` answer for the current or just-closed day. Balances, positions, risk limits, and each symbol's book with its execution counter stay, so an execution id never repeats. Earlier days are in the reporter's tables.

A journal from before milestone 22 does not replay. Start a new journal, and remove the old snapshot file (`EVENT_SNAPSHOT_PATH`) and the market-data state file with it: a snapshot bound to another journal, or of an older format, is preserved for diagnosis and turns snapshot writing off, and a market-data checkpoint for another journal refuses startup. Apply the reporter migrations, which empty the report for a rebuild.

The books hold at most 200,000 resting orders across all symbols (`MAX_RESTING_ORDERS`), so the close's one record always fits: with the longest ids the gateway allows, every byte escaped in JSON, a close of 200,000 orders takes about 50 MB. An order that would rest beyond the cap is refused as `OrderRejected { reason: "BookFull" }`, which the customer API answers with 409. An order that trades without resting is never refused, nor is one that takes as many resting orders out of the book as it adds. The session is part of the core snapshot (format version 4), which refuses resting orders while the market is closed.

The operator port is a separate, loopback-only listener in the exchange process. It is unauthenticated, like the warm replica's management port.

| Method | Path | Behavior |
|---|---|---|
| POST | `/session/open` | body `{"trading_day":"2026-10-01"}`; 200 with the session, 409 with the reason when refused, 422 for a value that is not a date |
| POST | `/session/close` | 200 with the session, 409 when already closed |
| GET | `/session` | `{"trading_day":"2026-10-01" or null,"open":true or false}` |

All three answer 503 when the exchange worker has stopped.

## Events and replay

`ExchangeInputEvent` covers cash deposits, share deposits, risk-limit changes, new orders, cancellations, and opening and closing the market. `ExchangeOutputEvent` covers their results, executions, and the close's expiries. `EventEnvelope.seq_num` is the contiguous journal sequence across inputs and outputs.

Journal sequence and matching sequence are different domains. A command may emit several envelopes, while accepted orders, cancellations, and expiries consume matching sequences. Subscriber checkpoints use journal sequence.

`prepare_input_event` computes the result, outputs, and prepared core change. `replay_event_log` validates contiguous journal sequencing from 1, prepares each recorded input, compares regenerated outputs with the following recorded outputs, then commits that plan. Recorded outputs are checked rather than applied a second time. Replay does not publish callbacks.

Replay rejects missing/unexpected outputs, output mismatches, and sequence mismatches. Full replay starts from an empty core at event sequence 1. Snapshot recovery validates a restored core and starts the same deterministic replay at the snapshot's recorded next event sequence, over only the later journal suffix.

## Durable event store

`src/exchange/event_store.rs` owns journal mutation. Independent subscribers have a separate read-only handle; they never call the recovery opener.

The format is an 8-byte `EXCHLOG1` header, followed by records:

```text
payload length (u32 LE) | CRC-32 (u32 LE) | JSON Vec<EventEnvelope>
```

Each record contains exactly one input and its outputs. The maximum JSON payload is 64 MiB. The runtime calls `encode_record` before persistence and passes those same bytes to `append_record` and later mmap publication. `append_record` takes one or more complete framed records (a whole group) and uses one `write_all` and one `sync_all`; a process crash can only cut the tail of a group, and recovery drops the torn record as before; after power loss a damaged complete record still refuses startup, as it always could.

`EventStore::open` acquires a nonblocking exclusive lifetime file lock, validates the header and records, and truncates an incomplete final frame. Complete records with invalid checksums, JSON, or command shape cause refusal. Deterministic replay then checks business correctness. Two writers cannot recover or append to the same journal concurrently through this API. `EventStore::open_existing_matching` is the warm-promotion opener: it never creates or initializes a journal, and it compares the locked file's device/inode with the expected identity before reading or repairing it.

New journal files are created with mode 0600, and initialization synchronizes both the file and parent directory. Existing file permissions are unchanged. Ownership is explicitly unlocked on drop; OS process death also releases the lock.

The journal remains one unbounded file. A start with no usable snapshot reads and replays it from the beginning. A valid core snapshot lets exchange startup read, validate, and replay only the later suffix. There is one journal sync per group of queued commands.

## Committed mmap stream and readers

`src/exchange/event_stream.rs` implements a separate, fixed-size, same-host delivery cache. The server uses 4 MiB of payload space plus an 80-byte header. The journal remains the durability source. No mmap flush is required for acceptance, and the cache is never used to rebuild authoritative core state.

The header binds the stream to the journal's device/inode and publishes the committed journal byte end, last envelope sequence, and cache window bounds. A header checksum, an atomic ready marker, and per-record checksums detect interrupted or damaged publication. Shared/exclusive file locks protect raw mapped copies. Unsafe pointer access is confined to the module and documented; no mapped references escape it.

The writer appends complete framed batches into the window. When the window fills, it starts a new window. An oversized batch bypasses the cache while still advancing the committed journal watermark. Readers never pin old windows or require a delivery acknowledgment from the writer.

`StreamReader::open(journal, stream, checkpoint)` opens the journal read-only and maps the stream read-only. Each reader owns its position. `next_batch` returns one complete validated batch, or `None` when caught up. It copies the needed record under a shared lock, releases the lock, then validates/deserializes. Consumer processing runs outside the lock.

If the required batch is no longer cached, the reader reads the framed journal record only up to the published watermark. Merely appended but unpublished bytes are invisible. The same byte/sequence cursor drives catch-up and live reading, eliminating a separate handoff race. Corruption, gaps, duplicates, invalid checkpoints, regressed watermarks, and wrong journal identity produce errors without silently skipping events.

`ReaderCheckpoint` stores journal identity, the next envelope sequence, and the next record byte offset. Opening with a checkpoint scans and validates record boundaries from the beginning. Consumers should atomically persist projection state with their checkpoint. The reader API alone cannot guarantee exactly-once external side effects.

This implementation is synchronized, not lock-free. A reader paused while copying can delay publication. It uses JSON, file-lock system calls, and polling; no target throughput or sub-microsecond latency has been established.

## Startup and recovery

Production `main` calls `recover_runtime_with_stream_and_snapshot` before binding the HTTP listener. It first attempts to load `EXCHSNP1`, a versioned CRC-protected snapshot of the complete deterministic core. The snapshot records the journal device/inode, a complete-batch byte boundary, next journal-envelope sequence, next matching sequence, ledgers, risk state, order lifecycle/execution indexes, and FIFO book state.

When that snapshot and its restored invariants validate, `EventStore::open_suffix` acquires the writer lock, validates only records after the boundary, repairs only a torn suffix tail, and deterministically replays the suffix. The mmap writer is then initialized from the recovered journal high-water mark. The cache initially contains no payload, so readers recover old batches from disk and then follow newly cached batches.

A missing snapshot performs the established full journal recovery and creates a checkpoint after startup. The primary writes snapshots only at startup; while it trades, the warm replica writes them (see "Warm replica"). A present invalid, corrupt, inconsistent, or journal-mismatched snapshot is preserved, reported, and falls back to full replay; it is not automatically replaced during that run. This makes a checkpoint a recovery accelerator, never a second journal or a silent empty-start path. Snapshot replacement writes and syncs a private temporary file, atomically renames it, then syncs its parent directory. It never compacts, truncates, or replaces the journal.

A process killed after durable append but before publication leaves a recoverable command. A kill during cache copying leaves the ready marker unset; readers refuse that snapshot. Writer restart reconstructs publication from the journal. An already-running reader can continue across writer restart while the same files remain in place.

| Setting | Default |
|---|---|
| `EVENT_LOG_PATH` | `exchange-events.log` |
| `EVENT_STREAM_PATH` | `EVENT_LOG_PATH` plus `.mmap` |
| `EVENT_SNAPSHOT_PATH` | `EVENT_LOG_PATH` plus `.snapshot` |
| `EVENT_SNAPSHOT_INTERVAL` | `10000` commands between the warm replica's snapshots, which also writes one right after each open; must be positive |
| HTTP listener | `127.0.0.1:4000` |
| `EXCHANGE_OPERATOR_ADDR` | `127.0.0.1:4004`; the operator port, which must be a loopback address |

A path under `/dev/shm` may be selected for the stream; it is volatile and is not the journal. A persistent-path mmap still does not become the recovery authority. Do not modify, truncate, unlink, or replace files while mapped. All participating programs must obey the protocol; these are advisory locks and trusted local files, not a hostile-process boundary.

If the disposable cache is missing, startup recreates it. If it has an incompatible identity/size/magic, startup refuses instead of overwriting it. Stop all writers and readers before removing only that cache and restarting the writer. Reopen readers afterward; preserve the journal and checkpoints. A checkpoint for a replaced/copied journal is refused rather than guessed compatible. Network filesystems and cross-host readers are outside the supported model.

## Diagnostic subscriber

The probe is an independent process and runs before database setup:

```sh
cargo run -- --event-probe exchange-events.log exchange-events.log.mmap /tmp/stock-reader.json --once
```

Start the exchange writer at least once so the stream exists. The probe prints one JSON array per complete command. Omit `--once` to follow live publication (10 ms idle polling). Omit the checkpoint path to start at sequence 1.

An optional checkpoint is saved through a temporary file, file synchronization, atomic rename, and parent-directory synchronization. It is saved after stdout is flushed. A crash between output and checkpoint can repeat a batch; this diagnostic output is at-least-once across such a restart. Use one checkpoint file per consumer. The probe exits nonzero on invalid data and never repairs or truncates the journal.

Raw events contain private account/order details. New stream files are mode 0600. The probe is an internal debugging tool, not a public market-data feed.

## Market data publisher

The first business subscriber runs as an independent process before database and authentication initialization:

```sh
cargo run -- --market-data exchange-events.log exchange-events.log.mmap market-data-state.json [LISTEN_ADDR]
```

The listener defaults to `127.0.0.1:4001`. The MDP uses `StreamReader` to catch up completely before binding and then follows live committed batches with 10 ms idle polling. Trading does not call or wait for this process.

Its private state contains open orders plus one-minute UTC OHLCV candles. Public bid/ask levels are aggregated with checked `u64` arithmetic. Accepted new-order batches validate their acceptance and adjacent two-sided execution pairs, apply every trade once to the incoming and known resting quantities, then rest any incoming remainder. The candle projection uses the first record of each validated execution pair, so one match contributes one trade and one quantity; it buckets that recorded execution timestamp by flooring to 60 seconds. Rejections and non-market-data commands do not change either projection. Successful cancellations remove the known L2 remainder, and a close removes every expired order; an order still projected after a close means the projection has drifted from the exchange, which is a terminal error.

The served view is one lock holding `Option<View>`: the L2 projection, the candles, and the reader checkpoint just after the last batch applied to them. `None` means unavailable, and every route (including `/health`) reads the same lock. Each batch is read outside the lock and applied in place under it; if it fails, the follower replaces the view with `None` before releasing the lock, so a half-applied view is never served, and the error is terminal. The view and its checkpoint are saved together at most once a second (counted from the end of the previous save) and once at the end of catch-up before the listener binds; a crash replays up to about a second of journal onto the last saved pair. A guard withdraws the view if the follower thread ends for any reason. The version-2 JSON state stores the checkpoint, open orders, and all candles; L2 aggregates are rebuilt on load. Saving serializes to one buffer, then uses a private temporary file, file and directory synchronization, and atomic rename. A version-1 state file is incompatible because its checkpoint has no matching candle projection; it fails closed and must be rebuilt from the authoritative journal.

Missing state triggers replay from journal sequence 1. Present invalid JSON, unsupported versions, invalid orders, incompatible checkpoints, and state paths that alias the journal or stream refuse startup. A terminal stream, validation, persistence, or projection-lock error marks the MDP unavailable. Its health and L2 routes then return 503 instead of serving stale data.

## Reporter v1

Reporter is the second independent business subscriber. Start it only after applying the versioned PostgreSQL migrations in order: `migrations/20260926000000_create_reporter_tables.sql`, `migrations/20260930000000_reporter_rejections.sql`, `migrations/20261005000000_reporter_expiry.sql`, then `migrations/20261005100000_reporter_trading_days.sql`. Each after the first empties the report so that the reporter rebuilds it from journal sequence 1; stop the reporter before applying them. The checkpoint row's `report_version` accepts only the current format (4), so an older reporter cannot save a position into a migrated report.

```sh
cargo run -- --reporter exchange-events.log exchange-events.log.mmap [LISTEN_ADDR]
```

It defaults to `127.0.0.1:4002` and requires `DATABASE_URL`. It catches up before binding `GET /health`; a terminal stream, decoder, projection, or database error makes that route return 503 without affecting trading or MDP.

| Table | One row per | Key |
|---|---|---|
| `reporter_checkpoint` | reporter (the journal identity, the next complete-batch cursor, and the trading day the journal is in there) | singleton |
| `reported_orders` | accepted order, with its trading day and current lifecycle: new, partially filled, filled, canceled (with `cancellation_sequence`), or expired (with `expiry_sequence`) | `(trading_day, order_id)` |
| `reported_trades` | adjacent two-sided execution pair, with the trading day of the two orders it fills; execution ids are unique per symbol, because each symbol's book numbers its executions from `exec_0` and keeps counting across days | journal sequence of the first execution record |
| `rejected_orders` | rejected submission, with its reason and the trading day the journal was in (NULL before the first open) | journal sequence of the command's input |
| `rejected_cancellations` | refused cancellation, with the requester, the reason, and the trading day the journal was in | journal sequence of the command's input |

A rejected submission's order id need not be unique (a client retry, a reused rejected id, another user's id), and a refused cancellation is a fact about the attempt, so neither touches `reported_orders`. A successful cancellation must come from the order's owner. Exact unsigned journal identities and prices are stored as PostgreSQL `NUMERIC(20,0)`.

Batches are applied in groups of up to 1,000 per SQL transaction, committed together with the checkpoint just after the last applied batch; a group also ends when the reporter has caught up. A pre-commit crash therefore retries the whole group, and a post-commit restart resumes after it. Any error rolls back the whole group and stops the reporter. Each trade is one statement: a data-modifying CTE fills both orders and inserts the trade only if both fills applied. A close is also one statement, an `UPDATE ... FROM unnest(...)` over its expiries, which must change exactly one resting row per expiry; afterwards no order of that day may still rest in the report. The reporter learns the trading day from the opens in the journal and saves it with each checkpoint, because order-changing batches do not name their day. Catch-up runs at about 1,600 commands/s; see `docs/performance/06-reporter-batched-transactions.md`.

The projection uses the shared committed-batch decoder with MDP, but owns its own order-state and database checks. Missing checkpoint with existing report rows, incompatible journal identity, malformed batches, duplicate trade/execution identities, missing resting rows, a cancellation that does not match the owner, and impossible state changes fail closed. The startup check reads all four report tables, so a database without the second migration is refused before anything is written. There is no reporting query API.

## Warm replica v1

The warm replica is a third independent process, and the only one that can become the exchange. It opens no database and serves no customer routes:

```sh
cargo run -- --warm-replica exchange-events.log exchange-events.log.mmap exchange-events.log.snapshot [LISTEN_ADDR]
```

The management listener defaults to `127.0.0.1:4003` and must be a loopback address. It is an unauthenticated, trusted local control plane.

| Method | Path | Behavior |
|---|---|---|
| GET | `/health` | 200 while following is healthy; otherwise 503 |
| GET | `/status` | `{"role":"warm-replica","next_event_sequence":N}` |
| POST | `/promote` | 409 while the primary holds the journal writer lock; 202 once fenced; 503 after a terminal error |

`ReplicaCore` owns only an `ExchangeCore` and its next journal sequence. The warm starts from a valid primary snapshot when one exists, otherwise from sequence 1, catches up before binding, then follows live batches through its own `StreamReader` in journal-only mode: every batch is read from the durable journal, and the mmap stream supplies only the committed watermark. Each batch goes through `replay_committed_batch`, the same prepare/compare/commit path as recovery. The warm's applied checkpoint is separate from the reader's physical cursor and advances only after a batch has been compared and committed, so a replay failure stops the follower at the last good batch.

The warm replica is the exchange's snapshot writer. Every `EVENT_SNAPSHOT_INTERVAL` applied commands, and right after each open (when the previous day has just been cleared and the state is smallest), it writes the journal-bound `EXCHSNP1` snapshot at exactly its applied checkpoint, with state and boundary taken together, through the same temporary-file, sync, and atomic-rename path the primary uses at startup. If the snapshot file was invalid when the warm started, it is preserved and this warm writes none. Snapshot cost therefore no longer stops trading. It slows the warm replica instead, which at full load can fall behind and later catch up.

Promotion calls `EventStore::open_existing_matching` with the applied checkpoint's journal identity. While the old primary holds the writer lock the result is 409, and the warm continues following with nothing changed. With the lock taken, the file's device/inode is compared with the followed journal before any read or torn-tail repair, so a path that now names a different journal is refused untouched. Only then is the journal recovered. The warm core is discarded and `promote_replica_with_stream_and_snapshot` rebuilds from the entire journal; mmap-derived state is never promoted. The promoted process opens a new stream writer, republishes the journal watermark, attaches the snapshot schedule, connects PostgreSQL, and binds `127.0.0.1:4000` and the operator port exactly as a normal primary does.

A 202 means the old writer is fenced and the hand-off has begun, not that the customer listener is ready; poll `/health` on port 4000. Errors after the fence are terminal and fail closed. Promotion replays the whole journal, so its duration grows with history. There is no heartbeat, automatic failover, or second host.

## HTTP read and write surface

Private exchange queries use the same queue and single owner, returning through oneshot without adding journal or stream events. They do not use the mmap reader.

| Method | Path | Access |
|---|---|---|
| POST | `/exchange/deposit` | authenticated |
| POST | `/exchange/shares/deposit` | authenticated |
| POST | `/exchange/orders` | authenticated; returns order view; 409 for a duplicate client order id, while the market is closed, or when the book is full (`BookFull`) |
| POST | `/exchange/orders/cancel` | authenticated |
| POST | `/exchange/risk/limits` | authenticated; caller's cap |
| GET | `/exchange/balance` | caller's balance |
| GET | `/exchange/positions` | caller's shares and reservations |
| GET | `/exchange/executions?symbol=&order_id=&start_time=&end_time=` | caller's fills; optional filters |
| GET | `/exchange/risk/limits?symbol=` | caller's limit/usage |
| GET | `/exchange/orders/{order_id}` | owner only; non-owner also gets 404 |
| GET | `/health` | exchange worker availability on the trading listener |

PostgreSQL and SQLx support user registration/login. `AppState` holds the database pool, command sender, and worker availability flag. Database calls are outside matching, sequencing, journal recovery, and subscriber delivery.

The separate MDP listener has its own public routes and no database dependency:

| Method | Path | Behavior |
|---|---|---|
| GET | `/marketdata/orderbook/{symbol}?depth=N` | public L2; depth defaults to 10 and is clamped to 1-50; unknown symbol is 404 |
| GET | `/marketdata/candles?symbol=&start_time=&end_time=` | public ascending one-minute OHLCV candles for an inclusive epoch-second range; all parameters are required, invalid bounds are 400, and no matching trades return an empty array |
| GET | `/health` | 200 after catch-up while following is healthy; otherwise 503 |

The previous `/exchange/orderbook/{symbol}` route and `ExchangeCommand::GetOrderBook` path have been removed. Internal core L2 methods remain for correctness tests.

## Benchmark

`--bench` is an in-process load generator over the production worker, journal, and mmap stream. To include snapshot cost, run `--warm-replica` on the benchmark's directory alongside it. It needs an empty directory and never deletes anything:

```sh
cargo run --release -- --bench EMPTY_DIR [--orders N] [--rate ORDERS_PER_SEC|0] [--symbols N] [--users N] [--depth RESTING_PER_SYMBOL]
```

The workload is deterministic (fixed-seed). Fixed-rate mode measures latency from each order's intended send time into an HdrHistogram; `--rate 0` measures maximum throughput. Output: orders/s, p50/p90/p99/p99.9/max, orders per journal sync, journal bytes per order, rejections, and process memory. See `docs/performance/00-benchmark-harness.md`.

## Verification and remaining scope

On Linux, `cargo fmt -- --check` and `cargo test --locked` pass: 157 unit tests plus the executable integration tests (verified 2026-10-05 on the office Ubuntu machine). Snapshot tests prove the primary writes none while trading, a warm replica writes one at its exact applied boundary and a restart replays only the suffix, and a warm replica preserves an invalid snapshot and writes none. Group-commit tests prove one sync per queued group, reads that see earlier writes in their group, every command answered unavailable and nothing published after a failed sync, and a mid-group fault that still syncs the commands before it. A differential test runs 20,000 random order/cancel steps through the planned matcher and the old in-place matcher and requires identical executions, books and indexes after every step. The crate uses Unix-only APIs and does not build on Windows. Warm-replica coverage proves journal catch-up and live following without writes, a snapshot start, output-mismatch rejection that leaves the applied checkpoint in place, promotion refused while another process owns the journal, a swapped journal refused without its torn tail being repaired, recovery of a durable batch hidden from mmap, and the warm replica following — and promotion rebuilding from — the journal rather than a differing mmap cache. Coverage includes the close expiring every resting order exactly as cancellations would (ledgers, risk usage, an empty book, oldest-first sequences, a refused cancel of an expired order, a refused oversized close, the resting-order cap, a full book of worst-case ids closing in one record, replay and restart across the close, and the next open clearing the previous day so its ids return, with snapshot recovery and full replay agreeing), independent fast/slow readers, window overwrite, oversize batches, checkpoint boundary/identity validation, cache/journal corruption, competing writers, durable-but-unpublished recovery, failed append with no publication, fatal publication failure, real cross-process reading, SIGKILL at publication boundaries, probe checkpoint resume, and MDP projection/recovery/HTTP behavior without PostgreSQL. Candle coverage proves one-trade-per-pair OHLCV aggregation, invalid timestamp rejection, combined-state round-trip, journal catch-up, range validation, persisted restart without duplicate volume, and live mmap following. Core-snapshot coverage restores normalized FIFO books and ledgers, verifies suffix-only replay with both sequence domains continuing, rejects a journal mismatch, repairs only torn suffix tails, preserves a corrupt artifact while falling back to full replay, keeps the prior checkpoint on replacement failure, and proves a failed append cannot advance a snapshot. The three ignored opt-in Reporter acceptance tests reset a supplied isolated database and apply the four migrations. One injects a checkpoint failure and verifies that the whole group rolls back, then checks every lifecycle, trade, rejected-order and refused-cancellation row (including a retried id, a reused rejected id, an intruder's cancellation, two symbols sharing execution ids, a close expiring an untouched and a partly filled order, and a second day reusing an id and trading) and no duplicates after a restart. Another fails the group after a full 1,000-batch group and verifies that only the failed group rolled back. The third shows that a close leaving an order resting in the report stops the reporter without recording the close, and that a refusal before the first open has no trading day. All three passed against PostgreSQL 16 on 2026-10-05.

These tests include core/runtime and actual executable checks. MDP coverage includes oracle comparison, multi-fill and cancellation behavior, malformed batches, aggregation beyond `u32::MAX`, state replacement failures, journal catch-up, MDP and exchange-stream restarts, live following, and fail-closed 503 responses. A manual isolated-database run also exercised authenticated trading HTTP plus the separate MDP process through rest, partial fill, cancellation, MDP restart, and resumed live publication. A live run of the real primary and warm executables against PostgreSQL exercised promotion end to end: 409 while the primary ran, live following, a `SIGKILL` of the primary, 202, identical balances, positions, and order state on the promoted primary, a new trade there, and journal sequences contiguous across the hand-off. This does not claim machine power-loss testing or performance benchmarking. Clippy still reports existing compatibility/dead-code and style warnings.

Measured with `--bench` on a Docker Desktop VM: about 37,000-39,000 orders/s on disk at maximum rate, 43,000 in memory, and p99 about 20-30 ms at 1,000 orders/s. With snapshots every 10,000 commands written by the warm replica, 200,000 orders run at about 21,900 orders/s (about 8,300 when the trading thread wrote them). The MDP catches up at about 56,000 commands/s and stays within a second of the exchange live; the reporter catches up at about 1,600 commands/s (`docs/performance/05` and `06`). Candle buckets are retained without a limit and have no rollups or external historical store. Tax/customer statements, settlement, historical reporting APIs, journal compaction/retention, automatic hot-warm failover, cross-host replication and recovery, mmap ingress, lock-free queues, pipelined journal sync, and CPU pinning remain separate milestones.
