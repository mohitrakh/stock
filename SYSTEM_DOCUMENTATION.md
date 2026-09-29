# Stock Exchange - Current Implementation

Verified against this checkout on 2026-09-27. This file describes implemented behavior. `stock-exchange-system-design.md` is the target architecture; `PROJECT_DIRECTION.md` is the milestone journal.

## Ownership and command flow

Axum handlers send `ExchangeCommand` values through a bounded Tokio queue (10,000 commands) to one dedicated exchange-worker thread. Commands may carry a temporary `oneshot` reply channel. Persisted `ExchangeEvent` values contain only replayable business data.

`ExchangeRuntime` owns the receiver, core, durable event store, mmap stream writer, optional core-snapshot schedule, in-memory replay history, and next journal sequence. After a snapshot restart the in-memory vector holds only the replayed suffix; the complete history remains in the durable journal. `ExchangeCore` owns the order manager, matching engine, and matching sequencer. The core does not access files, databases, queues, or HTTP.

A mutating command follows this order:

```text
prepare and validate the entire transition without changing live state
  -> number input and outputs as one batch
  -> serialize once, append to journal, sync_all
  -> commit the prepared core transition
  -> extend in-memory history and advance journal sequence
  -> publish the complete batch and committed watermark through mmap
  -> notify local execution callbacks
  -> periodically replace a snapshot of this committed core state
  -> answer the waiting HTTP request
```

Preparation includes every fill and cumulative settlement effect. Business rejections produce a durable input/rejection batch without changing trading state or consuming matching sequence. Internal faults halt processing; they are not recorded as ordinary client rejections.

An append error leaves the live core, journal sequence, in-memory history, callbacks, and mmap publication unchanged. Disk outcome can be ambiguous after an I/O error, so the writer stops and recovery decides whether a complete record survived. A publication error occurs after durable/core commit: the writer also stops, but the command can appear on recovery. An unavailable response is therefore not proof that a command did not happen.

The worker closes its receiver on exit. `AppState.exchange_available` becomes false, and `/health` returns 503. While the worker is available, health returns 200. This flag is not a full readiness/latency monitor.

## Core components

| Component | Ownership and current role |
|---|---|
| `ExchangeCore` | Coordinates read-only preparation and infallible installation of validated plans. |
| `OrderManager` | Owns lifecycle records, cash wallet, share positions, risk usage, execution indexes, and local execution callbacks. |
| `MatchingEngine` | Owns symbol books and order-to-symbol lookup. Prepares matching/cancellation against a clone of the affected book, then installs it at commit. |
| `OrderBook` | Holds bid/ask price levels and order-node lookup; matches price/time priority while skipping self trades. |
| `PriceLevel` / `Node` | Maintain FIFO order at a price through indexed linked nodes. |
| `Sequencer` | Supplies a candidate matching sequence with `peek`; `commit` advances it only when the prepared order/cancellation commits. |

The production path uses `prepare_add_order` / `commit_add_order` and `prepare_cancel_order` / `commit_cancel_order`. It does not use the former mutating `prepare_order -> apply_executions -> complete_cancel` path.

The order manager's `prepare_new_order` validates execution pairs, cumulative fills, projected cash/shares, collateral, risk, lifecycle changes, and execution views. `prepare_cancel_plan` validates ownership, state, and collateral release. Their commit methods install the prepared values. Preparation clones the affected symbol book, not the entire exchange.

## Prices, collateral, risk, and fills

`Price(u64)` stores positive integer minor units. The API also uses integer minor-unit prices: with cents, 1025 represents $10.25. Checked arithmetic protects notionals and ledger credits. One shared price/currency scale is assumed; product tick-size and currency metadata are not implemented.

A buy reserves its limit-price notional. A sell reserves shares. Settlement transfers cash from buyer to seller and shares from seller to buyer, releases price improvement for buys, and adjusts both orders' remaining quantity. The whole command is validated before any settlement is committed. Cancellation releases the unfilled collateral and eligible risk allowance.

`RiskManager` tracks limits and usage by user/symbol. The default daily cap is 1,000,000 shares. Usage means quantity traded today plus quantity still open. Open exposure is tracked separately so overnight resting orders remain in the next day's starting usage. Accepted submissions increase usage and exposure; fills reduce exposure without refunding today's usage; cancellation returns only the unfilled exposure. The effective trading day comes from the recorded order timestamp, in UTC 86,400-second buckets, and only advances. Preparation validates fill and cancellation risk changes before commit. Limit changes are journaled events. There are no market calendars or portfolio-wide limits, and the current API allows a trader to set their own cap.

Orders have new, partially-filled, filled, or canceled lifecycle states. Executions record each party's side and are indexed for account queries. The two execution-side records for a match must not later be counted as two distinct market trades by a market-data consumer.

`client_order_id` is optional, trimmed, and 1-64 characters. A provided value becomes the order id; duplicate ids are rejected instead of opening another order. Generated ids use UUIDs. This is order-specific duplicate protection, not general idempotency for deposits, cancellations, or all HTTP commands.

## Events and replay

`ExchangeInputEvent` covers cash deposits, share deposits, risk-limit changes, new orders, and cancellations. `ExchangeOutputEvent` covers their results and executions. `EventEnvelope.seq_num` is the contiguous journal sequence across inputs and outputs.

Journal sequence and matching sequence are different domains. A command may emit several envelopes, while accepted orders/cancellations consume matching sequences. Subscriber checkpoints use journal sequence.

`prepare_input_event` computes the result, outputs, and prepared core change. `replay_event_log` validates contiguous journal sequencing from 1, prepares each recorded input, compares regenerated outputs with the following recorded outputs, then commits that plan. Recorded outputs are checked rather than applied a second time. Replay does not publish callbacks.

Replay rejects missing/unexpected outputs, output mismatches, and sequence mismatches. Full replay starts from an empty core at event sequence 1. Snapshot recovery validates a restored core and starts the same deterministic replay at the snapshot's recorded next event sequence, over only the later journal suffix.

## Durable event store

`src/exchange/event_store.rs` owns journal mutation. Independent subscribers have a separate read-only handle; they never call the recovery opener.

The format is an 8-byte `EXCHLOG1` header, followed by records:

```text
payload length (u32 LE) | CRC-32 (u32 LE) | JSON Vec<EventEnvelope>
```

Each record contains exactly one input and its outputs. The maximum JSON payload is 64 MiB. The runtime calls `encode_record` before persistence and passes those same bytes to `append_record` and later mmap publication. `append_record` uses `write_all` and `sync_all`.

`EventStore::open` acquires a nonblocking exclusive lifetime file lock, validates the header and records, and truncates an incomplete final frame. Complete records with invalid checksums, JSON, or command shape cause refusal. Deterministic replay then checks business correctness. Two writers cannot recover or append to the same journal concurrently through this API. `EventStore::open_existing_matching` is the warm-promotion opener: it never creates or initializes a journal, and it compares the locked file's device/inode with the expected identity before reading or repairing it.

New journal files are created with mode 0600, and initialization synchronizes both the file and parent directory. Existing file permissions are unchanged. Ownership is explicitly unlocked on drop; OS process death also releases the lock.

The journal remains one unbounded file. A start with no usable snapshot reads and replays it from the beginning. A valid core snapshot lets exchange startup read, validate, and retain only the later suffix. There is one journal fsync per command.

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

A missing snapshot performs the established full journal recovery and creates a checkpoint after startup. A present invalid, corrupt, inconsistent, or journal-mismatched snapshot is preserved, reported, and falls back to full replay; it is not automatically replaced during that run. This makes a checkpoint a recovery accelerator, never a second journal or a silent empty-start path. Snapshot replacement writes and syncs a private temporary file, atomically renames it, then syncs its parent directory. It never compacts, truncates, or replaces the journal.

A process killed after durable append but before publication leaves a recoverable command. A kill during cache copying leaves the ready marker unset; readers refuse that snapshot. Writer restart reconstructs publication from the journal. An already-running reader can continue across writer restart while the same files remain in place.

| Setting | Default |
|---|---|
| `EVENT_LOG_PATH` | `exchange-events.log` |
| `EVENT_STREAM_PATH` | `EVENT_LOG_PATH` plus `.mmap` |
| `EVENT_SNAPSHOT_PATH` | `EVENT_LOG_PATH` plus `.snapshot` |
| `EVENT_SNAPSHOT_INTERVAL` | `10000` processed commands; must be positive |
| HTTP listener | `127.0.0.1:4000` |

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

Its private state contains open orders plus one-minute UTC OHLCV candles. Public bid/ask levels are aggregated with checked `u64` arithmetic. Accepted new-order batches validate their acceptance and adjacent two-sided execution pairs, apply every trade once to the incoming and known resting quantities, then rest any incoming remainder. The candle projection uses the first record of each validated execution pair, so one match contributes one trade and one quantity; it buckets that recorded execution timestamp by flooring to 60 seconds. Rejections and non-market-data commands do not change either projection. Successful cancellations remove the known L2 remainder.

For every batch, the MDP clones both projections, applies and validates the command, then persists both candidates with the reader's advanced checkpoint before replacing either served snapshot. The version-2 JSON state stores the checkpoint, open orders, and all candles; L2 aggregates are rebuilt on load. Saving uses a private temporary file, file and directory synchronization, and atomic rename. A version-1 state file is incompatible because its checkpoint has no matching candle projection; it fails closed and must be rebuilt from the authoritative journal.

Missing state triggers replay from journal sequence 1. Present invalid JSON, unsupported versions, invalid orders, incompatible checkpoints, and state paths that alias the journal or stream refuse startup. A terminal stream, validation, persistence, or projection-lock error marks the MDP unavailable. Its health and L2 routes then return 503 instead of serving stale data.

## Reporter v1

Reporter is the second independent business subscriber. Start it only after applying the versioned PostgreSQL migrations:

```sh
cargo run -- --reporter exchange-events.log exchange-events.log.mmap [LISTEN_ADDR]
```

It defaults to `127.0.0.1:4002` and requires `DATABASE_URL`. It catches up before binding `GET /health`; a terminal stream, decoder, projection, or database error makes that route return 503 without affecting trading or MDP.

`reporter_checkpoint` stores the journal identity and next complete-batch cursor. `reported_orders` preserves each submitted order's current lifecycle and outcome; `reported_trades` stores exactly one row for each adjacent two-sided execution pair. Exact unsigned journal identities and prices are stored as PostgreSQL `NUMERIC(20,0)`. Each batch performs projection writes and checkpoint advancement in one SQL transaction, so a pre-commit crash retries the whole batch and a post-commit restart resumes after it. The isolated Reporter acceptance test injects a failing checkpoint trigger after order/trade writes begin and verifies all of those writes roll back; it then restarts a committed reporter and verifies no rows are duplicated.

The projection uses the shared committed-batch decoder with MDP, but owns its own order-state and database checks. Missing checkpoint with existing report rows, incompatible journal identity, malformed batches, duplicate trade/execution identities, missing resting rows, and impossible state changes fail closed. There is no reporting query API in v1. Two known defects — a reused client order id halting the reporter permanently, and a rejected cancellation overwriting the owner's order row — are recorded in `DEFERRED_ITEMS.md`.

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

`ReplicaCore` owns only an `ExchangeCore` and its next journal sequence. The warm starts from a valid primary snapshot when one exists, otherwise from sequence 1, catches up before binding, then follows live batches through its own `StreamReader`. Each batch goes through `replay_committed_batch`, the same prepare/compare/commit path as recovery. The warm's applied checkpoint is separate from the reader's physical cursor and advances only after a batch has been compared and committed, so a replay failure stops the follower at the last good batch.

Promotion calls `EventStore::open_existing_matching` with the applied checkpoint's journal identity. While the old primary holds the writer lock the result is 409, and the warm continues following with nothing changed. With the lock taken, the file's device/inode is compared with the followed journal before any read or torn-tail repair, so a path that now names a different journal is refused untouched. Only then is the journal recovered. The warm core is discarded and `promote_replica_with_stream_and_snapshot` rebuilds from the entire journal; mmap-derived state is never promoted. The promoted process opens a new stream writer, republishes the journal watermark, attaches the snapshot schedule, connects PostgreSQL, and binds `127.0.0.1:4000` exactly as a normal primary does.

A 202 means the old writer is fenced and the hand-off has begun, not that the customer listener is ready; poll `/health` on port 4000. Errors after the fence are terminal and fail closed. Promotion replays the whole journal, so its duration grows with history. There is no heartbeat, automatic failover, or second host.

## HTTP read and write surface

Private exchange queries use the same queue and single owner, returning through oneshot without adding journal or stream events. They do not use the mmap reader.

| Method | Path | Access |
|---|---|---|
| POST | `/exchange/deposit` | authenticated |
| POST | `/exchange/shares/deposit` | authenticated |
| POST | `/exchange/orders` | authenticated; returns order view |
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

## Verification and remaining scope

On Linux, `cargo fmt -- --check` and `cargo test --locked` pass: 131 unit tests plus 6 executable integration tests (verified 2026-09-29). The crate uses Unix-only APIs and does not build on Windows. Warm-replica coverage proves journal catch-up and live following without writes, a snapshot start, output-mismatch rejection that leaves the applied checkpoint in place, promotion refused while another process owns the journal, a swapped journal refused without its torn tail being repaired, recovery of a durable batch hidden from mmap, and promotion rebuilding from the journal rather than a differing mmap cache. Coverage includes overnight risk rollover and durable replay, independent fast/slow readers, window overwrite, oversize batches, checkpoint boundary/identity validation, cache/journal corruption, competing writers, durable-but-unpublished recovery, failed append with no publication, fatal publication failure, real cross-process reading, SIGKILL at publication boundaries, probe checkpoint resume, and MDP projection/recovery/HTTP behavior without PostgreSQL. Candle coverage proves one-trade-per-pair OHLCV aggregation, invalid timestamp atomicity, combined-state round-trip, journal catch-up, range validation, persisted restart without duplicate volume, and live mmap following. Core-snapshot coverage restores normalized FIFO books and ledgers, verifies suffix-only replay with both sequence domains continuing, rejects a journal mismatch, repairs only torn suffix tails, preserves a corrupt artifact while falling back to full replay, keeps the prior checkpoint on replacement failure, and proves a failed append cannot advance a snapshot. The ignored opt-in Reporter executable test resets a supplied isolated database, applies the migration, injects a checkpoint transaction failure, then verifies rollback, journal-to-mmap catch-up, lifecycle/trade rows, and no duplicates after process restart. It passed against a freshly initialized local PostgreSQL 18 instance on 2026-09-27.

These tests include core/runtime and actual executable checks. MDP coverage includes oracle comparison, multi-fill and cancellation behavior, malformed batches, aggregation beyond `u32::MAX`, state replacement failures, journal catch-up, MDP and exchange-stream restarts, live following, and fail-closed 503 responses. A manual isolated-database run also exercised authenticated trading HTTP plus the separate MDP process through rest, partial fill, cancellation, MDP restart, and resumed live publication. A live run of the real primary and warm executables against PostgreSQL exercised promotion end to end: 409 while the primary ran, live following, a `SIGKILL` of the primary, 202, identical balances, positions, and order state on the promoted primary, a new trade there, and journal sequences contiguous across the hand-off. This does not claim machine power-loss testing or performance benchmarking. Clippy still reports existing compatibility/dead-code and style warnings.

Both subscribers are correctness-first and have not been throughput tested. Candle buckets are retained without a limit and have no rollups or external historical store. Tax/customer statements, settlement, historical reporting APIs, journal compaction/retention, automatic hot-warm failover, cross-host replication and recovery, mmap ingress, lock-free queues, group commit, and CPU pinning remain separate milestones.
