# Market Data Publisher v1

Implemented 2026-09-26. This milestone adds the first independent business subscriber to the committed exchange-event stream.

## Problem

The exchange previously served public L2 depth by sending `ExchangeCommand::GetOrderBook` through the same queue used for trading commands. That exposed a useful read, but it kept market data coupled to the single-owner matching process. A slow or unavailable market-data client should not add work to the trading worker, and a market-data failure must not stop order processing.

The completed mmap stream already provides the required boundary: it publishes only complete command batches after the journal and core state have committed. MDP v1 consumes that boundary in a separate OS process and builds its own read model.

## Process and data flow

Start the service with:

```sh
cargo run -- --market-data JOURNAL STREAM STATE_FILE [LISTEN_ADDR]
```

The default listener is `127.0.0.1:4001`. This mode is selected before dotenv, PostgreSQL, or authentication initialization, so the MDP can run with no database configuration.

```text
single-owner exchange worker
  -> durable journal
  -> committed mmap delivery cache
  -> independent StreamReader in the MDP
  -> private open-order projection
  -> public per-symbol L2 aggregates
  -> MDP-owned HTTP listener
```

Trading has no dependency on the MDP. Stopping the MDP does not block journal append, core commit, mmap publication, or the exchange HTTP response. On restart, the MDP resumes from its saved projection and checkpoint, or rebuilds from journal sequence 1 if no state file exists.

## Projection contract

`StreamReader` returns one durable command record at a time. The MDP treats that whole record as the atomic input to its projection.

For an accepted new order, the MDP validates the matching `OrderAccepted` output. The remaining outputs must be adjacent two-sided `ExecutionCreated` pairs. Both records in a pair must describe the same buy order, sell order, symbol, price, quantity, and timestamp, while using distinct execution ids. Execution ids may not be reused elsewhere in the same accepted-order batch.

Each pair is applied once as one trade. Exactly one side must be the incoming order. The other order must already be a known resting order on the opposite side and symbol. The trade price must equal that resting order's price, the incoming limit must cross it, and neither remaining quantity may underflow. Any incoming remainder then rests at its limit price.

Rejected orders, rejected cancellations, deposits, share deposits, and risk-limit changes leave the projection unchanged. A successful cancellation removes the known remaining quantity of the canceled order. Unknown orders, malformed pairs, duplicate orders, mismatched symbols or sides, bad prices, underflow, and aggregate overflow are terminal projection errors.

The projection stores only fields required for public L2: order id, symbol, side, price, and remaining quantity. It does not persist user ids, balances, positions, risk limits, or raw event batches.

## Atomic projection updates

Each command is applied to a clone of the last served projection. The candidate projection and the reader checkpoint after that command are written together before the shared HTTP snapshot changes.

The state file is versioned JSON with format version 1. It contains the `ReaderCheckpoint` and all open projected orders. Aggregate books are rebuilt from that order map during load with checked `u64` addition. This avoids persisting two independently mutable representations of the same book.

Saving uses a private temporary file, serialization, flush, `sync_all`, atomic rename, and parent-directory synchronization. If validation or saving fails, the last served snapshot is not replaced. A failed rename leaves the previous state file intact.

A present state file is never silently discarded. Invalid JSON, an unsupported version, duplicate or invalid projected orders, an invalid checkpoint, or a checkpoint bound to another journal refuses startup. The state path may not be the journal, mmap stream, or a hard-link alias of either.

The MDP catches up fully before binding its listener. Its follower then polls the existing `StreamReader` every 10 ms when caught up. A terminal reader, validation, persistence, or projection-lock error marks the service unavailable; both health and L2 return 503 rather than serving a stale book.

## Public API

The MDP owns these routes:

```text
GET /health
GET /marketdata/orderbook/{symbol}?depth=N
```

Health returns 200 only after initial catch-up and while the follower remains healthy. L2 uses the existing `OrderBookView` JSON structure. Depth defaults to 10 and is clamped to 1 through 50. An unknown symbol returns 404, and an unavailable follower returns 503.

The old exchange route `GET /exchange/orderbook/{symbol}` and its `ExchangeCommand::GetOrderBook` path were removed. Internal core snapshot methods remain as a test oracle. L2 quantities in both the core oracle and MDP response are now `u64`, allowing several valid `u32` orders at one price to aggregate without wrapping.

## Verification

Unit tests cover resting orders, rejected commands, successful and rejected cancellation, full and partial fills, multiple fills, self-trade-skip parity with `ExchangeCore`, side ordering, depth, unknown symbols, and aggregation above `u32::MAX`.

Malformed-input tests reject odd execution counts, inconsistent pairs, duplicate execution ids within and across pairs, missing resting orders, wrong sides and symbols, price disagreement, quantity underflow, duplicate projected orders, and `u64` aggregate overflow.

Recovery tests cover state/checkpoint round trips, missing-state rebuild, corrupt and unsupported state refusal, journal identity mismatch, unsafe state paths, failed replacement preserving the old state file, journal catch-up when early records are outside the mmap window, restart from saved state, live following, and exchange stream restart.

The executable integration test starts the real MDP binary without `DATABASE_URL`, queries its HTTP endpoint, kills and restarts it, verifies identical output, publishes another live batch, and verifies fail-closed 503 responses after a terminal follower error. A runtime-level test drives the real exchange writer, consumes its stream, restarts the writer on the same journal, and continues from the reader checkpoint. Projection snapshots are also compared with `ExchangeCore::l2_snapshot` as the correctness oracle.

The final repository gates pass: `cargo fmt -- --check`, `cargo test --locked --offline`, `cargo clippy --locked --offline --all-targets`, and `git diff --check`. Clippy exits successfully with the repository's existing warnings.

A manual acceptance run used an isolated temporary PostgreSQL cluster with the real exchange on port 4000 and the real MDP on port 4011. A resting ask appeared with quantity 5, a crossing buy reduced it to 3, and cancellation removed the symbol with a 404 response. After restarting only the MDP from its saved state and checkpoint, a newly committed ask at price 101 appeared with quantity 2. The temporary database, processes, journal, stream, and state were removed afterward.

## Boundaries and follow-up work

MDP v1 favors correctness over throughput. It clones the projection and synchronizes a complete JSON state file after every command batch. That work is outside the trading path, but it will not scale like a compact binary projection log or incremental snapshot design.

The mmap transport still uses JSON, polling, and cooperative file locks. It is a same-host delivery cache, not cross-host distribution and not a lock-free latency claim.

This milestone contains only L2. Trade tape, candles, historical analytics, reporting/database projections, multicast, external storage, paid depth tiers, authoritative exchange snapshots, hot-warm failover, and per-symbol matching workers remain separate architecture decisions.
