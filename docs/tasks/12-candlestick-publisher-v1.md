# Task 12 - Candlestick Publisher v1

## What changed

The existing independent market-data publisher now derives public one-minute UTC OHLCV candles
from the same complete committed batches as its L2 book. This is a read model: it does not query
or mutate `ExchangeCore`, delay order acceptance, or create another trading worker.

Each exchange match produces two adjacent `ExecutionCreated` records, one for each side. The
shared committed-batch decoder validates that pair; the candle projection then reads only the
first execution. One match therefore adds its quantity and one trade count exactly once.

The bucket is the recorded execution timestamp floored to 60 seconds. That timestamp is already
part of the committed history, so a journal replay produces the same candle result without a
subscriber clock read. It is deliberately not a new exchange-time model: the current execution
timestamp remains derived from the matched orders' recorded gateway timestamps.

## State and recovery boundary

MDP state format version 2 contains its open-order projection, every candle bucket, and the one
`ReaderCheckpoint` for both views. For each committed batch, MDP clones L2 and candles, applies
and validates both candidates, writes the complete state to a private temporary file, synchronizes
it, atomically renames it, then updates served memory.

That order is the important guarantee. A crash or write error leaves the old projection and old
checkpoint together, so the batch is retried. A successful replacement advances both views and
the checkpoint together, so restart does not double-count a trade. Missing state replays the
authoritative journal. Corrupt state, invalid candles, an incompatible checkpoint, and old
version-1 state refuse startup rather than guessing; remove only the disposable MDP state and let
it rebuild from the preserved journal.

## Public API

```text
GET /marketdata/candles?symbol=AAPL&start_time=60&end_time=119
```

All three parameters are required. Times are inclusive Unix-epoch seconds and `start_time` must
not exceed `end_time`. The response contains the symbol and ascending candle array. A valid range
with no trades is a successful empty array; malformed or missing parameters return 400. As with
L2, terminal reader, projection, or persistence failures make this route and `/health` return
503 rather than serving stale data.

## Deliberate boundaries

V1 retains all one-minute buckets in the MDP state. It has no configurable resolution, rollups,
retention, compaction, database analytics store, trade tape, separate candle process, or exchange
core change. Those need their own data-retention and performance decisions. Existing cooperative
mmap locks, JSON batches, and polling remain correctness-first transport choices, not latency
claims.

## Verification

The tests cover OHLCV updates across minute boundaries, one trade per two-sided execution pair,
invalid timestamp atomicity, combined L2/candle state round-trip, failed state replacement, and
the MDP executable catching up from journal, serving range queries, restarting without duplicate
volume, and following a live mmap publication. Existing MDP executable coverage continues to
prove fail-closed 503 behavior after a terminal follower error.

Verified on 2026-09-27 with `cargo fmt -- --check` and `cargo test --locked --offline`: 120 unit
tests and 5 executable integration tests passed. The opt-in PostgreSQL Reporter qualification is
unchanged.
