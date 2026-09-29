# 05 - Market Data That Keeps Up

Milestone 21, part 1. Code: `Follower`, `View` and `WithdrawOnExit` in
`src/exchange/market_data.rs`, and `CandleProjection::apply_batch` in `src/exchange/candles.rs`.

## The problem

The market-data publisher (MDP) is a separate process. It follows the exchange's committed events
and serves the public order book (L2) and the one-minute candles. After milestones 19 and 20 the
exchange could process about 25,000 orders a second. The MDP could process **about 39**.

For every single command, it did all of this:

```text
copy the whole order-book projection      (twice)
copy the whole candle history             (three times: once more inside the candle code)
apply the command to the copies
serialize the ENTIRE state to JSON        (written to the file in thousands of tiny writes)
fsync the file, rename it, fsync the directory
swap the copies in as the served view
```

That is two disk syncs and a full rewrite of the state for every order. The syncs and rename alone
cost around 10 ms on the benchmark disk, and the copies and JSON grew with the state. So under any real load
the public order book fell further behind every second. Catching up on a 200,000-order journal would
have taken about 86 minutes.

## Why it was built that way

Correctness. Each command was applied to a *copy*, and only if that worked was the copy saved
together with its reader checkpoint and then swapped in. So a bad batch could never be served
half-applied, and the file on disk always held a matching pair: state plus the exact journal
position that state represents. Both properties are worth keeping. The cost was paying for them on
every command.

## The idea

Keep both guarantees, but stop paying for them per command:

1. **Apply in place.** No copies. If a batch fails halfway, the whole view is withdrawn: it is
   replaced by "unavailable" before anyone else can look at it. Nothing half-applied is ever
   served.
2. **Save on a timer, not per command.** Save the state and its checkpoint together, as before, but
   at most once a second. After a crash, restart from the last save and replay the journal after
   it. Replay is deterministic, so the result is identical.

This is how stream processors checkpoint. Kafka Streams commits every 100 ms to 30 s, and Flink
checkpoints on an interval. It is the same lesson as group commit in `01`: amortize the expensive
step over many commands.

## How it works

### One lock, and "unavailable" lives inside it

```rust
struct View {
    orders: MarketDataProjection,   // L2 book
    candles: CandleProjection,      // one-minute candles
    applied: ReaderCheckpoint,      // reader position just after the last batch applied here
}
type Served = Arc<RwLock<Option<View>>>;   // None = unavailable
```

Before, there were two locks (book and candles) and a separate "available" flag, and the handlers
checked the flag *before* taking the lock. Now every route, including `/health`, reads one lock, and
"unavailable" is simply `None` inside it:

- The order book and candles always change together.
- A half-applied view cannot be observed. When a batch fails, the follower sets the slot to `None`
  *while it still holds the write lock*, so any reader that gets the lock afterwards sees `None`
  and returns 503. That property follows from the structure, not from getting a flag check in
  the right order.

### The follower step

`Follower::step` does this for each batch:

```text
next_batch()                      outside the lock (reading the journal/mmap takes time)
take the write lock
apply the batch to the book and the candles, in place
record `applied` = the reader's position after this batch
release the lock
```

If applying fails, the slot becomes `None` before the lock is released, and the error is terminal:
the process keeps running but serves 503 until it is restarted, exactly as before. The candle code
used to copy its whole history on every batch to stay unchanged on failure. That copy is gone,
because the failure now withdraws the entire view anyway.

### The save

`Follower::maybe_save` saves only when there are unsaved batches **and** a second has passed since
the previous save *finished*. It checks after every batch and while idle. Counting from the end of a
save matters: if a save ever took longer than the interval, counting from its start would trigger a
new save after every batch, and throughput would collapse back to the old behavior.

`Follower::save` copies the book, the candles, and `applied` under one read lock, so the three are
guaranteed to match. It then writes them outside the lock, the same safe way as before:

```text
serialize to one buffer → write a private temp file → fsync → rename over the old file → fsync directory
```

Three rules keep the saved pair correct:

- **Never save on an error path.** A failed batch withdraws the view, so there is nothing to save.
- **Never save `reader.checkpoint()`.** The reader has already moved past a batch that may have
  failed. Save the `applied` checkpoint stored with the view.
- **One unconditional save at the end of catch-up**, before the HTTP listener binds, so the file on
  disk always matches what the process is about to serve.

Each save logs its size and duration, for example
`market-data state saved through event sequence 630326 (59681 batches, 2924611 bytes) in 58 ms`.

### If the follower thread dies

A small guard (`WithdrawOnExit`) sets the view to `None` when the follower thread ends for any
reason, including a panic. Before, a panic outside the lock left `/health` answering 200 while the
data silently stopped moving.

### Crash between saves

The file holds the state as of the last save and the checkpoint just after it. On restart the MDP
loads both, opens the reader at that checkpoint, and replays the journal from there. Up to about a
second of batches is applied again, onto a state that has not seen them yet, so nothing is counted
twice. The format and version of the state file are unchanged.

## Results

The same 200,000-order journal (10 symbols, 10 users, 200,110 commands), on the container's disk.

**Catch-up speed**, from an empty state until `/health` answers:

| | Result |
|---|---|
| Before | 2,328 commands in 60 s, about **39 commands/s**. The full journal would take about 86 minutes. |
| After | the whole journal in **3.6 s**, about **56,000 commands/s** (about 1,400×) |

During that catch-up the new MDP saved 4 times, each taking 36–70 ms (median 49 ms) for about 2.9 MB
of state.

**Live**, with the exchange running and the MDP following:

| Exchange load | MDP |
|---|---|
| 5,000 orders/s for 20 s | kept up. Its last save matched the journal's end 10 ms after the exchange stopped; 20 saves, one a second. |
| Maximum, about 34,500 orders/s | kept up. Its final save came 26 ms after the exchange stopped; 6 saves. |

The saves run once a second, so "kept up" here means the MDP was never more than about a second
behind. Before, it fell behind at any load above about 39 commands/s. Running the MDP did not slow
the exchange: it did 34,543 orders/s at maximum with the MDP following, in line with earlier runs
without it.

## Tests

- `a_failed_batch_is_never_served_or_saved`: two good batches, each saved (interval zero), then a
  batch with a duplicated execution id. The step fails, the view becomes `None`, a save attempt
  fails because there is nothing to save, and the state file is byte-for-byte unchanged.
- `unsaved_batches_wait_for_the_interval_and_the_final_save_catches_up`: with a long interval, two
  batches are served immediately but not saved; the explicit final save writes them with the right
  checkpoint.
- `restarting_from_an_older_save_replays_to_the_same_view`: the real exchange produces deposits, two
  sells and a crossing buy. One follower saves after the first batch and then applies the rest
  unsaved, as if it crashed. A second follower loads that older save and replays, and ends with an
  identical book and candles (one trade, one candle, one ask, one bid).
- The three executable tests in `tests/market_data.rs` (catch-up, restart, live following, candles,
  and a corrupt state file) pass unchanged.
- The candle test that required "nothing changes on a failed batch" now only requires the failure.
  That guarantee moved up a level: the whole view is withdrawn.

## Options considered and rejected

- **Keep the per-command copies, only save less often.** The copies grow with the state and cost
  O(state) per command. Saving less often alone leaves most of that cost.
- **Validate first, then apply**, so the projection can never be half-applied. More code for a
  guarantee the "withdraw the view" rule already gives.
- **A separate saver thread.** Not needed: a save of about 3 MB takes about 50 ms once a second.
- **Make the interval configurable.** One constant (`SAVE_INTERVAL`, 1 s) is enough until a
  measurement says otherwise.

## What remains

- Candle history is never trimmed, so the state and each save still grow slowly. The trading-day
  boundary planned next bounds this.
- On restart the reader re-scans the journal from the start to validate its checkpoint (O(history)).
  That is a startup cost only.
