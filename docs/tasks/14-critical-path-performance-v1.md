# Task 14 - Critical-Path Performance v1 (Milestone 19)

## Goal

Measure the exchange for the first time, then fix what the measurements say is slowest, and prove
each fix with a before and an after. Each optimization has its own detailed write-up:

| File | Contents |
|---|---|
| `docs/performance/00-benchmark-harness.md` | the measuring tool, coordinated omission, HdrHistogram, baseline numbers |
| `docs/performance/01-group-commit.md` | one disk sync for many orders |
| `docs/performance/02-match-without-copying-the-book.md` | matching by plan and apply instead of copying the book |
| `docs/performance/03-no-history-in-ram.md` | dropping the unused in-memory event history |

The raw output of every run is in `docs/performance/results/`: one file per code stage, the
memory-pressure trace, and the profile.

This file ties them together: why this milestone came next, what the combined result is, what
changed in the exchange's rules, and what the numbers say to do next.

## Why this milestone came next

After Warm Replica v1 every component in the design existed in some form, but none of the design's
performance requirements had ever been checked: 43,000 orders/s on average, 215,000 at peak,
millisecond p99, measured with HdrHistogram. Several deferred ideas (group commit, lock-free
transport, binary encoding, CPU pinning, per-symbol workers) were all waiting on "measured evidence".
Cross-machine replication, the next availability step, adds a network round trip to every commit
and is only affordable with batching. Measuring first was the prerequisite for all of them.

A one-off probe before the milestone found about 300 orders/s on disk, with the disk sync taking
about 99% of each order, and per-order cost growing with book depth. The milestone was scoped to
exactly those findings, plus the memory leak found while reading the code.

## What was built

1. **`--bench`** (`src/exchange/bench.rs`): an in-process, deterministic, open-loop load generator
   that drives the production worker, journal, mmap stream and snapshot schedule, records latency
   from each order's intended send time in an HdrHistogram, and reports orders per sync, journal
   bytes per order and memory. It adds one dependency (`hdrhistogram`, default features off) and one
   counter (`JOURNAL_SYNCS` in `event_store.rs`).
2. **Group commit** (`runtime.rs`): the worker prepares and commits every queued command in memory,
   syncs the journal once for the whole group, and only then publishes, runs callbacks and answers,
   reads included.
3. **Planned matching** (`order_book.rs`, `matching_engine.rs`, `price_level.rs`, `core.rs`):
   `plan_order` works out fills read-only against the live book and `apply_plan` applies them at
   commit. No more copying the whole book on every order and cancel.
4. **Test-only history** (`runtime.rs`): the never-read `event_log` vector exists only in test
   builds.

Nothing else changed. The journal format, event schema, subscribers, snapshot format, warm replica
and HTTP API are the same, and an existing journal replays identically.

## Combined results

Docker Desktop on the development PC (6 vCPU, 8 GB WSL2 VM), release build, one run per cell,
snapshots off unless stated. "Disk" is the VM's virtual disk; "tmpfs" is RAM, where syncing is free.

| Measurement | Baseline | + group commit | + planned matching | + no history in RAM |
|---|---|---|---|---|
| Disk, max rate, 20k orders (orders/s) | 384 | 18,713 | 39,153 | 36,648 |
| Disk, orders per sync at max rate | 1.0 | 952 | 952 | 952 |
| Disk, fixed 1,000/s: achieved, p99 | 495/s, 5.1 s | 1,000/s, 22 ms | 999/s, 33 ms | — |
| Disk, fixed 10,000/s: achieved, p99 | 452/s, 40.6 s | 9,907/s, 168 ms | 9,979/s, 143 ms | — |
| Disk, fixed 43,000/s, 200k orders: achieved | not run¹ | 3,194/s | 25,663/s | 24,914/s |
| tmpfs, max rate, 200k orders (orders/s) | 2,796 | 3,309 | 43,438 | 42,667 |
| tmpfs, one symbol, 10,000 resting orders | 90 | 107 | 52,465 | 49,027 |
| tmpfs, 1M orders: orders/s, peak memory | — | — | 4,477, 1.92 GB | 30,865, 1.32 GB |
| Disk, 200k orders, **production snapshots** | 271² | 2,812 | 8,967 | 8,255 |

¹ At the baseline's ~400 orders/s this run would take over eight minutes and only repeat what the
10,000/s row shows: it falls hopelessly behind.
² Baseline with snapshots was measured over 20,000 orders; the later stages over 200,000.

Cells marked "—" were not part of that stage's run set. Treat differences under about 10% as noise
(for example 39,153 versus 36,648): each cell is a single run on a shared virtual machine.

**Headline:** on the same disk, maximum throughput went from **384 to about 37,000–39,000 orders/s
(about 100×)**. At 1,000 orders/s p99 went from **5 seconds to about 20–30 ms**. Cost per order no
longer depends on book depth, and the exchange no longer leaks 600–700 bytes of memory per order.
In memory (tmpfs) the exchange now meets the design's **average** target of 43,000 orders/s.

## Where the time goes now

A throwaway instrumented build (never committed) timed each step of the worker over 200,000 orders
on the final code. The figures are per order, averaged over each group, and include about 5% of
setup commands:

| Step | tmpfs | disk |
|---|---|---|
| prepare: risk, wallet, match plan, settlement validation | 9.0 µs | 8.8 µs |
| build the event envelopes and JSON-encode the record | 7.0 µs | 6.9 µs |
| commit the prepared change to the core | 5.8 µs | 5.7 µs |
| journal write + sync (amortized over the group) | 0.9 µs | **6.0 µs** |
| mmap publish + callbacks | 3.7 µs | 3.5 µs |
| release replies | 0.7 µs | 0.6 µs |

The instrumentation adds some overhead of its own (these builds ran at about 36,000/s instead of
about 43,000/s), so the proportions matter more than the sums. What they show:

- **No single step dominates any more.** The easy 100× is gone; further gains are several 1.5–2×
  steps.
- **JSON is about a quarter of the CPU time** (7 µs). A binary format would cut most of it. The
  research estimate was right that JSON only matters once the sync is amortized, and it now is.
- **mmap publication costs 3.7 µs** because every record takes and releases a file lock separately.
  Publishing a whole group under one lock is a natural follow-up to group commit.
- **On disk the worker is idle during each sync** (about 6 ms per group of about 1,000 orders,
  about 6 µs of a roughly 31 µs order in the disk profile). Syncing group N on a separate thread
  while preparing group N+1 would recover that share and lift disk max-rate throughput toward the
  in-memory figure. It does not explain the whole difference between the fixed-43,000/s disk run
  (about 25,000/s) and the in-memory max-rate run (43,000/s); part of that shortfall is still
  unexplained.

## The biggest remaining bottleneck: the core snapshot

The first baseline run used the production snapshot schedule and was unexpectedly slow in memory:
1,516 orders/s, against 2,796 with snapshots off. The cause is structural. Every 10,000 commands the
worker serializes the *entire* exchange state to JSON and fsyncs it, on the trading thread. That
state includes every order ever placed and every execution in the per-user index, so each snapshot
is bigger than the last and total snapshot work grows with the square of the history length.

After this milestone's fixes it is the dominant cost: **8,255 orders/s with production snapshots
versus about 25,000–43,000 without.** Fixing it properly needs two things: taking the snapshot off
the trading thread (or making it incremental), and bounding the state that grows forever. The second
involves product decisions (for how long `client_order_id` must stay unique, and how far back
`GET /exchange/executions` must reach), so it was recorded rather than folded in.

## Rules that changed

- **Group commit changes one failure rule.** Before: "a failed append leaves the live core
  unchanged". Now: "after a failed sync the live core is never used again; the worker halts and
  recovery from the journal is the only way back." Nothing in production ever relied on the old
  rule, and the durable-before-visible rule is unchanged: no reply, read, stream batch, callback or
  snapshot can observe a command before its group is synced. Details are in `01`.
- **Reads are answered after their whole group has been staged, synced and published**, instead of
  as soon as the worker reaches them. Under load that can add the rest of the group's preparation
  (up to about 1,000 commands) plus one sync. It is what stops a read from showing unsynced state.
- **A fault in the middle of a group** still syncs and answers the valid commands staged before it.
  Commands queued behind it are dropped, which the gateway reports as `503`.

## Verification

- `cargo fmt -- --check` is clean. `cargo test --locked` passes **136 unit tests** (131 before, plus
  3 for group commit and 2 for matching) and all executable integration tests. The opt-in Reporter
  acceptance test is ignored as before (it needs `REPORTER_TEST_DATABASE_URL`).
- `cargo build --release` reports 13 warnings, the same count as before this milestone. Every one
  predates it.
- The differential test replays 20,000 random order and cancel steps through the new and old matchers
  and requires identical executions, books and order indexes after every step.
- Four failure tests were rewritten for the new failure rule. Three check the outcome by recovering
  from the journal; the fourth checks that no callback or history entry appears.
- A review workflow (four reviewers covering durability, matching, benchmark method, and tests,
  each finding checked by three skeptics) raised 12 candidate issues. Two were upheld, both minor:
  a group could write the same snapshot several times when the snapshot interval is below the group
  size (fixed: one snapshot per group at most), and fixed-rate latencies include up to about 1 ms of
  the generator's own timer (documented in `00`). The rejected ones clarified documentation: failed
  syncs mean "outcome unknown", and power loss inside a group widens an existing recovery case
  (described in `01`). The benchmark now also refuses any non-empty directory. None of these
  changes affect the measured numbers: the production interval of 10,000 already exceeds the
  maximum group of 1,024.
- Every number above comes from `--bench`. The per-stage source snapshots were built as separate
  binaries and measured with the same fixed-seed workload.

## How to reproduce

On Linux, or in the `rust` container from Git Bash with `MSYS_NO_PATHCONV=1`. The in-memory runs need
a RAM filesystem big enough for the journal (about 750 bytes per order): Docker's default `/dev/shm`
is only 64 MB, so start the container with `--tmpfs /bench-mem:size=4g` (as the recorded runs did)
and use `/bench-mem/...` instead of `/dev/shm/...`:

```sh
cargo build --release
./target/release/stock --bench /tmp/b1 --orders 20000 --snapshot-every 0          # disk throughput
./target/release/stock --bench /tmp/b2 --orders 5000 --rate 1000 --snapshot-every 0 # latency at a fixed load
./target/release/stock --bench /dev/shm/b3 --orders 200000 --snapshot-every 0      # CPU only (RAM filesystem)
./target/release/stock --bench /dev/shm/b4 --symbols 1 --depth 10000 --orders 5000 --snapshot-every 0
./target/release/stock --bench /tmp/b5 --orders 200000                              # production snapshots
```

Each run needs its own empty directory. `--snapshot-every` belongs to this milestone's binary; milestone 20
removed it (the primary no longer snapshots while trading), so drop it when running newer code.

## Deliberately not done

- Pipelined sync (journaler thread), per-group mmap publication, and a binary journal format: all
  measured above as the next CPU and disk costs, all separate changes.
- Snapshot redesign and bounding ever-growing state: needs product decisions; see above.
- Subscriber throughput. MDP rewrites and syncs its whole state file after every command (118
  commands/s on disk in the pre-milestone probe). It is the first measured candidate for the next milestone.
- HTTP gateway measurement, CPU pinning, lock-free queues, per-symbol workers, and cross-machine
  replication.
- `PriceLevel` slot reuse and `total_quantity` walking a whole level: memory and L2-view costs, not
  on the order path.

## Gap to the design's targets

| Target | Now | Gap |
|---|---|---|
| 43,000 orders/s average | ~43,000/s in memory, ~25,000/s on this VM's disk, ~8,300/s with production snapshots | snapshots first, then pipelined sync |
| 215,000 orders/s peak | about 5× short even in memory | binary encoding, allocation reduction; maybe per-symbol parallelism |
| millisecond p99 | about 20–30 ms at low load, bounded by this disk's sync p99 | a power-loss-protected drive, or durability through a second machine |
