# 00 - The Benchmark Harness

Part of milestone 19, critical-path performance v1. Read this first: every number in the other
files in this folder came from this tool, and a number is only as good as the way it was measured.

## Why it had to exist

Eighteen milestones in, nothing in this exchange had ever been timed. The design document is
specific about what "good" means:

- 43,000 orders per second on average and 215,000 at peak (1 billion orders in a 6.5-hour day);
- round-trip latency "at the millisecond level, with a particular focus on the 99th percentile";
- measure latency with HdrHistogram.

Without a measurement, every performance idea is a guess. With one, each change gets a before and
an after, and the next thing to fix is whatever the numbers say is slowest.

## What it drives

```sh
cargo run --release -- --bench EMPTY_DIR [--orders N] [--rate ORDERS_PER_SEC|0]
                                         [--symbols N] [--users N]
                                         [--depth RESTING_ORDERS_PER_SYMBOL]
```

(`--snapshot-every` existed during milestone 19 and was removed in milestone 20, when the primary
stopped writing periodic snapshots. See "The snapshot switch" below.)

`src/exchange/bench.rs` builds the real production pipeline in one process:

```text
load generator (Tokio task)
  -> the same bounded command queue main uses (10,000 slots)
  -> the same dedicated exchange worker thread
       -> ExchangeRuntime: per command, prepare + encode + commit in memory;
                           per group, one journal write + sync, then mmap publish,
                           callbacks, replies
  -> oneshot reply per order back to the generator
```

It deliberately skips HTTP, JSON request parsing and JWT login. Those run on Tokio's thread pool in
parallel and are not on the single worker thread, so they do not limit throughput. Leaving them out
means the numbers describe the exchange, not Axum. The harness refuses to start if the directory
already holds a journal, and it never deletes anything.

## The workload

- **Setup (not timed).** Every user gets cash, and shares in every symbol, so no order is rejected
  for lack of collateral. The defaults are 100 users and 100 symbols, the design's "at least 100
  symbols".
- **Measured orders.** For each order, a random user, a random symbol, a random side, a price in a
  20-tick band (1000 to 1019), and a quantity of 1 to 10. Buys and sells land in the same band, so
  roughly half of the orders cross and trade, and the rest rest in the book.
- **Deterministic.** The randomness is a fixed-seed xorshift generator, so every run of every
  version of the code sends exactly the same orders. That is what makes before/after comparisons
  fair.
- **Optional depth.** `--depth D` first places `D` resting sells per symbol at a price far above the
  band (1,000,000 and up). They never trade; they only make the book deep, which is how the book-copy
  problem in `02` was isolated.

## Two modes, because throughput and latency are different questions

**Max mode (`--rate 0`)** sends orders as fast as the queue accepts them. It answers "how many
orders per second can the exchange process?" Its latency numbers are meaningless: the generator
keeps a 10,000-deep queue full, so every order's latency is mostly time spent waiting in that queue.
The tool labels them `(queue-bound at max rate)`.

**Fixed-rate mode (`--rate R`)** schedules order `i` to be sent at `start + i / R` seconds, and
answers "at this load, how long does each order take?".

### Coordinated omission, and why the clock starts at the *intended* send time

A naive load tester sends a request, waits for the reply, then sends the next one. Suppose the
exchange stalls for 2 seconds. The naive tester sends nothing during the stall, records exactly one
slow request, and reports that 99.9% of requests were fast. But at 1,000 orders/second, 2,000 real
clients would have been stuck in that stall. The tester "coordinated" with the system it was
measuring and omitted the bad period. Gil Tene named this *coordinated omission*.

This harness measures each order from the moment it *should* have been sent according to the
schedule, not the moment the generator got around to sending it. If the exchange stalls, the orders
scheduled during the stall pile up, and each of them is charged the full time it waited. That is why
the baseline at 1,000 orders/s reports a p99 of 5 seconds rather than a few milliseconds: that is
what 1,000 real clients a second would have experienced.

#### The generator's own error

The generator waits for each send time with Tokio's timer, which ticks once per millisecond. An
order can therefore leave up to about 1 ms after its scheduled time (about 0.5 ms on average), and
because the clock starts at the scheduled time, that delay is counted as latency. Fixed-rate
latencies are therefore accurate to about ±1 ms, and sub-millisecond differences between two
fixed-rate runs mean nothing. At high rates the same timer releases orders in 1 ms bursts rather
than evenly, which makes groups at fixed rates slightly bigger than a perfectly smooth stream would.
Neither effect matters for the results reported here: the effects measured are tens of milliseconds
to seconds.

## HdrHistogram

Latencies are recorded in an HdrHistogram (the `hdrhistogram` crate, default features off). It keeps
every value to 3 significant digits in memory that depends on the *range* of latencies (growing
logarithmically with the largest one), not on how many are recorded. So a run of a million orders
costs no more histogram memory than a run of ten with the same range, and p99.9 is exact to that
precision instead of estimated. It is
the tool the design document names.

## What the output means

```text
bench: 200000 orders, rate max, 100 symbols, 100 users, depth 0 per symbol, snapshots off
  throughput : 42667 orders/s over 4.688 s
  latency us : p50 248319  p90 306431  p99 346623  p99.9 351487  max 360959  (queue-bound at max rate)
  syncs      : 197 (1015.2 orders per sync)
  journal    : 753 bytes per order
  rejected   : 0
  memory     : VmHWM: 251672 kB, VmRSS: 250900 kB
```

- **throughput**: measured orders divided by the time from the first send to the last reply.
- **latency us**: percentiles in microseconds, from the intended send time to the reply.
- **syncs**: journal `sync_all` calls during the measured orders, and how many orders shared each
  one. Before group commit this is always 1.0.
- **journal**: journal bytes written per measured order (one record holds the order and all its
  outputs, as JSON).
- **rejected**: orders the exchange refused. It is always 0 here; a nonzero value means the workload
  is not measuring what it claims to.
- **memory**: `VmHWM` is the peak resident memory of the whole process, `VmRSS` is the current
  value. Both include the harness itself: it keeps one finished reply task per measured order until
  the end, plus the histogram and the generator. Compare memory only between runs with the same
  `--orders`.

## The snapshot switch

Production writes a snapshot of the whole exchange state every 10,000 commands, and
`--snapshot-every` defaults to that. The first baseline run showed the snapshot cost growing with
history and taking a large share of the time: throughput fell from 384 to 271 orders/s on disk and
from 2,796 to 1,516 in memory. After the optimizations it became the dominant cost (see the milestone
write-up). `--snapshot-every 0` turns
snapshots off, so each optimization can be measured on its own. With snapshots on, the snapshot due
at the end of setup can run just after the first measured orders are sent, which slightly lowers
the first measured seconds. Every comparison in `01` to `03`
uses snapshots off. The production schedule is measured separately and reported as its own row.

**Since milestone 20** the primary writes no periodic snapshots (only one at startup), so the switch
is gone. Snapshots are written by the warm replica, a separate process. To measure the cost of
snapshotting now, start `--warm-replica` on the benchmark's directory once its `.mmap` file appears,
as `04-snapshots-off-the-trading-thread.md` describes.

## Where it ran, and how far to trust it

All numbers come from Docker Desktop on the Windows development PC: a WSL2 virtual machine with 6
vCPUs and about 8 GB of RAM, the `rust:latest` image, and a release build. Two storage settings:

- **disk**: the container's own filesystem on the VM's virtual disk. Its sync is slow and noisy. A
  raw 600-byte write plus `sync_all` measured about 1.8 ms at p50 and 22 ms at p99.
- **tmpfs**: a RAM filesystem where `sync_all` costs about a microsecond. Running there removes the
  disk and shows pure CPU cost.

Each configuration ran once. Treat differences under about 10% as noise; the throughput effects
reported in these documents are between 2× and 500×, and the memory effect in `03` is about 1.5×. A real Linux machine with a server SSD will give
different absolute numbers, and the office Ubuntu box is the place to confirm them. The shape of the
results (which step dominates, and what changes when it is removed) is what these files rely on.

## What it does not measure

HTTP and authentication, network round trips, the market-data and reporter subscribers, recovery
time, and power-loss durability. Subscriber throughput is the first measured candidate for the next milestone; none is
selected yet.

## Baseline: the exchange before any optimization

Snapshots off, from `results/results-baseline.txt`:

| Run | Result |
|---|---|
| Disk, max rate, 20,000 orders | **384 orders/s**, 1.0 orders per sync |
| Disk, fixed 200/s | keeps up; p50 3.2 ms, p99 30.3 ms |
| Disk, fixed 1,000/s | falls behind: achieves 495/s; p50 2.1 s, p99 5.1 s |
| Disk, fixed 10,000/s | falls behind: achieves 452/s; p50 20.7 s, p99 40.6 s |
| tmpfs, max rate, 200,000 orders | **2,796 orders/s**; peak memory 378 MB |
| tmpfs, one symbol, 0 resting orders | 3,510 orders/s |
| tmpfs, one symbol, 1,000 resting | 869 orders/s |
| tmpfs, one symbol, 10,000 resting | **90 orders/s** |

With the production snapshot schedule the baseline was lower again: 271 orders/s on disk and 1,516
on tmpfs.

Two costs stand out. On disk, every order waits for its own sync, so throughput is about one
divided by the sync time. In memory, where syncing is free, throughput still collapses as the book
gets deeper. Those are optimizations `01` and `02`.
