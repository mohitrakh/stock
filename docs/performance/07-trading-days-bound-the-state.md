# 07 - Trading Days Bound the State

Milestone 22. The behaviour, and the reasons for it, are in `docs/tasks/17-trading-day.md`; this
file is the measurement. The main code:
- the close: `ExchangeCore::prepare_expiry` / `commit_close_market`;
- the next open: `OrderManager::start_trading_day`, called from `commit_open_market`;
- the warm replica's snapshot at each open: `WarmReplica::maybe_write_snapshot`;
- the benchmark: `--days` in `src/exchange/bench.rs`.

Raw output: `results/results-m22.txt`.

## The problem

Until this milestone the exchange kept every order and every fill forever. Its state only grew, and
everything that copies the state grew with it:

- **Snapshots.** A snapshot serializes the whole state, so each was bigger than the last: 82 MB at
  200,000 orders and still growing. Milestone 20 moved snapshots off the trading thread onto the
  warm replica, but it noted: "The snapshot is still O(history)."
- **The warm replica.** It writes those snapshots, so its work grew with history.
- **Restarts.** A restart loads the latest snapshot, so it slowed down as history grew.
- **Memory.** It grew without limit.

## The idea

Give the exchange a trading day, and keep only that day in memory.
- **The close** expires every order still resting.
- **The next open** drops the previous day's orders and fills. Balances, positions, risk limits
  and each book's execution counter carry over.
- **The warm replica** also writes a snapshot right after each open, when the state is smallest.

The state then holds at most one day, whatever the number of days.

```text
before:  state  ████████████████████████████████████████████▶ grows for ever
after:   state  ███████▆ ▁███████▆ ▁███████▆ ▁███████▆ ▁███████▆   one day at a time
                 day 1  ^  day 2  ^  day 3  ^ ...
                       open: the previous day leaves memory
```

## How it was measured

- **Machine:** the office Ubuntu machine: an i3-7100 with 2 cores and 4 threads, a SATA SSD, and
  release builds in the `stock-rust` container.
- **Binaries:** both were built from source. The milestone 21 binary is commit `a4860c7` and has no
  trading days.
- **Order volume:** the same for both, 1,000,000 orders. The new binary ran it as 5 days of 200,000
  orders (`--days 5 --orders 200000`); the old one as a single run (`--orders 1000000`).
- **Reference runs:** one day of 200,000 orders on each binary.
- **Workload:** the benchmark's usual deterministic orders at maximum rate, across 100 symbols and
  100 users.
- **Snapshots:** every run had a warm replica beside it, writing snapshots every 10,000 commands,
  and the new binary's also right after each open.
  - A poller recorded the snapshot file's size each time it changed.
  - The warm replica's log gave the write times.
  - "Caught up" means its `/status` had reached the last sequence the stream published.
- **Restarts:** the primary was restarted on each run's files and timed until it printed
  "recovered through". That includes loading the snapshot, replaying the rest, and the one snapshot
  a primary writes at startup.
- **Scripts:** `m22-measure.sh`, then `m22-followup.sh` on the same journals. Each configuration
  ran once; the throughput comparison ran three times.

## Results

**The same 1,000,000 orders:**

| | Milestone 21, one run | Milestone 22, 5 days |
|---|---|---|
| Exchange memory at the end (RSS / peak) | 1,234 / 1,318 MB | **263 / 297 MB** |
| Warm replica peak memory | 2,121 MB | **506 MB** |
| Largest snapshot | 421.6 MB | **85.4 MB** |
| Snapshot write, median / slowest | 3.4 s / 10.5 s | **0.73 s / 1.9 s** |
| Snapshot writing in total | 375 s (101 writes) | **81 s** (106 writes) |
| Warm replica caught up after the benchmark | 365 s | **78 s** |
| Restart at the end of the run | 11.7 s | **2.4 s** |
| Journal | 765 MB | 788 MB |

**Flat across days.** The new binary, day by day:

| Day | Orders/s | Close | Close record | Memory after the close (RSS) |
|---|---|---|---|---|
| 1 | 35,746 | 302 ms | 5,277 KB | 256 MB |
| 2 | 28,973 | 376 ms | 5,245 KB | 263 MB |
| 3 | 24,880 | 250 ms | 5,282 KB | 263 MB |
| 4 | 40,220 | 263 ms | 5,307 KB | 263 MB |
| 5 | 35,915 | 276 ms | 5,341 KB | 263 MB |

**Snapshot sizes:**
- **Milestone 21:** they grow steadily, 4.2 MB more every 10,000 commands, up to 421.6 MB.
- **Milestone 22:** they rise the same way during each day, to about 85 MB. Then they fall back to
  0.7 MB with the snapshot written right after the next open. They go through five identical
  teeth.

**One day against five:**

| | New, 1 day | New, 5 days | Milestone 21, 200,000 | Milestone 21, 1,000,000 |
|---|---|---|---|---|
| Restart | 2.45 s | 2.39 s | 2.37 s | 11.7 s |
| Largest snapshot | 84.6 MB | 85.4 MB | 84.8 MB | 421.6 MB |
| Warm replica peak memory | 468 MB | 506 MB | 469 MB | 2,121 MB |
| Exchange memory at the end (RSS) | 254 MB | 263 MB | 251 MB | 1,234 MB |

On one day the two binaries are the same, as they should be. Over five days the old binary grows
about fivefold and the new one stays where it was.

**A restart early in a day.** On the five-day journal:
1. The primary opened a sixth day.
2. A warm replica, started from the closed day's snapshot, replayed the open and wrote its snapshot
   right after it: 667 KB, in 85 ms.
3. The primary then restarted in **49 ms** (62 ms on a second run).

The ~2.4 s above is the worst case within a day: a restart just after the close, when the day is at
its biggest.

## What it costs, and what did not change

- **The close takes about a quarter of a second, once a day.** It expired 41,720 orders, 21% of the
  day's orders, and wrote one journal record of about 5.3 MB. The market is closed at that moment,
  so no order waits for it.
- **Throughput is unchanged.** Without a warm replica, three runs of 200,000 orders each:
  - new: 45,371, 41,449 and 40,043 orders/s;
  - milestone 21: 44,425, 36,979 and 38,612 orders/s.

  The difference is within this machine's run-to-run noise, so the resting-order cap and the other
  checks cost nothing measurable. With the warm replica beside the exchange, throughput varies more
  (24,880 to 40,220 orders/s from one day to the next), because both processes share 2 cores and
  one disk. But it does not fall as days go by.
- **Journal size per order is unchanged** at 753 to 757 bytes. Each close adds its expiries.
- **The warm replica still falls behind at maximum rate.** It wrote 81 s of snapshots during 31 s of
  trading, and needed 78 s after the benchmark to catch up. What changed is how its work grows:
  - before, with history: each snapshot was bigger than the last, so a run twice as long cost four
    times as much snapshot work;
  - now, with the length of the run only.

  At the old binary's 1,000,000 orders it needed 6 minutes. Snapshotting less often within a day
  would let it keep up at full speed. That was not done here; the interval stays at 10,000
  commands.
- **A restart at the end of a day still takes about 2.4 s.** It loads the day's snapshot (about
  85 MB), then writes a fresh one before taking orders. Most of the time is that snapshot work, not
  replay.
- **The journal is still one file.** It grew to 788 MB over five days. A promotion still replays
  all of it. Journal files per day, archiving, and promoting from the warm replica's own state are
  separate work.

## Options considered and rejected

- **Snapshot only at the open.** Every snapshot would be tiny. But a restart late in the day would
  replay the whole day, up to hundreds of thousands of commands. The periodic interval bounds that,
  and each periodic snapshot now costs at most one day.
- **Clear at the close instead of the next open.** The snapshot after a close would be tiny too.
  But the day's orders and fills would leave the API at the very moment traders look for them. The
  task write-up gives the reasoning.
- **Compare at a fixed rate instead of maximum rate.** At a moderate rate both warm replicas keep
  up longer, which hides how the work grows. Maximum rate makes the growth visible in a run of a
  few minutes.
