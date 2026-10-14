# 02 — Where an order's time goes

**Part 1. Measured 2026-10-14, 09:11 to 09:23 local time.** Raw output: `results/m25p1-stages.txt`
(the definitive run) and `results/m25p1-stages-first.txt` (a complete first run, 08:59–09:11, from
before the end-to-end mean was printed). Derived files:
- `results/m25p1-stages-summary.txt`: per configuration, every stage averaged over its three runs,
  by `scripts/m25p1-summarize.awk`;
- `results/m25p1-stages-decomposition.txt`: the checks below, by `scripts/m25p1-decompose.awk`.

Every derived number below can be recomputed with those two scripts. Script: `scripts/m25p1-stages.sh`.

## The question

Where does an order's time go, stage by stage? And so, what is worth optimizing?

## Method

- **Exchange:** the stage-timing build, `stock-m25p1-timed`. `01-instrumentation-overhead.md` shows
  its timing costs nothing measurable at 5,000 orders/s, and about 3% of throughput at the maximum
  rate. 1,000/s was not measured. The timing's cost per order is the same at any rate, so its share
  at 1,000/s should be smaller still; that is an inference.
- **Configurations,** three runs each:

  | Path | Replica | Rates |
  |---|---|---|
  | in-process (`--bench`) | alone, replicated | 1,000/s, 5,000/s, maximum (200,000 orders) |
  | HTTP (`--http-bench`, generator from the normal build) | alone, replicated | 1,000/s, 5,000/s |

  The 1,000/s and 5,000/s runs each last 20 s: 20,000 and 100,000 orders.
- **Every stage**, per run, as a histogram (`part-1-stage-timing.md`). The averages below are means
  over the three runs of each statistic. They are not percentiles of the pooled samples.
- **Machine:** the office machine. Load average 1.5 to 5.2, 30 to 32 containers running (the
  measured containers included).
- **One disturbed run, kept in the averages and flagged wherever it matters.** Run 1 of "HTTP
  replicated, 5,000/s" did not complete normally:
  - 1,084 orders could not connect, and 11 were unanswered at the 120 s deadline;
  - its end-to-end p99 was 274,431 µs, against 56,479 and 58,271 in runs 2 and 3;
  - its own `Sync` p99.9 was 59,007 µs and its maximum 73,535, against 9,983 and 11,295 in the other
    two runs.

  So, during run 1, single syncs took up to 73 ms: a disk stall. Behind it, orders backed up
  (`Queue` p99 105 ms), and new connections were refused. The cause of the stall is not known. That
  configuration's averages below include run 1, as every configuration's do. Where it changes a
  conclusion, the figure without it is given too.
- **Counts.** The in-process `WaitInGroup` and staging stages count one more command than there were
  orders, for example 20,001 against 20,000. That extra command is the market close the benchmark
  sends after its measured orders, which happens after the report's reset.

## The check: do the stage means add up?

Means add along a path, medians do not (`01-theory.md` §2). For each configuration:
- **in-process:** end-to-end mean = `Queue` + `Worker` + what no stage covers: the generator's own
  lateness and the reply's wake-up;
- **HTTP:** end-to-end mean = `HttpTotal` + what happens outside the server. `HttpAsk` = `Queue` +
  `Worker` + the reply's wake-up.

All values are means in µs, averaged over three runs:

| Configuration | End-to-end | Queue | Worker | Queue + Worker | Not covered |
|---|---|---|---|---|---|
| in-process alone 1,000/s | 5,621 | 1,500 | 2,963 | 4,462 | 1,159 (20.6%) |
| in-process alone 5,000/s | 6,989 | 1,942 | 3,886 | 5,828 | 1,161 (16.6%) |
| in-process alone max | 238,569 | 216,145 | 22,340 | 238,485 | 84 (0.0%) |
| in-process replicated 1,000/s | 8,322 | 2,506 | 4,959 | 7,466 | 856 (10.3%) |
| in-process replicated 5,000/s | 11,183 | 3,307 | 6,736 | 10,042 | 1,140 (10.2%) |
| in-process replicated max | 303,899 | 275,154 | 28,638 | 303,791 | 108 (0.0%) |

| HTTP | End-to-end | HttpTotal | Outside the server | HttpBeforeQueue | HttpAsk | Queue + Worker | Reply wake-up |
|---|---|---|---|---|---|---|---|
| alone 1,000/s | 7,194 | 5,654 | 1,540 (21.4%) | 88 | 5,530 | 5,186 | 344 |
| alone 5,000/s | 9,799 | 7,425 | 2,374 (24.2%) | 48 | 7,360 | 6,752 | 609 |
| replicated 1,000/s | 11,038 | 9,344 | 1,694 (15.3%) | 77 | 9,239 | 8,799 | 440 |
| replicated 5,000/s | 20,733 | 17,884 | 2,849 (13.7%) | 43 | 17,824 | 16,735 | 1,088 |

The stages account for the end-to-end mean. What is left is identified:
- **About 0.9–1.2 ms in-process, not covered by any stage.** This is the generator's own lateness:
  its 1 ms timer and the CPU it shares, about 0.6–1.3 ms by milestone 24's `send lag`. Added to it
  is the reply's wake-up. At the maximum rate this gap all but vanishes (84 and 108 µs), because
  there the generator sends as fast as it can and is never late against a schedule.
- **1.5–2.8 ms outside the HTTP server:** the generator's lateness, the loopback round trip, and the
  client reading the response. The server itself spends only 17–36 µs after the reply
  (serializing the response) and 43–88 µs before the queue (parsing, JWT, validation).
- **0.34–1.09 ms of reply wake-up on the HTTP path:** from the worker sending the reply until the
  handler runs again. That needs a free Tokio worker, so it grows with load.

**So on this machine, 10–24% of the latency a benchmark reports is the measuring apparatus and the
client side, not the exchange**: from 10.2% (in-process, replicated) to 24.2% (HTTP alone at
5,000/s).

### Inside the worker

`Worker` = the waits in its group + the order's own staging + the group's shared stages. Counting
the group stages whole:

| Configuration | Worker | Its stages | Not covered |
|---|---|---|---|
| in-process alone 1,000/s | 2,963 | 2,668 | 295 (10.0%) |
| in-process alone 5,000/s | 3,886 | 3,452 | 434 (11.2%) |
| in-process alone max | 22,340 | 22,171 | 169 (0.8%) |
| in-process replicated 1,000/s | 4,959 | 3,459 | 1,500 (30.3%) |
| in-process replicated 5,000/s | 6,736 | 4,982 | 1,754 (26.0%) |
| in-process replicated max | 28,638 | 28,974 | −337 (−1.2%) |
| HTTP alone 1,000/s | 3,177 | 2,802 | 375 (11.8%) |
| HTTP alone 5,000/s | 4,421 | 3,474 | 947 (21.4%) |
| HTTP replicated 1,000/s | 5,455 | 4,042 | 1,414 (25.9%) |
| HTTP replicated 5,000/s | 10,924 | 8,211 | 2,713 (24.8%) |

The gap is 10–11% in-process alone, 12–21% on HTTP alone, and 25–30% replicated, at the rates below
the maximum. **A likely explanation, not yet
proven, is that the group stages are averaged per group, while `Worker` is averaged per order.** An
order belongs to a group of a certain size, and a group's sync (and the replica's confirmation) is
probably longer when the group is larger. Larger groups contain more orders, so the average order
sees a longer sync than the average group has. That is size-biased sampling, a relative of the
inspection paradox (theory §3). Replication adds a second, variable wait (`Confirm`), which widens
the bias. At the maximum rate every group is full (1,024 commands), sizes do not vary, and the gap
disappears (0.8% and −1.2%).

Part 2 can test this directly by recording, per order, "staging ended → its group durable".

## Where the time goes at 1,000 and 5,000 orders/s

Shares of the end-to-end mean (from the decomposition file):

| Configuration | Queue | Sync | Confirm | Waits in group | Order's own CPU |
|---|---|---|---|---|---|
| in-process alone 1,000/s | 26.7% | 42.1% | — | 3.0% | 1.1% (61.2 µs) |
| in-process alone 5,000/s | 27.8% | 33.3% | — | 13.1% | 0.5% (37.4 µs) |
| in-process replicated 1,000/s | 30.1% | 20.2% | 15.9% | 3.7% | 0.7% (61.9 µs) |
| in-process replicated 5,000/s | 29.6% | 17.1% | 12.8% | 12.5% | 0.3% (33.4 µs) |
| HTTP alone 1,000/s | 27.9% | 32.5% | — | 3.8% | 1.3% (90.6 µs) |
| HTTP alone 5,000/s | 23.8% | 18.7% | — | 14.8% | 0.5% (47.7 µs) |
| HTTP replicated 1,000/s | 30.3% | 18.3% | 12.7% | 3.9% | 0.7% (74.6 µs) |
| HTTP replicated 5,000/s | 28.0% | 14.0% | 9.2% | 14.8% | 0.2% (42.3 µs) |

"Order's own CPU" is `Prepare` + `Encode` + `Commit`. "Waits in group" is `WaitInGroup` +
`WaitForGroup`.

What this shows:

1. **The disk dominates.** `Sync`, plus the replica's `Confirm` when replicated, is the largest
   single stage at these rates in-process, and on HTTP at 1,000/s. On HTTP at 5,000/s, `Queue` is
   larger: 23.8% against 18.7% alone, and 28.0% against 23.2% replicated. But most of `Queue` is
   waiting for the group in progress,
   whose cycle is mostly its sync (theory §3), so it is disk too. `Queue` + `Sync` + `Confirm` are
   42.5% (HTTP alone at 5,000/s) to 68.8% (in-process alone at 1,000/s) of the latency.
2. **The order's own CPU is irrelevant to latency here.** Prepare, encode and commit together are
   33–91 µs: 0.2–1.3% of the latency. Making encoding free would save at most its mean, 8.1–23.7 µs
   per order at these rates. Against end-to-end means of 5,621–20,733 µs, that is 0.05% (10.6 of
   20,733, HTTP replicated at 5,000/s) to 0.33% (23.7 of 7,194, HTTP alone at 1,000/s).
3. **The own CPU per order is higher at low load than at the maximum rate.** `Prepare` averages
   28.8 µs at 1,000/s against 5.3 µs at the maximum. `Encode` averages 15.2 against 2.8. The same
   code doing the same work is 5 times slower when the worker is mostly idle. A likely cause, *an
   inference*, is that a worker idling between groups loses its CPU caches and its core to other
   processes. A busy worker keeps both warm. It does not change the conclusion: even cold, the
   CPU is about 1%.
4. **The waits in a group grow with load.** At 5,000/s, `WaitInGroup` + `WaitForGroup` are 12.5–14.8%
   of the latency, against 3–4% at 1,000/s. Groups are larger (14–34 commands on average, against
   about 3), so each order waits for more of its neighbours to be staged. That is cheap CPU, but it
   is serial.

### The tail

| Configuration | End-to-end p99 | Sync p99 | Confirm p99 | Queue p99 | Worker p99 |
|---|---|---|---|---|---|
| in-process alone 5,000/s | 17,180 | 5,800 | — | 7,158 | 9,854 |
| in-process replicated 5,000/s | 31,114 | 6,642 | 5,350 | 13,663 | 19,079 |
| HTTP alone 5,000/s | 31,940 | 5,899 | — | 9,615 | 14,679 |
| HTTP replicated 5,000/s, all 3 runs | 129,727 | 8,494 | 6,898 | 47,364 | 74,244 |
| HTTP replicated 5,000/s, runs 2–3 only | 57,375 | 7,453 | 5,851 | 18,439 | 28,679 |

How the runs 2–3 row is computed, from `results/m25p1-stages.txt`:
- end-to-end p99: (56,479 + 58,271) / 2;
- `Sync` p99: (6,955 + 7,951) / 2;
- `Confirm` p99: (5,799 + 5,903) / 2;
- `Queue` p99: (18,783 + 18,095) / 2;
- `Worker` p99: (29,135 + 28,223) / 2.

The end-to-end p99 is three to eight times the disk's own p99, and the disturbed run makes it fifteen:
- **In-process alone,** the p99 (17.2 ms) is about three slow syncs' worth. That is queue build-up
  after a slow sync (theory §4).
- **HTTP replicated at 5,000/s, in the two normal runs,** the p99 is 57.4 ms. That is 7.7 times
  the sync p99 of 7.5 ms (57,375 / 7,453). Both `Queue` (18.4 ms) and `Worker` (28.7 ms) have p99s
  far beyond any one sync. This many busy processes share 4 hardware threads: the exchange, the
  generator, the replica, and the 30 other containers. So the worker and the Tokio threads likely
  wait for CPUs, as milestone 24 also found. That is *an inference*: CPU per container was not
  sampled.
- **With the disturbed run 1,** the averaged p99 is 129.7 ms, fifteen times the sync p99. That run had
  a disk stall: single syncs of up to 73.5 ms (see Method).

## Where the time goes at the maximum rate

At the maximum rate the generator keeps the queue full. Latency is then queueing by construction
(`Queue` is 90.5–90.6% of it), and what matters is how long one group's cycle takes, because that
sets the throughput.

A group of about 1,015 commands (`GroupSize` mean: 1,015.2 alone, 1,011.8 replicated):

| Stage of one group's cycle | Alone (ms) | Replicated (ms) | Source |
|---|---|---|---|
| staging all its commands | 13.02 | 12.94 | `WaitInGroup` + own staging + `WaitForGroup`, below |
| `Write` | 0.35 | 0.38 | per group |
| `Sync` | 5.98 | 8.35 | per group |
| `Confirm` | — | 4.44 | per group |
| `Publish` | 2.37 | 2.42 | per group |
| `Release` | 0.45 | 0.46 | per group |
| **cycle** | **22.17** | **28.97** | sum |
| `Worker` mean (measured) | 22.34 | 28.64 | per order |

**Staging.** For an order at position k in a group of n, `WaitInGroup` is the staging of the k
orders before it, and `WaitForGroup` the staging of the n − k − 1 after it. Their sum, plus its own
staging, is therefore the staging of the whole group. Alone:

```text
7,169.9 + (5.3 + 2.8 + 2.9) + 5,835.6 = 13,016.5 µs ≈ 13.02 ms
```

That is 13,016.5 / 1,015.2 = 12.8 µs per command. Of that, 11.0 µs is `Prepare` + `Encode` +
`Commit`, and about 1.8 µs is the rest of `stage_command`: dispatch, building the reply, the
bookkeeping.

**The cycle checks against the throughput.**
- Alone: 1,015.2 commands / 22.17 ms = **45,800 orders/s**. That figure is derived from the cycle.
  Measured with the same timed build: 44,494, 42,458 and 47,420 in these stage runs, and 42,377 to
  48,140 in the overhead runs.
- Replicated: 12.94 + 0.38 + 8.35 + 4.44 + 2.42 + 0.46 = 28.97 ms, and 1,011.8 / 28.97 ms =
  **34,900 orders/s**. Measured in these stage runs: 38,119, 32,242 and 35,194. Milestone 23
  measured 34,000 to 36,000.

**The cycle agrees with `Worker`**, the per-order mean, within 0.8% alone (22.17 against 22.34) and
1.2% replicated (28.97 against 28.64).

## What this means for Parts 2 and 3

**Part 2, a pipelined sync, is worth doing for throughput, and will barely change latency at
moderate load.** All the figures below are **projections** from the cycle above, not measurements.

At the maximum rate, alone, with depth 2:
- If the sync moves to a second thread and the worker keeps writing, publishing and releasing, the
  worker's cycle becomes 13.02 + 0.35 + 2.37 + 0.45 = 16.19 ms. That is longer than the sync
  (5.98 ms), so the worker sets the pace: 1,015.2 / 16.19 ms ≈ 62,700 orders/s, **+37%**.
- If publishing and releasing also move to the second thread, the worker's cycle is 13.37 ms against
  the other thread's 8.80 ms: 1,015.2 / 13.37 ms ≈ 75,900 orders/s, **+66%**.

Replicated, the gain is larger, because two waits are hidden (sync and confirm, 12.79 ms):
- the worker's cycle becomes 12.94 + 0.38 + 2.42 + 0.46 = 16.20 ms, against 12.79 ms on the other
  thread: 1,011.8 / 16.20 ≈ 62,500 orders/s, **+79%**.

At 5,000 orders/s, alone, the non-sync work in a cycle is about a third of it:
- **Staging.** By the same reasoning as at the maximum rate, `WaitInGroup` + own staging +
  `WaitForGroup` = 486.2 + 37.4 + 432.2 = 955.8 µs ≈ 0.96 ms per group of 16.6 commands, about
  58 µs per command. That is much more per command than the 12.8 µs at the maximum rate, because
  the CPU is colder at this load (point 3 above).
- **The rest of the non-sync work:** `Write` 0.045 + `Publish` 0.106 + `Release` 0.020 ≈ 0.17 ms.
  In total, about 1.13 ms.
- **The cycle:** 0.96 + 0.045 + 2.325 (`Sync`) + 0.106 + 0.020 ≈ 3.45 ms. That agrees with the
  rate, since 1 / (5,000 / 16.6) = 3.32 ms.

Hiding the non-sync work shortens the residual wait behind the group in progress by at most about
1.13 / 2 ≈ 0.57 ms, which is about 8% of the 6,989 µs mean. Each order still waits for its own sync.
So the expected latency gain at moderate load is **at most about 8%, and probably less**. The tail
may improve more, because a backlog after a slow sync drains faster. *All of this is a projection*,
to be checked by Part 4's measurement.

**Part 3, a binary format, is not worth doing for latency, and only modestly for throughput.**
- At the maximum rate, `Encode` is 2.8 µs of the 21.8 µs one order takes in the cycle
  (22.17 ms / 1,015.2), that is 12.8%. Even a free encoder could raise throughput by at most about
  15% (1 / (1 − 0.128) = 1.147).
- At moderate rates it is 0.05–0.33% of latency (point 2 above).
- And it costs a journal format change that every reader must follow (theory §8).

**The recommendation is to drop Part 3, or keep it only as a learning exercise after Part 2**
(`decisions.md`).

**The floor is the disk.** At moderate load, 42–69% of an order's latency is the SSD's flush and the
wait for it. On this hardware, no software change in this milestone can move that. A drive with
power-loss protection could (milestone 24, `04-raw-sync.md`).

## What this does not show

- **Command-weighted group stages.** The gap of 10–30% inside `Worker` is explained only by
  inference.
- **`Sync` replicated includes two small extra steps.** In the replicated flush, the measured
  `Sync` span also covers `store.end()` (a file-length read) and handing the new end to the
  replication link (`replication.written`), which run between the write and the sync. Both take
  microseconds against milliseconds of sync, but the stage is not `sync_all` alone.
- **CPU per container,** to confirm CPU contention in the HTTP-replicated tail.
- **Two machines.** The replica's sync shares this SSD, so `Sync` and `Confirm` compete for one disk.
  On two machines they would overlap (milestone 23 part 4).
