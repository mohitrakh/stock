# 04 — The final measurement: milestone 25 against milestone 24

**Part 4. Measured 2026-10-19 on the office machine.** Raw output:
- `results/m25p4-final.txt`, 15:12 to about 15:37: 84 runs, the main comparison;
- `results/m25p4-stages.txt`, 15:38 to about 15:41: a follow-up with the stage-timing builds, 12 runs,
  for the two configurations whose results disagreed with Part 2's;
- `results/m25p4-order.txt`, run after the independent review: a second follow-up, 12 runs, for the
  one configuration whose median got worse, with each build first in half the pairs.

Derived by `scripts/m25p2-summarize.awk`: `results/m25p4-final-summary.txt` and
`results/m25p4-stages-summary.txt`. Every figure below comes from those files, or from the raw
files where it is per run. Every derived figure shows its arithmetic.

## The question

What did milestone 25 change, end to end, against the exchange as milestone 24 left it? The
milestone's completion criterion: "the maximum rate and the p99 at 5,000 orders/s are measured
against the milestone 24 part 2 binary, on the same day, with three runs each. Either the
improvement is beyond the spread between runs, or the reason it is not is explained." The plan
also asked for tmpfs runs, which separate the software's share from the disk's, and for each
container's CPU during the HTTP runs, which settles milestone 24's open question: is the HTTP tail
CPU contention?

## Method

- **Binaries:**
  - `stock-m24p2`: milestone 24 part 2, the sync on the worker, no timing. It was built on
    2026-10-12 from the working tree later committed as `28e085a`.
  - `stock-m25final`: built 2026-10-19 from `67fb6bc`, the pushed end of milestone 25, with the
    pipelined sync.

  Part 1's `stock-m25p1` behaves as `stock-m24p2`. It adds only the timing hooks, compiled out, and
  the split of `append_record` into its two halves.
- **The generator for HTTP runs is `stock-m25final` for both builds** (`decisions.md`).
- **In-process runs of `stock-m24p2` print no mean**, because its `--bench` predates the mean. Those
  comparisons use throughput, p50 and p99.
- **Configurations, 3 runs each, interleaved.** Run 1 has the before-build first, run 2 the
  after-build first, run 3 the before-build first. So each build runs second in a different
  number of pairs, 1 against 2.
  - On the SSD:
    - in-process, alone and replicated, at 1,000/s, 5,000/s (20 s each) and the maximum rate
      (200,000 orders);
    - HTTP, alone and replicated, at 1,000/s and 5,000/s, with a CPU line.
  - On tmpfs, with both journals in RAM so a sync costs no disk time: in-process, alone and
    replicated, at 5,000/s and the maximum rate.
- **The CPU line.** Each container's CPU time is read from cgroup v2 (`cpu.stat`, `usage_usec`)
  once a second, and the host's from `/proc/stat`. It covers only the measured orders: from the
  generator's `http-bench:` line, after its setup, to its `throughput` line. Values are in hardware
  threads: 1.00 means one hardware thread busy for the whole window. The machine has 2 physical
  cores with 2 hardware threads each, 4 in all.
- **Scripts:** `scripts/m25p4-final.sh 3`, and the follow-ups `scripts/m25p4-stages.sh 3` and
  `scripts/m25p4-order.sh 6`.
- **The machine:** 1-minute load average 1.11–6.36 and 29–31 containers running in the main runs.
  No run had errors, unanswered orders or rejections. In every HTTP run, 29 other containers were
  running, so the CPU columns are not disturbed by containers starting or restarting.
- **The machine was faster than on 2026-10-14.** For example, `stock-m24p2` in-process alone at
  1,000/s had a p50 of 2,662 µs today. Part 1's equivalent build measured 5,595 µs on 2026-10-14
  (`results/m25p2-compare-summary.txt`). Results from the two days are separate datasets, and that
  difference matters below.

## Results on the SSD

Mean over 3 runs, with [lowest run, highest run]. "Separate" means the ranges do not overlap.

### Maximum rate (in-process, 200,000 orders), orders/s

| | Milestone 24 | Milestone 25 | Change |
|---|---|---|---|
| alone | 43,916 [36,756, 48,597] | 51,367 [48,077, 53,178] | +17.0%, overlap |
| replicated | 28,127 [24,371, 31,059] | 49,842 [45,926, 57,556] | **+77.2%, separate** |

### 1,000 and 5,000 orders/s, latency in µs

| Configuration | p50 | p99 | mean (HTTP only) |
|---|---|---|---|
| in-process alone 1,000/s | 2,662 → 2,403 (−9.7%, overlap) | 11,095 → 9,572 (−13.7%, overlap) | — |
| in-process alone 5,000/s | 3,284 → 2,849 (−13.2%, overlap) | 15,948 → 15,010 (−5.9%, overlap) | — |
| HTTP alone 1,000/s | 3,478 → 3,464 (−0.4%, overlap) | 14,236 → 14,490 (+1.8%, overlap) | 4,142 → 4,519 (+9.1%, overlap) |
| HTTP alone 5,000/s | 3,269 → 3,829 (+17.1%, overlap) | 37,204 → 24,154 (−35.1%, overlap) | 6,252 → 5,552 (−11.2%, overlap) |
| in-process replicated 1,000/s | 5,222 → 4,380 (**−16.1%, separate**) | 20,442 → 18,767 (−8.2%, overlap) | — |
| in-process replicated 5,000/s | 5,419 → 8,144 (+50.3%, overlap: [4,827, 6,303] against [6,163, 9,847]; see below) | 30,538 → 25,119 (−17.7%, overlap) | — |
| HTTP replicated 1,000/s | 6,288 → 5,810 (−7.6%, overlap) | 21,786 → 21,087 (−3.2%, overlap) | 7,955 → 7,564 (−4.9%, overlap) |
| HTTP replicated 5,000/s | 7,771 → 6,980 (−10.2%, overlap) | 55,690 → 28,676 (**−48.5%, separate**) | 12,391 → 9,269 (**−25.2%, separate**) |

Each entry is milestone 24 → milestone 25, and the change is (after − before) / before.
Throughput matched the offered rate in every run.

**What the SSD runs show:**
1. **Replicated, the maximum rate rose by 77%**, beyond the spread. Alone it rose by 17%, within the
   spread: milestone 24 alone ranged over 36,756–48,597, which is 32%.
2. **The p99 at 5,000/s fell in all four configurations**, by 6–49%. Only HTTP replicated is beyond
   the spread.
3. **The median at 5,000/s moved both ways**, all within the spread. In-process replicated rose by
   half in these three runs. A balanced check (below) puts that at +15%, still within the spread.

## Why the maximum rate alone rose less than in Part 2

The follow-up (`results/m25p4-stages-summary.txt`) ran the stage-timing builds of Part 1 (behaving
as milestone 24) and Part 2, interleaved, 3 runs each. In-process alone, at the maximum rate, all
values µs per group:

| One group's cycle | Part 1 timed (milestone 24 behaviour) | Part 2 timed (pipeline) |
|---|---|---|
| staging: `WaitInGroup` + own CPU + `WaitForGroup` | 8,017.0 + 13.3 + 6,818.4 = 14,848.7 | 7,969.3 + 12.8 + 6,494.7 = 14,476.8 |
| write + publish + release | 386.3 + 2,500.8 + 474.9 = 3,362.0 | 376.5 + 2,292.2 + 505.0 = 3,173.7 |
| sync | 4,468.9, on the worker's path | 3,114.2, hidden on the journaler |
| cycle | 22,679.6 | 17,650.5 |
| rate predicted: commands per group / cycle | 1,013.5 / 22,679.6 µs = 44,690 | 1,011.8 / 17,650.5 µs = 57,320 |
| rate measured | 45,208 [34,337, 51,398] | 56,857 [46,034, 63,011] |

(The rates are rounded to 10 orders/s: the exact quotients are 44,688 and 57,324.)

The predicted gain is 57,324 / 44,688 = +28.3%. Measured with the timed builds, it is 56,857 / 45,208
= +25.8%.

On 2026-10-14 the same table gave Part 1 a sync of 5.45 ms and staging of 13.42 ms, and Part 2
staging of 13.10 ms (`03-pipelined-sync.md`). Today Part 1's sync was shorter, 4.47 ms, so the
pipeline had less to hide. The staging was longer, 14.48–14.85 ms, so the worker's own work
weighed more. **The gain from hiding the sync is (work + sync) / work, which shrinks when the sync
shrinks or the work grows.**

The normal builds measured +17.0% and the timed ones +25.8%. Both sets of ranges overlap: one
build's maximum rate varied by up to 50% between runs today (34,337 to 51,398 for Part 1 timed).
So the normal-build maximum-rate runs alone are within the spread, for a reason the cycle model
accounts for. That meets the completion criterion's second branch.

## The median at 5,000/s replicated: not settled

**Three runs said +50%.** In-process replicated at 5,000/s, the normal builds measured p50 5,419 →
8,144 µs, and the timed follow-up 4,486 → 8,424 µs. Per run, though, the pipeline's median was
high exactly when it ran second in its pair (raw files):

| Run, position | Normal builds: milestone 24 p50 | Normal builds: milestone 25 p50 | Timed: Part 1 p50 | Timed: Part 2 p50 |
|---|---|---|---|---|
| run 1: before-build first | 4,827 | 9,847 (second) | 4,451 | 10,583 (second) |
| run 2: after-build first | 6,303 (second) | 6,163 (first) | 4,523 (second) | 4,723 (first) |
| run 3: before-build first | 5,127 | 8,423 (second) | 4,483 | 9,967 (second) |

With three runs, the pipeline ran second in two pairs.

**A balanced check, 6 runs** (`scripts/m25p4-order.sh 6`, `results/m25p4-order.txt`): each build ran
first in three pairs.

| Normal builds, 6 runs | p50 µs | p99 µs | syncs per run |
|---|---|---|---|
| milestone 24 | 6,560 [4,467, 11,399] | 30,671 [24,431, 42,751] | 6,819 |
| milestone 25 | 7,516 [5,563, 10,759] | 27,500 [22,895, 35,487] | 7,458 |
| change | +14.6%, overlap | −10.3%, overlap | |

The averages: p50 (4,887 + 4,467 + 5,691 + 4,779 + 8,139 + 11,399) / 6 = 6,560 and
(5,563 + 7,887 + 9,159 + 5,843 + 10,759 + 5,883) / 6 = 7,516. Position in the pair did not affect the two
builds the same way. Milestone 24 averaged a p50 of 7,326 µs first and 5,795 second; its highest,
11,399, came first. Milestone 25 averaged 6,538 first (7,887, 5,843, 5,883) and 8,494 second (5,563,
9,159, 10,759). Across the three sets of runs, 6 of the 7 runs where milestone 25 ran second had a
high median. Why the pipeline's second runs suffer is not known. **The
median moved by +15%, within a spread where one build's p50 ranged over 4,467–11,399 µs.** Whether
the pipeline costs the median at 5,000/s replicated is not settled by these runs.

**What the stage data shows, per run** (`results/m25p4-stages.txt`; `Sync` + `Confirm` mean per
sync; time the sync path is busy = syncs × that, against the run's measured seconds):

| Timed run | p50 µs | syncs | `Sync` + `Confirm` µs | sync path busy |
|---|---|---|---|---|
| Part 1, run 1 | 4,451 | 7,609 | 1,165.5 + 905.3 = 2,070.8 | 15.76 / 20.005 s = 78.8% |
| Part 1, run 2 | 4,523 | 7,189 | 1,244.1 + 927.2 = 2,171.3 | 15.61 / 20.012 s = 78.0% |
| Part 1, run 3 | 4,483 | 7,446 | 1,165.6 + 913.5 = 2,079.1 | 15.48 / 20.002 s = 77.4% |
| Part 2, run 1 | 10,583 | 6,046 | 1,897.6 + 1,351.2 = 3,248.8 | 19.64 / 20.017 s = 98.1% |
| Part 2, run 2 | 4,723 | 9,150 | 1,235.0 + 899.8 = 2,134.8 | 19.53 / 20.005 s = 97.6% |
| Part 2, run 3 | 9,967 | 5,733 | 2,009.8 + 1,414.0 = 3,423.8 | 19.63 / 20.013 s = 98.1% |

- **The journaler keeps the sync path busy about 98% of the time**, against about 78% with the sync
  on the worker. It starts the next sync as soon as anything is written, and at 5,000 orders/s
  something always is.
- **The pipeline's median followed the length of a sync, not the busy share.** In run 2 its sync
  path was 97.6% busy and each sync plus confirmation took 2,134.8 µs, as long as Part 1's
  (2,070.8–2,171.3). Its median, 4,723 µs, was then close to Part 1's, and its mean (6,974) and p99
  (21,599) were lower than any Part 1 run's. In runs 1 and 3 each sync took 3,248.8–3,423.8 µs, and
  the median doubled. The mean of the three, 2,935.8 µs per sync against 2,107.0, is 39% longer, but
  the ranges, [2,134.8, 3,423.8] against [2,070.8, 2,171.3], overlap.
- **A likely mechanism, *an inference*:** with the path always busy, an order arrives during a sync
  and waits for the rest of it and then the whole next one. That costs little while syncs are as
  short as Part 1's. It roughly doubles the wait when syncs lengthen, because every order pays one
  and a half long syncs. With the sync on the worker, the path is idle 22% of the time. Orders that
  arrive then wait only for their own sync.
- **Why the pipeline's syncs were longer in two runs is not known.** The replica's syncs share this
  SSD, and the shared disk may slow overlapping syncs. *That is an inference.* An alternative, that
  appending to the file during its own sync makes the sync slower, is weakened by the alone runs.
  There the worker also appends during every sync, and Part 2's syncs were not longer:
  - 1,805.9 against 1,957.4 µs at 5,000/s on 2026-10-14 (`results/m25p2-stages-summary.txt`);
  - 3,114.2 against 4,468.9 µs at the maximum rate today.

  A probe timing `sync_all` alone, beside a concurrent appender, and beside a second journal's
  syncs on the same disk would settle it. It is recorded as a follow-up, not done.

**On 2026-10-14 the same configuration improved** (p50 13,511 → 11,015, separate;
`03-pipelined-sync.md`). The syncs were longer then: Part 1's `Sync` + `Confirm` averaged 2,460.4 + 1,732.3 = 4,192.7 µs
(`results/m25p2-stages-summary.txt`). The sequential design kept the sync path about 72% busy:
per run (`results/m25p2-stages.txt`), 4,091 × 3,499.1 µs = 14.31 s, 3,602 × 3,960.2 µs = 14.26 s and
2,917 × 5,118.9 µs = 14.93 s, a mean of 14.50 s of 20, 72.5% (71.3–74.6% per run). Orders then
waited in the queue behind whole cycles, including the staging of large groups (`Queue` 4,644 µs,
group waits 2,336 µs; `03-pipelined-sync.md`). The pipeline removed those waits. *That those waits
outweighed the saturation that day is an inference from the stage means.*

## Results on tmpfs (no disk)

| Configuration | Milestone 24 | Milestone 25 | Change |
|---|---|---|---|
| alone, 5,000/s, p50 µs | 1,522 [1,464, 1,566] | 1,646 [1,616, 1,692] | **+8.1%, separate** |
| alone, maximum rate, orders/s | 65,505 [64,472, 67,338] | 66,859 [64,929, 67,855] | +2.1%, overlap |
| replicated, 5,000/s, p50 µs | 1,906 [1,793, 2,013] | 1,865 [1,732, 1,976] | −2.2%, overlap |
| replicated, maximum rate, orders/s | 50,441 [48,935, 51,487] | 61,192 [59,737, 62,929] | **+21.3%, separate** |

- **Alone, with free syncs, the pipeline only costs.** The p50 rises by 1,646 − 1,522 = 124 µs, and
  the maximum rate does not change. The likely cost, *an inference*, is the hand-off to the
  journaler and back: two thread wake-ups per group, with no disk time to hide.
- **Replicated, even without a disk, the pipeline raises the maximum rate by 21%.** What it then
  hides is, *by inference*, the replica's round trip: the network and the replica's own processing.

**The disk's share**, comparing SSD with tmpfs for the same build:

| | p50 at 5,000/s: the disk's share | maximum rate on SSD as a share of tmpfs |
|---|---|---|
| milestone 24 alone | (3,284 − 1,522) / 3,284 = 54% | 43,916 / 65,505 = 67% |
| milestone 25 alone | (2,849 − 1,646) / 2,849 = 42% | 51,367 / 66,859 = 77% |
| milestone 24 replicated | (5,419 − 1,906) / 5,419 = 65% | 28,127 / 50,441 = 56% |
| milestone 25 replicated | (8,144 − 1,865) / 8,144 = 77% | 49,842 / 61,192 = 81% |

The pipeline recovers much of the throughput the disk cost. Replicated, it goes from 56% to 81% of
what the software alone could do. At moderate load the disk is still 42–77% of the median latency,
close to half or more, as Part 1 found. (The 77% uses the three-run median, which the balanced
check suggests was high.)

## CPU: is the HTTP tail CPU contention?

Hardware threads in use during the measured orders, mean over 3 runs, out of 4:

| HTTP | build | exchange | replica | generator | other containers | host busy |
|---|---|---|---|---|---|---|
| alone 1,000/s | m24 / m25 | 0.34 / 0.42 | — | 0.21 / 0.23 | 0.34 / 0.40 | 1.84 / 1.92 |
| alone 5,000/s | m24 / m25 | 0.73 / 0.75 | — | 0.43 / 0.41 | 0.34 / 0.33 | 2.09 / 2.01 |
| replicated 1,000/s | m24 / m25 | 0.54 / 0.60 | 0.14 / 0.14 | 0.32 / 0.30 | 0.46 / 0.45 | 1.80 / 1.92 |
| replicated 5,000/s | m24 / m25 | 0.93 / 0.98 | 0.14 / 0.16 | 0.56 / 0.55 | 0.40 / 0.35 | 2.25 / 2.27 |

- **On average the host is busy 1.8–2.3 of its 4 hardware threads** in every HTTP configuration.
  The exchange uses at most about one hardware thread (0.98, replicated at 5,000/s). The other 29
  containers together use 0.33–0.46.
- **That is less spare capacity than "half the machine idle" suggests.** The 4 threads are 2
  physical cores with 2 threads each, and two threads on one core share its execution units. With
  about 2 threads busy, the work may already be spread over both cores. *How much a thread on a
  shared core slows was not measured.*
- **So the HTTP tail is not explained by average CPU saturation.** Milestone 24 suspected CPU
  contention for the HTTP p99 being 49–71% above in-process (`00-selection.md`). Bursts shorter
  than the one-second sample cannot be seen here, so *bursts are not ruled out*. But the HTTP p99
  at 5,000/s replicated fell by half with the pipeline (55,690 → 28,676 µs) at the same CPU use.
  That points at backlogs behind slow syncs, not at CPU, *an inference*.
- **The pipeline costs a little CPU in the exchange**: +0.02 to +0.08 hardware threads. That is
  the journaler thread and the extra wake-ups, *an inference*. The difference is within the spread
  except HTTP alone at 1,000/s (0.34 → 0.42, separate).
- **Host busy exceeds the containers' sum by 0.22–0.95 hardware threads.** For example, HTTP alone
  at 1,000/s, milestone 24: 1.84 − (0.34 + 0.21 + 0.34) = 0.95. The host column also counts work
  outside the containers: dockerd, the SSH session, the kernel's writeback and network. *Which of
  them, was not measured.*

## What milestone 25 achieved, overall

Today, milestone 24 against milestone 25:
- **The maximum rate:**
  - replicated +77% (28,127 → 49,842 orders/s, beyond the spread);
  - alone +17%, within a wide spread. The timed follow-up measured +25.8% and the cycle model gives
    +28.3%.
- **The p99 at 5,000/s:** lower in all four configurations, by 6–49%. HTTP replicated fell from 55.7
  to 28.7 ms, beyond the spread.
- **The median at moderate load:**
  - better at 1,000/s replicated (in-process −16%, beyond the spread);
  - elsewhere within the spread, in both directions;
  - at 5,000/s in-process replicated, +15% over 6 balanced runs, within the spread. The pipeline's
    median there doubled in the runs where its syncs were longer.
- **With no disk,** alone, the pipeline costs about 124 µs at the median.

**Not done, recorded:**
- the probe that would explain the pipeline's longer syncs in some runs;
- runs on two machines, where the replica would not share the SSD;
- an option to delay the journaler's next sync, to protect the median at moderate load. It would
  trade away some throughput, and needs its own measurement, if it is ever wanted.
