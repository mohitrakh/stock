# 03 — The pipelined sync, before and after

**Part 2. Measured 2026-10-14, 13:25 to 14:05 local time, on the office machine.** Raw output:
- `results/m25p2-compare.txt` (13:25–13:45): the normal builds, Part 1 against Part 2;
- `results/m25p2-stages.txt` (13:45–14:05): the stage-timing builds, Part 1 against Part 2.

Derived, by `scripts/m25p2-summarize.awk`: `results/m25p2-compare-summary.txt` and
`results/m25p2-stages-summary.txt`. Every figure below comes from those two files. Each derived
figure shows its arithmetic.

An earlier comparison, with the first version of Part 2, is kept as
`results/m25p2-compare-shipping-per-group.txt`. Replicated at moderate load it was broken, which led
to a fix (`part-2-pipelined-sync.md`, "A bug the measurement found"). It is not used below.

## The question

What does moving the sync to a journaler thread change?
- **The maximum rate**, which Part 1 projected to rise by about 37–66% alone and about 79%
  replicated (`02-where-the-time-goes.md`).
- **Latency at 1,000 and 5,000 orders/s**, which Part 1 projected to fall by at most about 8% at
  5,000/s.
- **Is the change beyond the spread between runs?** That is the milestone's completion criterion.

## Method

- **Binaries**, release builds in `~/stock-scripts/`:
  - `stock-m25p1` and `stock-m25p1-timed`: Part 1, the sync on the worker (built 2026-10-14 09:11
    from the Part 1 tree, committed as `5dde603`);
  - `stock-m25p2` and `stock-m25p2-timed`: Part 2 with the shipping fix (built 13:23 from
    `~/stock-m25p2`).

  The measured Part 2 binaries lack one later fix. Running alone, a failed publication must fail
  the whole release pass (review, second round). That path is never taken in these runs.
- **Configurations**, 3 runs of each build:
  - in-process (`--bench`), alone and replicated, at 1,000/s and 5,000/s for 20 s (20,000 and
    100,000 orders), and at the maximum rate (200,000 orders);
  - HTTP (`--http-bench`), alone and replicated, at 1,000/s and 5,000/s for 20 s. The generator is
    always `stock-m25p1`, so only the exchange differs.
- **Interleaved, alternating order.** For each configuration, run 1 has Part 1 first, run 2 Part 2
  first, run 3 Part 1 first. Part 1 found the second of two back-to-back runs about 9.5% faster at
  the maximum rate (`01-instrumentation-overhead.md`). Over three runs the order is still unequal,
  2 to 1.
- **Scripts:** `scripts/m25p2-compare.sh 3` (normal builds), then `scripts/m25p2-stages.sh 3` (timed
  builds), one after the other. Both use milestone 24's `m24-lib.sh`. The replica, when there is
  one, runs the same binary as the exchange.
- **The machine:** 1-minute load average 2.30–6.96 over the comparison and 2.55–6.97 over the stage
  runs, with 30–32 containers running (from every run's `# host` line). The same machine and load
  as Part 1's measurements.
- **Disturbances:** one run had errors. Part 2, HTTP replicated, 5,000/s, run 3 had 16 orders that
  failed. The output names only the first error, a failed connection; that all 16 were is an
  inference. That run also had the highest p99 of its configuration (114,111 µs). It is kept in the
  averages and flagged where it matters. No run had unanswered or rejected orders.

## Results: the normal builds

Mean over 3 runs, with [lowest run, highest run]. Latency in µs, end to end, measured from each
order's intended send time. "Separate" means the two builds' ranges do not overlap. "Overlap"
means the difference is within the spread between runs.

### Maximum rate (in-process, 200,000 orders)

| | Part 1 | Part 2 | Change |
|---|---|---|---|
| Alone, orders/s | 45,975 [43,894, 48,505] | 65,057 [64,399, 65,870] | **+41.5%**, separate |
| Replicated, orders/s | 35,297 [32,004, 38,993] | 48,256 [47,847, 48,539] | **+36.7%**, separate |

The change: (65,057 − 45,975) / 45,975 = +41.5%, and (48,256 − 35,297) / 35,297 = +36.7%. At the
maximum rate latency is queueing by construction, so it follows throughput. The p50 falls by 22.3%
alone and 18.3% replicated.

### 1,000 and 5,000 orders/s

| Configuration | Part 1 mean | Part 2 mean | Change | Part 1 p50 → Part 2 p50 | Part 1 p99 → Part 2 p99 |
|---|---|---|---|---|---|
| in-process alone 1,000/s | 5,858 [5,678, 6,005] | 5,465 [5,332, 5,578] | −6.7%, separate | 5,595 → 5,244 (−6.3%, separate) | 12,026 → 12,279 (+2.1%, overlap) |
| in-process alone 5,000/s | 7,883 [7,393, 8,328] | 7,075 [6,308, 8,365] | −10.2%, overlap | 7,556 → 6,568 (−13.1%, overlap) | 17,562 → 18,260 (+4.0%, overlap) |
| HTTP alone 1,000/s | 6,484 [5,906, 6,798] | 6,199 [5,585, 7,069] | −4.4%, overlap | 6,136 → 5,975 (−2.6%, overlap) | 15,802 → 15,295 (−3.2%, overlap) |
| HTTP alone 5,000/s | 10,080 [8,512, 12,009] | 7,278 [6,901, 7,844] | −27.8%, separate | 8,194 → 6,522 (−20.4%, overlap) | 55,716 → 27,332 (−50.9%, overlap) |
| in-process replicated 1,000/s | 9,593 [9,109, 9,854] | 9,516 [8,919, 9,991] | −0.8%, overlap | 9,220 → 9,210 (−0.1%, overlap) | 21,988 → 21,210 (−3.5%, overlap) |
| in-process replicated 5,000/s | 14,008 [13,541, 14,449] | 11,408 [10,872, 12,363] | **−18.6%, separate** | 13,511 → 11,015 (−18.5%, separate) | 32,826 → 25,498 (−22.3%, separate) |
| HTTP replicated 1,000/s | 10,936 [10,486, 11,393] | 10,262 [9,966, 10,583] | −6.2%, overlap | 10,519 → 9,900 (−5.9%, overlap) | 23,172 → 21,631 (−6.7%, overlap) |
| HTTP replicated 5,000/s | 16,976 [16,269, 18,307] | 12,477 [10,403, 15,349] | **−26.5%, separate** | 14,487 → 10,892 (−24.8%, separate) | 58,900 → 60,714 (+3.1%, overlap; Part 2's run 3 is the disturbed one) |

Each change is (Part 2 − Part 1) / Part 1, for example (11,408 − 14,008) / 14,008 = −18.6%.
Throughput matched the offered rate in every one of these runs: 4,992–5,000 orders/s per run at
5,000/s, and 999–1,000 at 1,000/s.

**What the normal builds show:**
1. **The maximum rate rose by about 37–42%**, far beyond the spread between runs. The milestone's
   criterion for the maximum rate is met.
2. **At 5,000/s, latency fell in every configuration.** The mean fell by 10–28%. The change is
   beyond the spread in three of four: in-process replicated, HTTP alone and HTTP replicated.
   In-process alone (−10.2%) is within it. Part 1 projected "at most about 8%"; that was too low,
   for the reason found in the stages below.
3. **At 1,000/s nothing changed beyond the spread**, apart from in-process alone (−6.7%, separate
   but small). That is as expected: at light load each order still waits for its own sync.
4. **The p99 at 5,000/s fell beyond the spread only in-process replicated** (−22.3%). Elsewhere it is
   within the spread, which for the p99 is wide: HTTP alone ranges over 27,855–93,119 µs for
   Part 1. The milestone's criterion asks for the p99 at 5,000/s to improve beyond the spread, or
   for an explanation if it does not: on this machine, three runs cannot settle the p99 in three of
   four configurations.

## Results: the stages

The timed builds, averaged over 3 runs (`results/m25p2-stages-summary.txt`). The timed builds'
end-to-end figures move the same way as the normal builds'. At the maximum rate alone the timing
costs more now: 60,899 orders/s timed against 65,057 normal for Part 2,
(65,057 − 60,899) / 65,057 = 6.4%. For Part 1 the same comparison today gives
(45,975 − 45,033) / 45,975 = 2.0%; `01-instrumentation-overhead.md` found about 3%. The likely reason, *an inference*, is that the journaler is a second thread recording
and merging.

### Why the maximum rate rose: the worker's cycle

For one group at the maximum rate, staging is `WaitInGroup` + own CPU + `WaitForGroup` (the
derivation is in `02-where-the-time-goes.md`). All values are means, in ms per group:

| | Part 1 alone | Part 2 alone | Part 1 replicated | Part 2 replicated |
|---|---|---|---|---|
| commands per group (`GroupSize`) | 1,013.5 | 1,011.8 | 1,010.1 | 1,008.4 |
| staging | 13.42 | 13.10 | 15.12 | 14.70 |
| write | 0.36 | 0.35 | 0.42 | 0.40 |
| publish | 2.39 | 2.18 | 2.59 | 2.62 |
| release | 0.45 | 0.48 | 0.50 | 0.49 |
| **worker's own work** | **16.62** | **16.12** | **18.63** | **18.21** |
| sync | 5.45 | 6.26 | 8.63 | 10.81 |
| replica's confirmation | — | — | 5.08 | 7.75 |
| **disk side** (sync + confirmation) | 5.45 | 6.26 | 13.70 | 18.56 |
| cycle, if sequential: work + disk | 22.07 | — | 32.33 | — |
| rate predicted | 1,013.5 / 22.07 ms = **45,900** | 1,011.8 / 16.12 ms = **62,800** | 1,010.1 / 32.33 ms = **31,200** | (below) |
| rate measured (timed) | 45,033 | 60,899 | 31,402 | 47,816 (estimate with the worker's waits: 49,000, below) |

Part 2 alone: staging 7,349.6 + (5.2 + 3.0 + 2.9) + 5,740.9 = 13,101.6 µs. The work is
13.1016 + 0.3463 + 2.1833 + 0.4848 = 16.116 ≈ 16.12 ms per group (the table's rounded terms add to
16.11), against a disk side of 6.26 ms. The sync is fully
hidden, and the worker sets the pace. The prediction, 62,800 orders/s, is within 3% of the measured
60,899: (62,800 − 60,899) / 60,899 = 3.1%. Part 1 projected 62,700 for exactly this arrangement
(the worker keeps publishing and releasing): **confirmed**.

**Part 2 replicated: the disk side is now the slower one.** It is 18.56 ms against the worker's
18.21 ms. Two things grew:
- `Sync`, from 8.63 to 10.81 ms;
- the confirmation, from 5.08 to 7.75 ms.

The primary's sync, the replica's sync and the worker's next write now run at the same time on the
one SSD, where before the worker waited out each sync. So each takes longer. *That is an
inference*: the SSD's queue was not observed.

When the disk side runs behind, 2,048 commands pile up and the worker stops. `PipelineFull`
recorded 129 waits per run on average (the summary's `n` is per run), each 3,651 µs on average.
Per group, that adds 129 × 3,651 µs / 198 groups ≈ 2.38 ms. So, as an estimate, one group takes
(18.21 + 2.38) ms, which gives 1,008.4 / 20.59 ms ≈ 49,000 orders/s. The measured rate is 47,816,
2.4% lower. The worker's own work plus its waits for the disk account for the replicated rate.
The disk side varies widely (`Sync` p99 39.9 ms, `Confirm` p99 32.7 ms). That is why the worker
has to wait even though the disk side's mean, 18.56 ms, is only a little longer than its own
18.21 ms.

Part 1's projection of +79% replicated assumed the disk side would stay at 12.79 ms, hidden behind
a worker cycle of 16.20 ms. Both grew, the disk side the most, and the measured gain is +52% with
the timed builds (31,402 → 47,816) and +36.7% with the normal ones. The two figures differ because
Part 1's timed replicated runs were slower than its normal ones: 31,402 against 35,297.

Alone, `PipelineFull` happened about 3 times per run (against about 198 groups): the bound is
almost never reached.

### Why latency fell at 5,000/s: groups no longer wait for each other

Part 1 projected the gain from hiding the non-sync work, about 1.13 ms per cycle at 5,000/s alone,
of which an order waits about half: at most about 8%. The stages show the gain comes from
somewhere else. All means are in µs:

| In-process, 5,000/s | Part 1 alone | Part 2 alone | Part 1 replicated | Part 2 replicated |
|---|---|---|---|---|
| `Queue` (waiting to be taken) | 1,792.6 | 183.4 | 4,644.2 | 220.1 |
| `WaitInGroup` + `WaitForGroup` (waiting for the rest of its group to stage) | 816.7 | 245.2 | 2,335.8 | 320.5 |
| commands per worker group (`GroupSize`) | 13.7 | 4.0 | 28.8 | 3.9 |
| waiting for the disk, per order | (in `Worker`; Part 1 had no per-order stage) | `WaitForSync` 3,988.9 | — | `WaitForSync` 10,296.4 |
| `Queue` + `Worker` | 5,403.4 | 4,664.3 | 13,365.2 | 11,257.0 |
| end-to-end mean (timed) | 6,511 | 5,814 | 14,742 | 12,570 |

The sums: 1,792.6 + 3,610.8 = 5,403.4, and 183.4 + 4,480.9 = 4,664.3.

In Part 1, an order arriving during a cycle waited in the queue for the whole rest of it: the
sync, the confirmation, the publication, the release, and then the staging of every order in its
own group. At 5,000/s replicated those groups held 28.8 orders, so waiting for the rest of the
group cost 2.3 ms. In Part 2 the worker takes an order almost at once (`Queue` 220 µs). It stages
it in a group of about 4 and writes it. The order then waits for the disk: the rest of the sync in
progress, plus the next one. The staging of everybody else no longer stands between the syncs.

Net, `Queue` + `Worker` fell by 739 µs alone (5,403.4 − 4,664.3) and by 2,108 µs replicated
(13,365.2 − 11,257.0). That is −13.7% and −15.8% of `Queue` + `Worker`. The end-to-end means fell
by 697 µs (−10.7%) and 2,172 µs (−14.7%). The gain is larger replicated because there the group
waits were larger: two sequential waits, sync then confirmation, made each cycle longer, so more
orders gathered in each group.

The cost: **more, smaller syncs, and more write calls.** At 5,000/s alone, Part 2 made 10,970 syncs
per run against 7,867 (+39%), because the journaler starts the next sync as soon as anything is
written. It also made 25,181 writes against 7,867. Orders per sync fell from 13.7 to 9.5 (the
"per sync" line of `--bench`). The disk can afford it at this rate.

### The order's own CPU at light load

At 1,000/s, prepare + encode + commit rose from 59.5 to 97.0 µs alone, and from 58.1 to 89.2 µs
replicated. The worker now handles almost every order alone (`GroupSize` 1.1) and sleeps between
orders, where before it staged two to four together. A colder CPU is the likely reason, *an
inference*, as in Part 1. The rise is 97.0 − 59.5 = 37.5 µs alone and 89.2 − 58.1 = 31.1 µs
replicated, against 5–10 ms of latency.

### Part 1's open question: the gap inside `Worker`

Part 1 found that 10–30% of `Worker` below the maximum rate was not covered by any stage. It
guessed that per-group stages under-count what an average order waits, because a large group
carries many orders and probably a longer sync: size-biased sampling. Part 2's `WaitForSync` is
recorded per command. With it, the gap shrinks:

| Below the maximum rate | Part 1 timed, not covered | Part 2 timed, not covered |
|---|---|---|
| in-process alone 1,000 / 5,000/s | 18.9% / 18.6% | 1.2% / 2.9% |
| HTTP alone 1,000 / 5,000/s | 15.6% / 22.7% | 1.3% / 2.6% |
| in-process replicated 1,000 / 5,000/s | 24.7% / 21.7% | 1.1% / 2.8% |
| HTTP replicated 1,000 / 5,000/s | 12.0% / 13.5% | 0.9% / 2.2% |

"Not covered" is `Worker` minus the stages along the order's path, as a share of `Worker`:
- Part 1: `WaitInGroup` + own CPU + `WaitForGroup` + the group stages counted whole (`Write`,
  `Sync`, `Confirm`, `Publish`, `Release`);
- Part 2: the same, with `WaitForSync` in place of `Sync` and `Confirm`.

Example, Part 2 in-process alone at 5,000/s:
133.8 + 48.5 + 111.4 + 30.5 + 3,988.9 + 28.7 + 8.3 = 4,350.1, against `Worker` 4,480.9.
Not covered: 130.8 µs, 2.9%.

Measured per order, the wait accounts for `Worker` to within 1–3%. That is **consistent with**
Part 1's explanation: averaged per group, the disk wait was under-counted. It is not a proof for
Part 1's binary, which had no per-order stage. These Part 1 figures are from today's runs, and
they differ from `02-where-the-time-goes.md`'s (10–30%), which came from another hour. The
remaining 1–3% is probably the reply's own sending, which no stage covers. *That is an inference.*

The size bias is visible directly. Alone at 5,000/s, `WaitForSync` averages 3,988.9 µs per order,
while a sync averages 1,805.9 µs. An order written during a sync waits for the rest of that sync
and then all of the next. If every sync took exactly the mean, that would be at most 2 × 1,805.9 =
3,611.8 µs. That the measured wait is longer fits the inspection paradox (theory §3): an order is
more likely to arrive during a long sync, and the syncs vary widely (p50 1,313 µs, p99 5,871 µs).
*This is a consistency check, not a proof.*

## What this proves, and what it does not

**Proven on this machine, today, beyond the spread between runs:**
- the maximum rate rises by 41.5% alone and 36.7% replicated;
- at 5,000/s the mean latency falls by 18.6% in-process replicated, 27.8% on HTTP alone and 26.5%
  on HTTP replicated;
- at 5,000/s in-process replicated the p99 falls by 22.3%.

**Consistent, within the spread:**
- 5,000/s in-process alone: −10.2% mean, −13.1% p50;
- 1,000/s: changes of 0–7%, which is what theory expects;
- the p99 at 5,000/s in three of four configurations.

**Not shown:**
- **Two machines.** The replica shares this SSD, which is why the disk side, not the worker, now
  limits the replicated maximum rate. With separate disks the replicated gain could approach the
  alone gain. *Not measured.*
- **The power-loss behaviour of the pipeline.** Correctness after a failed sync is proven by tests
  with an injected failure, not by cutting power.
- **Against the milestone 24 part 2 binary.** That is Part 4's final measurement.

## Projections against measurements

| Projected in Part 1 (`02-where-the-time-goes.md`) | Measured here |
|---|---|
| Maximum rate alone, with the worker still publishing: +37% (62,700 orders/s) | timed 60,899 (cycle predicts 62,800); normal +41.5% |
| Maximum rate alone, publishing moved too: +66% | not built: publishing stays on the worker |
| Maximum rate replicated: +79% | normal +36.7%, timed +52%; the shared SSD's disk side became the limit |
| Latency at 5,000/s: at most about 8% | −10% to −28% in mean: the gain came mostly from smaller groups waiting less for each other, which the projection did not count |
