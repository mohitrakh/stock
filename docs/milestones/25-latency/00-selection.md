# Milestone 25 — Why latency, and why in this order

## Why latency now

1. **The owner asked for it.** Before milestone 24, the owner expected latency to be next.
   Milestone 24 was put first because the measurements latency work needs did not exist
   (`docs/milestones/24-gateway/00-selection.md`, Part B). They exist now.
2. **The design asks for it.** It wants round-trip latency "at the millisecond level, with a
   particular focus on the 99th percentile latency" (`stock-exchange-system-design.md`,
   non-functional requirements).
3. **Milestone 24 part 1 built the tools:**
   - `--http-bench`, an open-loop HTTP generator, timed from each order's intended send time, with a
     `/health` control;
   - `--sync-probe`, which measures the disk on its own;
   - the scripts, which run interleaved comparisons on the office machine.

## What was known when the milestone started

Everything below was measured on the office Ubuntu machine on 2026-10-12 and is recorded in
`docs/milestones/24-gateway/performance/`.

| What | Value | Source |
|---|---|---|
| One durable 600-byte append, p50 | 0.80–0.90 ms | `04-raw-sync.md` (total of write + sync) |
| One durable 600-byte append, mean | 1.31–1.39 ms | same |
| One durable 600-byte append, p99 | 5.3–5.5 ms | same |
| The same on tmpfs, mean | 1.6 µs | same |
| In-process order at 5,000/s alone, p50 | 3.58 / 3.84 ms | `results/m24p1-http.txt` |
| In-process order at 5,000/s alone, p99 | 19.5 / 18.9 ms | same |
| Group cycle at 5,000/s alone | 1.46 / 1.65 ms | `00-selection.md`, "Correction after Part 1" |
| Share of that cycle spent in the flush | about 80–90% | `04-raw-sync.md` |
| Extra p50 through HTTP | +0.3 to +0.9 ms | `01-http-vs-in-process.md` |
| Extra p99 through HTTP at 5,000/s | +49% alone, +71% replicated | same |
| Median left unexplained by a simple model | about 0.9 ms | `00-selection.md`, correction |

And, from the Docker Desktop VM in milestone 19 (`docs/tasks/14-critical-path-performance-v1.md`):
about 25.5 µs of CPU per order, of which JSON encoding is about 6.9 µs.

## Why the parts come in this order

1. **Measure first.** Each candidate optimization targets a different stage:
   - a pipelined sync targets the worker's idle time during a sync;
   - a binary format targets encoding;
   - neither touches HTTP, queueing or the reply path.

   Without a stage-by-stage breakdown, any of them could be a large effort spent on a small share.
   The milestone 24 lesson applies too: a number that looked like "3–4 ms unexplained" on one day was
   0.9 ms three days later. So the breakdown must be measured, not modelled.
2. **The pipelined sync next.** On today's numbers it is the stage most likely to matter. The flush
   is most of each group cycle, and the worker does nothing while it runs.
3. **The binary format only if Part 1 says so.** About 7 µs of encoding against a median of about
   3,600 µs is about 0.2%. It is likely to matter only at the maximum rate, where the worker is
   CPU-bound.
4. **A final measurement against the milestone 24 part 2 binary**, on the same day, interleaved.

## What is deliberately not in this milestone

- **CPU pinning.** 2 cores, shared with about 30 containers, leave nothing to pin to.
- **Lock-free rings, Crossbeam, per-symbol workers, new component threads** (beyond the one sync
  thread). None is justified until a measurement points at the queue or the worker's CPU.
- **New hardware.** A server SSD with power-loss protection is recorded as the largest lever on
  the floor, about two orders of magnitude, but it is outside the software.
