# 01 — What the stage timing costs

**Part 1. Measured 2026-10-14.** Raw output: `results/m25p1-overhead.txt` (normal build first in
each pair) and `results/m25p1-overhead-reversed.txt` (timed build first). Scripts:
`scripts/m25p1-overhead.sh`, `scripts/m25p1-overhead-reversed.sh`.

## The question

The stage timing adds clock reads and histogram updates to every order (`01-theory.md` §5). Does it
change what it measures? If it did, Part 1's breakdown would describe the instrumented exchange
rather than the real one.

## Method

- **Two release builds of the same tree:** `stock-m25p1` (normal) and `stock-m25p1-timed`
  (`--features stage-timing`).
- **The in-process benchmark, alone,** in two configurations:
  - 100,000 orders at 5,000 orders/s (20 s): latency;
  - 200,000 orders at the maximum rate: throughput.
- **Three pairs of runs per configuration, the two builds alternating.** The first script ran the
  normal build first in every pair. When the result looked one-sided (below), a second script ran
  the same pairs with the timed build first.
- **Machine:** the office machine, 08:56 to 09:31 local time, load average about 2 to 3.

## Results

### At 5,000 orders/s (latency, µs)

| Script | Pair | Normal p50 | Timed p50 | Normal p99 | Timed p99 | Normal mean | Timed mean |
|---|---|---|---|---|---|---|---|
| normal first | 1 | 6,467 | 7,363 | 25,071 | 22,895 | — | — |
| normal first | 2 | 6,851 | 5,819 | 16,927 | 15,447 | — | — |
| normal first | 3 | 6,751 | 6,439 | 16,103 | 15,975 | — | — |
| timed first | 1 | 7,543 | 7,299 | 22,991 | 19,295 | 8,151.7 | 7,793.5 |
| timed first | 2 | 8,007 | 7,699 | 17,055 | 16,831 | 8,390.5 | 8,057.8 |
| timed first | 3 | 7,359 | 7,299 | 18,223 | 17,887 | 7,420.1 | 7,538.0 |

(The first script ran before the end-to-end mean was added to the output, so it has none.)

Averages of the six p50s per build:
- normal: (6,467 + 6,851 + 6,751 + 7,543 + 8,007 + 7,359) / 6 = 42,978 / 6 = **7,163 µs**;
- timed: (7,363 + 5,819 + 6,439 + 7,299 + 7,699 + 7,299) / 6 = 41,918 / 6 = **6,986 µs**.

The timed build is 177 µs *lower*, about 2.5%. The spread between runs of one build is about 2 ms
(5,819 to 7,699 for the timed build), so this difference is noise. **At 5,000 orders/s the
instrumentation has no measurable cost.**

### At the maximum rate (throughput, orders/s)

| Script | Pair | First in pair | Second in pair |
|---|---|---|---|
| normal first | 1 | normal 45,889 | timed 46,089 |
| normal first | 2 | normal 43,215 | timed 48,133 |
| normal first | 3 | normal 43,988 | timed 48,140 |
| timed first | 1 | timed 47,309 | normal 50,276 |
| timed first | 2 | timed 45,962 | normal 52,879 |
| timed first | 3 | timed 42,377 | normal 50,356 |

**The first script alone was misleading.** It showed the timed build faster in all three pairs
(47,454 against 44,364 orders/s on average). The reversed script shows the *normal* build faster in
all three pairs (51,170 against 45,216). So, in both scripts, **the second run of each pair was
faster**, whichever build it was.

Separating the two effects. Let *b* be the build's effect and *p* the advantage of running second:

| | normal | timed |
|---|---|---|
| mean when first | (45,889 + 43,215 + 43,988) / 3 = 44,364 (script 1) | (47,309 + 45,962 + 42,377) / 3 = 45,216 (script 2) |
| mean when second | (50,276 + 52,879 + 50,356) / 3 = 51,170 (script 2) | (46,089 + 48,133 + 48,140) / 3 = 47,454 (script 1) |

```text
normal, averaged over both positions: (44,364 + 51,170) / 2 = 47,767 orders/s
timed,  averaged over both positions: (45,216 + 47,454) / 2 = 46,335 orders/s
build effect:    46,335 − 47,767 = −1,432 orders/s  → the timed build is about 3.0% slower
position effect: in script 1, 47,454 − 44,364 = 3,090; in script 2, 51,170 − 45,216 = 5,954;
                 average 4,522 orders/s, about 9.5%, in favour of the second run
```

**At the maximum rate the instrumentation costs about 3% of throughput.** That is consistent with
the theory's estimate of well under a microsecond per order (§5). At the maximum rate one order
takes about 21–22 µs of worker time (`02-where-the-time-goes.md`), so 3% is about 0.65 µs.

## What the numbers say

1. **Part 1's stage numbers describe the real exchange.** At 5,000 orders/s the timing costs
   nothing measurable. At the maximum rate it costs about 3% of throughput, which is small next to
   the effects Part 1 reports. 1,000/s was not measured. The timing's cost per order does not depend
   on the rate, so its share at 1,000/s should be no larger; that is an inference.
2. **A run-order effect of about 9.5% at the maximum rate.** A run that immediately follows another
   was faster, whichever build it ran. Its cause was not measured. Candidates:
   - the state of the SSD after the previous run's writes;
   - the binary and its files already in the page cache;
   - CPU frequency;
   - background load that happened to fall between the pairs.

   It matters for every later comparison in this milestone. **Before-and-after comparisons must
   alternate which build runs first**, as Part 4 will do, or they will mistake position for
   improvement.
3. **The machine changed within the hour.** For the normal build, the 5,000/s p50s were 6.5–6.9 ms
   in the first script (08:56) and 7.4–8.0 ms in the second (09:23). Across both builds: 5.8–7.4 ms,
   then 7.3–8.0 ms. This is the same drift
   milestone 24 found between days, here within half an hour.

## What this does not show

- The cost on the HTTP path. The gateway adds 5 clock reads and 3 mutex-guarded records per order.
  That is not separated here, because the HTTP runs were made only with the timed build.
- Why running second is faster.
