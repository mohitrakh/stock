# 02 — The cost of one bcrypt

**Part 1 baseline. Measured 2026-10-12.** Raw output: `results/m24p1-probes.txt` (the "bcrypt"
sections). Script: `scripts/m24p1-probes.sh`. Tool: `stock --bcrypt-probe` (`src/exchange/probes.rs`).

## The question

`register` hashes the password and `login` verifies it, both with `bcrypt` at `DEFAULT_COST`
(12), inside the request's `async fn` (`00-selection.md` A.4). `01-theory.md` section 7 worked out
the size of that computation: 4,268,032 Blowfish block encryptions. It *estimated* about 0.21 s
from an assumed 50 ns per encryption. This file measures the real time on the office machine:

- how long one hash and one verify take;
- whether the time doubles per cost step, as the theory says;
- what happens when several run at once, as simultaneous logins would.

## Method

- **Machine and neighbours:** as in `04-raw-sync.md`. The load average was 2.6 to 3.2 during these
  runs (the raw file has each value), so about 2.6 to 3.2 hardware threads' worth of other work
  was competing for the 4 hardware threads.
- **Build:** release, `/scripts/stock-m24p1`, the `bcrypt` 0.19.1 crate the project locks.
- **What one sample is:** one `bcrypt::hash("bench-password-1", cost)`, or one
  `bcrypt::verify("bench-password-1", stored)` against a hash made at the same cost, timed with
  `Instant` in microseconds.
- **Concurrency:** `--threads T` starts T OS threads together, behind a barrier. Each does
  `--count` hashes, then `--count` verifies. Every sample from every thread goes in one histogram.
  "Rate" is all calls by all threads divided by the wall time.
- **Runs:**
  - cost 12, 1 thread, 20 hashes and 20 verifies;
  - cost 12 with 2, 4 and 8 threads, 5 of each per thread;
  - costs 10 and 11, 1 thread, 10 of each.

## Results

### One thread, cost 12

| call | n | mean | p50 | p90 | max |
|---|---|---|---|---|---|
| hash | 20 | 230.6 ms | 228.7 ms | 233.9 ms | 251.4 ms |
| verify | 20 | 228.9 ms | 228.6 ms | 229.9 ms | 233.2 ms |

4.35 calls per second in total.

**Hash and verify cost the same**, as the theory predicts: verify reads the cost and salt from the
stored string and recomputes the whole hash. The medians differ by 0.1 ms in 228.7.

### The time per cost step

| cost | rounds 2^cost | hash p50 | ratio to the step below |
|---|---|---|---|
| 10 | 1,024 | 57.2 ms | — |
| 11 | 2,048 | 115.1 ms | 115.1 / 57.2 = 2.01 |
| 12 | 4,096 | 228.7 ms | 228.7 / 115.1 = 1.99 |

Each step doubles the time, within 1%. The fixed part of the work (the initial `ExpandKey` and the
64 final encryptions) is too small to see.

### Checking the theory's arithmetic

`01-theory.md` 7.2 counted the encryptions at cost 12:

```text
2^12 × 2 × 521 = 4,096 × 1,042 = 4,268,032 Blowfish block encryptions
```

Measured time per encryption: 228.7 ms / 4,268,032 = 0.0000536 ms = **53.6 ns**. At 3.9 GHz (the
i3-7100 has no turbo boost, so it runs at 3.9 GHz under load) that is 53.6 × 3.9 ≈ **209 cycles per
encryption**, or 209 / 16 ≈ **13 cycles per Feistel round**. Blowfish's round function is
F(x) = ((S1[a] + S2[b]) XOR S3[c]) + S4[d], where a, b, c, d are the four bytes of x. The four
lookups within a round are independent of each other and can overlap. But each round's input is the
previous round's output, so the 16 rounds form one long chain where every step waits for the one
before. A chain like that keeps only a few of the core's execution units busy at a time (see
"Several at once" below). (Here a "cycle" is a unit of CPU work, used only to give a sense of
scale; every measurement in this milestone is a time.) The theory's 50 ns estimate was 7% low. The predicted total, about
0.21 s, against the measured 0.229 s, was the right order of magnitude.

### Several at once, cost 12

| threads at once | calls | hash p50 | verify p50 | total calls/s | speed-up over 1 thread |
|---|---|---|---|---|---|
| 1 | 40 | 228.7 ms | 228.6 ms | 4.35 | 1.00 |
| 2 | 20 | 231.0 ms | 230.4 ms | 8.42 | 8.42 / 4.35 = 1.94 |
| 4 | 40 | 254.5 ms | 247.6 ms | 15.42 | 15.42 / 4.35 = 3.54 |
| 8 | 80 | 506.9 ms | 501.5 ms | 15.10 | 15.10 / 4.35 = 3.47 |

What this shows:

- **Up to 2 threads**, each call runs at full speed on its own core: 231 ms, almost the 228.7 ms of
  one thread.
- **At 4 threads** the machine runs four calls at once on 2 cores, thanks to the second hardware
  thread on each core (Hyper-Threading). Each call slows only about 11% (254.5 / 228.7 = 1.11), so
  the total nearly doubles again. This fits bcrypt's shape: each Feistel round waits for the result
  of the round before, so one thread leaves most of a core's execution units idle, and the second
  hardware thread can use them. That is the likely explanation, an inference from the algorithm's
  structure, not a measurement of the core's execution units. Some of the slowdown is the background load, which was competing
  for the same threads.
- **At 8 threads** there are twice as many calls as hardware threads, so the OS time-shares them.
  Each call takes about twice as long (506.9 / 254.5 = 1.99), and the total stays at the machine's
  maximum, about 15 calls per second.

So **the whole office machine can do at most about 15 bcrypt calls per second at cost 12**, and
only by using all four hardware threads for nothing else.

## What this means for the gateway

1. **One login holds one Tokio worker for about 229 ms.** The exchange started 4 workers ("Tokio
   worker threads: 4", printed by the Part 1 binary at startup; see `results/m24p1-http.txt`).
   Four simultaneous logins or registrations therefore hold every worker for about 229 to 255 ms,
   and during that time no customer request is parsed or answered. `03-login-stall.md` measures
   what that does to orders.
2. **Logins are an expensive resource, about 15 per second for the whole machine.** An attacker
   needs only about 15 requests per second to keep all four hardware threads busy with bcrypt.
   Moving bcrypt to the blocking pool (Part 2) keeps the workers free, but not the CPU. That is why
   decision 7 caps the bcrypt calls in flight, and why Part 5 adds a per-IP limit on `login` and
   `register`.
3. **Choosing the cap.** With a cap of N bcrypt calls at once:
   - each call still takes about 229 ms while N ≤ 2 (one per core);
   - capacity is about N / 0.229 s, so 8.7 per second for N = 2;
   - the other hardware threads stay free for orders.

   Part 2 chooses N from these numbers and records it in `decisions.md`.

## What this does not show

- Other CPUs. The cost is a property of this machine. A modern server core would be faster, by an
  unknown factor; the doubling per cost step would hold.
- Variation in timing between different passwords. bcrypt's work does not depend on the
  password's content, only on the cost.
