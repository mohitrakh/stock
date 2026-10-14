# 01 — Order latency through HTTP, against the same orders in-process

**Part 1 baseline. Measured 2026-10-12.** Raw output: `results/m24p1-http.txt` (the eight
configurations) and `results/m24p1-control.txt` (the `/health` control). Scripts:
`scripts/m24p1-http.sh`, `scripts/m24p1-control.sh`. Tools: `stock --bench` and
`stock --http-bench` (see `part-1-baseline.md`).

## The question

Since milestone 19, every latency figure for this exchange has come from `--bench`, which sends
orders straight into the command queue and skips HTTP, JSON parsing and the JWT check. The design
measures latency from the moment an order enters the exchange, which for this project is the
customer HTTP port. So:

1. How much does the HTTP gateway add to an order's round trip, at the median and in the tail?
2. How much of that is the HTTP stack and the load generator themselves, before any exchange
   work?
3. Does the gateway's share change with load, and with replication?

## Method

- **Machine:** the office Ubuntu machine (`decisions.md`). During these runs the load average was
  2.4 to 4.3 (each run's value is in the raw file). The raw file's 30 to 32 "containers running"
  includes the measured exchange's own containers. Someone was also working on the machine
  interactively: a `ps` at 17:13 showed a terminal (Ghostty) and an editor (nvim) started at 16:53.
  That `ps` is not in the raw files.
- **Binary:** release build `stock-m24p1`. The trading path is the milestone 23 code. The control
  runs used a later build of the same tree, which only added `--target health` to
  the generator; the exchange code was identical.
- **Configurations:** {alone, replicated} × {1,000, 5,000 orders/s}, each run twice. Every run sends
  20 seconds of orders: 20,000 at 1,000/s and 100,000 at 5,000/s.
- **In-process:** `--bench --orders N --rate R`, in its own container, exactly as milestone 23
  part 6 ran it. With replication, a `--replica` container on its own volume dials it.
- **HTTP:** a new exchange process (new journal volume, new PostgreSQL database), then
  `--http-bench --orders N --rate R` in a container sharing the exchange's network namespace. With
  replication, a `--replica` container joins before the run starts, and the run waits until the
  primary reports `synchronous`.
- **Order of runs:** for each configuration, in-process run 1, HTTP run 1, in-process run 2, HTTP
  run 2. Interleaving means a drift in the machine's background load affects both kinds alike.
- **The same orders:** both tools draw the same users, symbols, sides, prices and quantities from the
  same seeded generator, with the same ids and the same setup deposits (`part-1-baseline.md`). The
  journals were not compared byte for byte. They differ anyway: HTTP setup sends its deposits 64 at
  a time, so the deposits land in a different order and with different sequence numbers. After
  setup, both exchanges hold the same balances, positions and an empty book. Since the measured
  orders are the same, the fills are the same.
- **Control:** `--http-bench --target health` against an idle exchange (alone), at 1,000 and 5,000
  requests/s, twice each. `GET /health` is answered by Axum alone: no JWT, no command queue, no
  journal.
- **Latency** is measured from each order's intended send time to its complete answer, in µs, with
  3 significant digits.

Every run kept up with its rate: throughput between 999 and 1,000 orders/s at 1,000/s, and between
4,995 and 4,999 at 5,000/s. Every HTTP order was answered `201`, with no errors. In-process, none
was rejected.

## Results

All values in microseconds (µs). "avg" is the mean of the two runs' percentiles. That is a simple
summary, not a percentile of the combined samples.

### Alone (no replica)

| Rate | Kind | Run | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| 1,000 | in-process | 1 | 2,995 | 7,623 | 11,607 | 16,183 | 20,479 |
| 1,000 | in-process | 2 | 2,907 | 6,771 | 10,679 | 16,399 | 23,375 |
| 1,000 | HTTP | 1 | 4,083 | 8,575 | 13,191 | 21,439 | 27,663 |
| 1,000 | HTTP | 2 | 3,491 | 7,975 | 12,207 | 15,311 | 17,711 |
| 1,000 | /health control | 1 | 987 | 1,562 | 2,999 | 5,851 | 11,623 |
| 1,000 | /health control | 2 | 1,096 | 1,641 | 2,159 | 7,315 | 22,559 |
| 5,000 | in-process | 1 | 3,581 | 10,087 | 19,503 | 46,655 | 55,967 |
| 5,000 | in-process | 2 | 3,841 | 11,159 | 18,911 | 43,679 | 53,247 |
| 5,000 | HTTP | 1 | 3,905 | 11,815 | 27,471 | 59,359 | 70,783 |
| 5,000 | HTTP | 2 | 4,143 | 14,687 | 29,711 | 64,607 | 84,223 |
| 5,000 | /health control | 1 | 1,637 | 2,705 | 6,375 | 21,983 | 37,919 |
| 5,000 | /health control | 2 | 1,565 | 2,591 | 5,967 | 52,607 | 76,671 |

### Replicated (a synchronous replica on the same machine)

| Rate | Kind | Run | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| 1,000 | in-process | 1 | 8,383 | 13,143 | 18,655 | 27,471 | 38,431 |
| 1,000 | in-process | 2 | 5,831 | 13,599 | 24,847 | 55,359 | 72,767 |
| 1,000 | HTTP | 1 | 9,055 | 16,215 | 28,495 | 44,575 | 54,047 |
| 1,000 | HTTP | 2 | 6,863 | 13,415 | 19,743 | 27,151 | 32,335 |
| 5,000 | in-process | 1 | 7,771 | 18,767 | 40,095 | 58,655 | 69,631 |
| 5,000 | in-process | 2 | 9,639 | 21,391 | 35,711 | 46,879 | 55,359 |
| 5,000 | HTTP | 1 | 10,151 | 21,679 | 66,879 | 109,247 | 126,143 |
| 5,000 | HTTP | 2 | 12,679 | 28,239 | 62,879 | 110,079 | 126,015 |

### The gateway's share: HTTP minus in-process (averages of the two runs)

How each cell is computed, for the first row's p50: in-process (2,995 + 2,907) / 2 = 2,951;
HTTP (4,083 + 3,491) / 2 = 3,787; difference 3,787 − 2,951 = **+836 µs**.

| Configuration | p50 in-proc → HTTP | Δ p50 | p99 in-proc → HTTP | Δ p99 | Δ p99 as % |
|---|---|---|---|---|---|
| alone, 1,000/s | 2,951 → 3,787 | +836 | 11,143 → 12,699 | +1,556 | +14% |
| alone, 5,000/s | 3,711 → 4,024 | +313 | 19,207 → 28,591 | +9,384 | +49% |
| replicated, 1,000/s | 7,107 → 7,959 | +852 | 21,751 → 24,119 | +2,368 | +11% |
| replicated, 5,000/s | 8,705 → 11,415 | +2,710 | 37,903 → 64,879 | +26,976 | +71% |

And at p99.9: alone 1,000/s 16,291 → 18,375 (+2,084); alone 5,000/s 45,167 → 61,983 (+16,816);
replicated 1,000/s 41,415 → 35,863 (−5,552, see "noise" below); replicated 5,000/s 52,767 →
109,663 (+56,896, more than double).

### The generator's own cost

| Run kind | Rate | send lag p50 | send lag p99 | client CPU per request | share of one hardware thread |
|---|---|---|---|---|---|
| HTTP orders, alone | 1,000 | 1,334 / 662 | 2,055 / 1,384 | 300.5 / 283.0 µs | 30% / 28% |
| HTTP orders, alone | 5,000 | 948 / 1,101 | 2,483 / 3,005 | 112.3 / 114.2 µs | 56% / 57% |
| HTTP orders, replicated | 1,000 | 552 / 882 | 1,613 / 1,920 | 333.5 / 250.0 µs | 33% / 25% |
| HTTP orders, replicated | 5,000 | 998 / 1,115 | 4,041 / 4,823 | 91.3 / 109.5 µs | 46% / 55% |
| /health control | 1,000 | 688 / 806 | 1,455 / 1,457 | 331.5 / 325.0 µs | 33% / 32% |
| /health control | 5,000 | 1,010 / 1,009 | 2,423 / 2,289 | 183.0 / 165.3 µs | 91% / 83% |

(Two values per cell: run 1 / run 2.)

## What the numbers say

**1. At the median, the gateway adds under a millisecond when the machine is lightly loaded.**
Alone, it adds +0.84 ms at 1,000/s and +0.31 ms at 5,000/s. Replicated at 1,000/s, +0.85 ms. These
are of the same size as the control itself (next point), so most of it is the cost of *any* HTTP
request from this generator on this machine, not something specific to orders.

**2. The HTTP floor on this machine is about 1 ms, and most of it is the generator being late.**
The `/health` control does no exchange work, yet its p50 is 1,042 µs at 1,000/s
((987 + 1,096) / 2) and 1,601 µs at 5,000/s ((1,637 + 1,565) / 2). Its p50 send lag, how late the
generator itself sent each request, is 747 µs and 1,010 µs. Medians do not subtract exactly. Still,
roughly 0.3 ms at 1,000/s and 0.6 ms at 5,000/s is the HTTP round trip itself (connection,
request, Axum routing, response), and the rest is the generator.

Why is the generator late? It sleeps on Tokio's timer, which has millisecond granularity, so an
average lateness of about 0.5 ms is expected (theory §10.2; Harness, "The generator's own error").
The extra comes from the generator sharing 4 busy hardware threads with everything else. The
in-process bench has the same timer, and so the same built-in lateness, but it does not report it.
Both kinds of run therefore carry roughly the same generator error. It is not a bias of HTTP
against in-process, but it does limit how small a difference these runs can show: about ±0.5 ms.

**3. In the tail, the gateway's share grows with load, and more with replication.** At 5,000/s,
alone, HTTP's p99 is 49% higher than in-process (+9.4 ms). Replicated, it is 71% higher
(+27.0 ms), and the p99.9 doubles (+56.9 ms). The control's p99 at 5,000/s is about 6.2 ms
((6,375 + 5,967) / 2), so the HTTP stack alone does not have a tail that long. The likely cause is
**CPU contention**. A replicated HTTP run at 5,000/s puts all of this on 4 hardware threads:
- the exchange worker thread;
- the exchange's 4 Tokio workers, parsing 5,000 requests per second and checking 5,000 JWTs;
- the generator: about half a hardware thread (table above);
- the replica process;
- about 30 other containers, with the load average around 4.

The in-process bench pays none of the exchange-side HTTP work. Any moment when the exchange worker
or a Tokio worker is waiting for a CPU adds directly to an order's latency. This is an
**inference**: the exchange container's own CPU use was not recorded in these runs. Part 6 should
sample it (`docker stats`) during each run to confirm or refute it.

**4. The generator costs a fixed amount, plus a little per request.** Its CPU per request falls
as the rate rises: about 290 µs at 1,000/s against about 113 µs at 5,000/s (alone, orders). Taken
as a share of one hardware thread, 0.29 at 1,000/s and 0.565 at 5,000/s (averages). A straight
line through those two points:

```text
slope     = (0.565 − 0.29) / (5,000 − 1,000) = 0.275 / 4,000 = 0.0000688 thread per (request/s)
          = 68.8 µs of CPU per request
intercept = 0.29 − 1,000 × 0.0000688 = 0.221 thread, whatever the rate
```

So about a fifth of a hardware thread is spent even before the per-request cost. That is a
two-point fit, an **estimate**. A likely explanation is the Tokio runtime parking and waking its
worker threads around every 1 ms timer tick when it has little else to do. The login-stall runs
(`03-login-stall.md`) support this: with login traffic keeping the generator's runtime busy, its CPU
per order at the same 1,000/s fell from about 333 µs to about 90–100 µs.

**5. Run-to-run noise is large, especially in the tails.** The two in-process runs replicated at
1,000/s differ by 2.6 ms at p50 (8,383 against 5,831) and by a factor of two at p99.9 (27,471
against 55,359). In one configuration HTTP's averaged p99.9 is even *lower* than in-process
(replicated 1,000/s: 35,863 against 41,415), which cannot be a real effect of adding work. It is
noise. Treat differences smaller than the gap between two runs of the same kind as noise. With two
runs per cell, only the larger effects above (p50 at light load; p99 and p99.9 at 5,000/s) are
clear.

**6. Today's in-process numbers are half of milestone 23's on the same machine.**
Alone at 5,000/s, the in-process p50 was 3.58 and 3.84 ms. Milestone 23 part 6 measured 7.87 and
7.20 ms on 2026-10-09, with an unchanged trading path. `00-selection.md` ("Correction after
Part 1") and `04-raw-sync.md` discuss this. The lesson used throughout this milestone: compare only
measurements taken on the same day, interleaved.

## What this does not show

- **The exchange's own HTTP CPU.** Not recorded; it is needed to confirm point 3.
- **Throughput at the maximum rate over HTTP.** It was not run. At `--rate 0` the generator would
  start 100,000 requests at once, opening thousands of connections, and would measure connection
  setup and port exhaustion rather than the exchange. A fair maximum-rate HTTP test needs a bounded
  number of connections, and it is not needed for this milestone's questions.
- **A client on another machine.** Every request crossed loopback inside one host. Real customers
  add network latency, and their requests would not compete for this machine's CPUs.
- **Statistical confidence.** Two runs per configuration on a shared, busy machine. The large
  effects are clear. Effects under about half a millisecond at the median, or under the run-to-run
  spread in the tail, are not established.

## How Part 6 will use this

Part 6 runs the same script, with the same seed, rates, durations and interleaving, on the final
binary, *on the same day as* a fresh run of this Part 1 binary. Points 1–3 above are the
comparison: Part 5's rate limiter must not move the p50 beyond the noise, and the tail effects
must be re-measured alongside the exchange's own CPU.
