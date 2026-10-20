# 01 — The feeds, measured

**Part 3. Measured 2026-10-20, 12:11 to 14:32, on the office machine.** Raw output:
`results/m26p3-measure.txt`, 156 runs. Summary, one row per run and each configuration's mean and
range: `results/m26p3-measure-summary.txt`, made by `scripts/m26p3-summarize.awk`. Every figure
below comes from those two files; every derived figure shows its arithmetic. The tool and the
method are in `../part-3-measurement.md`; how the work went is in `../notebook.md`.

## Method in one paragraph

One binary, `stock-m26p3` (SHA-256 prefix `29b0e9d5d6b1b56d`, built from the Part 3 tree before
one later change: the `--bench` refusal of `--start-at` with `--days` above 1, added during the
measurement, which no run uses; `../notebook.md` entry 24), runs every process: the exchange, the replica, the MDP, the private feed,
the HTTP generator `--http-bench`, the in-process generator `--bench`, and the feed client
generator `--feed-bench`. Each run starts a new exchange with a new journal and a new database.
The HTTP runs send 1,000 or 5,000 orders/s for 20 s from 100 traders over 100 symbols; the
maximum-rate runs send 200,000 orders through `--bench`. "100 clients of each feed" means 100
book-feed clients (one per symbol) and 100 private-feed clients (one per trader). Six runs per
configuration, interleaved with what it is compared with, the order balanced. The machine is an
Intel Core i3-7100 (2 physical cores, 4 hardware threads, 3.9 GHz) with one SATA SSD, shared with
29 other containers in every run (the CPU lines count them; the hosts notes' 30–33 "containers
running" include the run's own), with a 1-minute load average before a run of 1.37 to 7.74. No run
had a client failure, a warning, a halt, or an unanswered order.

**Reading the tables.** "Mean [min, max]" is the mean over the 6 runs with the lowest and highest
run. Latencies are in milliseconds (the raw files are in microseconds). "Push" is the time from an
order's intended send time to a client receiving a message caused by it; "HTTP" is the same order's
reply to the generator, from the same intended time.

## A. Push latency against the number of clients

p50 over 6 runs, in ms. "Book" and "user" are the two feeds.

| Configuration | Clients of each feed | HTTP reply | Book push | Private push |
|---|---|---|---|---|
| alone, 1,000/s | 1 | 4.24 [3.14, 5.61] | 9.97 [9.01, 11.26] | 10.14 [9.54, 11.38] |
| | 100 | 4.36 [3.59, 4.96] | 10.18 [9.64, 10.46] | 10.59 [10.06, 10.98] |
| | 1,000 | 6.53 [5.92, 7.79] | 14.81 [13.92, 16.62] | 16.16 [15.34, 17.95] |
| alone, 5,000/s | 1 | 6.03 [5.23, 6.62] | 11.68 [10.77, 12.82] | 11.82 [10.69, 12.61] |
| | 100 | 6.87 [6.09, 7.44] | 13.51 [12.68, 14.24] | 13.93 [13.04, 14.56] |
| | 1,000 | 46.76 [34.85, 61.60] | 181.20 [144.51, 245.25] | **3,655.68** [3,293.18, 4,218.88] |
| replicated, 1,000/s | 1 | 9.00 [7.64, 10.27] | 14.32 [12.45, 16.23] | 14.49 [12.58, 15.71] |
| | 100 | 8.99 [7.66, 9.83] | 14.51 [13.25, 15.42] | 15.10 [13.76, 16.02] |
| | 1,000 | 10.52 [9.45, 11.42] | 20.07 [17.20, 27.60] | 22.26 [18.66, 32.80] |
| replicated, 5,000/s | 1 | 10.81 [10.06, 11.36] | 16.30 [15.43, 16.80] | 16.45 [15.56, 17.12] |
| | 100 | 11.23 [10.18, 12.58] | 17.64 [16.50, 19.39] | 18.42 [17.07, 20.21] |
| | 1,000 | 35.94 [33.79, 40.26] | 124.02 [111.87, 136.19] | **2,722.13** [2,541.57, 2,899.97] |

**p99**, same runs (ms): alone 1,000/s, 100 clients: book 29.87, private 32.72 (HTTP 17.24);
alone 5,000/s, 100 clients: book 64.36, private 73.23 (HTTP 52.39); replicated 5,000/s, 100 clients:
book 78.56, private 85.09 (HTTP 68.40). All are in the summary file.

### What it shows

1. **A pushed message arrives 5.3–7.4 ms after the HTTP reply (p50 against p50), whatever the
   load, up to 100 clients** (sections A, C and D; the examples below are A's, and C's runs with
   the feeds reach 7.37 ms: private 16.17 − 8.80, alone). At 1,000/s alone with one client: book 9.97 − 4.24 = 5.73 ms,
   private 10.14 − 4.24 = 5.90 ms; replicated: 14.32 − 9.00 = 5.32 ms and 14.49 − 9.00 = 5.49 ms;
   at 5,000/s alone with 100 clients: 13.51 − 6.87 = 6.64 ms and 13.93 − 6.87 = 7.06 ms;
   replicated with 100 clients: 17.64 − 11.23 = 6.41 ms and 18.42 − 11.23 = 7.19 ms. Both feeds read the committed
   stream, and the stream publishes a batch only after its sync (and, replicated, after the
   replica's confirmation), the same moment the HTTP reply leaves. So the gap is the feeds' own
   delay. **The likeliest cause, an inference not yet tested:** each feed's follower sleeps 10 ms
   when it finds nothing new (`IDLE_POLL`), so a batch waits on average about 5 ms for the next
   poll (half of 10 ms, if batches arrive at random times); the rest, about 0.3–2.4 ms (5.32 − 5 to 7.37 − 5), would be
   applying, serializing and writing, and grows with load. The two feeds' gaps differ by only
   0.1–0.9 ms, which fits a cost they share: the same poll loop.
2. **From 1 to 100 clients costs almost nothing:** at 1,000/s alone, book 9.97 → 10.18 ms
   (+2.1%), private 10.14 → 10.59 ms (+4.4%), each inside the other's range.
3. **At 1,000 clients the machine runs out.** At 5,000/s alone the host's 4 hardware threads are
   busy; the MDP uses 0.71–0.78 of a hardware thread, the private feed 0.92–1.13, the two
   generators 0.85–0.91, and the exchange keeps only 0.45–0.49 (raw rows). Everything slows: the
   HTTP reply p50 rises from 6.87 to 46.76 ms, the book from 13.51 to 181.20 ms. The private feed
   falls behind by seconds (p50 3.66 s) and disconnects 315–626 of its 1,000 clients in each run as
   slow consumers (1013); replicated, with the exchange slower, it falls 2.72 s behind and
   disconnects 48–163 per run. The book feed disconnects none. At 1,000/s, 1,000 clients cost the book
   +4.6 ms and the private feed +5.6 ms over 100 clients, and nobody is disconnected.
4. **Why the private feed suffers first, an inference:** with 1,000 clients each private batch is
   about 3 messages × 10 connections per trader, about 30 socket writes, against about 10 for the
   book (10 clients per symbol). Every message is its own WebSocket write, flushed alone, so the
   private feed makes about three times the system calls for the same orders. Its CPU (up to 1.30
   hardware threads replicated) is the highest of any process in these runs.
5. **Replicated is slower by the replica's round trip, for both the reply and the push:** at
   1,000/s with 100 clients, HTTP +4.63 ms (8.99 − 4.36), book +4.33 ms (14.51 − 10.18). The
   feeds add nothing extra for replication.

**The first and last fifths** (raw rows) show no feed falling behind during a run up to 100
clients, though the end of a 5,000/s run is a little slower than its start. At 5,000/s alone with
100 clients, the book's p50 over the first fifth of the orders was 10.2–14.0 ms across the runs,
over the last fifth 12.8–15.5 ms, higher by at most 4.2 ms in a run; replicated, the last fifth
was higher in all 6 runs, by 0.7–5.8 ms (book) and 1.2–7.0 ms (private). Never toward seconds.
At 1,000 clients and 5,000/s the private feed is up to seconds behind (its first fifth's p50 was
0.58–0.88 s in the 3 runs of 12 where it was lowest, 1.03–2.72 s in the other 9); its last fifth was
slower than its first in all 6 replicated runs and in 4 of 6 alone (in two it was faster:
2.29 → 1.62 s and 2.72 → 1.99 s, as disconnections thinned its clients).

**The lag column of one-client runs is negative** (book: −45 to −56 ms as configuration means,
−39 to −62 ms per run): a lone book client
watches one symbol of 100, so its last message can come before the last order, which was for
another symbol. The lag is only meaningful when every symbol is watched, from 100 clients on.

## B. Do the feeds keep up at the maximum rate?

In-process `--bench`, 200,000 orders as fast as the exchange accepts them, 100 clients of each
feed. 6 runs each.

| | Exchange, orders/s | Book: last message after the last reply | Private: last message after the last reply |
|---|---|---|---|
| alone | 25,521 [24,572, 26,893] | 1.07 s [0.66, 1.59] | 2.76 s [2.07, 3.45] |
| replicated | 23,510 [21,597, 25,714] | 1.17 s [0.83, 1.77] | 2.17 s [1.80, 2.62] |

- **Every message arrived** in every run: each feed's messages covered 200,000 of 200,000 orders
  (raw rows, `covering`), and every in-process run with feeds, the same 200,000 orders each time,
  delivered exactly the same totals: 200,100 book messages and 671,374 private messages in all 30
  runs of B, C and E. (The bench does not check their order; Parts 1 and 2's tests do.)
- **The lag includes the market close.** The bench closes the market right after its last reply.
  The close's own duration, over the 6 runs of B alone, was 420, 513, 517, 536, 557 and 578 ms,
  median (517 + 536) / 2 = 526.5 ms (over all 48 in-process runs of B, C and E, median 440.5 ms,
  up to 815 ms). So, alone, the book finished about 1.072 − 0.527 = 0.55 s and the private feed
  about 2.755 − 0.527 = 2.23 s behind the exchange. Both then caught up.
- **So the feeds keep up at the rate the exchange reaches with them running** (about 24,000
  orders/s here), with a backlog of a few seconds at the end of a 200,000-order burst. They do not
  keep the exchange at its rate without them: see C.

## C. The exchange with the feeds running against without

The completion criterion: the mean must differ by less than 10%, or the difference must be
explained. "With" is the MDP, the private feed and 100 clients of each, all on the same machine.

### HTTP at 5,000 orders/s (the plan's mean and p99)

| | Without the feeds | With the feeds | Change |
|---|---|---|---|
| alone, mean | 7.42 ms [6.92, 7.76] | 12.28 ms [8.48, 19.22] | **+65.5%**, ranges separate |
| alone, p50 | 6.64 ms [6.34, 6.96] | 8.80 ms [6.45, 14.10] | +32.5%, overlap |
| alone, p99 | 24.62 ms [21.81, 28.78] | 73.17 ms [53.82, 137.60] | **+197%**, separate |
| replicated, mean | 12.23 ms [10.66, 13.24] | 16.02 ms [12.94, 19.65] | +30.9%, overlap |
| replicated, p50 | 11.09 ms [10.49, 11.69] | 11.73 ms [11.14, 12.10] | +5.8%, overlap |
| replicated, p99 | 39.84 ms [25.42, 66.43] | 84.59 ms [62.40, 166.53] | +112%, overlap |

Arithmetic: 12.28 / 7.42 = 1.655; 8.80 / 6.64 = 1.325; 73.17 / 24.62 = 2.972; 16.02 / 12.23 =
1.310; 11.73 / 11.09 = 1.058; 84.59 / 39.84 = 2.123. Throughput held at the offered 4,995–4,998
orders/s in every configuration.

### In-process, the maximum rate

| | Without the feeds | With the feeds | Change |
|---|---|---|---|
| alone | 62,322 orders/s [59,611, 64,194] | 24,246 [22,340, 26,591] | **−61.1%**, separate |
| replicated | 49,430 [45,699, 53,486] | 23,638 [22,460, 24,358] | **−52.2%**, separate |

Arithmetic: 24,246 / 62,322 = 0.389; 23,638 / 49,430 = 0.478.

### The criterion is not met; the explanation is the machine

The mean differs by far more than 10%: +65.5% at 5,000/s alone, −61% at the maximum. The feeds do
not slow the exchange through the exchange's own code: the exchange, the journal and replication
are unchanged, and the feeds only read files the exchange already writes. They slow it by sharing
one two-core machine with it.

**The CPU columns** (raw rows of section C, in hardware threads, over each measured window):
- **HTTP at 5,000/s alone.** Without the feeds: the generator 0.35–0.58, the other containers
  0.28–0.39, the host busy on 2.27–2.87 of its 4 hardware threads. With them: the MDP 0.26–0.29,
  the private feed 0.40–0.43, the two generators 0.71–0.77, the host busy on 2.73–2.95.
- **The maximum rate alone.** Without the feeds: the exchange 1.21–1.29, the host busy on
  1.45–2.15. With them: the MDP 0.71–0.87, the private feed 1.04–1.10, the feed bench 0.65–0.71,
  the host busy on 3.86–3.97 of 4, so nearly saturated; the exchange 0.66–0.81 (over a window
  that runs to the feeds' last message, so diluted by the seconds after the exchange finished).

**An observation not explained:** at 5,000/s the exchange's own CPU is lower with the feeds
(0.60–0.65) than without (0.93–1.03 in five runs, 0.61 in one), for the same 100,000 orders. The
same work in less CPU time suggests the exchange spends CPU without the feeds that it cannot spend
with them, for example spinning or polling while idle; this is not investigated here.

**Section E tests that explanation directly.**

## E. The same, with the exchange and the feeds on separate cores

In-process, maximum rate, alone. The exchange pinned to physical core 0 (hardware threads 0 and 2),
the MDP, the private feed and the feed bench pinned to core 1 (threads 1 and 3); the "without" runs
pin the exchange the same way. 6 runs each, ABBA.

| | Without the feeds | With the feeds | Change |
|---|---|---|---|
| exchange, orders/s | 62,017 [58,163, 64,518] | 40,658 [35,359, 44,432] | −34.4%, separate |
| book: last message after the last reply | — | 5.87 s [5.11, 6.72] | |
| private: last message after the last reply | — | 8.84 s [7.98, 9.84] | |

Arithmetic: 40,658 / 62,017 = 0.656.

- **Pinning recovers about 44% of the loss**, from −61.1% (C) to −34.4%: in orders/s the loss is
  62,322 − 24,246 = 38,076 unpinned and 62,017 − 40,658 = 21,359 pinned, so 16,717 of 38,076
  (43.9%) is recovered. Pinned alone without the feeds, the exchange runs as fast as unpinned
  (62,017 against 62,322 orders/s): one physical core is enough for it. So a large part of C's
  loss is CPU taken from the exchange.
- **What the rest is, an inference.** With "its own" core the exchange still lost a third. That
  core is not wholly its own: the 29 other containers are not pinned (they used 0.24–0.36 of a
  hardware thread in the pinned runs with the feeds; 0.06–1.02 in those without) and can run on it. And the two cores still share the L3
  cache, memory bandwidth, and the stream file's lock: the writer takes an exclusive `flock` on the
  stream for every publish, and each feed's reader takes a shared one for every batch it reads, so
  at tens of thousands of batches a second the writer can wait on two readers. None of these is
  measured (`../notebook.md` entry 21).
- **The feeds, confined to one core, no longer keep up live:** they finish 5.9 s (book) and 8.8 s
  (private) after the exchange, against 1.10 s and 2.33 s unpinned (C alone, the same
  configuration). Every message still arrives (coverage 200,000 of 200,000).

## D. Slow consumers under load

Alone, 5,000/s, 100 clients of each feed, with 0 and with 10 stalled clients (each subscribed to all
100 symbols, never reading). 6 runs each, ABBA.

| p50, ms | 0 stalled | 10 stalled | Change |
|---|---|---|---|
| HTTP reply | 6.97 [6.12, 7.87] | 7.02 [6.19, 7.60] | +0.7%, overlap |
| book push | 13.49 [12.50, 14.56] | 13.77 [13.05, 14.42] | +2.0%, overlap |
| private push | 14.02 [12.97, 15.10] | 14.09 [13.22, 14.63] | +0.5%, overlap |

- **Every stalled client was disconnected by the server, in every run:** "10 of 10 ended by the
  server" in all 6 runs, each connected at most 35.0–35.1 s, shorter than the book feed's silence
  limit (about 45–60 s), so the disconnection is the slow-consumer rule (decision 3). They found no
  close frame (`0 x 10`): a client that does not read cannot receive one either
  (`../notebook.md` entry 17).
- **The other clients were not delayed**: every change is within the spread between runs. No
  ordinary client was closed.
- **What the stalled clients cost the MDP:** 0.36–0.38 of a hardware thread with them, 0.27–0.30
  without (raw rows), about 0.09 more by the means (0.372 − 0.285), for serializing into their
  queues until they are cut off.

## What this means

1. **The feeds work at the loads they were built for.** Up to 100 clients of each feed, every
   message arrives, 5.3–7.4 ms after the order's HTTP reply (p50), at 1,000 and 5,000 orders/s,
   alone and replicated; slow consumers are cut off without delaying anyone.
2. **The push latency floor is likely the followers' 10 ms idle poll** (an inference: about 5 ms
   of the 5.3–7.4). A shorter poll, or a wake-up from the stream, would remove most of it.
3. **On this machine the feeds cannot share a host with the exchange without slowing it.** The
   completion criterion's 10% is not met: +65.5% mean at 5,000/s alone, −61% at the maximum.
   About 44% of the maximum-rate loss is CPU (pinning recovers it); the rest is something the
   cores, or the unpinned other containers, still share, untested.
4. **1,000 clients of each feed overload this machine at 5,000/s,** the private feed first: its
   one-write-per-message sending is the first candidate for an optimization (writing a
   connection's queued messages together, then flushing once).

## Decisions for the owner

**Decided 2026-10-20: option 1.** The owner accepted the explanation, CPU contention on the shared
two-core machine, and closed the part (`../decisions.md`). Options 2 and 3 stay open for later.

The criterion is "the mean must differ by less than 10%, or the difference must be explained". The
difference is explained above as contention on a two-core machine: about 44% of the maximum-rate
loss shown by pinning, the rest (the pinned −34%) only by hypothesis. Options:
1. **Accept the explanation as it stands** and close the part.
2. **Test the remaining hypothesis first:** a follow-up with the feeds running but no clients, and
   one with the stream lock held for shorter, to separate reading, fan-out and the lock.
3. **Optimize, then measure again:** a shorter idle poll (or a stream wake-up) for the push
   latency, and batched writes for the private feed's fan-out; each measured before and after.
