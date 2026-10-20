# Part 3 — Measurement

**Status: complete on 2026-10-20.** The tool is built, reviewed and fixed, and the full
measurement ran (156 runs, 12:11–14:32). Results: `performance/01-feeds-measured.md`. The
criterion "the mean must differ by less than 10%, or the difference must be explained" is not met
by the numbers; the difference is explained there as contention on a two-core machine, about 44%
of the maximum-rate loss shown by pinning, the rest a hypothesis. The owner accepted that
explanation and closed the part (`decisions.md`, 2026-10-20).
How the work went, dead ends included, is in `notebook.md` (entries 13 onward).

## What this part is

Parts 1 and 2 built two feeds. This part measures them, as the plan's Part 3 asks:
- push latency at 1,000 and 5,000 orders/s, alone and replicated;
- how push latency grows with the number of clients (1, 100, 1,000 of each feed);
- each feed process's CPU, with milestone 25's cgroup sampler;
- whether the feeds keep up at the exchange's maximum rate;
- the exchange's throughput and latency with the feeds running against without. The completion
  criterion: the mean must differ by less than 10%, or the difference must be explained;
- slow consumers under load: a client that stops reading is disconnected, and the others are not
  delayed.

Every configuration runs six times, as the completion criteria ask ("on the same day, interleaved,
six runs each with the order balanced"). The configurations each comparison is about are
interleaved with the order balanced: the client counts in section A, the feeds against none in C
and E, stalled clients against none in D, alone against replicated in B. Section A's other
dimensions (alone or replicated, 1,000 or 5,000 orders/s) run in separate blocks, so comparisons
across them are of runs further apart in time. The theory is `01-theory.md` §8.

## The tool: `--feed-bench`

A new mode of the same binary, `src/exchange/feed_bench.rs`. It opens many WebSocket clients and
reports what they received:

```text
stock --feed-bench --start-at UNIX_MS --rate ORDERS_PER_SEC|0 --orders N --journal PATH --stream PATH
                   [--book URL] [--user URL] [--book-clients N] [--user-clients N] [--stalled N]
                   [--symbols N] [--users N] [--idle SECONDS] [--deadline SECONDS]
```

- **Book client `k`** subscribes to symbol `S{k mod 100}`, the generators' symbols, so 100 clients
  cover every book once, and 1,000 clients cover each book 10 times.
- **User client `k`** signs in as `u{k mod 100}`, the generators' traders, with a token it signs
  itself with `JWT_SECRET`, as `--http-bench` signs its traders' tokens. 1,000 clients are 10
  connections per user.
- **A stalled client** connects at the start, subscribes to every symbol and never reads. The
  server must disconnect it (decision 3). After the run it reads what was buffered, to learn
  whether the server ended the connection. It connects at the start, not before, so that the run
  (20 s, plus the 15 s quiet tail) stays shorter than the book feed's silence limit (more than three
  15 s ping intervals without a word from the client): any end it finds is then the slow-consumer
  rule, not the silence rule. The bench prints how long the longest stalled client was connected.
- **Every client's setup** (connect, then subscribe or sign in) has 30 s. A client that fails is
  counted on a `failed` line; it does not discard the run.

### Linking a message to its order

**Push latency** is the time from an order's *intended* send time to a client receiving a message
caused by it, as `--http-bench` measures its replies from the intended time (coordinated omission,
milestone 24). The two benches are different processes, so:
- **both are given the same `--start-at`** (Unix milliseconds) and `--rate`, and order `o{i}` is
  due at `start_at + i / rate` in both. `--http-bench --start-at` anchors its schedule at that
  wall-clock time itself, not at the moment its sleep ends: a timer wakes late (tokio rounds up to
  its 1 ms tick, more on a busy host), and anchoring at the wake-up would have shifted every order
  after the time the feed bench assumes, inflating every push latency by that delay (the first
  version did; `notebook.md` entry 18). Lateness now shows as `send lag`, charged to the order's
  latency like any other delay;
- **each client stamps every arrival with the wall clock** (`SystemTime`). One host has one wall
  clock, so the two compare, provided it is not stepped during a run. An arrival before its order
  was due is impossible on one clock; the bench counts such arrivals as `early`;
- **every pushed message carries its batch's last sequence**: a book delta's `seq`, a private
  `order`, `fill` or `cancel_rejected`'s `seq`. After the run the bench reads the journal once
  (`StreamReader`, as the feeds do) and maps each new-order batch's last sequence to its order. So
  every message is timed from the order whose batch produced it: a resting order's fill from the
  incoming order that filled it. Every batch in the measured window is a new order, because the
  generators send nothing else; the close that ends an in-process run produces deltas and expiries
  that map to no order, and they are counted as `not from an order`.

At the maximum rate (`--rate 0`) no order has an intended time, so latency is not measured; the
bench reports what arrived and when the last message of each feed arrived. Compared with the
generator's last reply, that says how far behind the exchange each feed finished (the `lag`). In
an in-process run the last messages are the market close's (`--bench` closes the market right
after its last reply), so the lag includes the close: the bench prints the close's own duration
(`close N ms`), and only a lag beyond it is the feeds' delay (`notebook.md` entry 24).

### What it prints

```text
feed-bench: 100 book clients (one symbol each, of 100), 100 user clients (one user each, of 100), 10 stalled (every symbol, from the start), rate 5000/s, 100000 orders from unix ms ...
  book       : 100000 messages, 100000 with a sequence (0 not from an order), covering 100000 of 100000 orders; latency us p50 15087  p90 28847  p99 95999  p99.9 135423  max 147839  mean 18573.0; first fifth p50 17407 p99 52991, last fifth p50 17583 p99 129663
  user       : 312865 messages, 312865 with a sequence (0 not from an order), covering 100000 of 100000 orders; latency us p50 15391 ...
  closed     : book 0 of 100, user 0 of 100
  stalled    : 10 of 10 ended by the server (0 x 10); connected at most 35.0 s
  last msg   : book unix us ..., user unix us ..., any ...
  client cpu : 35% of one hardware thread over 20.1 s (start to last message)
```

(From a pilot, 2026-10-20 11:59: `results/m26p3-pilot-stalled-and-http-max.txt`. The other pilots
are `m26p3-pilot-inproc-max.txt` and `m26p3-pilot-pinned.txt`; the stopped first measurement is
`m26p3-measure-aborted.txt`.)

- **covering K of N orders**: how many orders at least one message was matched to. A run whose
  feed stopped early, or whose clients missed part of it, shows fewer than all.
- **First and last fifth**: the latency of the first 20% and the last 20% of the orders. A feed
  that falls behind during a run shows a slower last fifth.
- **closed**: clients the server closed before the run ended, with their close codes.
- **stalled ... (0 x 10)**: the stalled clients found the end of the stream but no close frame:
  the server's close frame cannot be written into a connection whose client does not read, any
  more than the deltas could (`notebook.md` entry 17).
- **client cpu** runs from the start to the last check during which a message arrived, so the quiet
  tail does not dilute it.
- **The run ends** when no message has arrived for `--idle` seconds after the start (the scripts use
  15), or at `--deadline`.

### `--bench --start-at`

The in-process generator, `--bench`, gained the same `--start-at` and the same `start` and
`last reply` lines, anchored the same way. It is how the maximum rate is measured: `--http-bench
--rate 0` sends every order at once, and the HTTP generator, not the exchange, is then the limit
(`notebook.md` entry 19). Milestone 25 measured the maximum the same way.

### The scripts

`performance/scripts/`:
- `m26p3-lib.sh`, on top of milestone 24's `m24-lib.sh`:
  - the MDP (`$M`), the private feed (`$U`) and the feed bench (`$F`) run each in its own
    container, in the exchange's network namespace, with the exchange's journal volume mounted
    read-only;
  - every container gets `--ulimit nofile=65536:65536` (`notebook.md` entry 13);
  - `measured_run` (HTTP): a new exchange (and replica), optionally the feeds, the feed bench in the
    background, then `--http-bench` in the foreground, with a common `--start-at` 20 s ahead;
  - `measured_inproc` (maximum rate): `--bench` in `$X`, writing its journal to `/data/b`, the
    feeds following that journal, a common `--start-at` 25 s ahead. The container outlives the
    bench (`notebook.md` entry 20). Optional CPU pinning: `XPIN` for the exchange and replica,
    `FPIN` for the feeds and the feed bench;
- `m26p3-cpu.awk`: each container's CPU over the measured window, from the generator's start to its
  last reply, or to the feeds' last message if that came later: the exchange, the replica, the
  MDP, the private feed, the generators together (the HTTP generator and the feed bench; in an
  in-process run the generator is inside the exchange's process), the other containers, and the
  host;
- `m26p3-measure.sh`: sections A to E, below;
- `m26p3-summarize.awk`: one row per run, and each configuration's mean and range over its runs.

### The measurement plan (`m26p3-measure.sh ABCDE 6`)

| Section | What | Configurations | Runs |
|---|---|---|---|
| A | push latency against clients | {alone, replicated} × {1,000/s, 5,000/s for 20 s, HTTP} × {1, 100, 1,000 clients of each feed}, the three client counts rotating | 72 |
| B | do the feeds keep up at the maximum | {alone, replicated} × 200,000 orders in-process at the maximum rate, 100 clients of each, ABBA | 12 |
| C | the exchange with the feeds against without | {alone, replicated} × {HTTP at 5,000/s (mean, p99), in-process maximum (throughput)}, 100 clients of each, ABBA | 48 |
| D | slow consumers under load | alone, 5,000/s, 100 clients of each, with 0 and with 10 stalled clients, ABBA | 12 |
| E | is C's maximum-rate loss CPU contention | as C's in-process alone, with the exchange pinned to physical core 0 and the feeds and their clients to core 1, ABBA | 12 |

156 runs, 2026-10-20 12:11 to 14:32 on the office machine: `performance/results/m26p3-measure.txt`
(copied from `~/m26-out/`), summarized in `performance/results/m26p3-measure-summary.txt`.

## What changed in the code

- **New: `src/exchange/feed_bench.rs`**, above, with four unit tests: order ids and message
  parsing, the stream required, latency from each order's intended time with the fifths, and every
  message timed from the order of its batch.
- **`src/exchange/http_bench.rs`**: `--start-at UNIX_MS`, anchoring the schedule at that time; two
  new output lines, `start` and `last reply`, in Unix microseconds; `cpu_micros`, `percentiles` and
  the new `unix_micros` are shared with the feed bench. Without `--start-at` it behaves as before.
- **`src/exchange/bench.rs`**: the same `--start-at`, lines and anchoring; refused with `--days`
  above 1, since each day reuses the order ids.
- **`src/main.rs`, `src/exchange/mod.rs`**: the `--feed-bench` mode.
- **`Cargo.toml`**: `tokio-tungstenite` 0.29 and `futures-util` 0.3 move from the dev-dependencies
  to the dependencies, for the bench's clients. Both were already in the tree (axum's `ws` feature
  and the tests), and `Cargo.lock` is unchanged: the build passes with `--locked`.

## Problems found while building it

Each is told in full in `notebook.md`:
1. **The open-file limit** (entry 13). The image's default soft limit is 1,024. With 1,000 clients
   of each feed the feed bench failed; with only the feeds raised, the HTTP generator held more
   than 1,024 connections on a saturated host, the exchange's accepts failed, and a run had 1,786
   connection errors, 6 orders unanswered after 120 s and an 11 s maximum latency. Every container
   now gets 65,536.
2. **The bench stopped early** (entry 14): `now − last_message` in `u64` wrapped when a client
   stamped an arrival just after `now` was read. Fixed with `saturating_sub`.
3. **The review stopped the first full measurement after 8 runs** (entry 18): the start-time bias,
   private messages timed only by their first appearance, setup steps that could hang the script,
   one failed client discarding a run, the CPU window, stalled clients that could meet the silence
   rule, the plan's six interleaved runs for every section, a summary without the lag, and
   `--stream` listed as required but never checked. All fixed, as described above.
4. **The maximum over HTTP** (entry 19), and **a feed losing its network** with the bench's
   container (entry 20).

## Tests

Run on the office machine on 2026-10-20, from the tree in `~/stock-m26`, after the full
measurement, on the final code (including the `--bench` guard added during it):

| Test | Result |
|---|---|
| `feed_bench::tests::*` (4): message parsing, `--stream` required, latency from each order's intended time with the fifths, every message timed from its batch's order | pass |
| The whole suite, normal build: `check.sh` (`cargo fmt -- --check`, `cargo test --locked`) | fmt exit 0; 252 passed, 1 ignored (248 + 4); integration tests 6 passed |
| The whole suite, feature build: `cargo test --locked --features stage-timing` | 255 passed, 1 ignored |
| Release build warnings: `cargo build --locked --release` | 12, as before |
| `stock --bench /tmp/x --days 2 --start-at 1` | refused, with the usage line |
| The tools themselves, by pilots on the office machine before the measurement | `results/m26p3-pilot-*.txt`, `notebook.md` entries 13–22 |
