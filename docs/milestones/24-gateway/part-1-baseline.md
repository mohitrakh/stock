# Part 1 — The baseline, before any change

**Status: complete on 2026-10-12.**

## What this part is

Every later part of milestone 24 changes the gateway: Part 2 moves bcrypt off the worker threads,
Part 5 adds rate limits to every request. To know what each change costs and what it fixes, the
behaviour of the gateway *before* any of them must be recorded first. A baseline cannot be
reconstructed afterwards. So Part 1 changes nothing in how the exchange behaves. It builds
measurement tools and runs them against the milestone 23 exchange.

It answers five questions, one measurement file each:

| Question | File |
|---|---|
| How much latency does the HTTP gateway add to an order? | `performance/01-http-vs-in-process.md` |
| How long does one bcrypt take on this machine, alone and in parallel? | `performance/02-bcrypt-cost.md` |
| How much do simultaneous logins delay orders? | `performance/03-login-stall.md` |
| How long does the office SSD take to make one record durable? | `performance/04-raw-sync.md` |
| What does a login answer reveal about whether an account exists? | `performance/05-login-enumeration.md` |

## What changed in the code

None of these changes alters what the exchange does with a request. Two add new modes to the
`stock` binary, which run instead of the exchange. One shares constants with them. One prints a
line at startup.

### New: `src/exchange/http_bench.rs` — `stock --http-bench`

An open-loop HTTP load generator. It is the customer side of the measurement: it sends orders to a
running exchange through its customer port, exactly as a client would.

**Flags.** `--orders N` (default 100,000), `--rate R` orders per second (default 1,000; 0 means as
fast as possible), `--users`, `--symbols` (both default 100), `--logins C` (default 0),
`--target orders|health` (default `orders`), `--deadline S` (default 0, no deadline), `--base` and
`--operator` (the two ports' URLs),
`--day` (the trading day to open).

**How it reaches the exchange.** The customer port (`127.0.0.1:4000`) and the operator port
(`127.0.0.1:4004`) bind loopback only. The generator therefore runs in a container that shares
the exchange container's network namespace (`docker run --network container:m24-x`), so for both
of them `127.0.0.1` is the same interface.

**Setup (untimed).** It mirrors `--bench`'s setup, so that after setup both exchanges hold the same
balances and positions and an empty book. The journals are not byte-identical: HTTP setup sends 64
deposits at a time, so they land in a different order with different sequence numbers.
1. `POST /session/open` on the operator port, for `--day` (`2026-01-02` by default, the date
   `--bench` uses). A refusal, which happens if the market is already open, stops the run: every run
   needs a new journal.
2. For each of the 100 users: one `POST /exchange/deposit` of `CASH_PER_USER`. For each user and
   each of the 100 symbols: one `POST /exchange/shares/deposit` of `SHARES_PER_SYMBOL`. That is
   100 + 10,000 = 10,100 setup requests, run with at most 64 in flight (`SETUP_CONCURRENCY`).
3. With `--logins C`: C accounts registered with `POST /users/register`, each with an email unique
   to this run.

These deposits go through the customer routes, which is exactly what Part 4 removes. After Part 4
the setup will use the operator routes instead. That change, and the check that it journals the
same history, belong to Part 4.

**Traders' tokens.** Traders are named `u0` to `u99`, as in `--bench`. The generator signs each
trader's JWT itself, with HS256 and `JWT_SECRET` (which the script passes to both containers), and a
24-hour expiry. Milestone 23's Python client did the same. It works because the exchange never
checks that a token's `sub` is a row in the users table: a valid signature is all it checks (theory
§2.2). The traders therefore need no database rows, and setup needs no logins. The token string,
including `Bearer `, is built once per user and shared with an `Arc<str>`, so no per-order work goes
into it.

**The orders.** They are the same as `--bench`'s, draw for draw:
- the same generator: xorshift64 seeded with `SEED`;
- the same draws in the same order: user, symbol, side, price (`BAND_LOW` + one of `BAND_TICKS`
  ticks), quantity 1 to 10;
- the same id, sent as `client_order_id` `o{i}`.

Both benchmarks therefore present the exchange with the same orders, at the same intended times, and
produce the same fills. To make this hold by construction, the constants `--bench` used inline
became shared items in `bench.rs`: `SEED`, `CASH_PER_USER`, `SHARES_PER_SYMBOL`, and the `Rng` type
(now `pub(crate)`).

**The open loop.** Order i's *intended* send time is `start + i × 1,000,000,000 / R` nanoseconds.
The generator sleeps until then (`tokio::time::sleep_until`), then spawns a task that sends the
order and waits for the whole answer, body included. Latency is measured from the intended time to
the moment the answer is complete. So:
- a stall in the exchange is charged to every order scheduled during it;
- time the generator itself loses before sending is charged too. It counts as latency, which is
  the safe direction for a measurement.

**Send lag.** For each order the generator also records `now − intended` at the moment it sends
it. That is how late the generator itself was. The bench has the same 1 ms timer granularity, so a
lag of about 0.5 ms on average is expected. A much larger lag means the generator, not the
exchange, is the bottleneck. The bench never reported this, so it is new information.

**Client CPU.** Before the first measured order and after the last answer, the generator reads its
own CPU time (user plus system) from `/proc/self/stat`. Linux reports those two fields in clock
ticks of `USER_HZ`, fixed at 100 per second for that file, so each tick is 10 ms. Over a 20-second
run that resolution is about 0.05%. It prints the CPU per order and as a share of one hardware
thread. The generator runs on the same 4 hardware threads as the exchange (theory §10.6), so this is
how much of the machine it took from the thing it measures.

**Statuses and errors.** Every answer's HTTP status is counted. A transport error (connection
refused, reset) is counted apart, with the first one printed. A healthy run shows `201 x N` and
`errors: 0`.

**The login load (`--logins C`).** C tasks, each logging in to its own account in a loop: send,
wait for the answer, send again, until the last order is answered. This is a *closed* loop on
purpose. It models C clients each hammering the login as fast as the server answers, which keeps
exactly C bcrypt computations in progress. It is the background load; only the orders are the
measured traffic. Login latency and count are reported.

**The deadline (`--deadline S`).** Added after the first login-stall attempt never ended: a few
orders were never answered, so its login loops never stopped, and it was stopped by hand after
17 minutes (`performance/03-login-stall.md`). Orders still unanswered S seconds after the
first one was due are abandoned (their tasks aborted) and counted as `unanswered`. Throughput is
then computed over S seconds. The login loops save each result into a shared, mutex-guarded list
as it arrives, so at the end they can be abandoned mid-request without losing what they had done.
Without the flag (0, the default) the generator waits for every answer, as before.

**The control (`--target health`).** The same schedule, the same per-order generator work, but
each request is `GET /health`. That handler is answered by the gateway alone: no JWT check, no
command queue, no journal, no exchange worker. Its latency is therefore the floor of
*generator + HTTP + Axum* on this machine. Subtracting it from the order latency separates the
generator and HTTP stack from the exchange. Setup is skipped, since `/health` needs none.

**The HTTP client.** hyper's own client, `hyper_util::client::legacy::Client` with an
`HttpConnector`, HTTP/1.1 and keep-alive, and an unlimited pool of idle connections. When every
pooled connection is busy it opens a new one, so the open loop is never limited by a fixed number of
connections. Theory §10.5 explains why that matters: by Little's law, 5,000 orders/s at 10 ms means
about 50 requests in flight. Setup leaves up to 64 connections open and idle, so the measured phase
starts warm.

Options considered for the client:
1. **hyper's client (chosen).** hyper, hyper-util and http-body-util were already in `Cargo.lock`
   through axum. Turning on their client features added two small crates, both from hyper's
   author: `want` 0.3.2 (a signal between the connection pool and a waiting request) and
   `try-lock` 0.2.5 (`want`'s only dependency). Earlier notes in this milestone said "no new
   crates"; the build showed these two, and the `Cargo.toml` comment says so.
2. **`reqwest`.** The usual high-level client. It would add a large tree (TLS, cookies, redirects,
   proxies) that a loopback benchmark does not need.
3. **A hand-written HTTP/1.1 client over `TcpStream`.** A few hundred lines to do correctly
   (keep-alive, `Content-Length`, chunked bodies, errors). Bugs there would show up as measurement
   errors, and are hard to tell apart from the exchange's own behaviour.
4. **Extending the milestone 23 Python script.** Rejected by the owner (decision 6): it lives
   outside the repository, and with 16 threads it could not keep an open loop above about 1,600
   orders/s at 10 ms (theory §10.5).

### New: `src/exchange/probes.rs` — `stock --sync-probe` and `stock --bcrypt-probe`

- `--sync-probe DIR [--count N] [--size BYTES]`: appends `size`-byte records to a new file in `DIR`
  (opened `append` + `create_new`, so it never overwrites anything and always starts empty), each
  followed by `sync_all`, the same pair the journal uses. It times the write and the sync apart.
- `--bcrypt-probe [--count N] [--cost C] [--threads T]`: T OS threads start together behind a
  `Barrier`; each times N `bcrypt::hash` and N `bcrypt::verify` calls at cost C. It also prints
  `available_parallelism`, the CPU count the process sees.

Both put their samples in HdrHistograms with 3 significant digits and print the mean, p50, p90,
p99, p99.9 and max.

### Changed: `src/main.rs`

- Three new modes dispatched before the exchange starts: `--http-bench`, `--sync-probe` and
  `--bcrypt-probe`. The usage line lists them.
- At startup the exchange prints `Tokio worker threads: N`, from
  `tokio::runtime::Handle::current().metrics().num_workers()`, just before it binds the customer
  port. `00-selection.md` A.4 assumed 4 on the office machine. This line turns that assumption into
  a fact recorded in every run's output, and it still holds inside a container with CPU limits,
  where the count could differ. All Part 1 runs printed 4.

### Changed: `src/exchange/bench.rs`

Only the shared items described above: `SEED`, `CASH_PER_USER`, `SHARES_PER_SYMBOL` and `Rng` became
`pub(crate)` named items instead of inline literals. Its behaviour and output are unchanged.

### Changed: `Cargo.toml`, `Cargo.lock`

`hyper` (features `client`, `http1`), `hyper-util` (features `client-legacy`, `http1`, `tokio`) and
`http-body-util` became direct dependencies. `Cargo.lock` gained `want`, `try-lock`, and dependency
edges between crates already present (24 lines).

## Tests added

In `http_bench.rs`:
- `cpu_time_is_utime_plus_stime_even_with_spaces_in_the_name`. The command-name field of
  `/proc/self/stat` is in parentheses and may contain spaces and parentheses itself, so the parser
  counts fields from the *last* `)`. The test feeds a name with both, and checks that fields 14 and
  15 (250 and 50 ticks) give 3,000,000 µs, and that an unreadable file gives 0.
- `the_workload_flags_parse_and_defaults_hold`. Flags parse, defaults hold, a trailing `/` is
  trimmed from the base URL, `--target` accepts only `orders` and `health`, and a missing value or
  an unknown flag is refused.

The probes are measurement tools whose output is read by a person. They have no logic beyond timing
a call, so they have no unit tests; their first runs are checked in the performance files (for
example, the sync rate is checked against the inverse of the mean time).

Verified on the office machine on 2026-10-12, inside `stock-rust` (cargo 1.98.1):
- `cargo fmt -- --check` is clean;
- `cargo test --locked`: 211 unit tests passed (the 209 of milestone 23 plus the two above) and every
  executable integration test passed, with the three PostgreSQL acceptance tests ignored by default;
- the release build has 12 warnings, none in the new or changed files (milestone 23 recorded 13).

## How the measurements were run

All on the office machine, from the scripts in `performance/scripts/`, copied to `~/stock-scripts/`:
- `m24-lib.sh`: shared helpers. Starts a fresh exchange (new volume, new PostgreSQL database with
  every migration applied, new `JWT_SECRET`, optionally a synchronous replica), runs the generator
  in its network namespace, and runs `--bench` the way milestone 23 part 6 did. It prints the load
  average and the number of running containers before every measured run.
- `m24p1-probes.sh`, `m24p1-http.sh`, `m24p1-stall.sh`, `m24p1-enum.sh`: one per measurement file.

The binary is `~/stock-scripts/stock-m24p1`, a release build of the Part 1 tree. The raw output of
every script is in `performance/results/`, unedited.

The scripts live in the repository this time. Milestone 23's scripts lived only on the office
machine (`~/stock-scripts/`), so its measurements could not be reproduced from the repository.

### How the tool grew during the part

The generator was changed twice while measuring, and each change is part of the record:
1. `--target health` was added after the first HTTP runs showed the generator using a large share
   of a hardware thread. A control was needed to separate the generator and HTTP stack from the
   exchange.
2. `--deadline` was added after the first login-stall attempt never finished.

Neither changed the exchange's code. The probes, the enumeration run and the HTTP runs used the
first build. The control used the second, and the final stall run the third (all named
`stock-m24p1`, each replacing the last).

## Results

Each performance file has the details, the arithmetic and the caveats.

- **The disk** (`04`). One durable 600-byte append on the office SSD takes 0.80–0.90 ms at p50,
  1.31–1.39 ms on average and 5.3–5.5 ms at p99. Of that, `sync_all` alone is 0.78–0.88 ms at p50
  and 1.28–1.36 ms on average. On tmpfs the same code takes 1.6 µs on average. On
  the same afternoon, the flush was about 80–90% of the exchange's group cycle at 5,000 orders/s.
- **bcrypt** (`02`). One hash or verify at cost 12 takes 228.6–228.7 ms (p50) on one thread. Each
  cost step doubles it (57.2, 115.1, 228.7 ms for costs 10, 11, 12). The whole machine maxes out at
  about 15 calls per second, with 4 or more running at once.
- **HTTP's share** (`01`). Through HTTP, an order's p50 rises by 0.3–0.9 ms at light and moderate
  load, about the cost of any HTTP request from this generator (the `/health` control's p50 is
  1.0–1.6 ms, mostly the generator's own lateness). The tail grows with load and with replication:
  at 5,000 orders/s HTTP's p99 is 49% higher alone (+9.4 ms) and 71% higher replicated (+27 ms),
  likely from CPU contention (an inference).
- **The login stall** (`03`). One login loop at a time pushes the order p99 from about 14 ms to
  0.1–0.2 s. Four loops push the order p50 to 0.15–0.19 s. With sixteen, connections already open
  are still served slowly, but new connections, to the customer or the operator port, get no answer
  for tens of seconds. In one run with eight, about 45–50 orders were never answered; the login
  loops therefore never stopped, and new connections went unserved until the run was stopped by
  hand 17 minutes later.
- **Enumeration** (`05`). A login for a known email answers "invalid password" in 231 ms on average.
  For an unknown email it answers "user not found" in 0.65 ms. Both are 400. One request tells them
  apart, by body or by time.
- **A correction to the selection reasoning.** The same in-process benchmark ran twice as fast as on
  2026-10-09, with no change to the trading path. `00-selection.md` now records that most of the
  "unexplained 3–4 ms" did not recur, and that only same-day, interleaved comparisons are
  trustworthy on this shared machine.

## Independent review

A separate review read the code and every document against the raw files, and recomputed every
derived number. It found no bug in `http_bench.rs` or `probes.rs`. It confirmed:
- the draw order, ids, seed and setup amounts match `--bench`;
- latency is measured from the intended time;
- every average, delta and percentage in the performance files, the CPU fit, and the bcrypt
  arithmetic.

It found these errors in the documents, all fixed:
1. **A misread timestamp (the important one).** `03-login-stall.md` said that in the first C = 8
   run "not one order reached the journal in 17 minutes". The journal's last change (17:19:35) was
   the *end* of the order phase, not its start, and its size shows all but about 45–50 orders were
   journaled. A few orders went unanswered, so the login loops never stopped. The file now explains
   this, with the arithmetic. (Re-checking the review's own estimate of bytes per order: from the two
   C = 16 runs it is 710,503 bytes / 993 orders ≈ 716 bytes, not the review's 740.)
2. **"C = 16 stops answering at all" was too strong.** Logins kept succeeding at 15.1 per second, and
   one run answered 19,795 orders. What failed was *new* connections. Corrected everywhere.
3. `01-theory.md` §7.3 said "a few hundred requests per second" saturate the machine. Its own
   estimate gives about 19 per second, and 15 was measured. Corrected.
4. The probe timings at C = 16 were about 30, 81 and 131 s, not 30, 60 and 90 s. Each timed-out probe
   adds about 20 s. Corrected.
5. `01-http-vs-in-process.md` said the control's build added `--deadline`. It added only
   `--target health`.
6. Throughput at 1,000/s ranged 999–1,000, not 995–1,000.
7. Status lines said the milestone was "not started", and the latency candidate still cited the
   "3–4 ms" that the correction retracts. Updated.
8. `02-bcrypt-cost.md` said each S-box lookup waits for the one before. In Blowfish the four lookups
   of a round are independent; the chain runs from round to round. Corrected.
9. The login accounts' usernames were not unique across runs (`bench{i}`), and `users.username` is
   unique, so a second run against the same database would fail at registration. They are now
   `bench-<run>-<i>`. The scripts always use a fresh database, so no result was affected.
10. Smaller mismatches:
    - "journal the same history": the journals differ in deposit order;
    - `sync_all` figures quoted as whole-append figures;
    - container counts that included the measured containers.

    All corrected.

It also raised three points as plausible rather than confirmed, and they were handled:
- "The machine's state, not the code" is an inference by elimination; it is now worded as one.
- The interactive-user observation came from a `ps` that is not in the raw files; that is now said.
- With a deadline and unanswered orders, the generator printed answered orders divided by the
  deadline as if it were a throughput. It now prints "not measured" in that case.

## Compatibility

No file format, event, route or behaviour changed. A Part 1 binary runs any milestone 23 journal.
