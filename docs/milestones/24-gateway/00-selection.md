# Milestone 24 — Why this milestone, and why not latency

This file records how the milestone was chosen on 2026-10-12. The owner expected the next milestone
to be about latency. A review of the code at commit `27ce277` on `dev-rakh` recommended the gateway
and trust boundary instead, and the owner accepted. The argument has two halves, and each is
written out here with every number, where it came from, and the arithmetic behind anything
derived:

1. The customer API lets any customer change state in ways no exchange allows, or block every
   other customer. That makes the durability of milestones 8 to 23 protect a ledger that anyone
   can write fake money into.
2. Latency work done now would shave microseconds of CPU while the milliseconds sit in the disk,
   and the measurements it needs do not exist yet.

Sources are cited as file plus section. "results-m23" means
`docs/performance/results/results-m23.txt`, the raw output of milestone 23 part 6. "Task 14" means
`docs/tasks/14-critical-path-performance-v1.md`. "Harness" means
`docs/performance/00-benchmark-harness.md`.

---

## Part A — The trust boundary is open

A *trust boundary* is the line across which data comes from someone the system does not trust.
For this exchange the line is the customer HTTP port, `127.0.0.1:4000`: anything a customer sends
must be checked there or behind it, and some things must never be accepted from a customer at all.
`01-theory.md` explains the idea in depth. This section lists what the code allowed on 2026-10-12.

### A.1 Anyone can create money and shares

- `POST /users/register` (`user_controller::register`) creates an account for anyone who sends a
  username, an email and a password of at least 8 bytes. There is no approval step.
- With that account's token, `POST /exchange/deposit` (`exchange_controller::deposit`) sends
  `ExchangeCommand::Deposit { user_id: <the caller>, amount: <anything> }`. Nothing checks where
  the money comes from.
- `POST /exchange/shares/deposit` does the same for shares, in any symbol, any quantity up to the
  limits below.

Milestone 9 made every *trade* conserve cash and shares: a fill moves value between two parties
and creates none. That invariant is still true. But it says nothing about the edges where value
enters. Deposits are the edge, and the edge is open. The journal had already recorded both
deposits as "placeholders for real custody" (`PROJECT_DIRECTION.md`, Known Prototype Limitations).

What a real exchange does instead: customers never deposit into the matching engine at all. Cash
and securities sit with a clearing house or custodian. The exchange learns a member's buying power
from a trusted back-office feed, or a broker in front of the exchange enforces it. Either way the
instruction comes from an authority, not from the trader.

### A.2 Anyone can raise their own risk limit

`POST /exchange/risk/limits` takes `{ symbol, max_daily_quantity }` and sets the limit for the
*caller*. The design document states the requirement twice, once as "the exchange is a regulated
facility, so we need to make sure it runs risk checks". A check that the checked party can switch
off is not a check: any trader whose order is refused for the cap can raise the cap and send the
order again.

### A.3 One customer can block every other customer

Each of these is a denial of service: an action by one customer that stops the exchange serving
others.

**The deposit totals.** Milestone 23 part 1 made a deposit refuse (`Overflow`) when it would take
the exchange's total cash, or one symbol's total shares, past `u64::MAX` = 18,446,744,073,709,551,615.
That guard is what makes it impossible for any later fill to overflow a balance: if the total of
all balances fits in a `u64`, so does every single balance. But nothing withdraws money. So one
customer can deposit up to the total, and from then on every deposit by anyone is refused. The
milestone 23 journal recorded this as "not fixed" and named the fix: "operator-only deposits".

**The book.** The books hold at most `MAX_RESTING_ORDERS` = 200,000 resting orders across all
symbols (milestone 22 part 2), so that the close's one journal record always fits the 64 MiB record
limit. Nothing divides that space between users. One customer with enough collateral can rest
200,000 orders. After that every order that would rest is refused with `BookFull` — for everyone —
until orders trade, are cancelled by their owner, or expire at the close.

**The symbols.** There is no list of products. Orders, share deposits and risk limits accept any
symbol of 1 to 64 bytes without control characters. Every symbol ever used keeps an empty book and
its execution counter in memory and in every snapshot (milestone 22 kept them so that execution ids
never repeat). A script that names a new symbol per request grows the state without limit: the
memory, the snapshot, the restart time and the warm replica's snapshot cost all grow with it.

### A.4 Logins and registrations can stall order entry

This one needs the most explanation, because it is about how the async runtime works.

**What the code does.** In `src/controllers/user_controller.rs`:

```rust
pub async fn register(...) {
    ...
    let password_hash =
        hash(&payload.password, DEFAULT_COST).map_err(...)?;   // bcrypt, cost 12
    ...
}

pub async fn login(...) {
    ...
    let password_match = verify(&payload.password, &u.password_hash).map_err(...)?;
    ...
}
```

`bcrypt::DEFAULT_COST` is 12 in the `bcrypt` 0.19.1 crate this project locks (checked in the crate
source in the cargo registry on 2026-10-12).

**What cost 12 means.** bcrypt's cost is a base-2 logarithm: cost 12 runs the expensive part of the
algorithm (the "EksBlowfish" key setup, which re-derives Blowfish's 4 KiB of S-boxes from the
password and salt) 2^12 = 4,096 times. Each step up in cost doubles the work. It is slow on purpose:
the slowness is what makes guessing passwords from a stolen hash expensive. `01-theory.md` explains
the algorithm and the arithmetic.

How long cost 12 takes on the office i3-7100 was not known on 2026-10-12. Typical published
figures for desktop CPUs are a few hundred milliseconds per hash. That is an **estimate**. Part 1
measures it (`performance/02-bcrypt-cost.md`).

**Why it stalls order entry.** Both calls run *inside an `async fn`*, on whatever Tokio worker
thread is polling that request. An `async fn` only gives its thread back to the runtime at an
`.await`. bcrypt has no `.await` inside it: it is one long CPU computation. While it runs, that
worker thread can do nothing else.

`main` is annotated `#[tokio::main]`. Without arguments, that builds Tokio's multi-threaded runtime
with one worker thread per logical CPU it can see. On the office i3-7100 that is 4 (2 cores × 2
hardware threads), assuming the container is not given a CPU limit; Part 1 records the real number.

Those same worker threads parse every customer request, check every JWT, enqueue every order, and
send every reply. (The exchange core itself runs on its own dedicated `std::thread`, spawned in
`serve_primary`. So matching is not stopped; it is the HTTP side, before and after matching, that
can be blocked.)

So: four simultaneous registrations would hold all four workers for one bcrypt each. During that
time no order request can be parsed, no reply can be sent, and every order caught behind them is
charged the full wait. Registrations need no account at all. A loop of registrations, or of logins
with a wrong password for an account the attacker registered, keeps the gateway mostly blocked.

How long the stall is, and how it grows with the number of simultaneous logins, is measured in
Part 1 (`performance/03-login-stall.md`), on the milestone 23 code, before anything changes.

### A.5 Login reveals which emails have accounts

`login` answers an unknown email with `"user not found"` and a wrong password with
`"invalid password"`. Anyone can therefore check whether an email has an account. Worse, an unknown
email returns straight after the database lookup, while a known email runs one bcrypt first. Even if
the two messages were made identical, the response *time* would still tell them apart: about one
bcrypt longer for an existing account. This is a *timing side channel*. The fix is to run a bcrypt
verify against a fixed dummy hash when the email is unknown, so both paths take the same time.

### A.6 Smaller gateway faults

- `GET /users/` returns a hard-coded list of two invented users, "Mohit" and "John". It is a
  leftover from the first commits.
- `AuthUser::from_request_parts` calls `jwt_secret()`, which calls `std::env::var("JWT_SECRET")`, on
  every authenticated request. Reading the environment takes a process-wide lock and copies the
  string. The cost is small, but the value can never change while the process runs, so it should
  be read once at startup.
- Login failures are returned as `AppError::Validation`, which maps to HTTP 400. Authentication
  failures should be 401.

### A.7 Why this matters more than another optimization

Milestones 8 to 23 built a system that loses no acknowledged command when a process crashes, a
write is torn, or a whole machine is lost, and that replays to identical state on any machine.
That machinery protects whatever the journal contains. On 2026-10-12 the journal would faithfully
replicate and replay fake deposits, self-raised limits and a book filled by one customer. The
milestone 23 journal had already put "the gateway and trust boundary" first among the candidates;
this review agrees with that order.

---

## Part B — Why not latency first

### B.1 What the design asks

From `stock-exchange-system-design.md`, the non-functional requirements:

> **Latency.** The round-trip latency should be at the millisecond level, with a particular focus
> on the 99th percentile latency. The round trip latency is measured from the moment a market
> order enters the exchange to the point where the market order returns as a filled execution.

Two points matter for what follows. The target is "the millisecond level", not microseconds. And
the measurement starts when the order *enters the exchange*, which for this project is the HTTP
gateway.

### B.2 What was measured

From results-m23, section `m23p6-bench`. Machine: the office Ubuntu machine, as described in
`decisions.md`. Exchange and replica in two containers sharing its 2 cores and one SSD. Command:
`--bench --orders 100000 --rate 5000`, two runs per configuration. The bench sends one order every
200 µs (1 s / 5,000) and times each one from its *intended* send time to its reply (Harness,
"Coordinated omission"):

| Configuration | Run | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| Alone | 1 | 7.871 ms | 13.623 ms | 30.383 ms | 64.991 ms | 75.071 ms |
| Alone | 2 | 7.203 ms | 11.871 ms | 16.687 ms | 27.583 ms | 34.079 ms |
| Replicated | 1 | 11.399 ms | 20.575 ms | 28.751 ms | 40.895 ms | 49.055 ms |
| Replicated | 2 | 12.359 ms | 24.463 ms | 49.247 ms | 88.895 ms | 108.671 ms |

(The raw file prints microseconds: `p50 7871` means 7,871 µs = 7.871 ms.)

So the median is several milliseconds and the p99 is tens of milliseconds. "Millisecond level"
would be roughly 1 ms; the gap is about one order of magnitude on the median and one to two on the
p99. It is real, but it is not the gap of a system that is broken.

### B.3 Where the CPU time per order goes

Task 14, section "Where the time goes now". A throwaway build timed each step of the worker over
200,000 orders at the maximum rate, in the Docker Desktop VM (a WSL2 virtual machine, 6 vCPUs),
on its virtual disk. The figures are per order, averaged over each group:

| Step | µs per order |
|---|---|
| prepare: risk, wallet, match plan, settlement checks | 8.8 |
| build the event envelopes and JSON-encode the record | 6.9 |
| commit the prepared change to the core | 5.7 |
| journal write and sync, shared across the group | 6.0 |
| mmap publish and callbacks | 3.5 |
| release replies | 0.6 |
| **total** | **31.5** |

How to read it:

- The unit is **microseconds of time on the worker thread**, not CPU cycles. (At 3.9 GHz one
  microsecond is about 3,900 cycles, so 6.9 µs is about 27,000 cycles. Cycles are never used in
  this milestone's documents; times are.)
- Five of the six rows are CPU work: 8.8 + 6.9 + 5.7 + 3.5 + 0.6 = **25.5 µs**.
- The sync row is not CPU work. During a sync the worker *waits* for the disk. At the maximum rate
  a group holds about 1,000 orders and its sync takes about 6 ms (Task 14), so the wait is shared:
  6 ms / 1,000 orders = 6 µs per order.
- The instrumentation itself slowed that build to about 36,000 orders/s (Task 14), so the
  proportions are more trustworthy than the exact sums.

### B.4 What each recorded latency item would buy

The milestone 23 journal listed the latency candidate as "a binary record format instead of JSON,
a pipelined journal sync, and CPU pinning". Taking each in turn:

**A binary record format.** It would replace most of the 6.9 µs spent building envelopes and
encoding JSON. That is 6.9 / 25.5 ≈ 27% of the CPU per order, "about a quarter" in Task 14's words.
Where does that help?

- At the *maximum rate* the worker is CPU-bound: it is busy all the time, so cutting a quarter of
  its CPU raises throughput by up to about a third (1 / (1 − 0.27) ≈ 1.37). That is a real gain,
  for throughput.
- At *5,000 orders/s* the worker is mostly idle. 5,000 orders × 25.5 µs = 0.13 s of CPU per second
  of time, so it is busy about 13% of the time. The rest is waiting for the next order or the
  disk. Saving 7 µs from an order whose median latency is about 7,500 µs changes that median by
  7 / 7,500 ≈ 0.1%. That is far below the run-to-run noise (the two "alone" medians above differ by
  0.67 ms, about 9%).

So the binary format is a throughput optimization, not a latency one at the loads where latency is
measured.

**A pipelined journal sync.** Today the worker stages a group, then *waits* for its sync, then
releases its replies, then starts the next group. During the wait it does nothing. Pipelining
means handing the sync of group N to another thread and preparing group N+1 meanwhile.

- At the maximum rate this recovers the idle time: about 6 µs of the 31.5 µs per order in Task
  14's disk profile, plus it lets groups form while the disk works. Throughput rises toward the
  in-memory figure.
- Under load it also shortens queues, and queueing is what makes the p99 much larger than the p50.
- At light load it cannot help the floor: an order still waits for *its own* group's sync before
  it may be answered (durable before visible), plus, when replicated, the replica's sync and one
  network round trip.
- It changes the failure model. Since milestone 19 the in-memory core is allowed to be ahead of the
  disk by one group, and after a failed sync it is never used again. With pipelining it would be
  ahead by two groups. The rule stays the same, but the reasoning and the tests must be redone.

It is the one item that matters for latency under load, and the right first *code* step of a
latency milestone. It is still not the first step: see B.6 and B.7.

**CPU pinning.** Pinning binds a thread to one CPU core so the scheduler never moves it, and
usually goes with *isolating* that core (the `isolcpus` kernel option) so nothing else runs there.
It removes scheduler jitter of the order of microseconds to tens of microseconds, and keeps the
thread's caches warm. It only helps when there are cores to spare. The office machine has 2
physical cores. They are shared by the exchange worker, the Tokio threads, the replica, the warm
replica, the market-data process, the reporter, PostgreSQL, the load generator, and about 29
unrelated containers. Reserving one of two cores for the worker would starve everything else. On
this hardware pinning would make things worse.

### B.5 Part of the median is not explained

A simple model of where an order's time goes at 5,000 orders/s, from results-m23:

**Run 1 (alone).** The bench reported `syncs : 8267 (12.1 orders per sync)` over `20.009 s`.

- Syncs per second: 8,267 / 20.009 s = 413.2 per second.
- Mean length of one group cycle (stage, sync, release): 1 / 413.2 per second = 2.42 ms.
- Orders per group: 12.1 (as printed; check: 5,000 orders/s / 413.2 groups/s = 12.1).

An order that arrives at a random moment:

1. waits for the group that is already in progress to finish. On average that is half a cycle:
   2.42 / 2 = 1.21 ms;
2. then waits for its own group's full cycle: 2.42 ms;
3. and carries the generator's timer error. The bench waits for each send time with Tokio's timer,
   which ticks once per millisecond, so an order can leave up to 1 ms late, about 0.5 ms on
   average. The clock starts at the scheduled time, so that delay counts as latency (Harness,
   "The generator's own error"): 0.5 ms.

Model total: 1.21 + 2.42 + 0.5 = **4.13 ms**. Measured median: **7.871 ms**. Unexplained:
7.871 − 4.13 = **3.74 ms**.

**Run 2 (alone).** `syncs : 7896 (12.7 orders per sync)` over `20.006 s`.

- 7,896 / 20.006 s = 394.7 per second; cycle 1 / 394.7 = 2.53 ms.
- Model: 2.53 / 2 + 2.53 + 0.5 = 1.27 + 2.53 + 0.5 = **4.30 ms**.
- Measured median **7.203 ms**; unexplained 7.203 − 4.30 = **2.90 ms**.

So roughly 3 to 4 ms of the median is not accounted for by this model.

How far to trust the model: it is crude, and it is not a measurement.

- It uses the *mean* cycle. Cycles vary: one slow sync makes a long cycle, and a long cycle
  collects more orders. More orders therefore experience long cycles than short ones (the
  *inspection paradox*). That pushes the real median above the model, so some of the "unexplained"
  time may simply be cycle variance.
- It assumes the cycle is mostly sync. It also includes staging, publication and the replies, and
  the reply path crosses from the worker thread back to a Tokio task, which needs a Tokio worker to
  be free.

Only a build that timestamps each stage of each order can say where the rest goes. That is the
right *first* step of a latency milestone, before any optimization.

#### Correction after Part 1 (2026-10-12): most of the "unexplained" time did not recur

Part 1 ran the same in-process `--bench --orders 100000 --rate 5000`, alone, on the same office
machine, three days later (`performance/results/m24p1-http.txt`). The code was unchanged on the
trading path: Part 1 changed only measurement tools. The exchange was about twice as fast:

| Run | Date | syncs / s | mean cycle | model | measured p50 | unexplained |
|---|---|---|---|---|---|---|
| m23 run 1 | 2026-10-09 | 8,267 / 20.009 = 413.2 | 2.42 ms | 4.13 ms | 7.871 ms | 3.74 ms |
| m23 run 2 | 2026-10-09 | 7,896 / 20.006 = 394.7 | 2.53 ms | 4.30 ms | 7.203 ms | 2.90 ms |
| m24 run 1 | 2026-10-12 | 13,731 / 20.004 = 686.4 | 1.46 ms | 0.73 + 1.46 + 0.5 = 2.69 ms | 3.581 ms | 0.89 ms |
| m24 run 2 | 2026-10-12 | 12,133 / 20.002 = 606.6 | 1.65 ms | 0.82 + 1.65 + 0.5 = 2.97 ms | 3.841 ms | 0.87 ms |

On 2026-10-12 the same model leaves about 0.9 ms unexplained, not 3–4 ms. The same day, the
office SSD's raw flush measured 1.28–1.36 ms on average (`performance/04-raw-sync.md`), close to
the 1.46–1.65 ms cycle. So on that day the cycle was mostly the flush.

What this means:
- **Something other than the trading code moved the median by a factor of two between the two
  days.** This is an inference by elimination: the trading path was unchanged, so the difference
  must lie in the machine's state. Nothing here measured *which* part of that state:
  - the background load (the office machine is shared with about 29 other containers, and on
    2026-10-12 someone was also working on it interactively);
  - the SSD's internal state, on which its flush time depends;
  - or the environment of the milestone 23 runs.

  Either way, a single day's measurement on this machine cannot support a claim about a 3–4 ms
  software overhead.
- **The conclusion of this file does not change.** The milliseconds are still in the disk:
  today's median of about 3.6 ms is mostly flush cycles. The measurement base for latency work
  still had to be built, and Part 1 built part of it.
- **For the latency milestone:** compare before and after on the same day, interleaved, and over
  several runs, as Part 1's scripts do. Never compare against numbers from another day.

This correction is kept here, beside the original reasoning, rather than replacing it, so that the
reasoning and the evidence that revised it can both be read.

### B.6 What has never been measured

- **The HTTP path.** The bench deliberately skips HTTP, JSON request parsing and JWT checks
  (Harness, line 44 and "What it does not measure"): "Those run on Tokio's thread pool in parallel
  and are not on the single worker thread, so they do not limit throughput." That is true for
  *throughput*. It does not tell us their *latency*, and the design measures latency from when the
  order enters the exchange, which here is the HTTP port. So the latency a customer sees has never
  been measured. Milestone 23 part 6's Python client logged per-order latencies, but no
  distribution was reported, and a Python client on the same two cores is not a precise
  instrument.
- **The office SSD's raw sync.** The only raw figure is from the Docker Desktop VM's virtual disk:
  a 600-byte write plus `sync_all` took about 1.8 ms at p50 and 22 ms at p99 (Harness, "Where it
  ran"). The office machine's ADATA SU650 has never been measured on its own. Without that floor,
  nobody can say how much of the 7 ms median is the disk and how much is software.
- **Real hardware.** Every replicated figure is two containers on one machine with one SSD. There
  is no run with two machines with their own disks and a real network.

### B.7 The floor is the disk

What `fsync` (Rust's `File::sync_all`) promises: when it returns, the data and the file's
metadata are on stable storage and will survive a power cut. A drive keeps a volatile write cache
in RAM. To keep that promise it must either write the cached data to flash and only then
acknowledge, or have a way to finish the write if power fails.

- A **consumer SATA SSD** such as the ADATA SU650 has no such way. On a flush it must program the
  data into NAND flash before answering. That takes on the order of milliseconds, and it varies a
  lot with what else the drive is doing (garbage collection, other writers).
- **Server drives with power-loss protection** (PLP: capacitors that hold enough charge to finish
  writing the cache after power fails) can treat their cache as stable. They acknowledge a flush
  almost immediately. Their sync then commonly takes tens of microseconds.

This is general knowledge about drive classes, not something measured in this project. Its
consequence is the point: on this machine, with this disk, the milliseconds are in the disk, and no
change to the software's microseconds moves them. A drive with power-loss protection would move the
floor by about two orders of magnitude, more than any code change on the latency list.

### B.8 Conclusion

- Latency work done now would optimize microseconds of CPU while the milliseconds sit in the disk.
- The measurements a latency milestone needs do not exist: the HTTP path, a stage-by-stage
  breakdown of one order, and the raw sync of the real disk.
- Meanwhile the trust boundary is open in ways that make the system's correctness promises
  meaningless for anyone who reads its API.

So milestone 24 is the gateway and trust boundary. It also takes the measurements a later latency
milestone needs. Part 1 records the HTTP path and the raw sync, on the milestone 23 code, before
anything changes, and Part 6 records them again at the end.

The order recommended for after milestone 24 (`PROJECT_DIRECTION.md`, "What Not To Work On Yet"):
pushed updates (WebSocket), then latency starting with measurement, then reporter `COPY`.
