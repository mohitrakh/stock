# Milestone 24 — Decisions

Every decision the owner took for this milestone, the date, the options that were shown, and the
reasoning offered with each. The milestone specification in `PROJECT_DIRECTION.md` ("24. Gateway
and Trust Boundary", "Decisions for the owner") lists the same seven; this file is the record of
what was chosen.

A decision recorded here can still be changed by the owner before the part that uses it starts.
If one changes, the old entry stays and a new dated entry follows it, so the history of why the
code looks the way it does is never lost.

## 2026-10-12 — Selection of the milestone

**Chosen:** the gateway and trust boundary, over the latency milestone the owner had expected.

**Options shown:** the four candidates recorded after milestone 23 (gateway; reporter `COPY`;
pushed updates; latency). The reasoning is in `00-selection.md`.

## 2026-10-12 — Where the measurements run

**Chosen:** on the office Ubuntu machine, reached over SSH (`ubuntu@10.0.127.253`, key
authentication). The owner asked that work stop if the machine could not be reached; it could.

**Options shown:**
1. *Both* — build and test on the Windows PC's Docker Desktop, measure on both, keep the two sets
   apart.
2. *Docker Desktop only* — every step runnable from the development PC, but the numbers do not
   compare with milestone 23's office figures, and Docker Desktop's virtual disk is noisier (raw
   sync 1.8 ms p50 / 22 ms p99, `docs/performance/00-benchmark-harness.md`).
3. *Office machine only* — one consistent machine.

**Consequence:** every number in `performance/` comes from the office machine unless it says
otherwise. Builds and tests also run there, inside the `stock-rust` image, exactly as milestones 22
and 23 did, so `cargo test --locked` results and measurements come from the same toolchain.

The machine, as found on 2026-10-12: Intel Core i3-7100 at 3.9 GHz (2 cores, 4 hardware threads),
15 GiB RAM, one ADATA SU650 SATA SSD (223.6 GB, 85% full), Linux 6.8.0-138. About 29 other
containers (the count before any of this milestone's containers started; the "containers running"
figure printed by the scripts counts the measured exchange's own containers too, hence 30–32) (the AIMess stack, a Gitea lab, PostgreSQL, MongoDB, Redis, RabbitMQ, ClamAV, LiveKit
and others) were running on it at the same time. They share the CPU, memory and SSD with every
measurement in this milestone, as they did in milestone 23. Each measurement file records what was
running when it was taken.

## 2026-10-12 — Decision 1: what a product is

**Chosen:** a product is a symbol only. No tick size, no lot size.

**Options shown:**
1. *Symbol only* (recommended) — this milestone is about who may do what; a tick size changes
   price validation, every client and the benchmark's workload, and belongs with the instrument
   reference data work that a real exchange does separately.
2. *Symbol, tick size and lot size now.*

## 2026-10-12 — Decision 2: an unknown user id on the operator port

**Chosen:** the operator handler checks that the user exists in PostgreSQL's `users` table before it
enqueues a deposit or a risk limit. The check is not journaled and is not on the trading path.

**Options shown:**
1. *Check the users table* (recommended) — a mistyped id cannot receive money that nobody can
   ever trade or withdraw.
2. *Accept any id* that passes the gateway's identifier rule — the exchange core has never known
   which users exist, and this would keep it that way.

**Why the check is not journaled:** the core does not know the user list, and the users table is
outside the journal. Journaling the result of a database lookup would make the decision depend on
something replay cannot see. The check therefore happens before the command exists: a refused
deposit is never journaled, exactly like a request the gateway refuses for bad JSON.

## 2026-10-12 — Decision 3: the reporter and listings

**Chosen:** no reporter table for products. The shared decoder and the reporter accept the new
listing events and ignore them.

**Options shown:**
1. *No table* (recommended) — the product list is small, the operator port can read it, and
   nothing in the reporter's queries needs it yet.
2. *A `products` table* with the listing's journal sequence.

## 2026-10-12 — Decision 4: the limits

**Chosen:**
- `MAX_RESTING_ORDERS_PER_USER` = 2,000 — 1% of the 200,000-order book (`MAX_RESTING_ORDERS`).
- Writes (new orders and cancellations): 100 per second per user, burst 200.
- Reads: a separate bucket per user.
- `register` and `login`: limits per client IP address.

The exact read rate and the per-IP rates are set in Part 5 from the Part 1 measurements and
recorded here when they are.

**Options shown:** the proposed numbers above, or numbers of the owner's choosing.

## 2026-10-12 — Decision 5: the rate limiter

**Chosen:** a hand-written token bucket, no new dependency.

**Options shown:**
1. *Hand-written token bucket* (recommended) — a few dozen lines, no dependency, and worth
   understanding line by line.
2. *The `governor` crate* — a well-used implementation of GCRA (the generic cell rate
   algorithm), keyed by user.

`01-theory.md` explains both algorithms and why they admit the same traffic.

## 2026-10-12 — Decision 6: the HTTP load generator

**Chosen:** written in Rust, inside the crate, as a new `--http-bench` mode of the `stock` binary.

**Options shown:**
1. *Rust, in the crate* (recommended) — reproducible from the repository; accurate open-loop
   timing at 5,000 orders/s; reuses the in-process bench's workload and HdrHistogram.
2. *Extend `~/stock-scripts/m23p6-load.py`* — less code, but it lives outside the repository, and a
   Python client sharing two cores with the exchange is likely to measure itself.

## 2026-10-12 — Decision 7: a cap on bcrypt calls in flight

**Chosen:** yes. bcrypt runs on Tokio's blocking pool, and a semaphore caps how many calls run at
once.

**Options shown:**
1. *Cap it* (recommended) — without a cap, enough simultaneous logins can still take every core,
   even off the worker threads.
2. *Blocking pool only* — the worker threads are protected, the CPU is not.

The cap's value is chosen in Part 2 from the bcrypt cost measured in Part 1, and recorded here.

## 2026-10-12 — The bcrypt cap's value (Part 2)

**Chosen:** half the CPUs the process sees, at least one: `max(1, available_parallelism / 2)`. That
is 2 on the office machine (4 hardware threads). It is computed at startup, not configured.

**Why, from `performance/02-bcrypt-cost.md`:**
- With 2 calls at once, each runs at full speed (231 ms, against 229 ms alone). There is one core
  each.
- With 4 at once the machine does more bcrypt in total (15.4 per second against 8.4), but only by
  occupying all 4 hardware threads, which leaves nothing for orders. That is exactly what the cap
  exists to prevent.
- Half the CPUs therefore keeps login throughput near one core's worth per permit, and always leaves
  half the hardware threads for the exchange. On the office machine that means the two bcrypt calls
  occupy one hardware thread on each of the 2 physical cores, and the exchange runs on their
  Hyper-Threading siblings, which share those cores' execution units. It is half the hardware
  threads, not half the cores' capacity.

**Capacity this gives:** 2 / 0.229 s ≈ 8.7 logins (or registrations) per second on the office
machine. Requests beyond that wait for a permit asynchronously, holding no thread.

**Options considered:**
1. *A fixed number*, such as 2. It would be wrong on a larger machine, where it is too few, and
   on a 1-CPU container, where it is too many.
2. *An environment setting.* It is configuration nobody asked for, and the formula has no
   machine-specific tuning to do.
3. *`available_parallelism / 2`* (chosen).

This was a design choice inside decision 7, made while implementing Part 2. The owner can still
change it before the milestone closes.

## 2026-10-14 — The milestone closes after Part 2; Parts 3 to 6 are dropped

**Chosen:** close milestone 24 with Parts 1 and 2 done, and drop Parts 3 to 6. The code and documents
of Parts 1 and 2 stay.

**The owner's reasoning:** this is a learning project. Anyone being able to deposit unlimited money,
set their own risk limits, invent symbols, or flood the exchange is acceptable. Custody, KYC and
protection against abusive customers are not goals. That removes the purpose of:
- Part 3, the product list: it stops customers inventing symbols to grow the state without limit;
- Part 4, operator-only deposits and risk limits: entirely about who may create money;
- Part 5, rate limits and the per-user book share: they stop one customer from hurting the others;
- Part 6, the final measurement of those changes.

**Why Parts 1 and 2 stay:**
- Part 1's tools (`--http-bench`, `--sync-probe`, `--bcrypt-probe`, the scripts) and its baseline
  measurements are the starting point of the latency milestone that follows.
- Part 2 fixed a performance fault, not only an abuse one. A single client logging in repeatedly
  made the slowest orders ten times slower, because bcrypt ran on the threads that serve orders.

**What stays open as known limitations** (recorded in `PROJECT_DIRECTION.md`, Known Prototype
Limitations):
- customers deposit any cash and shares and set their own risk limits;
- one customer can deposit up to the exchange's total and so block every later deposit;
- one customer can fill the 200,000-order book;
- symbols are unbounded;
- there is no rate limit;
- fake logins can make real logins wait, about 8 per second on the office machine, without
  affecting orders;
- registration reveals whether an email has an account.

**Options shown:** drop Part 4 only; keep Part 5 for the rate-limiting lesson; or drop everything
after Part 2. The owner chose the last.
