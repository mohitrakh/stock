# 03 — How simultaneous logins delay orders

**Part 1 baseline. Measured 2026-10-12.** Raw output: `results/m24p1-stall.txt` (the full run with
deadlines) and `results/m24p1-stall-first.txt` (the first attempt, stopped by hand). Script:
`scripts/m24p1-stall.sh`.

## The question

`00-selection.md` A.4 predicted, from how Tokio schedules work (theory §8), that logins stall order
entry. bcrypt runs inside the login's `async fn`, about 229 ms per call (`02-bcrypt-cost.md`), and it
blocks the Tokio worker thread that runs it. The exchange has 4 such workers. How bad is it in
practice?

## Method

- **Exchange:** `stock-m24p1`, alone (no replica), a fresh journal and database for every run. Every
  run printed `Tokio worker threads: 4`.
- **Orders:** `--http-bench --orders 20000 --rate 1000`, so 20 seconds at 1,000 orders/s. That is a
  light load: alone at 1,000/s the in-process p50 is about 3 ms (`01-http-vs-in-process.md`).
- **Logins:** `--logins C` registers C accounts, then runs C tasks that each log in with the right
  password, wait for the answer, and log in again, until the last order is answered. So about C
  logins are in progress at any time: C = 0, 1, 2, 4, 8, 16, two runs each.
- **Deadline:** `--deadline 120`. An order still unanswered 120 s after the first was due is
  abandoned and counted.
- **Probes** (C ≥ 8 only): the script sleeps 30 s, then asks `GET /health` on the customer port and
  `GET /session` on the operator port (each with a 10 s limit) and reads the journal's size, three
  times. Each probe is a *new* connection made by `curl` inside the exchange's container. When the
  probes answer at once, they run at about 30, 60 and 90 s after the generator starts. When they
  time out, each takes about 20 s, so at C = 16 they ran at about 30, 81 and 131 s (17:46:41 →
  17:47:32 → 17:48:22 → 17:48:52 in run 1).

  At C = 8 the orders' phase had already ended before the first probe (setup takes a few seconds,
  then the orders take about 21 s), so the C = 8 probes describe an exchange that was idle again.
- **Machine:** the office machine, load average 1.4 to 5.0 during the runs. The raw file's
  "containers running 31" counts every container on the host, this run's own exchange included.

## Results

### Order latency (µs) and login throughput

| C | Run | order p50 | p90 | p99 | p99.9 | max | logins done | logins / s | login p50 |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 1 | 5,723 | 9,455 | 13,751 | 24,015 | 38,335 | — | — | — |
| 0 | 2 | 4,783 | 8,847 | 13,567 | 26,895 | 37,855 | — | — | — |
| 1 | 1 | 3,013 | 7,363 | 142,079 | 229,247 | 242,431 | 85 | 4.2 | 232,831 |
| 1 | 2 | 4,331 | 8,527 | 200,063 | 234,495 | 240,255 | 85 | 4.2 | 232,959 |
| 2 | 1 | 3,563 | 7,995 | 173,951 | 223,615 | 232,703 | 166 | 8.3 | 236,415 |
| 2 | 2 | 4,563 | 8,799 | 102,527 | 204,159 | 222,975 | 165 | 8.2 | 237,567 |
| 4 | 1 | 192,767 | 426,751 | 629,759 | 702,463 | 1,016,831 | 291 | 14.3 | 261,119 |
| 4 | 2 | 152,063 | 321,791 | 564,223 | 821,247 | 840,703 | 297 | 14.8 | 258,815 |
| 8 | 1 | 365,311 | 871,423 | 1,439,743 | 1,797,119 | 1,813,503 | 306 | 14.5 | 510,719 |
| 8 | 2 | 447,231 | 1,019,391 | 1,505,279 | 1,702,911 | 2,142,207 | 305 | 14.5 | 506,879 |
| 16 | 1 | 586,239 | 1,300,479 | 1,933,311 | 2,523,135 | 10,133,503 | 1,813 | 15.1 | 983,039 |
| 16 | 2 | 546,815 | 1,067,007 | 1,555,455 | 1,942,527 | 2,081,791 | 1,811 | 15.1 | 983,551 |

For C ≤ 8 every order was answered (`201 x 20000`, no errors), and throughput was 950 to 1,000
orders/s.

### C = 16: new connections are refused service

| Run | answered in 120 s | unanswered | connection errors | failed logins | new-connection probes at ~30 s and ~81 s |
|---|---|---|---|---|---|
| 1 | 19,986 (18,802 × `201`, 1,184 connection errors) | 14 | 1,184 | 9,934 | `/health` and operator port: no answer within 10 s, both times |
| 2 | 19,986 (19,795 × `201`, 191 connection errors) | 14 | 191 | 4,313 | the same |

"Connection errors" are orders for which the generator could not establish a connection
(`client error (Connect)`). The generator also printed "167 orders/s over 120.000 s": with a deadline
and unanswered orders, it divides by the deadline, so that figure is not a throughput.

What kept working, and what did not:
- **Requests on connections already open were still served.** Logins kept succeeding at 15.1 per
  second for the whole run (1,813 and 1,811 of them). Run 2 answered 19,795 orders, with a maximum
  latency of 2.08 s.
- **New connections were not served.** That covers the generator's new order connections (191 and
  1,184 failures), the failed logins (which reconnect after an error), and both probes, which are
  new `curl` connections. With every worker thread inside a bcrypt, the task that accepts new
  connections, and the new connections' first requests, rarely got a thread.
- At the third probe (about 131 s), after the orders' deadline had passed and the login loops had
  stopped, both ports answered new connections in under 0.4 ms again.

**Was the generator itself to blame?** Later the same day, in one run of a different script, this
generator's login loops did spin. They retried failed logins at once without yielding, reached 366%
CPU with the exchange idle, and its deadline never fired (`07-login-stall-after-part-2.md`). That did
**not** happen in these two runs, and the raw output shows it:
- the generator's own CPU was 103.5 and 101.5 µs per order, that is about
  103.5 µs × 20,000 = 2.07 s of CPU over the 120 s run. Spinning at 366% would be about 440 s;
- the 120 s deadline fired in both runs;
- the failures came slowly: 9,934 failures / 120 s / 16 loops ≈ 5 per loop per second, about one
  every 190 ms.

So these rows stand. An earlier version of this file said the opposite. That was wrong, and the
second independent review caught it. The loops now back off 10 ms after a failure anyway.

**A re-measurement confirms them.** With the fixed generator, the Part 1 exchange at C = 16 ran at
383–390% CPU, all bcrypt, while the generator used 0.1%. New connections, `/health` and the operator
port went unanswered for over a minute, 14 orders were unanswered at 120 s, and 33 and 1,415 orders
could not connect.

### The first attempt at C = 8: the run never ended

The first version of the script had no deadline (`results/m24p1-stall-first.txt`). Its C = 8 run
never finished. About 17 minutes after it started, the exchange was checked by hand. These checks
were typed into a terminal, not captured in the raw file, so they are recorded here:
- `GET /health` on the customer port, a new connection: no answer within 10 s, three times;
- `GET /session` on the operator port, a new connection: no answer within 10 s;
- the exchange container's CPU (`docker stats`): 377.6%, all 4 hardware threads, which is bcrypt;
- the journal: 17,584,448 bytes, last modified at 17:19:35 local time, and unchanged over a further
  30 s.

**What this means: almost every order had been journaled, a few were never answered, and the
logins never stopped.** The run started at 17:19:14 (the `host` line in the raw file). After a few
seconds of setup and about 20 s of orders, the journal's last change at 17:19:35 is the *end* of
the order phase, not its start. Its size agrees: the two complete C = 8 runs ended with journals of
17,620,363 and 17,616,241 bytes: 35,915 and 31,793 bytes more. How many bytes one order adds can be
read from the two C = 16 runs:
- they answered 18,802 and 19,795 orders, 993 apart;
- their journals ended at 16,737,139 and 17,447,642 bytes, 710,503 apart;
- 710,503 / 993 ≈ 716 bytes per answered order. That is close to `--bench`'s figure of about
  753 bytes per order.

So the first attempt's journal was short by 31,793 / 716 ≈ 44 to 35,915 / 716 ≈ 50 orders: roughly
45–50 orders never reached the journal.

The generator's login loops stop only when every order is answered. With a few orders never
answered, the 8 loops ran forever and kept every core busy with bcrypt, and new connections such
as the probes went unserved for the whole 17 minutes. An earlier version of this file said "not one
order reached the journal in 17 minutes". That was a misreading of the timestamp, found by the
independent review.

The two later runs at C = 8, with the deadline, answered every order (p50 0.37–0.45 s). So a run
that leaves orders unanswered happened once in three at C = 8, and at C = 16 in both runs.

## What the numbers say

**1. One login at a time is enough to damage the tail.** With C = 1 or 2 the median order is
unaffected (3.0–4.6 ms, within the noise of C = 0). But p99 jumps from about 13.7 ms to 102–200 ms,
and p99.9 to 204–234 ms, about one bcrypt. So between 1% and 10% of orders wait behind a login's
bcrypt.

Why only some orders? Tokio spreads tasks over its 4 workers, and an idle worker steals tasks from a
busy worker's queue (theory §8.2). Most orders therefore avoid the blocked worker. A likely reason
some cannot is Tokio's per-worker "LIFO slot": a task just woken by a worker goes into a slot that
other workers do not steal from. If that worker then starts a 230 ms bcrypt, the task waits for it.
This explanation is an **inference** from Tokio's documented design, not something measured here.

**2. Four logins at a time take the whole gateway.** At C = 4, as many as the worker threads, the
median order takes 152–193 ms and the p99 0.56–0.63 s. A median of about two-thirds of a bcrypt
means most orders find every worker busy and wait for one to finish. That is the arithmetic of
`00-selection.md` A.4, now measured.

**3. Beyond four, it gets worse in proportion, then new connections stop being served.**
- At C = 8, the median order waits 0.37–0.45 s and the p99 about 1.5 s.
- At C = 16, existing connections are still served slowly, but new connections, `/health` and the
  operator port included, get no answer for tens of seconds. Hundreds of orders cannot even
  connect, and 14 are not answered within two minutes.
- In one run at C = 8, a few orders were never answered. As long as the logins continued, new
  connections went unserved for at least 17 minutes.

The operator port is a separate listener, but it is served by the same Tokio runtime, so it starves
with the customer port. That matters operationally: during a login flood, an operator who has to
open a new connection to close the market cannot get an answer.

**4. Logins themselves cap at the machine's bcrypt maximum.** Logins per second: 4.2 at C = 1,
8.2–8.3 at C = 2, then 14.3–15.1 from C = 4 on. `02-bcrypt-cost.md` measured the machine's maximum
at 15.1–15.4 bcrypt calls per second. So from C = 4 on, the machine does nothing but bcrypt as fast
as it can, and order entry gets what is left: almost nothing.

**5. What an attacker needs.** Four connections, each logging in again as soon as it is answered,
about 15 requests per second. A *registration* costs the same bcrypt and needs no account, and the
attacker can create the accounts for logins themselves. Either way it is a few kilobytes per second
of traffic.

**6. A side observation about the generator.** With C ≥ 1 its CPU per order falls from about 333 µs
to about 86–103 µs at the same 1,000 orders/s. This supports `01-http-vs-in-process.md` point 4: much
of the generator's CPU at low load is its runtime waking and parking threads, which a busier
runtime does less. (At C = 16 the "share of one hardware thread" reads 2%. That is because the
120 s deadline is the denominator, not because the generator used less CPU per order.)

## What Part 2 must show

Part 2 moves bcrypt to Tokio's blocking pool behind a cap of 2 calls at a time (decision 7). The same
script on the Part 2 binary should show:
- order latency at every C within the noise of C = 0, because the workers never run bcrypt;
- no unanswered orders and no connection errors at C = 16, with `/health` and the operator port
  answering throughout;
- logins capped at about 2 / 0.229 s ≈ 8.7 per second (two calls at a time at full speed), rather
  than the machine's maximum of about 15, with the extra logins waiting in line instead of taking
  the CPU.

Run on the same day as a fresh run of this Part 1 binary, as everywhere in this milestone.
