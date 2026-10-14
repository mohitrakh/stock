# Milestone 25 — Theory

The background needed to read this milestone's code and numbers cold. Milestone 24's
`01-theory.md` already covers open-loop measurement, coordinated omission, HdrHistogram,
percentiles, Little's law, `fsync` and drive caches, and Tokio's threads. This file builds on those
and does not repeat them.

Contents:

1. A round trip is a sum of stages
2. Means add up; medians do not
3. Group commit seen as a queue
4. Why the tail is so much longer than the median
5. Measuring without changing what is measured
6. Pipelining: overlapping the disk with the CPU
7. What durability forbids a pipeline to do
8. What a record format costs

---

## 1. A round trip is a sum of stages

The design states it directly:

```text
Latency = Σ executionTime along the critical path
```

An order through HTTP passes through these stages, one after another. Each is a span between two
timestamps in `src/exchange/stage_timing.rs`:

```text
client ── network ──▶ HTTP server task (Tokio worker)
                         HttpBeforeQueue   parse JSON, check JWT, validate, build the Order
                         ─ send into the bounded queue ─
                         Queue             wait in the queue (and for queue space)
exchange worker ─────▶  WaitInGroup       wait behind earlier commands of the same group
                         Prepare           risk, collateral, match plan, settlement checks
                         Encode            envelopes + JSON record
                         Commit            apply to the in-memory core
                         WaitForGroup      wait for the rest of the group to be staged
                         Write             write_all of the whole group      ┐
                         Sync              sync_all of the whole group       │ per group,
                         Confirm           replica's confirmation (if any)   │ shared by
                         Publish           mmap publication + callbacks      │ every command
                         Release           send every reply                  ┘ in the group
                         ─ oneshot reply ─
HTTP server task ────▶  (reply received)  ── HttpAsk ends here
                         serialize the response
                         ─ HttpTotal ends here ─
client ◀── network ──
```

`Worker` spans everything the worker does for one command, from dequeue to its reply.
`HttpAsk` spans everything from the gateway's point of view, from building the command to receiving
the reply: `Queue` + `Worker` + the time for the reply to wake the waiting task.

Two kinds of stage behave differently:
- **Per-command stages** (Prepare, Encode, Commit) cost each order its own CPU time.
- **Per-group stages** (Write, Sync, Confirm, Publish, Release) are paid once per group, and every
  command in the group waits for all of them. A 1 ms sync is 1 ms of latency for each of the 12
  orders in its group, but only 1/12 ms of worker time per order.

That is why a stage's *CPU cost per order* and its *contribution to latency* can be very different.
Milestone 19's 6 µs of sync per order (amortized at the maximum rate) and today's roughly 1 ms of
sync in every order's latency are the same disk, seen two ways.

## 2. Means add up; medians do not

For stages that happen one after another on the same path, the **mean** total time equals the sum
of the stage means. This is the linearity of expectation, and it holds whatever the distributions
look like:

```text
E[A + B] = E[A] + E[B]
```

**Medians do not add.** An example with two independent stages, A and B. Each takes 1 ms with
probability 0.6 and 10 ms with probability 0.4:
- **The medians.** More than half of A's values are 1 ms, so median(A) = 1 ms, and median(B) = 1 ms
  too. Their sum is **2 ms**.
- **The distribution of A + B:**
  - 2 ms with probability 0.6 × 0.6 = 0.36;
  - 11 ms with probability 2 × 0.6 × 0.4 = 0.48;
  - 20 ms with probability 0.4 × 0.4 = 0.16.
- **The median of A + B.** The cumulative probability is 0.36 at 2 ms and 0.36 + 0.48 = 0.84 at
  11 ms. It first passes 0.5 at 11 ms, so median(A + B) = **11 ms**, more than five times the sum of
  the medians.
- **The means do add.** E[A] = 0.6 × 1 + 0.4 × 10 = 4.6 ms, and the same for B, so their sum is
  9.2 ms. And E[A + B] = 0.36 × 2 + 0.48 × 11 + 0.16 × 20 = 0.72 + 5.28 + 3.2 = 9.2 ms, the same.

So this milestone checks a stage breakdown in two ways:
1. **The sum of stage means against the end-to-end mean.** This must agree, up to the gaps between
   timestamps that no stage covers, and anything left over is a stage nobody has measured.
2. **The stage medians and p99s next to the end-to-end median and p99.** These show where the
   typical and the slow order spend their time, without claiming they add up.

## 3. Group commit seen as a queue

The exchange worker runs this loop, `ExchangeRuntime::run`:

```text
loop:
    take every command waiting (at least one, at most 1,024)    ← one group
    stage each: prepare, encode, commit
    write + sync once for the group (+ the replica's confirm)
    publish the group, then release its replies
```

It is a queue whose server takes everyone waiting at once. That is called a **batch-service
queue**. From one order's point of view:

1. It arrives while a group is in progress, almost always.
2. It waits for that group to finish (part of `Queue`).
3. It is taken in the next group, and waits for that group's whole cycle: the staging of the
   commands before it (`WaitInGroup`), its own staging, the staging of those after it
   (`WaitForGroup`), then the shared write, sync, publish and release.

If the cycle took exactly C every time, an order arriving at a random moment would wait C/2 on
average for the cycle in progress, then C for its own: 1.5 C in total. At 5,000 orders/s on
2026-10-12, with C ≈ 1.46 ms, that is about 2.2 ms. The measured median was 3.6 ms. Part 1 measures
where the difference goes.

**Cycles are not equal, and that makes waits longer than this model says.** This is the
*inspection paradox* (also called the waiting-time paradox): an order is more likely to arrive
during a long cycle than a short one, because long cycles cover more of the timeline. Here is an
example. Half the cycles take 0.5 ms and half take 2.5 ms, so the mean cycle is 1.5 ms. But
2.5 / (0.5 + 2.5) = 83% of the time is covered by long cycles. An arriving order therefore lands in
a long cycle 83% of the time. Its expected residual wait is:

```text
(0.83 × 2.5 + 0.17 × 0.5) / 2 ≈ 1.08 ms
```

That is not the 0.75 ms that "half the mean cycle" suggests. The general formula for the mean
residual wait is E[C²] / (2 E[C]), which grows with the *variance* of the cycle. Sync times on this
SSD vary a lot (p50 0.8 ms, p99 5.3 ms), so the variance is large.

## 4. Why the tail is so much longer than the median

At 5,000 orders/s, the p99 (about 19 ms in-process) is about five times the median (about 3.6 ms).
Three effects stack:
- **Slow syncs.** The SSD's own p99 sync is about 5.3 ms, about 6 times its median (0.88 ms).
- **Queue build-up after a slow sync.** While one slow sync runs, orders keep arriving and wait. The
  next group is larger, so its staging takes longer, and the following groups need several cycles
  to drain the backlog. One 5 ms sync at 5,000 orders/s lets 5,000 × 0.005 = 25 orders pile up,
  about two groups' worth.
- **CPU contention.** When the worker, or the Tokio thread that must wake a reply, waits for a CPU
  that another process holds, every order in flight waits. The office machine is shared, so this
  happens.

Part 1's per-stage p99s show which of these dominates: a p99 that lives in `Sync`, or one that
lives in `Queue` and `WaitInGroup`.

## 5. Measuring without changing what is measured

Instrumentation costs time, and so it can change what it measures (the *observer effect*). Its
costs here:
- **Reading the clock.** `Instant::now()` on Linux calls `clock_gettime(CLOCK_MONOTONIC)`. This goes
  through the vDSO, a small piece of kernel code mapped into every process so that this call needs
  no system call. It typically takes tens of nanoseconds. (That is general knowledge, not measured
  here.) Part 1 adds 8 clock reads per order in-process, plus 9 per group (10 when replicated),
  shared by the whole group:
  - `queued_at` when the command is built;
  - the wait in the group;
  - the end of its staging;
  - 4 inside `stage_input_event`;
  - one after its reply.

  Through HTTP the gateway adds 5 more. At tens of nanoseconds each, that is well under a
  microsecond per order, against about 25 µs of work.
- **Recording a sample.** One HdrHistogram `record` is an index computation and an increment, a few
  nanoseconds.
- **Locks.** The worker records into thread-local histograms with no lock, and merges them into the
  global ones once per group. Only the gateway takes a lock per sample, three times per HTTP order.
  An uncontended mutex costs tens of nanoseconds; a contended one can cost microseconds.
- **Merging.** Once per group, 16 histograms are added into the global ones, while the worker holds
  that lock.

Rather than estimate these costs, Part 1 *measures* them. The same benchmark runs on a build without
the feature and a build with it, interleaved, and the difference is the instrumentation's cost
(`performance/01-instrumentation-overhead.md`).

`Instant` is a **monotonic** clock: it never goes backwards, unlike wall time, which can be stepped
by NTP. Every duration in this milestone is a difference of two `Instant`s. One exception is
`Queue`. It is the worker's dequeue time minus the gateway's `queued_at`, two instants taken on
different threads. That is still valid, because both read the same system-wide monotonic clock. It
uses `saturating_duration_since`, so it reads 0 rather than going negative if the two reads are a
few nanoseconds out of order.

## 6. Pipelining: overlapping the disk with the CPU

Today's cycle is strictly sequential:

```text
group N:    [stage N][write N][ sync N ][publish N, release N]
group N+1:                                                    [stage N+1][write][ sync ]...
                              ▲ the worker is idle here, waiting for the disk
```

A **pipelined** sync hands the sync to another thread and keeps staging:

```text
worker:     [stage N][write N][stage N+1      ][write N+1][stage N+2 ...
syncer:                       [ sync N ][release N]      [ sync N+1 ][release N+1]
```

Two consequences follow:
- **Throughput.** In a pipeline, throughput is limited by the slowest stage, not by the sum of the
  stages. With staging taking S and syncing taking D per group, a sequential loop finishes a group
  every S + D. A two-stage pipeline finishes one every max(S, D). At the maximum rate, where S and D
  are both a large share, this can approach a doubling. At light load S is small, and nothing
  changes: an order still waits for its own sync.
- **Latency under load.** Shorter cycles mean less waiting behind the group in progress (section 3)
  and faster recovery after a slow sync (section 4). The p99 under load should improve more than
  the p50.

**Amdahl's law** bounds the gain. If a fraction f of an order's latency is in stages that the
change speeds up by a factor k, the latency becomes (1 − f) + f / k of what it was. If the sync were
90% of a cycle and became fully hidden (k → ∞), the cycle could shrink by at most 10×, and only
where staging is not on the critical path. Since every order must still wait for its own sync, the
real gain at light load is close to zero.

## 7. What durability forbids a pipeline to do

The rule since milestone 8 is **durable before visible**: nothing about a command may be seen until
the disk holds it, and with replication until both disks hold it. A pipeline must keep that,
group by group:
- Replies, reads, mmap publication and callbacks of group N wait for group N's sync. They wait also
  for every group before N, because group N may contain a read that sees group N−1's effects.
- Groups are released strictly in order.
- **A read in group N+1 reflects group N's state**, which is committed in memory but possibly not
  yet synced. That is allowed only because the read's answer is held until group N+1 is durable,
  and N+1 is durable only after N is.

**The failure rule.** Since milestone 19, the in-memory core can be one group ahead of the disk.
After a failed sync, that core is never used again: the worker halts and recovery rebuilds from the
journal. With a pipeline of depth 2, the core can be two groups ahead. The rule is unchanged, but
both groups must then be answered "unavailable", neither may be published, and the tests must prove
it.

## 8. What a record format costs

Each order's record is encoded once (`encode_record`) and read back by:
- recovery;
- the replica's checks;
- the warm replica;
- the mmap readers: the MDP, the reporter and the probe.

JSON's costs:
- Every field name is written in every record, as text.
- Every number is formatted as decimal text. Formatting a `u64` takes a loop of divisions by 10,
  and a float takes much more.
- Strings are escaped.
- Reading it back means parsing all of that.

A **binary format** writes fixed-width numbers and length-prefixed strings in a schema both sides
know. It is smaller and faster, but not human-readable, and a schema change needs explicit
versioning. Milestone 19 measured JSON at about 6.9 µs per order, a quarter of the worker's CPU. Part
1 measures it again on the office machine (`Encode`). Part 3 is done only if it matters.
