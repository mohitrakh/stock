# 01 - Group Commit: One Disk Sync for Many Orders

Part of milestone 19. Code: `src/exchange/runtime.rs` (`run`, `handle_group`, `stage_command`,
`stage_input_event`, `flush`) and `EventStore::append_record` in `src/exchange/event_store.rs`.

## The problem

The exchange promises that once it answers "accepted", the order survives a crash or a power cut.
It keeps that promise by writing every command to the journal and calling `sync_all` (the `fsync`
system call) before answering. `fsync` returns only when the drive says the bytes are on stable
storage, and that is slow: about 1.8 ms at p50 and 22 ms at p99 on the benchmark machine's virtual
disk. Consumer NVMe drives measure 0.5 to 10 ms, and drives with power-loss protection 20 to 150 µs.

Before this change, the single exchange worker handled one command at a time:

```text
order 1: [prepare ~50us][write][ sync ~1.8 ms ][commit][publish][reply]
order 2:                                                              [prepare][write][ sync ~1.8 ms ]...
```

Each order waited for its own sync, so throughput could never exceed one order per sync: roughly
1 / 1.8 ms, or about 550 orders a second at best. The baseline measured **384 orders/s** on disk.
About 98% of each order's time was the worker waiting for the disk. The exchange logic itself took
roughly 50 µs per order at that point, most of it copying order books (fixed in `02`).

## The idea

A sync makes *everything written so far* durable, not just the last write. So if ten orders are
waiting, write all ten, sync once, and all ten are durable for the price of one sync. Databases have
done this for decades (PostgreSQL, MySQL/InnoDB); it is called **group commit**. (Kafka also batches
appends, but by default it acknowledges once replicas have the data rather than after a shared
fsync.)

```text
group 1: [prep 1][prep 2]...[prep n][write all n][ one sync ~1.8 ms ][publish n][reply n]
group 2: (everything that queued up during group 1's sync)
```

## How it works, step by step

`ExchangeRuntime::run` now takes a *group* instead of a single command:

1. **Collect.** Block until at least one command arrives, then take every other command already
   waiting in the queue, up to `MAX_GROUP` (1,024), without waiting for more.
2. **Stage each command, in queue order** (`stage_command`):
   - A write (order, cancel, deposit, share deposit, risk limit) is prepared and validated exactly as
     before (`prepare_input_event`), numbered, and encoded to its framed journal record. Then it is
     **committed to the in-memory core immediately**, so the next command in the group sees its
     effects. A buy that fills against a sell staged earlier in the same group works because of this.
   - A read (balance, positions, executions, risk limit, order) is answered from the core at its
     place in the queue.
   - Either way, the reply is **not sent**. It is wrapped in a closure (`HeldReply`) and kept.
3. **Flush** (`flush`): concatenate all the group's records and hand them to
   `EventStore::append_record`, which does **one `write_all` and one `sync_all`**.
4. **Only after the sync returns**: publish each record to the mmap stream, run execution callbacks,
   then release every held reply, reads included.
5. Count the group's write commands toward the snapshot schedule and write at most one snapshot
   for the group (every command in it is already in the core, so one checkpoint covers them all).
   Then take the next group.

### Why nobody can see an order before it is durable

The rule this exchange has kept since milestone 8 is *durable before visible*. Group commit keeps
it. Everything that leaves the worker waits for the sync:

| Way to observe an order | When it happens now |
|---|---|
| The HTTP reply | after the sync (held reply) |
| A balance, position, or order read | after the sync, even if the read was staged before it |
| The mmap stream (market data, reporter, warm replica) | after the sync |
| Execution callbacks | after the sync |
| A core snapshot | after the sync, and only on a group boundary |

Reads needed care. A read staged after a write in the same group sees that write's effect in
memory. If the read were answered at once, a client could see a balance that later vanished because
the sync failed. So reads are held too. On failure a held read is dropped rather than answered, and
the gateway turns a dropped reply into `503 unavailable`.

### What happens when things fail

- **The sync fails.** The core in memory is now *ahead* of the disk: it contains commands that
  may never have become durable. Every command in the group is answered `exchange unavailable`,
  nothing is published, and the worker halts. The process never uses that core again. A restart
  rebuilds from whatever the journal really holds. After a failed write or sync, some of the group's
  records may still have reached the disk, so `unavailable` means "outcome unknown", not "rejected".
  That was already true of a single command before group commit; now it covers a whole group, and
  `client_order_id` is how a client finds out. Stopping is also what PostgreSQL does after
  "fsyncgate" (2018): after a failed `fsync` it stops and recovers from the log rather than
  continuing.
- **Publication fails after a good sync.** The group is durable, but the worker halts before every
  batch is published, and every reply in the group, even for batches already published, is
  `unavailable`. On restart the recovered journal is republished. As before group commit, a
  publication failure means "durable, outcome not confirmed".
- **One command faults mid-group** (an internal invariant failure, such as crediting a seller who
  already holds `u64::MAX`). That command is answered `unavailable` at once. The valid commands
  staged before it are still synced, published and answered. Commands queued behind it are dropped
  unanswered (the gateway reports `503`). Then the worker halts. This is tested in
  `a_fault_mid_group_still_syncs_the_commands_before_it_and_drops_those_after`.
- **A crash in the middle of the group's write.** The group is written as back-to-back framed
  records in one `write_all`. If the *process* crashes, the operating system still holds everything
  written so far, so only the tail can be cut. On restart, `EventStore::open` keeps every complete
  record and truncates the torn one, exactly as it always has. None of those clients got a reply,
  because replies wait for the sync.
- **Power loss before the sync completes.** The drive may keep some of the unsynced pages and lose
  others, in any order. Recovery then either truncates a torn tail, or finds a record whose bytes
  are all present but whose checksum fails. The journal's long-standing rule treats the second case
  as corruption and refuses to start, so an operator must look. That was already possible for the
  one unsynced record before group commit. Group commit widens the unsynced window from one record
  to one group (up to 1,024 records), and so makes it more likely. Treating a bad record in the
  unsynced tail as its end, as PostgreSQL's WAL recovery does, would change the recovery policy.
  It is deliberately not part of this milestone.

### The one rule that changed

Milestone 11 promised that "a failed append leaves the live core unchanged", because the core was
committed only after the sync. With group commit the core is committed *before* the sync, which is
unavoidable: command 2 in a group must see command 1. The new rule is: **after a failed sync the live
core is never used again.** The worker halts, and recovery from the journal is the only way back.
Four tests that used to inspect the live core after a failed append were rewritten. Three now
recover from the journal and check that instead (for example
`append_failure_leaves_nothing_durable_or_visible`); the fourth,
`append_failure_does_not_publish_execution_callbacks`, checks that no callback ran and no history was
recorded. Nothing in
production ever read the core after a failed append, because the worker already stopped on the
first store error.

### Natural batching: no timers, no tuning

Nothing waits on purpose. There is no "collect orders for 1 ms" timer. A group is simply whatever
arrived while the previous group was syncing, so the system tunes itself to the load:

- **Quiet exchange:** the queue is empty when an order arrives, so the group has one order, and
  latency is the same as before (one sync).
- **Busy exchange:** the next group holds everything that arrived during the whole previous cycle,
  meaning its sync *and* its preparation. At 10,000 orders/s the measured cycle averaged about 5 ms
  (the sync's slow tail pulls the average well above its 1.8 ms median), so groups averaged about
  52 orders. The slower the cycle, the bigger the next group. Throughput grows with load instead of
  collapsing.

The benchmark shows this pattern. Orders per sync were 1.1 at 200/s, 1.7 at 1,000/s, 51.8 at 10,000/s,
and 952 at full speed.

`MAX_GROUP` (1,024) caps the group so the first command never waits behind an unbounded amount of
preparation work. At full speed the groups hit the cap (952 to 1,020 orders per sync).

## Results

Disk, snapshots off. The baseline column is the code before this milestone; the second column is
group commit alone:

| Run | Baseline | Group commit | Change |
|---|---|---|---|
| Max rate, 20,000 orders | 384 orders/s | **18,713 orders/s** | **49×** |
| Orders per sync at max rate | 1.0 | 952 | |
| Fixed 200/s: throughput / p50 / p99 | 200 / 3.2 ms / 30 ms | 200 / 3.2 ms / 35 ms | same (quiet: groups of 1) |
| Fixed 1,000/s: throughput / p50 / p99 | **495** / 2.1 s / 5.1 s | **1,000** / 3.6 ms / 22 ms | keeps up; p99 about 230× lower |
| Fixed 10,000/s: throughput / p50 / p99 | **452** / 20.7 s / 40.6 s | **9,907** / 9.5 ms / 168 ms | keeps up |

At 1,000 orders/s the old exchange simply could not keep up. Orders queued, and by the end each
waited seconds. With group commit the worker keeps up by syncing back to back with about 1.7 orders
per sync. It has spare capacity because groups grow with load, not because it sits idle.

### What it did *not* fix

At low load, latency is unchanged: one order still waits for one sync, and p99 is still the disk's
own p99. Group commit adds throughput, not speed per order. On this disk, p99 cannot go below about
20 ms while every reply waits for a local sync. Millisecond p99 needs either a faster, power-loss-
protected drive or durability by copying to a second machine instead of syncing (the
cross-machine replication milestone).

Group commit also exposed the next bottleneck. With the disk out of the way, the same code on tmpfs
(where syncs are free) managed only **3,309 orders/s** for 200,000 orders, and at a fixed 43,000/s on
disk it fell behind at 3,194/s. The worker was now busy with CPU work, not waiting for the disk. That
work was copying order books, which `02` fixes.

## Tests

- `a_queued_group_shares_one_sync_and_its_reads_see_earlier_writes`: deposit, share deposit, sell,
  buy, and a balance read in one group. It asserts **exactly one sync** (a per-handle counter
  `EventStore::syncs`, test builds only), that the buy fills against the sell staged before it, that
  the read reports the post-fill balance, and that recovery from the journal rebuilds the identical
  core.
- `a_failed_group_sync_answers_every_command_unavailable_and_publishes_nothing`: the write gets
  `exchange unavailable`, the read is dropped, and the mmap reader sees nothing.
- `a_fault_mid_group_still_syncs_the_commands_before_it_and_drops_those_after`: described above,
  checked through recovery.
- Four existing failure tests stopped inspecting the live core. Three check the journal through
  recovery instead, and one checks that no callback or history entry appears (see "The one rule
  that changed").

## Options considered and rejected

- **A commit delay** (wait a fixed time, for example 1 ms, to gather more orders, like
  PostgreSQL's `commit_delay`). It adds latency when the exchange is quiet, and natural batching
  already forms large groups when it is busy.
- **Journal only the inputs, as LMAX does**, then run business logic after the journal write.
  This exchange journals outputs too, and its subscribers read them. Changing that would change the
  journal format and every subscriber. That is a different milestone.
- **A separate journaler thread that syncs group N while the worker prepares group N+1**
  (pipelining). This is the next step for disk throughput. Today the worker still waits during each
  sync, which is why a fixed 43,000/s on the benchmark disk reaches about 25,000/s (see the
  milestone write-up). It is left out to keep this change small and its correctness easy to check.
- **`sync_data` instead of `sync_all`.** It is a small gain for appends, whose size change must be
  synced anyway, and it was not measured, so it was left alone.
