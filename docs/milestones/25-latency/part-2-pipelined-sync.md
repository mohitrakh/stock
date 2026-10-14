# Part 2 — A pipelined journal sync

**Status: complete on 2026-10-14.** Code, tests, mutation check, two rounds of independent review,
the PostgreSQL acceptance tests, and the measurement in `performance/03-pipelined-sync.md`.

**Results in one paragraph.** The maximum rate rose by 41.5% alone (45,975 → 65,057 orders/s) and
by 36.7% replicated (35,297 → 48,256), on the office machine, three interleaved runs each, far beyond
the spread between runs. At 5,000 orders/s the mean latency fell by 10–28%, beyond the spread in
three of four configurations. At 1,000/s nothing changed beyond the spread, as theory expects. The
latency gain was larger than Part 1 projected, because orders no longer wait for the rest of a
large group to be staged. A first version that shipped every group to the replica at once was
broken replicated at moderate load; the measurement found it (see "A bug the measurement found").

## What this part is

Until now the worker did everything for a group in one sequence: stage it, write it, sync it, wait
for the replica, publish it, answer it, and only then take the next group. During the sync and the
replica's confirmation it did nothing. Part 1 measured that at the maximum rate one group's cycle is
22.17 ms alone, of which the sync is 5.98 ms, and 28.97 ms replicated, of which the sync and the
confirmation are 12.79 ms (`performance/02-where-the-time-goes.md`).

This part moves the sync, and the wait for the replica, to a second thread: the **journaler**. The
worker writes a group and goes on to stage the next one while the disk syncs. Durable before visible
still holds: a group is released (published, its callbacks run, its replies sent, reads included)
only once the disk holds it, and with replication the replica's disk too, and groups are released in
queue order.

The owner confirmed this part on 2026-10-14 ("go ahead with next steps of our current milestone"),
knowing from Part 1 that it mainly raises throughput and helps latency only a little at moderate
load (`decisions.md`).

## The design, and why

### What the worker does now

```text
worker:     take G1, stage, write ─┐ take G2, stage, write ─┐ release G1 │ take G3 ...
journaler:                         └ sync (covers G1) ──────┘└ sync (covers G2) ─┘
```

The worker's loop (`ExchangeRuntime::pipeline` in `src/exchange/runtime.rs`):
1. **Release** every group in flight that the disk now holds, oldest first (`release_durable`).
2. If `MAX_UNDURABLE` (2,048) commands are waiting for the disk, wait until a sync ends, and go back
   to 1. This is the pipeline's depth.
3. **Take** the next commands:
   - with nothing in flight, block on the queue, as before;
   - with something in flight and commands queued, take them;
   - with something in flight and nothing queued, wait for whichever comes first: a command, or a
     sync ending. A sync ending goes back to 1.
4. **Stage** the group: the same code as before, now `stage_group`. Each write is prepared, encoded,
   and committed in memory; each read is answered from the core at its place; every reply is held.
5. **Write** the group's records with one `write_all` (`write`), note where the journal now ends
   (the group's `end`), and tell the journaler that end and the journal sequence after it.
6. Put the group in flight and go back to 1.

The journaler's loop (`JournalShared::run`):
1. Wait until the worker has written past what is durable.
2. Take the journal's end as the worker last wrote it. With replication, hand that end to the
   replication link, which ships every record up to it to the replica while this disk syncs.
   `sync_all`, then with replication wait for the replica to confirm that end. This is
   `make_durable`, the same function the worker uses on its own path.
3. Record it as durable and wake the worker.

The records go to the replica **once per sync, not once per group**. A first version shipped each
group as soon as it was written, and the measurement caught it: see "A bug the measurement found"
below.

### One sync covers everything written before it began

This is the key choice. The obvious design is "one sync per group, in order": the worker hands
group N to the journaler, which syncs it, then group N+1, and so on. At moderate load that is
**worse than no pipeline at all**. A worked example at 5,000 orders/s (an order every 0.2 ms) with
syncs of 1.3 ms, in that obvious design with at most two groups in flight. The 1.3 ms is
illustrative, chosen near the raw sync mean of 1.31–1.39 ms that milestone 24 measured
(`00-selection.md`); Part 1 measured syncs of 2.3 ms on average at 5,000 orders/s, when they
carry 16.6 orders each. The conclusion does not depend on the value:

| Time (ms) | What happens |
|---|---|
| 0 | Order A arrives, is staged and written as group 1; its sync starts. |
| 0.2 | Order B arrives. Group 1 is syncing, one slot is free: B is group 2, queued for the next sync. |
| 0.4 | Order C arrives. Both slots are full: C waits in the queue. |
| 1.3 | Group 1 is durable and released. Group 2 (B alone) starts its sync. C and the 4 orders behind it become group 3. |
| 2.6 | Group 2 is durable. Group 3's sync starts. |
| 3.9 | Group 3, with C, is durable. C waited 3.5 ms. |

Without a pipeline, C would have been in the group taken at 1.3 ms and synced by 2.6 ms: 2.2 ms. The
tiny group 2 used a whole sync for one order, and everything behind it waited a full sync longer.

`sync_all` makes durable everything written to the file before the call, whoever wrote it. So the
journaler does not sync groups; it syncs **the journal up to where the worker has written**. The same
example with that rule:

| Time (ms) | What happens |
|---|---|
| 0 | A is staged and written; a sync starts, covering the journal through A. |
| 0.2, 0.4, ... | B, C and the others are staged and written as they arrive. The worker is no longer waiting for the disk. |
| 1.3 | The first sync ends: A is released. A new sync starts at once, covering everything written by now, B, C and the rest. |
| 2.6 | That sync ends: B, C and the rest are released. C waited 2.2 ms. |

C waits exactly as long as without a pipeline, the staging no longer lies between a sync's end and
the next sync's start, and at the maximum rate the worker never waits for the disk while the next
group stages. This is the classic "group commit with a flusher" arrangement of database logs: the
writer appends, and a flusher syncs as far as the log has grown.

### The depth: commands, not groups

Decision 2 fixed the depth at 2: "one group syncing while the next is staged; the worker waits if
both slots are full". With one sync covering everything written, a "slot" is not a worker group: the
worker may write several small groups while one sync runs, and they all ride the next sync. Counting
worker groups would bring back the problem above, with tiny groups using up the slots. So the depth
is counted in **commands**: at most `MAX_UNDURABLE` = 2 × `MAX_GROUP` = 2,048 commands may wait for
the disk, which is two full groups. The worker also never takes more than the room left
(`MAX_GROUP.min(MAX_UNDURABLE - in flight)`).

What the bound protects:
- **Memory.** Each command in flight holds its record, its executions and its reply.
- **The failure rule.** The in-memory core is at most 2,048 commands ahead of the disk.

At the maximum rate the bound is reached only if the disk is slower than staging. Part 1's cycle
says it is not: a sync of 5.98 ms against 13.02 ms of staging per full group. So the worker should
rarely wait there. The new `PipelineFull` stage measures how often and how long it does.

### Waiting on the queue and the journaler at once

When groups are in flight and nothing is queued, the worker must wake for whichever comes first. If
it waited only for the sync, an order arriving meanwhile would not be staged and written until the
sync ended, and it would miss the next sync: the timeline above would fall back to the sequential
one. If it waited only for the queue, a quiet exchange would never release a group whose sync had
ended.

The queue is a Tokio `mpsc` channel. The worker thread now owns a **current-thread Tokio runtime**
used only to wait (`waiter.block_on(select!(queue, journaler))`). It does no work of its own: it parks
the thread until the queue or the journaler wakes it. The journaler signals with a
`tokio::sync::Notify`. `notify_one` stores a permit if nobody is waiting, so a sync that ends just
between the worker's check and its wait is never lost: the wait then returns at once.

Rejected:
- **A new `ExchangeCommand` variant** that the journaler sends into the command queue. It would put
  an internal signal into the gateway's type, which every match on it would have to handle.
- **Polling** (try the queue, sleep briefly, check the journaler). It adds up to the sleep's length
  to every wait, or burns CPU.

### Running alone

With replication, `Replication::confirm(end)` returns true when the operator's "run alone" let a sync
through before the replica held it. The stream's header must then be synced after publishing, so
that the primary's own record of what it acknowledged survives a power loss (milestone 23 part 5).
One sync can now cover several groups, and the worker can release groups covered by two syncs at
once. So the journaler records `alone_until`, the end of the last sync that running alone let through,
and the worker syncs the header when it releases a group at or before that end, which it had not
released before (`alone_until > in_flight.released`).

### The failure rules

The rule since milestone 19 is that after a failed sync the core is never used again: the worker
halts and recovery rebuilds from the journal. With the pipeline:

- **A failed sync.** The journaler records the failure and stops; it never syncs again, because
  after a failed `fsync` the disk's state is unknown. The worker answers *every* command in flight
  with the halt message (a read is dropped, which the gateway reports as unavailable), publishes
  none of them, and halts. That is up to 2,048 commands. Some of them may in fact be on the disk;
  none was acknowledged, so the restart is free to keep or lose them. The journal alone decides.
- **A failed publication.** As before, the group's commands are answered with the halt message
  (they are durable, so a restart publishes them). Now any later group, in flight or released in
  the same pass, is failed too: publishing it past the gap would break the stream. Groups released
  in the same pass but published before the failure are answered normally, except running alone:
  an acknowledgement then needs the stream's header synced after publishing, which a failed pass
  never reaches, so every group of the pass is answered with the halt message. The same holds if
  the header sync itself fails.
- **A command that cannot be staged.** As before, it is answered at once and ends its group: the
  commands before it go on to the disk, those after it are dropped. Then the worker **drains**: it
  waits until every group in flight, including that valid part, is durable, releases them, and
  halts.
- **A group the disk refuses to write.** The groups before it may still become durable: the worker
  drains them, then answers that group with the halt message, and halts.
- **The queue closing** (every sender gone, at shutdown). The worker drains and returns.

On a halt the worker drops the journaler, which stops it. It is not joined: with replication paused
it may be blocked waiting for a replica that never comes, and joining would block the halt. It holds
only a duplicate of the journal's descriptor. The journal's lock is released when the worker's own
store is dropped, which unlocks it explicitly (`EventStore::drop`).

### What did not change

- **The synchronous path.** Startup's term record (`start_pending_term`) and the tests use
  `commit_here`: write, sync on the worker's own thread, release. It is the same `write`,
  `make_durable` and `release` code the pipeline uses, in one sequence.
- **Replication's protocol.** As before, the records up to an end are handed to the link
  (`Replication::written`) just before the sync of that end, so they are shipped while the disk
  syncs, and then `Replication::confirm` waits for the replica. Both now run on the journaler,
  once per sync. `confirm` already took the highest end (`synced.max(end)`), so syncs covering
  several groups need no change there. The link's `written`, which `/replication` shows as
  `journal_end`, is now what has been handed to a sync, which can be less than what the worker
  has written; records beyond it are unsynced and unacknowledged, which the handshake already
  treats as never existing.
- **The order of the records on disk.** Groups are written in queue order by the one worker thread.
  Only the moment of the sync moves.
- **Group staging, encoding, publication and callbacks.** The same code, moved into `stage_group`,
  `write`, `release` and `publish`.

### A bug the measurement found: shipping to the replica per group

The first version kept the old place of `Replication::written`: the worker handed each group to
the replication link right after writing it, so the link shipped it at once. Every test passed,
including all replication tests, and the maximum rate replicated rose by 39%. But the first full
comparison (`results/m25p2-compare-shipping-per-group.txt`, 2026-10-14 12:46–13:15) showed
replicated runs at moderate load broken:

| Replicated, 3 runs each | Part 1, p50 | First Part 2, p50 | First Part 2, orders per primary sync |
|---|---|---|---|
| in-process 1,000/s | 8,880 µs | 2,231,978 µs (2.2 s) | 231.2 (Part 1: 4.5) |
| in-process 5,000/s | 11,375 µs | 612,692 µs | 686.3 (Part 1: 22.5) |
| HTTP 1,000/s and 5,000/s | 10,636 / 12,596 µs | 12 orders unanswered within the 120 s deadline in every run; in every run about half the requests or more could not even connect: 10,003–10,508 of 20,000 at 1,000/s, 58,073–62,035 of 100,000 at 5,000/s | — |

Alone, and replicated at the maximum rate, the same binary was fine.

**Diagnosis** (`results/m25p2-shipping-diagnosis.txt`): the timed build, replicated, 5,000 orders at
1,000/s. `Confirm` had a p50 of 80 µs but a p99 of 397,567 µs and a maximum of 797,183 µs. The
worker wrote 4,640 groups of 1.1 orders on average, while the primary synced only 466 times.

**Cause.** The replica (`src/exchange/replica.rs`, `follow`) reads one frame, appends its records,
**syncs**, and answers. It has no journaler of its own: it syncs once per frame. The link sends a
frame whenever the worker reports more written, so shipping per group gave the replica one frame,
and one sync, per group: about 4,640 syncs to the primary's 466 in that run, ten times as many, on
the same SSD. Each replica sync takes about as long as a primary one, so the replica could not
keep up. Frames queued in the socket, and the primary's `confirm` waited behind the backlog, for up
to 0.8 s. At the maximum rate, groups are full (1,024 commands), one group is one sync on both
machines, and nothing went wrong; that is why the smoke run and the maximum-rate runs looked fine.

In Part 1 this could not happen: the worker shipped a group, then synced it, then waited, so one
frame went out per sync.

**Fix.** Ship once per sync: the journaler hands the link the end it is about to sync, just before
the sync (`make_durable`). The replica again gets one frame per primary sync, still in time to sync
in parallel with the primary. With the fix, the same diagnosis runs gave p50 6,083 µs at 1,000/s
(2.2 orders per sync) and 9,999 µs at 5,000/s (25.1 orders per sync).

**The lesson.** A pipeline moves work between threads; anything downstream that does one expensive
step per message receives as many messages as the producer makes, unless someone coalesces them.
The primary's journaler coalesced writes into syncs; the replica, one hop further, did not. The
unit tests could not see it: they check order and durability, not how many syncs each side does.

**The test that now guards it:** `the_replica_gets_records_once_per_sync_not_once_per_group` holds
the first sync, writes a second group, and checks that the link's `journal_end` has not moved
until the next sync begins. The mutation check puts shipping back into `write` and requires the
test to fail.

The alternative, coalescing on the replica (read every frame that has arrived, append them all,
sync once, then answer), would also work and would help any other bursty producer. It changes the
replica, which this milestone does not otherwise touch; shipping per sync restores exactly Part 1's
traffic. Recorded as a possible later change.

## What changed in the code

### `src/exchange/runtime.rs`

- **`MAX_UNDURABLE`** = 2 × `MAX_GROUP` = 2,048: the pipeline's depth.
- **`run`** now calls `pipeline` after startup's `release_held` and `start_pending_term`. The old
  loop is gone.
- **`pipeline`**: the worker's loop described above.
- **`release_durable`**: releases every group the disk holds, oldest first. On a failed sync, fails
  everything in flight; on a failed publication, fails everything after it.
- **`drain`**: waits until every group in flight is released.
- **`stage_group`** (from the first half of the old `handle_group`): stages a group's commands,
  with the same stage timing as before.
- **`write`** (from the first half of the old `flush`): one `write_all` of a group's records, then
  the journal's end (`store.end()`, one `fstat`). *(Both paths.)* The
  un-replicated flush did not call `store.end()` before. Now every group needs its end, to know when
  it is durable. An `fstat` is one system call with no disk access; its cost is not measured
here, and is assumed (an estimate) to be a few microseconds against a sync of milliseconds.
- **`make_durable`**: with replication, hands the records up to the end to the link
  (`Replication::written`); then the sync, then the replica's confirmation; records `Sync` and
  `Confirm`. The journaler calls it with its own handle; the synchronous path calls it with the
  store's.
- **`release` and `publish`** (from the second half of the old `flush` and `handle_group`): publish
  each group's batches, run their callbacks, sync the stream's header if a sync ran alone, then
  answer every command.
- **`commit_here`**: one group, written, synced and released on the calling thread. `flush`
  (startup's term record) and the test-only `handle_group` use it.
- **New types:**
  - `Group`: its staged commands, held replies, `end`, and in the timed build its dequeue and write
    times;
  - `InFlight`: the groups written but not released, how many commands they hold, the journal's end
    as written, and the end of the last group released;
  - `Journaler`, `JournalShared`, `Durability`: the journaler thread and the state it shares with
    the worker (a `Mutex` with a `Condvar` the journaler waits on, and the `Notify` the worker waits
    on).
- **`fail_all`**: answers every command of some groups with the halt message.
- **Test-only:** `ExchangeRuntime::sync_gate`, two channels: before each sync the journaler sends
  the end the sync covers, then reads the answer: `true` lets it run, `false` fails it, and a
  closed channel lets every sync run. The pipeline tests hold syncs with it (`hold_syncs`).

### `src/exchange/event_store.rs`

`sync_journal(&File)`: one `sync_all` and the global sync counter (`JOURNAL_SYNCS`, which `--bench`
prints). `EventStore::sync` calls it, and so does the journaler with its duplicate of the
descriptor. `sync_all` (Linux `fsync`) makes the file durable, whichever descriptor asks.

### `src/exchange/stage_timing.rs`

- Two new stages:
  - **`WaitForSync`**, per command (reads and other commands too, like `Worker`): from its group
    being written to the worker beginning to release it. Unlike `Sync`, which is per sync, it
    weighs a large group by its many commands. Part 1 could
    not explain 10–30% of `Worker` below the maximum rate, and suggested that per-group stages
    under-count what an average order waits (`02-where-the-time-goes.md`, "Inside the worker"). This
    stage measures the per-order wait directly. For a group of reads, which writes nothing, it is
    measured from the group's dequeue.
  - **`PipelineFull`**, per wait: the worker stopped because 2,048 commands waited for the disk.
- The meaning of three stages changes:
  - `Sync` and `Confirm` are now **per sync**, not per group, and run on the journaler thread,
    which merges its samples into the report after each sync;
  - `Sync`, replicated, no longer includes `store.end()` and `Replication::written`: the first
    moved into `write`, the second just before the sync's timer starts (Part 1 noted that both
    were inside it);
  - `Publish` and `Release` are per group, as before, but several groups can be released in one
    pass.
- The module's comment said that stage *medians* add up. They do not; means do. Corrected.

## Tests

All run on the office machine on 2026-10-14, inside `stock-rust` (cargo 1.98.1), from the tree in
`~/stock-m25p2`.

| Test | What it proves | Result |
|---|---|---|
| `runtime::tests::the_next_group_is_written_during_a_sync_and_nothing_shows_before_its_own_sync` | Through the real worker loop (`run`), with the journaler's syncs held by the gate: a deposit is written; its sync is held; a second deposit is **written while the first sync is held** (the pipeline works); a balance read is sent after it. Nothing is answered or published. Letting one sync through answers and publishes only the first deposit: the second deposit and the read, which saw it, stay unanswered. The second sync answers both, and the read shows 30. | pass |
| `runtime::tests::a_failed_sync_with_a_group_in_flight_answers_neither_and_halts` | Two deposits written, the first's sync held, then failed. Both are answered "exchange unavailable", neither is published, the worker halts, and its queue is closed. The restart recovers from the journal. | pass |
| `runtime::tests::a_fault_behind_a_group_in_flight_releases_that_group_then_halts` | A share deposit is written and its sync held. A buy then hits an internal fault (a seller balance forced to `u64::MAX`): it is answered at once, but the share deposit is not answered until its sync, after which the worker halts. The restart has the share deposit and not the buy. | pass |
| `runtime::tests::at_most_two_groups_wait_for_the_disk` | 2,053 deposits queued before the worker starts, syncs held: the worker writes exactly 2,048 records and, 200 ms later, still 2,048. Opening the gate answers all 2,053, and the balance is 2,053. | pass |
| `runtime::tests::the_replica_gets_records_once_per_sync_not_once_per_group` (added after the measurement found the shipping bug) | A replicated runtime with no replica connected and its syncs held. The link's `journal_end` equals the end the first held sync covers, and stays there while a second group is written behind it; it moves only when the next sync begins. Running alone then lets both syncs finish and both deposits be answered. | pass |
| The 4 group-commit tests from milestone 19 (`a_queued_group_shares_one_sync_and_its_reads_see_earlier_writes`, `a_failed_group_sync_answers_every_command_unavailable_and_publishes_nothing`, `a_fault_mid_group_still_syncs_the_commands_before_it_and_drops_those_after`, `publication_failure_halts_worker_but_recovers_the_committed_command`) | Unchanged. The first three drive `handle_group`, now `stage_group` + `commit_here`, the same parts the pipeline runs. The fourth drives `run`, so the pipeline, end to end. | pass |
| The replication tests (`replication::tests`) | Unchanged. Every one that runs a primary starts it with `run` (the `Machines::primary` and `Machines::promoted` helpers), so through the pipeline and the journaler's `confirm`: a command is answered only once the replica holds it; a lost replica pauses the primary; running alone; terms and fencing. | pass |
| `runtime::tests::a_group_records_every_worker_stage` (feature build) | Now also requires `WaitForSync`. | pass |
| **Mutation check**, `performance/scripts/m25p2-mutate.sh`, output `performance/results/m25p2-mutate.txt` | Each new test, run alone, on the real tree (control), then on copies each missing one guarantee (below). | see below |
| The whole suite, normal build: `~/stock-scripts/check.sh` | First run (before the review and the shipping fix): 217 passed. Final run, about 14:06–14:08 (after the measurement), with every fix: | 218 passed, 1 ignored (213 before, plus the 5 new tests) |
| The whole suite, feature build: `cargo test --locked --features stage-timing` | First run: 220 passed. Final run: | 221 passed, 1 ignored |
| `cargo fmt -- --check` | Twice it asked for reflowed lines in `runtime.rs` (six, then two); reflowed. | clean |
| Release build warnings | | 12, as before |
| PostgreSQL acceptance tests: `REPORTER_TEST_DATABASE_URL=postgres://postgres:bench@stock-test-pg:5432/reporter_m25p2 cargo test --locked --test reporter -- --ignored`, on a fresh database in `stock-test-pg` | The reporter, unchanged, still applies the journal correctly, including rollback and restart. | 3 passed |

The final run of the whole suite, both builds and the acceptance tests, on the tree with every fix
including the second review's: `performance/results/m25p2-final-tests.txt`.

The mutations, each against the one test that covers it:

Command: `TREE=~/stock-m25p2 bash ~/stock-scripts/m25p2-mutate.sh` (the script's default tree,
`~/stock-m25`, is the git checkout, which has Part 2 only once it is committed). A first run at
12:43, with the four tests and five mutations then existing, gave the same results for those. The
final run, 2026-10-14 13:24, on the tree with the shipping fix and the new test: raw output in
`performance/results/m25p2-mutate.txt`, below.

| Mutation | Covering test | Must | Result |
|---|---|---|---|
| Control: the real tree, each of the 5 tests alone | — | pass | 5 × ok |
| Each group shipped to the replica as soon as it is written (the first version's bug) | `the_replica_gets_records_once_per_sync...` | fail | FAILED (0.02 s) |
| Release groups without waiting for their sync (`take_while(\|_\| true)`) | `the_next_group_is_written_during_a_sync...` | fail | FAILED (0.11 s) |
| No pipeline: the worker drains (waits for the disk) after every group | `the_next_group_is_written_during_a_sync...` | fail | FAILED (10.01 s: the second deposit is never written during the first sync, and the wait times out) |
| A failed sync is recorded as durable | `a_failed_sync_with_a_group_in_flight...` | fail | FAILED (0.01 s) |
| A fault halts at once, without draining the group in flight | `a_fault_behind_a_group_in_flight...` | fail | FAILED (0.02 s) |
| No bound on the commands waiting for the disk | `at_most_two_groups_wait_for_the_disk` | fail | FAILED (10.01 s: the worker writes past 2,048, and the wait for exactly 2,048 times out) |

## Independent review

Two reviewers ran on 2026-10-14: one on the Rust code, one on the scripts, the awk and the
documents. Neither found a path that shows a command before it is durable, or out of order. The
code reviewer traced and confirmed the `Notify` and `Condvar` wake-ups, the `end` / `durable` /
`released` / `alone_until` arithmetic, the in-memory runtime, and that `MAX_UNDURABLE - commands`
cannot underflow. The other ran each mutation's replacement against a copy of `runtime.rs` (each
applies once and changes only what its label says) and tested the awk.

Findings, and what was done:
1. **(plausible, medium) A test could be flaky.** `the_next_group_is_written_during_a_sync...`
   waited until the first deposit was *in the file*, then sent the second, and assumed the held
   sync covered only the first. But the journaler reads the end to sync only when it wakes. On a
   slow machine it could wake after the second deposit was written, sync both at once, and the
   test would fail although the code is right. **Fixed:** the test gate now also reports the end
   each held sync covers (`hold_syncs` returns it). The test waits for that report, checks that it
   is the journal's length with the first deposit alone, and only then sends the second; it also
   checks that the next sync covers more. The failed-sync test does the same, so its second
   deposit really is in flight behind the first sync.
2. **(confirmed, low) A failed publication answered too much.** When one pass released several
   groups and publication failed in a later one, the earlier groups, already on the stream, were
   also answered "unavailable". Nothing was shown early, but the answer was wrong for them, and
   the documents said otherwise. **Fixed** in `release`: each group remembers whether it was
   published; only the group that failed and those after it get the halt message, unless the
   header sync fails, which fails the whole pass. No test covers the multi-group case: it needs a
   publication failure after the first of several groups released together, which the stream's
   test hook (fail every publication from now on) cannot produce.
3. **(confirmed, low) Documents ahead of the facts.** This file claimed a finished mutation check
   and a measurement file before either existed; `SYSTEM_DOCUMENTATION.md` linked the missing
   measurement file and still gave Part 1's test counts; `WaitForSync` was called "per order"
   although every command records it; the worked example's 1.3 ms sync and the cost of an `fstat`
   had no source. All corrected above; the README, `PROJECT_DIRECTION.md`,
   `EXCHANGE_PIPELINE_TODO.md` and `SETUP.md` are updated when the part closes.
4. **(confirmed, low) `m25p2-summarize.awk` divided a stage's sums by the number of runs even when
   the stage was missing from some runs** (a stage with no samples is left out of a report, as
   `PipelineFull` may be). Fixed: it divides by the runs that reported the stage.
5. **(plausible, low) `results/m25p2-smoke.txt` is an excerpt**, assembled from a manual run's
   terminal output, not a script's raw file. Its header now says so.

Both fixes (1, 2) change only failure handling and tests; on the normal path the only change is
one boolean kept per released group.

**Second round**, after the shipping fix (one reviewer, on the fix, the new test, the mutation and
the documents). It confirmed `next_sequence` always matches `end` (including after a staging fault
mid-group), that `Replication::written` never goes backwards, that the handshake still sees a
record boundary at `written`, and that the gate tests are deterministic. Findings:
1. **(confirmed, medium) Running alone, fix 2 above let commands be acknowledged without the
   header sync.** If a pass running alone published its first group and then failed on the second,
   the first group's commands were answered normally, but the stream header (this primary's record
   of what it acknowledged alone) was never synced. **Fixed:** running alone, a failed publication
   or a failed header sync answers the whole pass with the halt message, as before fix 2. It is on
   a failure path only. The measured binaries were built before it; the final test run includes it.
2. **(confirmed, low) Documents:** the connection-error counts, the comparison's end time, the
   mutation table (the new mutation had not run yet), the stale test counts in
   `SYSTEM_DOCUMENTATION.md`, and the claim that only one boolean changed on the normal path (the
   shipping fix also moved `Replication::written`). All corrected.

**Not covered by any test** (checked by reading the code): a write failure with groups in flight
(the drain before the halt), that the journaler never syncs again after a failure, and a
publication failure after the first of several groups released together, or while running alone.

## Compatibility

- **The journal, the stream, the snapshot, the replication protocol:** unchanged. A Part 2 primary
  writes byte-identical records in the same order; only the moment of the sync differs.
- **Behaviour seen by a client:** unchanged rules. A reply still means the command is durable (and
  replicated). A reply can now come back slightly sooner or later than before, depending on load;
  that is what `performance/03-pipelined-sync.md` measures.
- **After a crash:** as before, the journal decides. The window of written but unsynced commands is
  now up to 2,048 commands rather than one group of up to 1,024. None of them was acknowledged.
