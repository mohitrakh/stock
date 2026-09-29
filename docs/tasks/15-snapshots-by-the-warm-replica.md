# Task 15 - Snapshots Written by the Warm Replica (Milestone 20)

## Goal

Stop the primary's trading thread from ever pausing to write a core snapshot, while keeping restarts
fast. The warm replica, which already rebuilds the same state, writes the snapshots instead.

The detailed explanation and all measurements are in
`docs/performance/04-snapshots-off-the-trading-thread.md`, and the raw output is in
`docs/performance/results/results-m20.txt` and `results-m20-followup.txt`. This file records why
the milestone was chosen, the decisions, and what is left.

## Why this was next

After milestone 19 the benchmark showed that production throughput was set by snapshots, not by
matching or the journal:

- Every 10,000 commands the trading thread serialized the whole exchange to JSON and synced it. At
  200,000 orders one snapshot was 82 MB and took up to about 1.5 s, and nothing traded meanwhile.
- The state includes every order and execution ever, so each snapshot was larger than the last.
- With snapshots on, the exchange did about 8,300 orders/s instead of about 26,000.

Research into how other systems handle this all pointed the same way:

- In Raft, every server snapshots independently, using copy-on-write so work continues.
- Aeron's "standby snapshots" are taken by a standby node so the active members are not delayed.
- Redis forks a child process to write the snapshot.
- LMAX snapshots at night, when trading is quiet.

Snapshotting on a follower is established practice, and this exchange already has the follower.

Alternatives ranked after this one:
- Making market data and the reporter keep up: important, but not on the trading path.
- A trading-day boundary to bound state: it needs product decisions, and it is what keeps follower
  snapshots small next.
- Replicating the journal to a second machine: recovery must be bounded first.

## What changed

1. **`ExchangeRuntime`** lost its periodic snapshot schedule (`SnapshotSchedule`,
   `maybe_write_snapshot`). It still writes one snapshot at startup, after recovery and before
   the listener binds. `recover_runtime_with_stream_and_snapshot` and
   `promote_replica_with_stream_and_snapshot` no longer take an interval.
2. **The warm replica** (`WarmReplica`) gained a `SnapshotWriter`. After every batch it has replayed
   and checked, it counts one command. Every `EVENT_SNAPSHOT_INTERVAL` commands (default 10,000) it
   writes the snapshot at exactly its applied checkpoint, and logs how long the write took. `main`
   passes the interval to `warm_replica::run`.
3. **`StreamReader::journal_only`**: the warm replica reads every batch from the durable journal and
   uses the mmap stream only for the committed watermark.
4. **`--bench`** lost `--snapshot-every`, because the primary has no schedule left to switch.
   Snapshot cost is now measured by running a warm replica beside the benchmark.
5. `ReplicaCore::snapshot` became available outside tests, since the warm replica now uses it.

## Decisions

**Who writes: the warm replica, not a new process.** A dedicated snapshotter would be the warm
replica minus promotion. Reusing it keeps one follower, and it matches the plan for the second
machine, where the standby takes the snapshots.

**The warm replica reads the journal, never the mmap cache.** Until now the warm replica's state was
advisory: promotion rebuilds from the journal precisely because a cache can be valid yet different.
A snapshot is different: on restart the primary trusts everything before its boundary and replays
only what follows. So the state it captures must come from the journal's own bytes, each batch
checked by deterministic replay, exactly as primary recovery builds it. Reading the journal costs a
few system calls per record, and the pages are already in the OS cache. The existing test that plants
a valid but different batch in the cache now shows the warm replica ignoring it.

**State and position are captured together.** The boundary (journal device and inode, byte offset,
next event sequence) comes from the warm replica's applied checkpoint, which advances only after a
batch has been replayed and checked, in the same step that writes that core. etcd shipped a bug
where these two drifted apart. Here they cannot, because both are read on the one follower thread at
the same moment.

**The primary keeps its startup snapshot.** Without it, a restart after running with no warm replica
would replay the whole journal. With it, a restart replays at most one uptime's worth of journal,
and usually just the suffix after the warm replica's latest snapshot.

**Two writers of one file are safe.** The primary writes at startup and the warm replica writes
periodically. Each uses a uniquely named temporary file and an atomic rename, so the file is always
one complete snapshot. If an older snapshot replaces a newer one, the only effect is a slightly
longer replay; any valid snapshot is correct.

**An invalid snapshot is still preserved.** If the file is invalid when the warm replica starts, the
warm replica rebuilds from the journal and writes nothing, keeping the evidence exactly as primary
recovery does.

**No fallback periodic snapshots in the primary.** Two writers on two schedules is more to reason
about, for a setup (running without a warm replica) that is already correct, just slower to restart.

## Results in short

| | Before | After |
|---|---|---|
| Throughput, 200,000 orders on disk, snapshots every 10,000 commands | ~8,300 orders/s | **~21,900 orders/s** (2.6×) |
| Latency at 5,000 orders/s: p90 | 221–291 ms | **34–44 ms** |
| Latency at 5,000 orders/s: max | 0.7–1.04 s | **170–442 ms** |
| Restart from the latest snapshot vs full replay | — | 3.9 s vs 25.2 s |

For reference, the primary alone with no snapshots anywhere did 26,084 orders/s, with a p99 of
95–257 ms at 5,000 orders/s. The warm replica on the same machine costs about 16% in throughput,
because it shares the CPUs and the disk.

## What is left

- **The snapshot is still O(history).** It now slows the warm replica instead of trading. At 5,000
  orders/s the warm replica keeps up. At full speed it wrote 21 snapshots (median 0.9 s, up to 1.6 s),
  was 56% through the journal when the benchmark ended, and caught up 14.5 s later. A lagging warm
  replica means an older snapshot, and so a longer replay on restart. Trading and durability are
  unaffected.
- **Bounding the state is the real fix.** With a trading-day boundary, day orders expire at the close
  and terminal orders can leave the core, so a snapshot's size tracks the current day rather than all
  of history. That is the next planned milestone.
- **Same-host sharing.** The warm replica's snapshot writes share the disk with the journal's syncs,
  and probably cause part of the remaining latency tail. The planned second machine separates them.
- **Promotion waits for a snapshot write in progress**, up to about 1.6 s at 200,000 orders, because
  the write runs on the follower thread. Bounded state shrinks this too.
- **Promotion still replays the full journal.** Promoting from the warm replica's own snapshot plus
  the journal suffix is now possible, and would make failover much faster. It was left for the
  failover milestone.
- **Without a warm replica**, snapshots are taken only at primary startup.

## Verification

- `cargo fmt` is clean. `cargo test --locked` passes 138 unit tests (136 before, plus 2 new) and the
  integration tests. The release build still reports the same 13 warnings.
- New tests:
  - `warm_replica_writes_snapshots_that_the_primary_restarts_from`: the primary writes no snapshot
    while trading; the warm replica writes one at the exact boundary; a restart replays only the
    suffix and matches the live core.
  - `warm_replica_preserves_an_invalid_snapshot_and_writes_none`.
- Updated test: `warm_follows_and_promotion_rebuilds_from_the_journal_not_a_differing_valid_mmap_cache`.
- The measurements used the milestone 19 binary for "before" and the new binary with a live warm
  replica process for "after", with the same fixed-seed workload on the same VM.
- An independent read-only review found no correctness or safety defect: a warm-written snapshot
  always matches the journal up to its boundary, and no mix of writers can publish an unsafe one.
  It did find gaps, now closed:
  - `journal_only` is applied in one place, so every way of opening the warm replica reads the
    journal. The differing-cache test now opens from a snapshot, the way production does.
  - A test now starts a warm replica from a non-empty, warm-written snapshot and keeps following.
  - A batch that fails replay is shown not to produce a snapshot.
  - The warm replica now refuses to write a snapshot when the journal path no longer names the
    journal it follows, so a swapped journal cannot leave snapshots stuck off.
  - Stale comments were corrected.
