# 04 - Snapshots Off the Trading Thread

Milestone 20. Code: `WarmReplica::maybe_write_snapshot` and `SnapshotWriter` in
`src/exchange/warm_replica.rs`, `StreamReader::journal_only` in `src/exchange/event_stream.rs`, and
the removal of the periodic snapshot schedule from `ExchangeRuntime` in `src/exchange/runtime.rs`.

## The problem

A core snapshot is a checkpoint of the exchange's entire state: every order ever placed, every
execution in the per-user index, the books, the ledgers, and both sequence numbers. It is bound to an
exact position in the journal. On restart the exchange loads it and replays only the journal after
that position, instead of replaying all of history.

Until this milestone, the primary wrote one every 10,000 commands on its trading thread. The worker
serialized the whole state to JSON, wrote it, and synced it, and trading stopped while it did:

```text
trading thread:  [orders][orders][orders][ SNAPSHOT 0.5-1.5 s: no orders at all ][orders]...
                                          ^ every 10,000 commands, and longer each time
```

Two facts made it the biggest remaining cost after milestone 19:

- **It grows with history.** State includes every order ever placed, so each snapshot is bigger than
  the last. At 200,000 orders it was 82 MB and took up to about 1.5 s.
- **It blocks everything.** No order, cancel, or read is answered during a snapshot. Every order that
  arrives waits for it to finish, which shows up as the tail of the latency distribution.

Measured with production snapshots on, the exchange managed about 8,300 orders/s, against about
26,000 with none.

## The idea

Take the snapshot somewhere else. The warm replica already rebuilds the same core from the same
committed batches and checks every one by deterministic replay, so it holds exactly the state the
primary would snapshot. Let it write the snapshot, and let the primary only read it when it starts.
This is documented practice elsewhere:

- In Raft, every server snapshots independently.
- Aeron's "standby snapshots" are taken by a standby node so the active members are "not delayed".
- Redis forks a child process to write its snapshot, so the main process never does the disk work.

```text
before:  primary trading thread ── writes snapshot every 10,000 commands (trading stops)

after:   primary trading thread ── never writes a snapshot while trading
                                   (one snapshot at startup, before it accepts orders)
         warm replica ─ follows the journal ─ replays each batch ─ every 10,000 commands
                        writes the snapshot at exactly the position it has applied
```

## How it works

1. **The primary stops.** `ExchangeRuntime` no longer has a snapshot schedule. It still writes one
   snapshot at startup, after recovery and before the listener binds, so the next restart does not
   replay the whole journal. After that, the trading thread never writes a snapshot.
2. **The warm replica writes them.** After each batch it has replayed and checked (`follow_once`), it
   counts one command. Every `EVENT_SNAPSHOT_INTERVAL` commands (default 10,000) it calls
   `maybe_write_snapshot`. That records the boundary from its *applied checkpoint* (journal device
   and inode, the byte where the next record starts, and the next event sequence) and writes the
   core it has at that same moment. State and position are captured together, so the snapshot
   always describes exactly the journal up to its boundary. etcd once shipped a bug where these two
   drifted apart.
3. **Same file, same format, same safety.** The file is the same `EXCHSNP1` snapshot, written the same
   way: private temporary file, sync, atomic rename, sync the directory. The primary's startup code
   is unchanged. It validates the snapshot against the journal and replays only the later part.
4. **Primary and warm replica can both write the file.** The primary writes at startup and the warm
   replica writes periodically. Each writes its own uniquely named temporary file and renames it
   into place, so a reader always sees one complete snapshot. If an older one lands last, the next
   restart simply replays a little more journal. Any valid snapshot is safe; only freshness varies.
5. **Never a snapshot of the wrong journal.** Before each write the warm replica checks that the
   journal path still names the journal it follows (device and inode). If the file was moved aside
   and replaced, it writes nothing, instead of covering the new journal's valid snapshot with one
   that every restart would refuse.
6. **A bad snapshot is still preserved.** If the snapshot file is invalid when the warm replica
   starts, the warm replica rebuilds from the journal and writes **no** snapshots. That keeps the
   bad file for diagnosis, the same rule primary recovery follows. Removing the file re-enables
   snapshots on the next warm start.

### Why the warm replica now reads only the journal

The warm replica used to read most batches from the mmap cache, a fast copy in shared memory. It had
been deliberately built so that nothing read from the cache could become authoritative. A cache can
be structurally valid and still disagree with the journal, so promotion always rebuilt from the
journal instead.

A snapshot the primary loads on restart *is* authoritative: the primary replays only what comes after
it. So the warm replica now reads every batch from the durable journal (`StreamReader::journal_only`)
and uses the mmap stream only to learn how far the committed journal goes. Its core is therefore
built exactly the way journal recovery builds one, from the same bytes, with every output checked,
and its snapshots can be trusted like the primary's own. The existing test that plants a different
but valid batch in the cache now shows the warm replica ignoring it
(`warm_follows_and_promotion_rebuilds_from_the_journal_not_a_differing_valid_mmap_cache`).

## Results

The container's disk, 100 symbols, a snapshot every 10,000 commands. "Before" is the milestone 19
binary, with snapshots on the trading thread. "After" is the new binary with a warm replica running
beside it and writing the snapshots. Both share the same 6-vCPU VM and the same disk.

**Throughput, 200,000 orders at maximum rate:**

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Before | 8,244 orders/s | 8,385 orders/s | |
| After | **20,954 orders/s** | **22,633 orders/s** | **22,052 orders/s** |

About **2.6× faster** (8,300 to about 21,900 orders/s). For reference, the new primary alone, with no
snapshot writer anywhere, did 26,084 orders/s. Running the warm replica on the same machine costs
about 16%, because it shares the CPUs and the disk.

**Latency at a fixed 5,000 orders/s (100,000 orders), two runs each:**

| | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| Before | 19 / 16 ms | 291 / 221 ms | 655 / 547 ms | 745 / 641 ms | **1,040 / 702 ms** |
| After | 13 / 15 ms | **34 / 44 ms** | **252 / 118 ms** | **422 / 151 ms** | **442 / 170 ms** |
| Primary alone, no snapshots anywhere | 14 / 16 ms | 36 / 89 ms | 95 / 257 ms | 190 / 297 ms | 210 / 317 ms |

- p90 fell about **6×**, and the worst order waited 170–442 ms instead of 0.7–1 s. The periodic
  freezes are gone: the old binary even fell slightly behind the offered rate (4,756 orders/s on
  its first run) because of them.
- The "after" rows are mostly within the range of the no-snapshot rows. Most of the remaining tail is
  this virtual disk's own sync latency, not snapshots. The exception is run 1's p99.9 and max (422 and
  442 ms), a little above the no-snapshot runs' worst (297 and 317 ms). That is most likely the warm
  replica's large snapshot writes competing for the same disk as the journal's syncs. Putting the
  warm replica on its own disk or machine would remove that.

**Restart time** from the same 200,000-order journal, measured by starting a new warm replica
(it loads the snapshot and replays the rest before it reports ready):

| Start from | Ready after |
|---|---|
| The warm replica's latest snapshot, plus the short suffix | **3.9 s** |
| Nothing (full journal replay) | 25.2 s |

A primary starts faster still, because it reads only the journal after the snapshot, whereas a
follower first scans the whole journal to check its starting position. Restarts stay fast even
though the primary no longer writes periodic snapshots.

## What it costs: the work moved, it did not disappear

The snapshot is still O(history). It now slows the warm replica instead of trading:

- **At 5,000 orders/s** the warm replica keeps up. When the benchmark ended it was a few hundred
  events behind the primary.
- **At full speed (about 22,000 orders/s)** it falls behind. It wrote 21 snapshots, median 0.9 s and
  up to 1.6 s each, 17.6 s in total. When the benchmark finished it had applied only 56% of the
  journal, and it caught up 14.5 s later.

A lagging warm replica does not affect trading or durability. Two smaller costs remain. A `/promote` request waits for any snapshot write in progress (up to about 1.6 s at 200,000 orders, and growing with history), because the write runs on the follower thread. And after a promotion there are no periodic snapshots until a new warm replica is started. It means the newest snapshot is older,
so a restart replays a longer suffix, and a promotion (which already replays the full journal) is
unaffected. The real fix is to stop the state from growing forever, so a snapshot's size tracks
the current day rather than all of history. That is the trading-day boundary, the next planned
milestone.

## Tests

- `warm_replica_writes_snapshots_that_the_primary_restarts_from`: the primary processes five
  commands, and its snapshot file stays byte-for-byte what it wrote at startup. A warm replica with an
  interval of two then writes snapshots. The last one sits exactly at the fourth command's boundary.
  A restarted primary loads it, replays only the fifth command (2 envelopes), and ends with a core
  identical to the live one.
- `warm_replica_preserves_an_invalid_snapshot_and_writes_none`: with a garbage snapshot file and an
  interval of one, the warm replica catches up correctly and the garbage file is untouched.
- `warm_follows_and_promotion_rebuilds_from_the_journal_not_a_differing_valid_mmap_cache` (updated):
  the warm replica's core now matches the journal's batch, not the planted cache batch.
- All 138 unit tests pass.

## Options considered and rejected

- **Copy the core on the trading thread and write it on a background thread.** The copy is still
  O(history) and still stops trading, only for a shorter time.
- **`fork()` and let the child write it, as Redis does.** Forking a multi-threaded Rust process
  (Tokio's threads) is unsafe, and even Redis reports the fork itself stalling for up to a second
  on large datasets.
- **Snapshot less often, or only at night as LMAX does.** It reduces how often trading freezes but
  not how long each freeze lasts, and this exchange has no quiet period yet: there is no trading-day
  boundary.
- **A separate `--snapshotter` process.** It would be the same code as the warm replica minus
  promotion. Reusing the warm replica keeps one follower, and it matches the plan for the second
  machine, where the standby will take snapshots.
- **Keep periodic snapshots in the primary as a fallback when no warm replica runs.** Two writers on
  two schedules is more to reason about. Without a warm replica, a restart replays from the
  primary's own startup snapshot, which is correct, just slower after a long run.
