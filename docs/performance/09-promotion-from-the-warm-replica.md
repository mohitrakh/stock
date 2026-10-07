# 09 - Promotion From the Warm Replica

Milestone 23, Part 3. The reasons are in `docs/tasks/19-two-machines.md`; this file is the
measurement. The main code:
- promotion: `WarmReplica::try_promote` in `src/exchange/warm_replica.rs`, `EventStore::open_suffix`
  in `src/exchange/event_store.rs`, and `promote_replica` in `src/exchange/runtime.rs`;
- the snapshot rule: `WarmReplica::maybe_write_snapshot`;
- the reader: `StreamReader::next_batch` in `src/exchange/event_stream.rs`;
- the checksum: `crc32` in `src/exchange/event_store.rs`.

## The problem

A failover needs two things from the warm replica: to be close behind the primary, and to take
over quickly. On the five-day workload, at the benchmark's maximum rate, it had neither:
- **Promotion threw the warm replica's core away.** It re-read the whole journal, and the new
  primary replayed all of it from an empty core: 25.8 s at the end of the fifth day, more with
  every day.
- **The warm replica fell behind and stayed behind.** It wrote a snapshot every 10,000 commands.
  A snapshot costs about its size, and the size grows through the day, so a day's snapshot work
  grew with the square of its length. The lag grew for the whole run, to 2.4 million events, and
  the warm replica needed 85 s after trading stopped to catch up. Even at 20,000 orders/s, two
  thirds of the maximum, it fell behind the same way.
- **A promotion could not start before the warm replica caught up.** Its control port opens only
  after its first catch-up. Asked to promote the moment trading stopped, it answered after 90 s,
  and the customer port after 103 s.

## Where the warm replica's time went

Before changing anything, a build with timers added replayed the five-day journal in a warm
replica (`~/stock-scripts/m23p3-probe.sh`):
- **With snapshots only after each open:** ready after 27.9 s.
- **With a snapshot every 10,000 commands:** ready after 103.1 s. Its 107 snapshots took 71 s:

  | Part of a snapshot | Share |
  |---|---|
  | Building the state to write | 20% |
  | Serializing it to JSON | 21% |
  | Its CRC-32 checksum | 28% |
  | Writing the file | 3% |
  | Syncing the file to disk | 29% |

- **The CRC-32 went bit by bit:** 64 MiB took 313 ms, about 200 MB/s. It also checks every record
  a reader reads, and every record the primary writes.
- **The replay itself paid six system calls per record.** The reader took the stream's shared lock
  and released it, checked the journal's size, then seeked and read the record header and payload.
  A replay from memory, as promotion did, was about 9 s faster on the same journal.

## The changes

1. **Promotion reuses the warm replica's core.** It reads only the journal after the warm replica's
   applied checkpoint and replays those records into its core. No startup snapshot is written.
2. **Snapshots by journal growth.** After the snapshot right after each open, the next is due once
   the journal has grown by four times the last snapshot's size. The snapshot work is then
   proportional to the journal applied, however big the state gets.
3. **A reader consults the stream only when it reaches the last committed end it validated.**
   Records before that end are read with two positioned reads.
4. **The `crc` crate's table-driven CRC-32**, which sqlx already depended on: 64 MiB now takes
   40 ms, 7.8 times faster, with the same checksum values.

## How it was measured

- **Machine:** the office Ubuntu machine, an i3-7100 with 2 cores and 4 threads and a SATA SSD;
  release builds in the `stock-rust` container. The primary, the warm replica and the benchmark
  all share the 2 cores and the disk.
- **Workload:** five trading days of 200,000 orders each (`--bench --days 5 --orders 200000`):
  1,000,000 orders and 3,662,307 envelopes in a 788 MB journal. It ran at the maximum rate and at a
  fixed 20,000 orders/s. A warm replica started beside the benchmark as soon as the journal
  appeared, as in milestone 22.
- **Binaries:** "before" is Part 2's (commit `b2916d3`); "after" is this part's.
- **Lag:** the primary's committed sequence, from the stream header, was sampled every 0.2 s. It
  was compared with the warm replica's position at each snapshot it logged, and with its
  `/status` once that answered.
- **Promotion:** timed until the customer port answered. Run 1 promoted a caught-up warm replica.
  Run 2, a second benchmark, requested the promotion the moment the benchmark ended, retrying
  until the warm replica accepted it.
- **Scripts:** `~/stock-scripts/m23p3-measure.sh` (lag and promotion), `m23p3-replay.sh` (full
  replay) and `m23p3-restart.sh` (a restart at the end of the fifth day).
- **Noise:** each configuration ran once, unless noted. Throughput on this shared machine varies by
  about a fifth from run to run: 27,051 orders/s before and 29,774 after, at maximum rate.

## Results

**Promotion:**

| | Before | After |
|---|---|---|
| A caught-up warm replica, until the customer port answered | 25,816 ms | **58 ms** |
| Requested as trading stopped at maximum rate, until the customer port answered | 103,255 ms | **502 ms** |
| The same at 20,000 orders/s | 19,440 ms | **1,309 ms** |

A promotion now replays only what the warm replica had not applied. At 20,000 orders/s it waited
for the snapshot the warm replica was writing when trading stopped: 1.1 s of the 1.3 s. In an
earlier run with a growth factor of 2 it found none in progress and took 72 ms.

Part 2 measured 18,885 ms for the same "before" binary. That promotion came right after a sixth day
had opened, when the state was under 1 MB. Here it came at the end of the fifth day: among other
things, the promoted primary then wrote an 85 MB startup snapshot instead of one under a megabyte.

**The warm replica at maximum rate:**

| | Before | After |
|---|---|---|
| Snapshots over five days | 106, 87.2 s in total | 36, 11.0 s in total |
| Lag during trading | grew for the whole run, to 2.43 million events | swung within each day between 0 and 247,000 events |
| Behind when trading stopped | not yet serving: it had never caught up | 120,000 events |
| Catch-up after trading stopped | 85,275 ms | **1,662 ms** |

Within a day the lag rises while the big late-day snapshots are written, and falls back after the
next open. In one of the two runs with this rule the lows rose by about 50,000 events a day; in the
other they did not. On this machine, sharing 2 cores with the primary and the benchmark, the
maximum rate is right at the warm replica's limit.

**The warm replica at 20,000 orders/s:**

| | Before | After |
|---|---|---|
| Lag during trading | grew for the whole run, to 2.19 million events | median 0, at most 99,000 |
| Catch-up after trading stopped | 66,145 ms | 1,203 ms, the end of a snapshot |

**Replay and restart:**

| | Before | After |
|---|---|---|
| A warm replica replaying the five days, with snapshots only after each open | 29,119 ms | **19,157 ms** |
| A primary restart at the end of the fifth day, from the warm replica's last snapshot | 2,394 ms (milestone 22) | 2,878 ms |

The replay is 34% faster: about a third from the checksum and two thirds from the reader. The
restart at the end of the fifth day replayed 6.4 MB of journal after an 85 MB snapshot. With a
growth factor of 4, while snapshots are being written, a restart replays at most about four
snapshot sizes past the last one, plus whatever the warm replica had not applied yet: about 4 s at
worst on this machine while the warm replica keeps up. A growth factor of 2 left 27.8 MB to replay
there, and that restart took 2,969 ms.

## Choosing the growth factor

Both factors were measured at maximum rate and at 20,000 orders/s:
- **At maximum rate**, with a factor of 2 the warm replica never caught up during the run. Its lag
  reached 299,000 events, and it needed 2.3 s after trading stopped. With 4 it caught up within
  the first 6 s, stayed within 183,000 and 247,000 events in two runs, and needed 1.4 s and 1.7 s.
- **At 20,000 orders/s** both stayed bounded. With 2 the peaks were lower, 55,000 events against
  99,000, because each snapshot late in the day is smaller.
- **For a restart**, a larger factor replays more journal. Estimated from the snapshot sizes, the
  worst case is about 3 s with 2, and about 4 s with 4.

Four was chosen because the goal was to keep up at the full rate. `EVENT_SNAPSHOT_GROWTH` changes
it.

## What it costs

- **Promotion trusts the warm replica's core** instead of rebuilding from the journal. The core was
  built from the same file, with every output checked, on top of the snapshot the warm replica
  started from, which promotion trusts as a restart does. Promotion refuses a path that now names
  another file, and a journal shorter than what the stream published. It no longer re-reads the
  journal before the warm replica's position, so it would not notice that part being rewritten in
  place while keeping its length. Nothing may modify the journal.
- **No snapshot at promotion.** The warm replica's last one stays the restart point. Until a new
  warm replica starts, no snapshots are written at all, as before.
- **A promotion request still waits for a snapshot in progress,** because snapshots are written on
  the follower thread: up to about 2.5 s at the end of a day.
- **A failing snapshot is retried less and less often.** Each failed attempt costs about as much as
  a written snapshot, so the next waits for four times as much journal as it did.
- **Restarts replay a little more journal,** as above.
- **A reader behind the stream checks it less often.** It notices a broken stream when it reaches
  the last committed end it validated, not at the next record. The records it reads before then
  were committed when it validated that end, and they cannot change.

## Options considered and rejected

- **Write snapshots on a background thread.** The follower would stop only to build the state,
  about a fifth of a snapshot's time, and a promotion would not wait for a write. But which
  snapshots get written would depend on timing, and the writer would still compete for the same
  two cores. This is the next step if the warm replica must keep up at full rate with room to spare
  on this machine.
- **Read many records per system call.** That would save a little more of the replay, but it needs
  a buffer that must stay consistent with the watermark checks. Reading committed records
  directly, without consulting the stream, already removed four of the six calls.
- **Keep a fixed command interval, only larger.** Each snapshot late in the day would still cost
  85 MB, and early in the day restarts would replay more than they need to.
