# 08 - Restarts Without Re-reading History

Milestone 23, Part 2. The reasons are in `docs/tasks/19-two-machines.md`; this file is the
measurement. The main code:
- the journal header and its id: `src/exchange/event_store.rs` (`JOURNAL_HEADER_LEN`,
  `journal_id_of`);
- the reader's checkpoint check: `StreamReader::open` in `src/exchange/event_stream.rs`.

## The problem

A reader keeps a checkpoint: the journal byte where its next record starts, and that record's first
sequence. Market data saves one with its state, the reporter in PostgreSQL, and the warm replica
gets one from the snapshot it starts from. When a reader starts again, it must make sure the
checkpoint still points at a record boundary of the same journal.

Until this part, `StreamReader::open` checked that by reading and decoding **every record from the
start of the journal** up to the checkpoint. A restart therefore cost the whole history, however
little was left to do. Milestone 22 bounded the state to one trading day, and the snapshot after an
open shrank to 0.7 MB, yet a warm replica starting from that snapshot still read the whole journal
first.

The checkpoint also named the journal by its file's device and inode. A copy of the journal at
another path, or on another machine, has a different inode. Milestone 23 needs exactly such a
copy to be accepted.

## The change

1. **The journal names itself.** A new journal's header is the magic `EXCHLOG2` and 16 random
   bytes: the journal id. Checkpoints, snapshots, the stream header and the reporter's checkpoint
   row name the journal by that id.
2. **A checkpoint is checked where it points.** The reader reads only the record at the
   checkpoint: it must be complete, pass its checksum, and begin with the checkpoint's next
   sequence. A checkpoint at the committed end must match the published last sequence instead.
   Nothing before the checkpoint is read.

## How it was measured

- **Machine:** the office Ubuntu machine: an i3-7100 with 2 cores and 4 threads, a SATA SSD, and
  release builds in the `stock-rust` container.
- **Journals:** five trading days of 200,000 orders each, the benchmark's defaults
  (`--bench --days 5 --orders 200000`). That is 1,000,000 orders and 3,662,307 envelopes in a
  788 MB journal.
  - **Before:** milestone 22's own journal from its Part 4 measurement.
  - **After:** a new journal of the same workload built by this part's binary. It is 16 bytes
    longer, because the header grew from 8 to 24 bytes.
- **The snapshot:** in both cases, day 6 was opened, and the warm replica's snapshot written right
  after that open (667 KB) is the one the warm replica starts from.
- **What was timed:** from starting each process until its `/health` answered.
- **Scripts:** `~/stock-scripts/m23-before.sh` and `m23-after.sh`.
  - The "before" journal was hard-linked, not copied. Its inode, which the old identity check
    needs, was unchanged, and nothing was copied.
  - Each configuration ran once.

## Results

| Startup | Before | After |
|---|---|---|
| Market data, restart from its saved checkpoint | 9,654 ms | **9 ms** |
| Warm replica, start from the 667 KB snapshot | 9,620 ms | **68 ms** |
| Market data, built from sequence 1 | 17,393 ms | 16,723 ms |
| Promotion, until the customer port answered | 18,350 ms | 18,885 ms |

- **Restarts.** Before, a restart re-read about 9.6 s of history even with nothing left to do,
  and that cost grew with every trading day. Now a restart costs loading the reader's own state
  plus what is new. That is about a thousand times less for market data, and 140 times less for
  the warm replica, whose start is now mostly loading its snapshot. It stays the same however long
  the journal grows.
- **Full builds and promotion are unchanged**, as expected. A build from sequence 1 must read
  everything. Promotion still re-reads and replays the whole journal, which is milestone 23's
  Part 3.

**A copy is the same journal.** The same files were copied to another directory, as onto another
machine (`~/stock-scripts/m23-copy.sh`):

| | Milestone 22 binary | This part |
|---|---|---|
| Warm replica on the copies | refused: "snapshot belongs to a different journal", then "stream and journal identities differ"; it did not start | ready after 67 ms from the copied snapshot |
| Market data from its copied state | not run | ready after 9 ms |

## What it does not check any more

The old check walked every record before the checkpoint, so each restart also re-verified the
whole prefix's checksums. Now a restart verifies only the record it starts at. Damage earlier in
the journal is found only by what reads that part again:
- a full replay, when there is no usable snapshot;
- a reader starting from the beginning, which is refused at the damage, as the test
  `a_checkpoint_is_checked_where_it_points_without_rereading_history` shows;
- a promotion, which still reads the whole journal until Part 3.

The primary's normal restart does not re-read it: with a snapshot, it validates only the records
after the snapshot. A reader resuming after the damage has already consumed and validated those
records, the first time it read them. Nothing scrubs the journal in the background; that is a known
limitation.

## Options considered and rejected

- **Keep the identity as device and inode, and add the id beside it.** A copy on another machine
  would still fail the device and inode check, so the id would have to win anyway.
- **Remember the checksum of the record before the checkpoint.** That would catch a journal that
  has the same id but has diverged before the checkpoint, which can happen only by copying files
  by hand. In the replication design, a reader never sees a record that is not on both machines,
  so its checkpoint can never point into a tail that a failover discards. It would also add
  fields to the checkpoint, the snapshot boundary and the reporter's row, for a case the protocol
  rules out. Part 5 revisits divergence with its epochs.
- **An index of record offsets, or one journal file per day.** Both bound the scan instead of
  removing it, and both add files to keep consistent. The checkpoint already says where to look.
