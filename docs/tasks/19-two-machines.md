# Task 19 - Two Machines (Milestone 23)

## Goal

Survive the loss of a machine without losing an acknowledged command, and resume trading on the
other machine in seconds. The specification, with the owner's decisions, is the "23. Two Machines"
section of `PROJECT_DIRECTION.md`. Part 1, which closed the holes a second machine cannot fix, has
its own write-up: `docs/tasks/18-one-order-cannot-stop-the-exchange.md`. This file records the
remaining parts as each completes.

The parts, as split on 2026-10-06:
1. no command can stop the exchange (task 18);
2. a journal that names itself, and restarts that do not re-read history;
3. promotion from the warm replica's own state, and a warm replica whose lag stays bounded;
4. replication over TCP, with the primary waiting for the replica;
5. epoch-fenced promotion on the second machine, and the old primary rejoining as the replica;
6. measurement and failure tests.

## Why Part 2 came next

Two things stood between the exchange and a second machine, both measured on milestone 22's
five-day journal (788 MB, 1,000,000 orders):
- **Every reader restart re-read the whole journal.** A market-data restart took 9.7 s and a warm
  replica 9.6 s, even when they had almost nothing to apply. That cost grows by about 2 s with
  every 200,000-order day.
- **A journal was identified by its file's device and inode.** A copy on another machine has a
  different inode, so every snapshot and every subscriber checkpoint would be refused there. The
  milestone 22 binary, started on a copy of its own files, did not start at all.

Part 3 (fast promotion) builds on both: promotion will start from the warm replica's checkpoint,
which must be cheap to check and valid on the other machine.

## Part 2 - A journal that names itself

### What changed

- **The journal header.**
  - A new journal starts with the magic `EXCHLOG2` and 16 random bytes: the journal id
    (`JOURNAL_HEADER_LEN` = 24). The id is chosen once, when the journal is created, and never
    changes.
  - `EventStore` reads it on open and returns it with `journal_id()`, and `journal_id_of` reads it
    from any open journal.
  - A journal in the old format (`EXCHLOG1`) is refused with its own error, `OldFormat`, which says
    to start a new journal.
- **Everything that named the journal by device and inode now names it by its id:**
  - the stream header (`EXCHBUS2`; bytes 8 to 24, where device and inode were);
  - reader checkpoints (`ReaderCheckpoint { journal_id, next_sequence, byte_offset }`), and with
    them the market-data state file (version 3) and the probe's checkpoint file;
  - snapshot boundaries (snapshot format version 5);
  - the reporter's checkpoint row: column `journal_id UUID`, migration
    `20261006000000_reporter_journal_id.sql`, `report_version` 5;
  - the promotion fence (`EventStore::open_existing_matching(path, journal_id)`) and suffix recovery
    (`EventStore::open_suffix(path, journal_id, boundary)`). Both still compare the id while
    holding the writer lock, before reading past the header or repairing anything.

  A byte-identical copy of the journal at another path, as on another machine, is therefore the
  same journal.
- **A checkpoint is checked where it points.** `StreamReader::open` used to read and decode every
  record from the start of the journal to prove that a checkpoint was on a record boundary. Now it
  reads only the record at the checkpoint, which must be complete, pass its checksum and begin
  with the checkpoint's next sequence. A checkpoint at the committed end must match the published
  last sequence. A restart no longer depends on the journal's length.
- **Path aliasing still uses device and inode**, because that question really is about files:
  "is this snapshot path the journal itself?". Only the question "which journal is this?" moved to
  the id.

### Why the checkpoint check is enough

A checkpoint is written by the reader that consumed the records before it, and each of those
records passed its checksum and sequence checks at the time. On restart, the record at the
checkpoint can pass its checksum, its framing and an exact sequence match only if the checkpoint
really is the boundary it claims to be.

The old full scan also re-verified the whole prefix at every restart. That is now done only by
what reads the prefix again:
- a full replay, when there is no usable snapshot;
- a reader starting from the beginning;
- a promotion, which reads the whole journal until Part 3.

The primary's normal restart does not do it: with a snapshot it validates only the suffix. The test
`a_checkpoint_is_checked_where_it_points_without_rereading_history` damages an early record. A
reader resuming after the damage starts normally, and a reader starting from the beginning is
refused. Nothing scrubs the journal in the background; that is recorded as a known limitation.

### Compatibility

The journal format changed, so start a new journal, as milestones 9, 13 and 22 required:
- a journal from before this part is refused (`OldFormat`);
- an old stream file (`EXCHBUS1`), or one naming another journal, stops the primary from starting;
  delete it, since it is only a cache;
- an old snapshot (version 4) is refused for its version and preserved, startup replays the
  journal, and snapshot writing stays off until the old file is removed;
- an old market-data state file is refused for its version (3 is current); remove it with the old
  journal;
- an old probe checkpoint file no longer parses; remove it;
- apply the fifth reporter migration, which empties the report for a rebuild. A reporter from
  before it cannot save a checkpoint into the migrated table, and neither can a checkpoint inside
  the 24-byte header.

### Options considered and rejected

- **Keep device and inode, and add the id beside them.** A copy on another machine would still fail
  the device and inode comparison, so the id would have to override it anyway.
- **Put the id in a first journal record** instead of the header. That would add an event every
  subscriber must skip, and it would consume sequences. The header is where a file says what it is.
- **Derive the id from the first record.** Two journals that start with the same command would get
  the same id.
- **Store the checksum of the record before the checkpoint**, to detect a journal that kept its id
  but diverged. That can happen only by copying files by hand: replication will never let a reader
  see a record that is not on both machines. It would add fields to every checkpoint and to the
  reporter's row for a case the protocol rules out. Part 5's epochs deal with divergence.

### Found by the independent review

- **An older copy of the journal would have started trading.** The id cannot tell an older copy of
  the same journal from the journal itself. Suppose a shorter file from a backup is put in place.
  - **Before the fix:** the primary would have accepted it, published a smaller watermark over the
    stream, lost the acknowledged commands after the backup, and reused offsets that readers had
    already consumed. Device and inode used to refuse a file renamed into place.
  - **The fix,** two checks:
    - `StreamWriter::open` refuses to start when the existing stream already published more than
      the recovered journal holds. A record is published only after its sync, so that can only mean
      lost history.
    - Promotion refuses a journal whose complete records end before what the warm replica already
      applied (`EventStoreError::ShorterThanApplied`), before it cuts off a torn tail.
  - **What is still not caught:** an older journal restored without a stream file, with an
    unreadable stream header, or together with its own old stream file. That primary starts
    trading. Readers whose checkpoints lie beyond the restored end are refused when they restart,
    but only until the new history grows past their checkpoints. Restoring an older journal is
    unsupported: to go back, start a new journal. This is recorded as a known limitation.
- **A reporter acceptance test had stopped testing anything.** The test that closes with an order
  still resting in the report rewrote its journal, and the rewrite gave the journal a new id. The
  restarted reporter then stopped at the id check, before it reached the close. Now a rewrite keeps
  the id, and the three acceptance tests that expect the reporter to stop check its error output
  for the reason.
- **Smaller:**
  - an old snapshot is now refused for its version, rather than for a field that no longer parses;
  - new tests cover a checkpoint in the middle of the journal with the wrong sequence, a refused
    open that leaves a torn file untouched, and a real version 2 market-data state;
  - the docs no longer claim that the primary's normal restart re-reads the old part of the journal.

### Found by re-checking the fixes

The reviewer confirmed that the new stream check refuses no legitimate restart: not after a crash
before or during publication, not with a torn stream header, not on a promoted warm replica. It
also confirmed that no test still passes vacuously. Its smaller findings, all fixed:
- **Promotion compared the file's raw length.** Take an older copy whose torn last record reaches
  past the applied length: it passed the check, and recovery then cut the torn record off. Now the
  check uses the length of the complete records, still before anything is cut. The test covers a
  copy cut inside its last record, and one whose last record claims more bytes than it has.
- **The stream refusal named only one cause.** A disk that reports writes as synced without keeping
  them (some virtual machines ignore fsync) can lose published records in a power loss too. The
  message now names both causes. The operator notes say what starting anyway means: deleting the
  stream file and resetting every reader, since they already consumed the lost history.
- **The known limitation was incomplete.** A restored older journal is also missed when its stream
  file is gone or its header is unreadable. That is the likely case, because operators already
  delete the stream file whenever they replace the journal. The limitation now says so.
- **Two tests could miss a regression.**
  - Nothing showed that promotion passes the applied length.
    `promotion_refuses_an_older_copy_of_the_journal_it_followed` now does.
  - The stream test tripped both of its conditions at once. It now checks a journal behind only in
    length, only in last sequence, and in both, then that the whole journal still starts.

### Verification

Tests:
- Journal:
  - `a_journal_keeps_its_id_and_a_copy_is_the_same_journal`: the id survives reopening, a copy at
    another path opens as the same journal, and the promotion opener and suffix recovery refuse a
    different id;
  - `a_journal_from_before_journal_ids_is_refused_untouched_with_its_reason`;
  - the existing promotion tests now compare ids, and a foreign journal with a torn tail is still
    refused untouched.
  - `promotion_refuses_a_journal_shorter_than_what_was_applied_untouched`, including a copy whose
    torn last record reaches past the applied length;
  - the copy test also shows that a refused open leaves a torn file untouched.
- Warm replica: `promotion_refuses_an_older_copy_of_the_journal_it_followed`.
- Stream:
  - `a_checkpoint_is_checked_where_it_points_without_rereading_history`;
  - `a_checkpoint_is_valid_on_a_copy_of_the_journal`;
  - `a_journal_behind_what_its_stream_published_is_refused`: behind in length, in last sequence,
    and in both;
  - the checkpoint test refuses checkpoints in the middle of the journal (a wrong sequence, offsets
    inside records) and at its end (a wrong sequence, beyond the end), and one naming another id.
- Snapshot: `an_older_snapshot_is_refused_for_its_version`, and the identity test also loads the
  snapshot against a copy of the journal.
- Market data: a real version 2 state file is refused for its version.
- Process and acceptance tests: the hand-encoded fixtures (probe, market data, warm replica,
  reporter) write the new headers. The reporter acceptance test applies five migrations, and it
  checks that a milestone 22 reporter, and a checkpoint inside the header, cannot save into the
  migrated table.

`cargo fmt -- --check` is clean. `cargo test --locked` passes: 172 unit tests and the executable
integration tests. All three PostgreSQL acceptance tests pass, and each that expects the reporter
to stop now checks the reason in its error output.

Live, on the office Ubuntu machine (`~/stock-scripts/m23-before.sh`, `m23-after.sh`,
`m23-copy.sh`), the same five-day workload. The measurement is
`docs/performance/08-restarts-without-rereading-history.md`.

| Startup | Before | After |
|---|---|---|
| Market data, restart from its saved checkpoint | 9,654 ms | 9 ms |
| Warm replica, start from the 667 KB snapshot | 9,620 ms | 68 ms |
| Market data, built from sequence 1 | 17,393 ms | 16,723 ms |
| Promotion | 18,350 ms | 18,885 ms (Part 3) |

The same run covered the real binaries end to end:
- the primary recovered the new journal and opened day 6 on its operator port;
- the warm replica wrote a 667 KB snapshot right after that open;
- market data was built, then restarted from its saved state;
- the warm replica started from its snapshot and was promoted, and the customer port answered.

Copied to another directory, the files were accepted as the same journal: the warm replica was
ready in 67 ms and market data in 9 ms. The milestone 22 binary refused its own copied files and
did not start.

An older copy of a journal, put back in place (`~/stock-scripts/m23-rollback.sh`), was refused:
- **The setup:** a real primary opened and closed two days, with a copy of the journal taken after
  the first. A warm replica caught up, the primary stopped, and the first-day copy (477 bytes
  instead of 930) went back in place.
- **Restored as backup tools do it,** as a new file renamed into place: the warm replica kept
  reading the journal it had followed, and its promotion was refused: "event log is 477 bytes,
  shorter than the 930 bytes already applied from it".
- **Copied over the followed file itself** (`m23-rollback.sh in-place`): the warm replica noticed
  that the journal had shrunk below what was published ("committed history regressed") and stopped
  following, so nothing could be promoted onto the copy.
- **The primary,** in both cases, refused to start: "the journal ends before what its stream
  already published: acknowledged commands are missing (an older copy of the journal, or a disk
  that lost synced writes)".
