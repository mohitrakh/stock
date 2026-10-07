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

## Part 3 - Promotion from the warm replica's own state

### Why Part 3 came next

After Part 2, readers restarted in milliseconds, but the two things a failover needs most still
grew with history or fell behind. Measured on the five-day workload at maximum rate, with a warm
replica beside the benchmark:
- **Promotion threw the warm replica's core away.** It took the writer lock, re-read the whole
  journal, and the new primary replayed all of it from an empty core: 25.8 s at the end of the
  fifth day, a little more with every day.
- **The warm replica fell behind.** It wrote a snapshot every 10,000 commands. A snapshot costs
  about its size, and the size grows through the day, so a day's snapshot work grew with the square
  of its length: 87 s of snapshots for 37 s of trading. The warm replica was 2.4 million events
  behind when trading stopped, and needed 85 s to catch up.
- **A promotion could not even start before the warm replica caught up.** Its control port opens
  only after its first catch-up. Asked to promote the moment trading stopped, it answered after
  90 s, and the customer port after 103 s.

Part 4 gives the replica a journal of its own. It will then promote from that journal and its own
core, exactly as this part does with the shared journal.

### What changed

- **Promotion reuses the warm replica's core.**
  - It takes the writer lock and reads only the journal after the warm replica's applied
    checkpoint (`EventStore::open_suffix`). The new primary replays those records into the warm
    replica's core (`promote_replica`), checking every output, before it writes anything.
  - Those records are the warm replica's lag, plus any batch the old primary synced but never
    published. A torn tail after them is cut off, as at any recovery.
  - Promotion costs the warm replica's lag, not the journal's history: 58 ms for a caught-up warm
    replica on the five-day journal.
  - The promoted primary writes no startup snapshot. At the end of a day that took about 2 s, and
    the warm replica's last snapshot, at most its growth rule behind, stays the restart point.
- **Snapshots by journal growth.** The warm replica writes a snapshot right after each open, as
  before, and then whenever the journal has grown by `EVENT_SNAPSHOT_GROWTH` (default 4) times the
  last snapshot's size, instead of every 10,000 commands.
  - The work of writing snapshots is then proportional to the journal the warm replica applies,
    however big the day's state gets: 11 s of snapshots over the five days instead of 87 s.
  - While snapshots are being written, a restart replays at most about four snapshot sizes of
    journal past the last one, plus whatever the warm replica had not applied yet: within the
    day while it keeps up.
  - `EVENT_SNAPSHOT_GROWTH` replaces `EVENT_SNAPSHOT_INTERVAL`; 0 writes a snapshot after every
    command, which the tests use.
- **A reader that is behind reads records straight from the journal.**
  - `StreamReader::next_batch` used to take the stream's shared lock, copy and check its header,
    check the journal's size, then seek and read the record: six system calls per record.
  - Now it consults the stream only once it reaches the last committed end it validated. Records
    before that end are committed and never change, so it reads them with two positioned reads.
  - Every reader catches up faster: the warm replica replayed the five days in 19.2 s instead of
    29.1 s, and market data, the reporter and the probe share the same reader.
- **Faster checksums.** The CRC-32 was a hand-rolled loop that went bit by bit. At about 200 MB/s
  it was 28% of the time of writing a snapshot, and about a seventh of a warm replica's replay. It
  is now the `crc` crate's 16-table implementation, which sqlx already brought in. The checksums
  are the same, so no file format changed.

### Why the warm replica's core can be trusted

The core was built from the journal record by record, through the same prepare, compare and commit
path as recovery, and every output was checked against the journal. It starts from the snapshot
the warm replica loaded: the primary's startup snapshot, or one an earlier warm replica wrote. That
snapshot passed its checksum, version, journal id and boundary checks, but the state in it was not
replayed. Promotion therefore trusts it exactly as a restart from that snapshot does. Part 2's
promotion rebuilt from the whole journal and depended on no snapshot.

Three checks make the core stand for the journal at promotion:
- **It is the same file.** `open_suffix` gets the warm replica's own handle on the journal and
  refuses a path that now names a different file (`NotTheFollowedFile`), even a byte-identical
  copy. The journal is append-only, and recovery cuts off only an unpublished torn tail, so the
  bytes the warm replica read cannot change under it. A copy at the same path could differ in
  them, and nothing short of reading it again could tell.
- **It is the same journal.** The id in the header must still be the followed journal's.
- **It still reaches what was published and applied.** Every published record was synced first,
  and readers may already have consumed it, so a file shorter than the stream's published end, or
  than what the warm replica applied, lost committed history and is refused (`ShorterThanApplied`).

All three comparisons run while holding the writer lock and before anything is read or repaired,
so a refused file is left untouched. The published end comes from the stream header, read just
before without waiting for a writer: only a live primary can hold the stream, and then it holds
the journal too, so the promotion is refused anyway. Any header with a valid checksum counts, even
mid-publication, as the primary itself trusts it; otherwise the end the warm replica last
validated counts. The live path changes no core state that replay skips: after committing a
command, it only notifies execution subscribers, and production registers none.

Part 2's promotion opener (`open_existing_matching`) read the whole journal so that it could
measure the length of its complete records. Promotion no longer reads the part before the warm
replica's position, so that opener is gone. The checks above cover most of what it caught:
- an older copy renamed into place is not the followed file;
- an older copy written over the followed file is refused when it is shorter than what the
  stream published.

A file rewritten in place that keeps at least that length is not caught. Promotion compares the
file's raw length, not its complete records, and does not read the part it rewrote. Nothing may
modify the journal; restoring an older one was already unsupported.

### Options considered and rejected

- **Keep rebuilding from the whole journal at promotion.** It does not depend on how the warm
  replica got its state, but it grows with history, and it needs the whole journal to be read
  again exactly when time matters most.
- **Promote from the warm replica's latest snapshot instead of its live core.** That replays
  everything since the snapshot, when the live core is already up to date.
- **Check the checksum of the last record the warm replica applied, instead of the file.** That
  would also accept a byte-identical copy put in place. But the warm replica would have to track
  each record's checksum, and it could check nothing when it has applied no record since it
  started. Starting a new warm replica on a replaced file costs only its snapshot and the journal
  after it.
- **Write snapshots on a background thread.** The follower would no longer stop for them, but
  which snapshots get written would then depend on timing, and the writer would still compete for
  the same two cores. It is the next step if the warm replica must keep up at full rate with room
  to spare on this machine.
- **Keep a fixed interval, only larger.** Fewer snapshots, but each late in the day still costs
  85 MB, and early in the day restarts would replay more than necessary. The growth rule adapts to
  the state's size.

### Compatibility

No file format changed: the checksums are the same values, and snapshots are still version 5.
- `EVENT_SNAPSHOT_INTERVAL` is gone; `EVENT_SNAPSHOT_GROWTH` sets the warm replica's snapshot rule.
- A warm replica that starts without a snapshot now writes one after its first command, then
  follows the growth rule.
- A promotion no longer writes a startup snapshot, and refuses a journal path that names a
  different file from the one the warm replica read. Start a new warm replica on it instead.
- The crate depends on `crc` directly; sqlx already depended on it, so nothing new is built.

### Found by the independent review

It found nothing of high severity: the promotion and reader code is sound for a journal used as
the protocol intends. Its findings, all fixed:
- **A test had stopped testing that the warm replica ignores the stream's cache.** A reader is
  offered the cached copy only when it stands at the committed end it validated, and the test's
  warm replica started behind, so it read the journal whether or not it was journal-only. Now the
  warm replica catches up first, and only then is a record published whose cached copy differs from
  the journal. Without journal-only mode the test fails. This matters more now that the warm
  replica's core becomes the primary's.
- **A failing snapshot was retried after every command.** A warm replica that starts with no
  snapshot has a last size of zero, so every command was due. If writes kept failing, for example
  because the directory is not writable, each command rebuilt and serialized the whole state, about
  a second late in the day. Now a failed attempt makes the next one wait for the growth factor
  times as much journal as it did, so failures get rarer as the state grows.
- **Promotion now trusts the snapshot the warm replica started from,** and the reasoning above did
  not say so. It does now. No difference between a snapshot's state and a replay was found.
- **An older copy written over the followed file could pass promotion when the warm replica
  lagged:** it reached what the replica had applied. Promotion now also requires the stream's
  published end, read from the stream header as the primary itself trusts it. That catches the copy before anything is read or
  repaired, which before happened only at startup, after the torn tail had been cut. A rewrite in
  place that keeps the length is still not caught, as recorded above.
- **Smaller:**
  - the CRC test now includes a 43-byte vector, long enough for the crate's 16-bytes-at-a-time
    loop;
  - a test now writes another journal over the followed file, checking the id on the promotion
    path itself;
  - the descriptions of the stream cache, and a test comment about a damaged cache, now say that a
    reader takes only the record at the end it validated from the cache;
  - a few numbers that disagreed between the documents now agree.
- **Older, not from this part:** if the primary dies while publishing to the stream, with its ready
  marker cleared, the warm replica stops when it reaches the end it validated, and cannot be
  promoted. Recorded as a known limitation; Part 6's failure tests will cover it.

### Found by re-checking the fixes

The reviewer confirmed every fix above and found nothing of high or medium severity. It confirmed
that the published-end check never refuses a legitimate promotion: a live primary holds the
journal, and every published byte was synced before it was published. Its smaller findings, all
fixed:
- **`/promote` could hang.** The published end was read under a shared stream lock that waits. A
  primary frozen in the middle of publishing, holding the stream, would have made `/promote` wait
  instead of answering 409. It is now read without waiting: only a live primary can hold the
  stream, and then it holds the journal too, so the promotion is refused anyway.
- **A stream marked not ready fell back to a weaker check** than the primary's own. The primary
  trusts any header with a valid checksum, even mid-publication, since every record before the end
  was synced first. Promotion now does the same.
- **The back-off on the second way a snapshot fails was untested.** That is a journal path that no
  longer names the journal the replica follows. The test now covers both ways.
- **Wording:**
  - the restart bound now says it holds while snapshots are being written, and adds what the warm
    replica had not applied yet;
  - the back-off is described as multiplying the wait, and growth 0 as the exception;
  - the snapshot writer's fields are described as they are after a failed attempt.

### Verification

Tests:
- Journal:
  - `promotion_opens_only_the_file_the_warm_replica_followed`: a byte-identical copy put at the
    path is refused before its torn tail is repaired, and the followed file opens;
  - `a_journal_shorter_than_what_was_applied_is_refused_untouched`;
  - the suffix opener's tests from Part 2 now run without a followed file: it never creates or
    initializes a journal, and it checks the id before repairing a foreign torn tail;
  - the CRC matches the standard check values, including a vector longer than 16 bytes.
- Warm replica:
  - `warm_replica_snapshots_once_the_journal_grows_by_the_factor_times_the_last_snapshot`: over 200
    commands that each make the state bigger, a snapshot is written exactly when the journal has
    grown by the factor times the last snapshot's size, and the gaps between snapshots grow;
  - `failed_snapshots_are_retried_less_and_less_often`: with every attempt failing, whether the
    write fails or the journal path names another journal, 200 commands make a handful of
    attempts, each after at least four times the journal of the one before;
  - `promotion_refuses_an_older_copy_of_the_journal_it_followed`: renamed into place, refused as
    another file; written over the followed file, refused as shorter than what was applied. A new
    warm replica cannot follow the renamed copy either;
  - `promotion_refuses_a_journal_shorter_than_what_the_stream_published`: a lagging warm replica,
    with an older copy written over the followed file that reaches what it applied but not what was
    published, refused untouched;
  - `promotion_refuses_another_journal_written_over_the_followed_file`: the id check on the
    promotion path;
  - `warm_follows_and_promotes_from_the_journal_not_a_differing_valid_mmap_cache`, rewritten so
    that the cache is really offered;
  - the promotion tests now hand over the replica's core and only the records it had not applied:
    none when caught up, and only the durable batch hidden from the stream otherwise.
- Stream:
  - `a_reader_behind_reads_committed_records_without_consulting_the_stream`: a reader delivers the
    committed records behind the end it validated, even after the stream breaks, then reports the
    break at that end;
  - `the_published_end_is_read_as_the_writer_trusts_it_and_never_waits`: the end in a header with
    a valid checksum, even mid-publication; the last validated end when the header is torn or a
    writer holds the stream, without waiting.

`cargo fmt -- --check` is clean. `cargo test --locked` passes: 179 unit tests and the executable
integration tests. All three PostgreSQL acceptance tests pass. Each test added for a review
finding fails when its fix is removed (`~/stock-scripts/m23p3-mutate.sh` and `m23p3-mutate2.sh`).

Live, on the office Ubuntu machine: five days of 200,000 orders, with a warm replica beside the
benchmark, before and after this part. The measurement is
`docs/performance/09-promotion-from-the-warm-replica.md`.

| | Before | After |
|---|---|---|
| Promotion of a caught-up warm replica, until the customer port answered | 25,816 ms | 58 ms |
| Promotion requested as trading stopped, maximum rate, until the customer port answered | 103,255 ms | 502 ms |
| The same at 20,000 orders/s | 19,440 ms | 1,309 ms |
| Warm replica's catch-up after trading at maximum rate | 85,275 ms | 1,662 ms |
| Warm replica's lag at 20,000 orders/s | grew to 2.19 million events | median 0, at most 99,000 |
| Snapshot time over the five days, maximum rate | 87.2 s | 11.0 s |
| Warm replica's full replay of the five days | 29,119 ms | 19,157 ms |
| Primary restart at the end of the fifth day | 2,394 ms (milestone 22) | 2,878 ms |

Part 2's rollback check (`m23-rollback.sh`) still refuses an older copy of the journal both ways.
Renamed into place, promotion refuses it as a different file from the one the warm replica
followed. Copied over the followed file, the warm replica notices that history regressed and stops
following. Either way the primary refuses to start on it.

At maximum rate the warm replica's lag now swings within each day between 0 and about 250,000
events instead of growing for the whole run. On this machine, which it shares with the primary and
the benchmark, that rate is at its limit: in one of two runs the lows rose by about 50,000 events a
day.
