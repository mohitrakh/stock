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
  promoted. Recorded as a known limitation; fixed in Part 6, where the warm replica now waits.

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

## Part 4 - Synchronous replication

### Why Part 4 came next

Until this part, one disk held the only journal. Losing that machine lost every acknowledged
command since the last copy. The owner's decision for this milestone: a command is answered, and
becomes visible to anyone, only once it is on both machines' disks. If the replica cannot be
reached, the primary pauses until an operator either promotes the replica (part 5) or tells the
primary to run alone. The machines talk over TCP, and the second machine is a second container on
the office machine, with its own volume and network address.

### What changed

- **A replica process.** `stock --replica PRIMARY_ADDR JOURNAL STREAM` keeps a byte-identical copy
  of the primary's journal, and a stream of its own. It has no exchange core, no customer port and
  no database. Part 3's warm replica runs beside it on that machine, following the copy, ready for
  part 5's promotion there.
- **The primary replicates** when `REPLICATION_LISTEN_ADDR` is set. Each group:
  1. is written to the journal;
  2. is shipped to the replica while the primary syncs its own disk;
  3. waits for the replica's confirmation that its disk holds it too;
  4. only then is published and answered.

  The replica checks every record (framing, checksum, sequence), appends exactly those bytes,
  syncs, and confirms how far it is durable. Nothing that exists on one machine only is ever
  answered or published.
- **The commit point.** The primary sends the replica the end that both disks hold. The replica
  publishes on its own stream only up to there, so its readers never see a record that might
  still be cut.
- **A restart holds back what the replica may lack.** A replicated primary recovers its journal,
  but publishes only what its stream had already published. The rest is published, and nothing is
  served, until the replica confirms it holds it.
- **Pause, never guess.** There is no timeout. Without a replica the worker simply waits, and
  commands queue behind it:
  - `/health` answers 503 "paused: waiting for the replica";
  - `GET 127.0.0.1:4004/replication` shows "paused";
  - `POST /replication/run-alone` lets the primary continue without its replica.

  Acknowledged commands then exist on that machine only, until a replica holds everything the
  primary has synced, which turns running alone off by itself.
- **A silent link is dropped.** The primary sends the commit point at least every second, and the
  replica answers every frame. Either side drops a link that stays silent for 10 s, as when the
  other machine lost power or the network was cut without a word, and the replica dials again.
- **The replica stops on a failure of its own disk,** as the primary's worker does: a failed read,
  write or sync of its files could otherwise make it confirm what its disk does not hold.
- **The bench replicates too** when `REPLICATION_LISTEN_ADDR` is set, so throughput can be measured
  with a replica.

### The link

The replica dials the primary, so a restarted replica just reconnects and the primary needs no
replica address. Frames are binary: a length, a kind, a body, and each read refuses a body longer
than the frame expected can be. When the replica connects, it says which journal it holds, how far
its copy is committed, how long it is, and the checksum of the bytes between those two points. Its
committed end is what its stream published. Then:
- **Its committed part** is on both disks, so it is identical on both machines. The primary's
  journal must reach it, at a command boundary with the same next sequence. A shorter journal lost
  committed history and is refused: this catches a primary started on an older copy of its own
  journal.
- **Beyond that,** the replica keeps what it holds only if the primary holds the same bytes there.
  Otherwise it cuts back to its committed end and takes the primary's records instead. That is
  always safe: anything the primary lacks there it never synced, so it was never acknowledged.
- **The primary then sends what the replica lacks,** straight from its journal file: first the
  catch-up, then each group as the worker writes it. Records are sent whole, at most 1 MiB in one
  frame unless a single record is bigger.

A replica that only lost its link keeps everything it holds, and the primary resumes after it.

A stream is a cache and is never synced, so after a power loss its header can be as old as the
kernel's writeback delay, about 30 s by default, or gone. The replica's committed end would then be
older than it really was. So the replica syncs its stream's header within about two seconds of it
moving: after a power loss its committed end is at most about that old. A stream that is gone
makes the whole copy its tail. That costs reading, not memory:
- both machines compute the checksum from their journal files a megabyte at a time;
- the replica computes its own before it connects, and waits up to 5 minutes for the primary's;
- it keeps only each unpublished record's end and sequence in memory, 16 bytes a record. It
  publishes by moving its stream's end, and its readers read the records from its journal.

### Why it cuts back to the committed end, and nowhere else

The first version checked only that the replica ended at one of the primary's command boundaries,
with the same next sequence. A test showed that this was not enough. A primary that restarted
without a record it had sent but never synced can write a different record in its place, with the
same length and the same sequence. The replica then looked like a prefix of the primary and kept
the wrong record. The checksum of everything beyond the committed end catches any difference, and
cutting back to a point both disks are known to hold needs no search.

The cut is safe as long as the primary's disk keeps what it synced. If it lost writes it had
reported synced, or the primary was started on an older copy of its journal, the primary lacks
acknowledged records. A journal that ends before the replica's committed end is refused. But the
replica cannot tell records beyond that end, at most about two seconds' worth after a power loss,
from records the primary never synced, and it cuts both. Only a third machine could tell them
apart: the three-machine quorum recorded as this milestone's follow-up.

### Options considered and rejected

- **The primary dials the replica.** Then the primary must know the replica's address, and a
  restarted replica must wait to be dialed.
- **The replica publishes as soon as its own disk holds a record.** A record the primary never
  synced could then reach the replica's readers, and later be cut.
- **A timeout after which the primary gives up its replica and runs alone.** That trades the
  recovery point of zero for availability without asking, and only an operator can choose between
  promoting the replica and running alone. The 10 s timeout on a silent link changes nothing the
  primary may acknowledge: it only lets the replica dial again.
- **TCP keepalive instead of heartbeats.** The standard library cannot turn it on, and Linux's
  defaults notice a dead peer after more than two hours. Heartbeats need no new dependency, and a
  test can play a primary that falls silent.
- **A commit point synced with every frame on the replica,** in a file of its own. It would add a
  second sync to every group on the replica's disk. Syncing the stream's header at most once a
  second bounds what a power loss can make it forget to about two seconds, at almost no cost.
- **Sending each group from the worker to a replication thread through a channel.** Catch-up has
  to read the journal file anyway; sending everything from the file needs one path, not two.
- **The replica keeping its unpublished records in memory, for its stream's cache.** The first
  version did, and compared at most 64 MiB beyond the committed end. With its stream far behind
  or gone, it would have read every record since into memory, three times over, and a longer
  difference meant fetching it all again. Its only reader, the warm replica, reads the journal
  just as well.
- **Raft, or more than one replica.** Out of scope for this milestone; a three-machine quorum is
  its recorded follow-up.

### Compatibility

No file format changed. Without `REPLICATION_LISTEN_ADDR` the primary behaves as before, one write
and one sync per group, except that recovery now always syncs the journal it recovered, once, before
anything is served.
- **New settings and commands:**
  - `REPLICATION_LISTEN_ADDR` on the primary, the bench, and a warm replica being promoted, since
    the promoted primary then replicates too;
  - `stock --replica PRIMARY_ADDR JOURNAL STREAM` on the second machine;
  - on the operator port, `GET /replication` and `POST /replication/run-alone`; both answer 404
    when the journal is not replicated.
- **`/health` answers 503 "paused: waiting for the replica"** while the primary is paused.
- **An existing journal can be replicated as it is.** A new replica fetches all of it from the
  primary, starting with its header.
- **The replication port is the only listener meant to be reached from another machine.** The
  link is neither authenticated nor encrypted: use a private network.
- **A restarted primary that holds records back writes no startup snapshot.** The last one stays
  the restart point until the warm replica writes the next.
- **While a write waits for the replica, everything behind it waits,** reads included, and the
  queue of 10,000 commands fills. Reads alone are answered without waiting, since every state they
  can show is already on both disks, or was let through by running alone.

### Found by the independent review

It found nothing that lets a command be answered or published before both disks hold it in normal
operation. A separate check of the memory fix above confirmed three of the same findings. All
fixed, except where noted:
- **A dead link went unnoticed** (high). The replica only waited for the primary's next frame. When
  the primary's machine lost power, or the network was cut without a word, it waited forever and
  never dialed again, while a restarted primary waited for it. Now:
  - the primary sends the commit point at least every second, and the replica answers every frame;
  - either side drops a link silent for 10 s;
  - the replica gives up a dial after 5 s instead of the minutes the system's own retries take.
- **A restarted primary trusted records that only its page cache held.** A process killed between
  writing a group and syncing it leaves the group in the page cache. The restarted primary
  recovered it and, once the replica confirmed it, published and served it, though a power loss
  could still take it from its own disk. Recovery now syncs the journal before anything is served,
  whether or not it cut a torn tail. Without replication the same gap existed, and is closed too.
- **The replica went on after a failed write or sync of its own files.** Its counters had already
  moved. After a failed sync, a reconnect could report as durable records whose sync failed; after
  a write failed on a full disk, every handshake failed, forever. A failure of its own disk now
  stops the replica.
- **Its committed end could be far older than it was.** A stream is never synced, so after a power
  loss its header can be about 30 s old. A cut back to there fetches again what the primary still
  holds, and the check that refuses a primary on an older copy of its journal is weaker by as
  much. The replica now syncs its stream's header within about two seconds of it moving. Not fixed:
  within those two seconds the replica cannot tell what the primary never synced from what its disk
  lost after syncing, as described above.
- **Running alone could stay on for good under load.** It ended only when a confirmation reached
  what the worker had written, and a busy worker had always written more. Now it ends once the
  replica holds everything the primary has synced, including the moment such a replica attaches;
  the group in flight then waits for the replica. A replica slower than the sustained load still
  cannot catch up until the load drops.
- **A stream that would not open was never retried.** The replica went on confirming records it
  could not publish, reconnecting every second. It now opens the stream whenever it has none, and
  stops if it cannot.
- **A restart that held records back wrote its startup snapshot beyond what it published.** A
  warm replica starting from that snapshot before the replica confirmed found it invalid. It then
  replayed from the start and wrote no snapshots for the rest of its life. Now no startup snapshot
  is written while records are held back.
- **Smaller:**
  - the replica computed its hello, over its whole copy when its stream was gone, after
    connecting, inside the primary's 5 s handshake timeout; it now computes it first;
  - a confirmation was checked against what the worker had written, not what this link had sent;
  - a hello's length was not checked before its body was read, and one slow peer held up the only
    listener thread. Every frame is now read with the limit of the frame expected, and each
    handshake runs on its own thread;
  - a new copy's directory entry was not synced;
  - a promoted warm replica read `REPLICATION_LISTEN_ADDR` only after fencing the old primary. A bad
    value now stops it before.
- **Tests:**
  - the restarted-primary test reached the checksum only if the primary wrote its record before the
    replica connected; it now waits for that;
  - a replica that keeps its tail would also have passed if the primary had cut it, since it then
    fetches the same bytes. The handshake's decisions now have their own test;
  - new tests cover a torn tail and a damaged record, a silent link on either side, an idle link, a
    peer that never says hello, and a stream that will not open.

### Found by re-checking the fixes

The re-check confirmed all twelve fixes and found the new tests sound. Its findings:
- **A warm replica could still lose its own snapshot for good.** The fix above stops a held-back
  primary from writing its startup snapshot past what it published. But the warm replica's own
  snapshot can lie there too: after a power loss leaves the stream's header behind, or while a
  restarted primary holds records back. A warm replica that started then found its snapshot
  invalid, rebuilt from sequence 1, and wrote no snapshots for the rest of its life. This could
  happen without replication too, whenever a warm replica starts before its primary after a power
  loss. Now a snapshot whose checkpoint is a command boundary of the journal, but which the stream
  cannot open yet, counts as ahead, not invalid. The warm replica still rebuilds from sequence 1,
  which is safe, and writes no snapshot behind that one, so the restart point never moves back. Whether a snapshot is ahead is decided from the journal, not from the
  stream's header, which a running primary may be rewriting. Waiting for the stream to catch up
  was rejected: with the primary dead, the warm replica could then never be promoted.
- **The hello was computed again before every dial.** Without its stream, its checksum covers the
  whole copy, and while the primary was unreachable the replica re-read it every second or two. It
  is now reused until the copy changes. A cut clears it: a copy refilled to the same length can
  hold other bytes.
- **A final check of these two fixes found both first versions wrong,** in ways their tests did not
  show:
  - the cached hello survived a cut. A replica refilled to the same length then described the bytes
    it had cut, and the primary, seeing a difference, cut a record it had just acknowledged;
  - the warm replica wrote its first snapshot at its first command, behind the one it had found.

  Each now has a test that fails without its fix.
- **A test bound was tight:** the slow-peer test now allows 4 s, still less than the 5 s the old
  listener needed.
- **Not changed:** a promotion still binds the replication port only after fencing the old primary,
  as it binds the customer and operator ports. A port already in use then leaves no primary until
  the operator starts one; recorded as a limitation.
- **Docs:**
  - the tests were listed under the wrong headings;
  - the count of removals was off by one;
  - a measurement said 197 syncs where one run had 196;
  - the note that allows a stream under `/dev/shm` now excludes the replica's, whose published end
    is the committed end it reports.

### Verification

Tests, with a primary and a replica over a real TCP connection on one machine:
- `a_command_is_answered_only_once_the_replica_holds_it`: without a replica a deposit gets no answer
  and the primary reports "paused"; once the replica connects, the answer arrives, the journals
  are byte-identical, and the replica's stream publishes both commands;
- `a_replica_that_loses_its_link_comes_back_and_the_primary_waits_meanwhile`;
- `running_alone_releases_a_paused_primary_until_a_replica_catches_up`: running alone answers the
  waiting deposit and the next one, and a replica that catches up turns it off by itself;
- `a_replica_holding_what_a_restarted_primary_lost_takes_the_primarys_records`: the case the first
  version got wrong. The replica holds a record the restarted primary never had, and the primary
  has written a different record of the same length in its place before the replica connects;
- `a_replica_keeps_what_it_holds_and_publishes_it_once_committed`: a copy whose stream published
  nothing, as after a power loss before the stream was ever written back, keeps its journal
  untouched and publishes all of it;
- `a_replica_of_another_journal_is_refused_and_left_untouched`;
- `a_restarted_primary_publishes_and_serves_nothing_before_its_replica_holds_it`: a primary whose
  stream is gone answers, publishes and snapshots nothing until its replica has the whole journal;
- `an_idle_link_stays_up`: heartbeats keep a link with nothing to send up past the link timeout;
- `a_peer_that_never_says_hello_holds_up_no_replica`;
- `a_replica_whose_stream_belongs_to_another_journal_stops`.

With the test playing one side over a socket:
- `a_replica_publishes_only_up_to_the_commit_point`: the replica confirms two records and publishes
  neither, then each as the commit point reaches it, and a commit point beyond its journal ends the
  session;
- `the_handshake_keeps_an_identical_tail_cuts_a_different_one_and_refuses_lost_history`: an
  identical tail is kept; one byte different, or longer than the primary's journal, is cut back to
  the committed end; a committed end the primary lacks, or one that is not its command boundary, is
  refused;
- `a_silent_link_is_dropped_on_both_sides`: a primary that welcomes the replica and falls silent,
  and a replica that never answers;
- `a_replica_refilled_after_a_cut_describes_its_new_bytes`: cut back, refilled to the same length
  with another record, and dialing again before the commit point moved.

Without a connection:
- `frames_are_bounded_and_positions_round_trip`: a frame longer than the one expected is refused
  before anything is allocated;
- `a_replica_cuts_a_torn_tail_and_refuses_a_damaged_record`;
- `a_file_checksum_read_in_chunks_equals_the_checksum_of_its_bytes`;
- `a_snapshot_ahead_of_the_stream_is_kept_and_snapshots_continue`: a warm replica whose snapshot
  lies beyond what the stream published rebuilds the published part and keeps that snapshot, then
  writes the next one past it once the primary publishes the rest.

`cargo fmt -- --check` is clean. `cargo test --locked` passes: 197 unit tests and the executable
integration tests. All three PostgreSQL acceptance tests pass, and the release build still has its
12 warnings. Each test written for a review finding, or for the commit point, fails when its fix is
removed: nine removals with `~/stock-scripts/m23p4-mutate.sh`, on a throwaway copy of the tree,
three with `m23p4-mutate2.sh` for the later fixes, and one by hand for the commit point. Every one
failed, one of them by hanging until killed.

Live, a primary and a replica in two containers on the office Ubuntu machine, each with its own
volume and network address (`~/stock-scripts/m23p4-live.sh`). Commands came from the operator port:
1. **No replica.** `/replication` showed "paused", `/health` answered 503, and opening a day got no
   answer within 3 s.
2. **The replica connected.** The waiting open was applied and the mode turned synchronous. The day
   was closed, and the journals were byte-identical (477 bytes).
3. **The replica was killed.** The next open waited. After 3.1 s the operator ran the primary alone,
   and the open was applied.
4. **The replica restarted.** It resumed at byte 477, caught up, and the primary logged "the replica
   caught up; synchronous again". Journals identical, 1,175 bytes.
5. **The primary was killed and restarted.** The replica reconnected and resumed at byte 1,175, its
   whole copy. Journals identical, 1,386 bytes.
6. **The replica restarted without its stream file.** It kept its whole journal and resumed at byte
   1,386 instead of fetching it again. Journals identical, 1,632 bytes.
7. **The network was cut** (`docker network disconnect`), so no packet arrived on either side. The
   primary dropped the silent link after 9.9 s; the replica, after the same timeout, logged
   "nothing heard from the other side in time". Once the network was back, the replica was attached
   again 11.1 s after the cut. Journals identical, 1,843 bytes.

The live run was repeated twice more, after each later round of fixes: the same seven steps and
journal sizes, with the link dropped after 9.8 and 10.2 s and the replica back 10.8 and 11.3 s
after the cut.

Measured, with both containers sharing the machine's 2 cores and its SSD
(`docs/performance/10-synchronous-replication.md`):

| | Alone | Replicated | Replica's files in memory |
|---|---|---|---|
| Maximum rate, orders/s, two runs | 44,610 and 43,968 | 32,372 and 34,511 | 45,274 and 40,081 |
| 5,000 orders/s, p50 | 7.4 and 7.5 ms | 14.7 and 12.3 ms | 7.5 and 7.0 ms |
| 5,000 orders/s, p99 | 18.3 and 25.6 ms | 53.3 and 31.9 ms | 25.2 and 27.0 ms |

With the replica's files in memory there is no difference beyond the noise: on this machine the
whole cost is the replica's sync waiting for the same disk as the primary's. After each replicated
run the two journals were byte-identical.

## Part 5 - Epoch-fenced promotion on the second machine

### Why Part 5 came next

After Part 4 every acknowledged command is on both machines, but nothing could use the second copy:
losing the primary's machine still stopped the exchange. This part promotes the replica's machine,
fences the old primary so that it can never acknowledge anything again, and lets it rejoin as the
new primary's replica. The owner's decisions for it:
- the epochs are compared in the replication handshake, not only journaled;
- promotion on the second machine reuses Part 3's: stop the replica process, then promote the warm
  replica there.

### What changed

- **Terms.** Each promotion starts a primary term with the next epoch, one after the highest its
  copy's index has ever listed, and journals it: a `TermStarted { epoch }` record, before anything
  else the new primary writes. The first primary's
  term is epoch 0 and has no record. The record changes no business state, so replay, the warm
  replica, the market-data process and the reporter pass it by, and snapshots keep their format.
- **A term index beside every journal copy,** `<journal>.terms`: each term's epoch, the sequence of
  its record and the record's offset. Whoever writes a term start saves the index first: the
  promoted primary before it journals its term, the replica before it appends a term start it
  received. When the index is loaded, an entry beyond the journal's end is dropped, and one exactly
  at the end is a term begun whose record was never written (see below). The index also keeps the
  highest epoch it has ever listed, which no cut or drop lowers. An index that is missing, names
  another journal, or lists a record that is not the term start it says, is rebuilt by one pass
  over the journal; that pass reads only the first 136 bytes of each record, and decodes a record
  only where they name a term start. A rebuilt index knows the epochs its journal holds, and the
  highest of the index it replaces if that one names this journal. An index that misses a later
  term is not noticed; only an older index copied in its place could. The index sits beside the
  journal's real path, so processes reaching the journal through a link share it.
- **Epochs in the handshake.** The replica's hello carries the latest epoch its copy holds, and the
  welcome carries the primary's. The primary refuses:
  - a replica that has seen a later term than its own: this primary was replaced, and nothing may
    confirm it again;
  - a replica from an earlier term whose committed records go past the point where the next term
    began in this journal: it acknowledged commands after the promotion, so the two histories split.

  The replica, for its part, refuses a primary from an earlier term than one it holds. A replica
  from an earlier term whose committed records all precede the next term is welcome. What it holds
  beyond them it never acknowledged, and the usual cut back drops it.
- **Promotion on the second machine.** The operator stops the replica process there, then asks the
  warm replica there to promote (`POST 127.0.0.1:4003/promote`). The replica process holds the
  copy's writer lock, so the promotion answers 409 until it has stopped; nothing then confirms the
  old primary. The promoted primary takes the next epoch, adds it to the index before it listens
  for a replica, and journals `TermStarted` before any other command. With
  `REPLICATION_LISTEN_ADDR` set it then waits, like any replicated primary, for a replica or for the
  operator to let it run alone.
- **The old primary rejoins as the replica:** `stock --replica NEW_PRIMARY JOURNAL STREAM` on its own
  journal and its own stream file. Its committed end is what its stream published, which is what
  it acknowledged; the handshake checks that it ends before the new term began, cuts back what it
  held beyond, and the new primary sends it the rest, term start included.
- **A promotion that stops before its term is journaled is not lost.** The term's entry in the
  index then sits exactly at the journal's end. A later plain restart of a primary on that journal
  takes it as a term begun and journals it first, byte for byte as it would have been. A replica
  that already holds that record keeps it if it connects after the record is journaled; before, it
  cuts it back and takes it again. A new promotion instead takes the epoch after it: on a replica's
  copy, such an entry may be another primary's term start, indexed and never written.
- **A primary running alone syncs its stream's end before it answers.** That end is what the split
  check reads when it rejoins. A stream is otherwise never synced, so after a power loss it could
  claim less than was acknowledged, and the commands acknowledged alone would be cut instead of the
  rejoin being refused.
- **The reporter resumes where it was.** Its checkpoint in PostgreSQL names the journal by its id,
  and the new primary's journal is the same journal, so a reporter started on the new primary's
  machine continues from it. It now prints where it resumes. The market-data process starts there
  like any market-data process.
- **`/replication` shows the epoch.**

### The operator's failover

1. On the replica's machine, stop the replica process.
2. `POST 127.0.0.1:4003/promote` to the warm replica there, until it answers 202.
3. With replication on, the new primary waits: `POST 127.0.0.1:4004/replication/run-alone` there, or
   bring a replica.
4. Stop the reporter on the old machine if it still runs. Once the new primary's `/health` answers
   200, start the reporter, and market data, on the new primary's machine. Before that, the new
   primary may still hold back records the old reporter already read, and the reporter would refuse
   a stream that published less than its checkpoint.
5. When the old machine returns, start it as `stock --replica NEW_PRIMARY:PORT JOURNAL STREAM` on its
   own journal and stream file, never as a primary. Started as a replicated primary it would only
   wait, since nothing will confirm it again. Started without replication it would serve at once,
   and its history would split from the new primary's.

Never let the old primary run alone after a promotion. It can, since it cannot know the promotion
happened, and it would then acknowledge commands the new primary never sees. The epochs cannot
prevent that, but they detect it: when it tries to rejoin, its committed records go past the start
of the new term, and it is refused rather than silently cut. That rests on its stream's end, which
a primary running alone syncs before every answer, so it must rejoin with its own stream file.

### Why the epochs fence the old primary

A command is acknowledged only once a replica confirms it, or while the operator lets the primary
run alone. After a promotion the old primary's only replica has become the new primary, so nothing
confirms it. Any other copy that has received the new term's start refuses it, because its epoch is
older. What the old primary wrote after the promotion it could not acknowledge, and the rejoin
cuts it.

A rejoining replica's committed records are what it acknowledged as a primary, or what was confirmed
in its own term. Each was confirmed by the replica that is now the new primary, so they all lie
before the point where the new term began. Committed records past that point can only come from
running alone during the new term, which is exactly what the handshake refuses.

### Options considered and rejected

- **Journal the epochs, but fence by ownership and a checksum.** Smaller: the replica's machine
  stops its replica before the promotion, so the old primary gets no confirmation, and a checksum of
  the rejoining replica's last committed bytes detects a split. The owner chose real epoch fencing:
  this one does not refuse an old primary that a copy holding the new term dials by mistake. (No
  epoch helps against a copy that never saw the new term: it does not know there is one.)
- **One standby process on the second machine,** the replica and the warm replica together, with a
  single `/promote`. One step less for the operator, but it rewrites tested code from Parts 3 and
  4. The replica's writer lock already orders the two steps.
- **The epoch in the core, carried by snapshots.** The core has no use for it, and the replica
  process, which has no core, needs it too. The index serves both, and can always be rebuilt from
  the journal.
- **The epoch in every record.** Every record would grow, and a replica would still have to find its
  last committed record to read it.
- **Finding the term starts by reading the whole journal at every start.** Restarts would again cost
  the journal's history; the index costs one small file per copy.

### Compatibility

- **A new event, `term_started`, in input and output.** A journal that holds one cannot be read by an
  older binary. A primary and its replica must run the same version, as before.
- **The handshake frames grew by the epoch,** so an older replica cannot talk to this primary, nor
  the reverse.
- **A `.terms` file now appears beside every journal** a primary or a replica opens, after one pass
  over the journal the first time.
- **The reporter prints the sequence it resumes from.**

### Found by the independent review

It found nothing that lets an old primary be confirmed after a promotion. Its findings, all fixed
except where noted:
- **A promotion that stopped before its term was journaled forgot it.** The term's entry sat
  exactly at the journal's end and was dropped on load, so the primary restarted in the old term.
  Many failures land in that window: a port in use, PostgreSQL unreachable, a crash. After a power
  loss it was worse: the replica had already synced the term start, so its epoch was the later one,
  and the restarted primary refused its only replica for good. Now an entry at the end with the
  next sequence is a term begun, and a primary restarted on that journal journals it, byte for byte
  as it would have been.
- **The split check rested on a stream end that a primary never syncs.** An old primary that ran
  alone after the promotion, then lost power, could claim less than it had acknowledged, and its
  rejoin would cut what it acknowledged alone instead of being refused. A primary running alone now
  syncs its stream's end before it answers, and the docs say to rejoin with its own stream file.
- **The index path depended on the path's spelling:** it now sits beside the journal's real path.
  A term start out of order, which means the replica and its primary disagree about the history,
  now stops the replica instead of making it dial again forever. Not fixed: an index that misses a
  later term is not noticed; the index is saved before every term start, so only an older copy put
  in its place could do that.
- **Tests:**
  - no test cut back a term start;
  - no test had a real replica holding a later term dial a real primary;
  - no test used an epoch that came from a term start not yet committed;
  - no test restarted a primary between beginning its term and journaling it.

  Each has one now.
- **Docs:**
  - the rejoin command lacked its stream argument;
  - "started as a primary it would only wait" holds only with replication;
  - the reporter must start on the new primary only once it serves, after the old reporter stops;
  - a claim about a third copy was wrong;
  - "nothing prevents it until Part 5" was left in the limitations.

### Found by re-checking the fixes

The re-check confirmed the fixes. Its findings, all fixed:
- **A promotion could take another primary's epoch.** The first version of the fix made a promotion
  reuse a term begun at the journal's end. On a replica's copy, that entry can be another primary's
  term start, which the replica indexed and then failed to write. A promotion now always takes the
  epoch after every term its index holds, begun ones included; only a plain restart journals a term
  begun. The final checks, below, found a gap in this fix.
- **Two tests proved less than they claimed.** The test of a primary refusing a replica that holds
  a later term passed even without the primary's check, because the replica refuses in turn with a
  similar message; it now checks the primary's own words. The running-alone test now checks that
  the worker is told when it answers alone, which is what makes it sync its stream's end; nothing
  tested that.
- **Docs:**
  - a replica holding the term start a restarted primary journals again keeps it only if it
    connects afterwards, and otherwise takes it again;
  - a primary started without replication does not sync its stream's end either;
  - the replica's reasons to stop now include the epochs.

  The runtime's tests no longer leave index files behind.

### Found by the final checks

A last independent check of the final changes, and my own last pass over them:
- **A copy could forget another primary's epoch, then take it.** The re-check's fix took the epoch
  after every term the index held. But a replica's index stops holding a term start it never wrote:
  - the replica's next start cuts that entry;
  - an entry past the journal's end is dropped on load, when a power loss cut the write short;
  - a promotion that failed between its two saves of the index left neither entry.

  A promotion of that copy then took the other primary's epoch, with a term start byte-identical to
  that primary's. When that primary rejoined, the equal epochs skipped the split check, and only the
  byte checks stood between the two histories and a silent fork. Now the index also keeps the
  highest epoch it has ever listed, which no cut or drop lowers, and a promotion takes the epoch
  after it. `a_replica_forgets_a_term_it_indexed_but_never_wrote` now promotes that copy afterwards
  and expects term 2.
- **A promotion ran a plain restart's check first** (my own pass). It looked for a term begun, to
  journal it again, so on a copy holding one it printed "resuming term N" and then started term
  N + 1. Only a plain restart runs that check now.
- **Docs:**
  - a new term is one after the highest epoch the index listed, not one after the journal's last;
  - "killed under load" was a kill after trading days;
  - a primary started directly on the replica's copy is no promotion: it begins no term of its
    own, so the epochs cannot fence the old primary. That is now among the limitations.

The handshake still compares the epochs the journal holds, not the highest the index listed. Say a
promotion fails after saving term 1, and the operator restarts that machine's replica so that the
old primary, paused in term 0, can go on. The replica forgets the entry. If the highest counted in
the handshake, each side would refuse the other, and the old primary would stay paused for good.
A promotion's epoch is the only place the highest is used.

Not covered by a test: the stream sync of a primary running alone. Removing it fails no test, since
showing it needs a power loss.

### Verification

Tests:
- `the_index_is_rebuilt_from_the_journal_and_kept_in_step_with_it`: the index is built by one pass;
  an entry at the journal's end is a term begun and one beyond it is dropped; a later epoch is
  required; a cut forgets the terms it removed; neither a cut nor a drop lowers the next epoch; a
  wrong index is rebuilt, still above the highest epoch it listed; and a record that only mentions
  a term start is not one;
- `a_term_start_changes_no_order_and_must_match_its_output`;
- `epochs_fence_a_replaced_primary_and_refuse_a_split_history`: against a primary in term 1, a
  replica that has seen term 2 is refused, one from term 0 that committed only what came before
  term 1 is welcomed in term 1, one from term 0 that committed past it is refused as a split, and
  one in term 1 may have committed anything;
- `a_replica_refuses_a_primary_from_an_earlier_term`;
- `a_primary_refuses_a_replica_that_holds_a_later_term`: a real replica whose copy holds the start
  of term 1, not yet committed, dials a real primary of term 0;
- `a_promoted_replica_starts_a_term_and_the_old_primary_rejoins_as_its_replica`: the old primary
  acknowledged two deposits and wrote a third that was never confirmed; the replica's copy, promoted
  into term 1, answers a new deposit once the old primary, rejoined as its replica, confirms it. The
  old primary dropped its third deposit, holds the term start, and its journal is byte-identical to
  the new primary's;
- `a_term_begun_but_never_journaled_is_journaled_when_the_primary_restarts`;
- `a_promotion_takes_the_epoch_after_a_term_its_copy_indexed_but_never_wrote`;
- `a_replica_forgets_a_term_it_indexed_but_never_wrote`: and that copy, promoted afterwards, takes
  term 2;
- `a_term_start_cut_back_is_forgotten_and_taken_again`.

`cargo fmt -- --check` is clean. `cargo test --locked` passes: 207 unit tests and the executable
integration tests.

Live, on the office Ubuntu machine (`~/stock-scripts/m23p5-live.sh`): machine A, the primary, with a
reporter beside it; machine B, the replica and a warm replica on B's copy; two containers, each with
its own volume and network address.
1. **Trading on A.** Days opened and closed through A's operator port: 57 commands acknowledged, the
   last opening 2026-01-30. A's reporter had saved its checkpoint at sequence 115.
2. **A and its reporter were killed,** as a machine lost.
3. **Promotion on B.** While B's replica ran, `/promote` answered 409. The replica was killed, the
   warm replica promoted into term 1, and it paused, waiting for a replica: `/health` 503. Run
   alone, it journaled the term and served 375 ms after the replica was stopped.
4. **B held every acknowledged command:** its session showed 2026-01-30 open, A's last answer. A
   reporter started on B printed "resuming ... at sequence 115", A's reporter's checkpoint, and
   moved on to 117, past the term start.
5. **A restarted as a primary** in term 0: paused, `/health` 503, and an open sent to it got no
   answer. It had written that open, but nothing would ever confirm it.
6. **A rejoined as B's replica.** It cut the 247 bytes of that open, which it never acknowledged,
   and followed B in term 1 from byte 13,072. B turned synchronous, and the two journals were
   byte-identical, 13,737 bytes. A's term index holds term 1, starting at sequence 115.

Run again after each round of fixes, the last time with the final build: the same six steps. That
time A acknowledged 57 commands, the last opening 2026-01-30; its reporter had saved sequence 115
and resumed there on B; B served 426 ms after its replica was stopped; and A cut the 247 bytes of
the open it wrote as a primary. The journals were identical at 13,737 bytes, and A's index recorded
1 as the highest epoch it has listed. Over the five runs B served 375 to 697 ms after its replica
was stopped; the figure includes the script's own polling, each check a `docker exec` with 0.2 s
between checks. The scripts remove the fencing's checks and each round's fixes one at a time, and
each removal makes the test that covers it fail: eighteen removals, one check removed under two
tests (`~/stock-scripts/m23p5-mutate.sh`, `m23p5-mutate2.sh`, `m23p5-mutate3.sh` and
`m23p5-mutate4.sh`). All three PostgreSQL acceptance tests pass, and the
release build still has its 12 warnings.

## Part 6 - Measurement and failure tests

### Why Part 6 came next

Parts 4 and 5 were each verified live, but with commands sent one at a time from the operator
port. Nothing had killed the primary while customers traded, and the time from a promote request
to the first accepted order had never been measured. Two of the milestone's completion criteria
waited on this part: no acknowledged command lost when the primary is killed at any moment under
load, over repeated runs; and throughput, latency and recovery time measured and written up.

One known limitation was also left to this part, from Part 3's review: if the stream's writer dies
while it publishes, the warm replica stops for good and cannot be promoted. On the second machine
that writer is the replica process, and the failover runbook kills it right before the promotion.

### What changed

- **The warm replica waits out an interrupted publication.** A writer clears the stream's ready
  marker, copies the record and header, and sets the marker again, all under the stream's
  exclusive lock. One killed in between leaves the marker cleared, and every reader got "stream
  publication interrupted; restart the writer to recover". The warm replica made that fatal: it
  exited, and the machine had nothing left to promote. Now that one error, and no other, means
  wait: the warm replica stays where it is, keeps polling, and follows again once a restarted
  writer has repaired the stream. `/status` reports `"stream_interrupted": true` meanwhile, and the
  log says so once on each change. The error is a type of its own, `PublicationInterrupted` in
  `src/exchange/event_stream.rs`, so the warm replica tells it apart without matching text; its
  message is unchanged.
- **A customer load client,** `~/stock-scripts/m23p6-load.py`. It signs tokens with the run's
  throwaway `JWT_SECRET`, funds 16 users, and has them send orders back to back over HTTP, each
  with its own `client_order_id`. It runs in a container that shares the exchange container's
  network namespace, since the customer port binds loopback only, so it outlives a killed primary.
  Every 201 goes to an acknowledgement file the moment it arrives. Afterwards it asks the surviving
  exchange for every acknowledged order, as its user. On the promoted machine it also times the
  failover itself.
- **Four live scripts and a mutation script** on the office machine, described under Verification.

### Why waiting is safe

- **A live writer is never seen half done.** A reader takes the stream's shared lock before it
  reads the marker, and the writer holds the exclusive lock from clearing it to setting it. A
  cleared marker seen by a reader therefore means a writer died there, or, in a writer that is
  still alive, panicked there; in that case it still holds the journal's writer lock, and a
  promotion is refused with 409.
- **Nothing moves while it waits.** No batch is applied and no snapshot is written; the applied
  checkpoint stays at the last command the warm replica checked.
- **The promotion never needed the stream.** It fences the journal's writer lock, takes the end the
  stream published from a header whose checksum holds (a torn one falls back to the last end the
  warm replica validated), and reads everything after its checkpoint from the journal. The
  promoted primary opens the stream as its writer and publishes again, which repairs it.

What it does not change:
- a warm replica started, or restarted, while the stream is interrupted still refuses to start:
  opening a reader checks the marker. With a snapshot present it first logs, misleadingly,
  "snapshot ahead of the stream". Restart the stream's writer first, which repairs the stream;
- the market-data process and the reporter still stop on an interrupted stream, as before. Neither
  is on the failover path, and both resume from their own state once the writer has restarted.

### Options considered and rejected

- **Matching the error's text.** It works until someone rewords the message. A type of its own
  cannot drift.
- **`io::ErrorKind::Interrupted`.** It is std's signal for "retry the system call", and std's own
  read loops retry it silently.
- **Repairing the stream from the warm replica.** Only the stream's writer writes it; a reader that
  writes would need the writer's lock and could race a writer that restarts.
- **Promoting automatically when the stream stays interrupted.** Automatic failover is outside this
  milestone.
- **Driving the failover with `--bench`.** It runs in-process, so killing the primary kills the
  client, and it keeps no acknowledged ids. A separate HTTP client does both.
- **Timing the failover from the host.** Each `docker exec` check costs about 100 ms, more than the
  failover; Part 5's 375 to 697 ms included that polling. The client in B's namespace retries every
  5 ms.

### Compatibility

No file format changed. `/status` on the warm replica has a new field, `stream_interrupted`.

### Found by the independent review

The review found the code correct, and:
- **(medium) the documents still said the warm replica cannot be promoted** after an interrupted
  publication, and `/status`'s description lacked the new field. Updated with this part.
- **(low) a warm replica started while the stream is interrupted still exits.** Not a regression,
  and it fails closed; recorded above and among the known limitations rather than widened into a
  change to how readers open.
- **(low) `/status` started as `false`** even when the catch-up before the listener binds had just
  found the stream interrupted, until the follower's first loop corrected it. It now starts from
  the warm replica's own state.

### Found by the live runs

The scripts' own mistakes, fixed before the runs reported here:
- the database was dropped while the last run's reporter still held a connection, so the drop
  failed and the next reporter met old tables. The scripts now drop it with `WITH (FORCE)`;
- the gap script's machine B had no `REPLICATION_LISTEN_ADDR`, so its promoted primary had no
  replication to run alone from, and the client waited for ever. B now listens as in the failover
  script, and every step of the client gives up after 60 s;
- `docker kill` of the primary alone never left it with records the replica lacked: the kernel
  still delivers what a killed process had sent. Every other failover run now cuts the primary's
  network 0.3 s before the kill, as a lost machine would.

### Verification

Tests:
- `an_interrupted_publication_leaves_the_warm_replica_waiting_and_promotable`: a deposit is
  published after the warm replica caught up, then the marker is cleared. The warm replica waits,
  twice, at sequence 3; the promotion picks up the two envelopes past it; the promoted primary
  matches the old one, continues at sequence 7, and a fresh reader reads all three deposits from
  the repaired stream. Before the fix it failed with the error above: the reproduction;
- `a_warm_replica_follows_again_once_a_restarted_writer_repairs_the_stream`: the flag clears and
  the warm replica catches up with the restarted writer;
- `warm_replica_process_waits_out_an_interrupted_publication_and_stays_promotable` (in
  `tests/warm_replica.rs`): the real executable keeps running, reports the interruption on
  `/status`, answers `/health`, and is promoted (202).

`cargo fmt -- --check` is clean. `cargo test --locked` passes: 209 unit tests and the executable
integration tests. All three PostgreSQL acceptance tests pass, and the release build still has its
12 warnings. `~/stock-scripts/m23p6-mutate.sh` removes the fix piece by piece (waiting, recognising
the error, reporting it, clearing it): each of the six removals fails the test that covers it.

Live, on the office Ubuntu machine, two containers each with its own volume and network address,
sharing its 2 cores and SSD (`docs/performance/11-failover-under-load.md`, raw output in
`docs/performance/results/results-m23.txt`):
1. **The primary killed under load, ten times** (`~/stock-scripts/m23p6-failover.sh 10`). A traded
   for 2.2 to 6.7 s with 16 customers, then was killed; in five runs its network was cut 0.3 s first.
   In every run B's journal was a byte-identical prefix of A's, every acknowledged order was on B
   (83,186 in all), the reporter resumed on B at A's reporter's checkpoint, and A rejoined as B's
   replica with identical journals. In three runs A held 5.7 to 7.1 KB that B never received; on
   rejoining it cut exactly that. From the promote request to the first accepted order: 26 to
   74 ms, median 42 ms.
2. **The replica killed, then its network cut, under load** (`m23p6-faults.sh`). Nothing was
   acknowledged from 0.5 s after the kill until the operator ran the primary alone 3.4 s later,
   nor from 0.5 s after the cut until the network came back 12.6 s later; `/health` answered 503
   in both. The primary dropped the silent link after 10.6 s. Each time the replica was back and
   the primary synchronous about 1 s later. All 27,308 acknowledged orders were on the primary,
   and the journals were identical.
3. **B's replica killed 40 times under load,** with the Part 5 and the Part 6 binary
   (`m23p6-gap.sh 40`). With either binary the warm replica never met an interrupted publication:
   the window is a copy of a few kilobytes per group, so live kills rarely hit it, and the tests
   above create that state directly. After the kills A was lost and B promoted; all 49,555 and 65,342 acknowledged
   orders were on B.
4. **Throughput and latency** (`m23p6-bench.sh 2`). At the maximum rate, 45,000 to 56,000 orders/s
   alone and 34,000 to 36,000 replicated; at 5,000 orders/s, a p50 of 7 to 8 ms alone and 11 to
   12 ms replicated. With the replica's files in memory the difference disappears within the
   noise: on this machine the cost is the replica's sync waiting for the same disk, as Part 4
   found. The journals were byte-identical after every replicated run.
