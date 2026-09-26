# Committed mmap Event Stream

Implemented 2026-09-26. This milestone connects the durable exchange to independent same-host readers. It does not build a market-data publisher.

## Why this task

The exchange already had deterministic replay, a framed durable journal, and atomic prepare/commit processing. It lacked a supported external reader and a safe boundary between historical catch-up and live delivery. The target system design places mmap between the exchange and subscribers. Building that boundary first lets later market-data, reporting, and replication work consume committed history without reaching into mutable core state.

The durable journal and mmap have different jobs. The journal remembers accepted history across restart. mmap shares a bounded window of committed event bytes between processes. Replay applies historical inputs to rebuild the core; a subscriber reads recorded batches to build its own projection. None of these operations is a substitute for the others.

## Command and startup integration

The production command path is:

```text
prepare without live mutation
  -> encode the whole input/output batch once
  -> append framed bytes and sync the journal
  -> commit prepared core state and advance in-memory history
  -> publish the batch and committed watermark through mmap
  -> local execution callbacks and HTTP reply
```

`recover_runtime_with_stream` is wired into `main`, before the listener binds. It opens/replays the journal first, then initializes publication from the validated journal length and last envelope sequence. Recovery starts with an empty payload cache; old events remain readable from disk. Both journal and matching sequences retain their existing meanings.

`EVENT_LOG_PATH` defaults to `exchange-events.log`. `EVENT_STREAM_PATH` defaults to the journal path plus `.mmap`. The stream can be placed under `/dev/shm`; that does not move or replace the durable journal. The default capacity is 4 MiB plus a fixed 80-byte header.

## Journal changes

The `EXCHLOG1` file format is unchanged: each record is `[u32 length][u32 CRC-32][JSON batch]`. The 64 MiB payload ceiling is now enforced before appending as well as while recovering. Recovery also verifies that each physical record holds one input and its outputs, so a logically complete command split across frames cannot pass replay and later surprise a reader.

The runtime serializes before changing state and reuses those framed bytes for persistence and transport. `EventStore::append_record` synchronizes the bytes. A test-only envelope helper retains convenient fixture construction.

`EventStore::open` acquires an exclusive nonblocking writer lock before recovery, truncation, or append. New files use mode 0600; file and parent directory are synchronized during initialization. Drop explicitly unlocks before closing. Parallel subprocess tests caught why this matters: a briefly inherited descriptor during fork/exec can otherwise retain a lock after the original owner drops it. Process death also releases the lock.

## mmap format and synchronization

The stream is a fixed-size file mapped using `memmap2::MmapRaw`. This avoids long-lived Rust slices over memory another process modifies. Private copy helpers perform bounds-checked pointer copies while holding advisory file locks. The file is never resized after initialization. All participants must follow this protocol and leave the mapped files in place.

| Byte offset | Field |
|---|---|
| 0 | 8-byte magic `EXCHBUS1` |
| 8 | journal device, u64 LE |
| 16 | journal inode, u64 LE |
| 24 | payload capacity, u64 LE |
| 32 | committed journal byte end, u64 LE |
| 40 | last committed envelope sequence, u64 LE |
| 48 | journal byte offset represented by cache start, u64 LE |
| 56 | cache byte length, u64 LE |
| 64 | CRC-32 of bytes 0 through 63, u32 LE |
| 68 | four reserved bytes |
| 72 | aligned native-endian AtomicU64 ready marker: 0 invalid, 1 published |
| 80 | cached framed journal records |

The format is for cooperating processes on the same host, not a network protocol. Journal identity is installed under the initialization lock; a second journal cannot claim the same stream path. Paths aliasing the journal, foreign magic/identity, and incompatible sizes are refused.

The writer holds an exclusive stream lock, clears the ready marker before overwriting any cache bytes, copies the new record and header, then publishes the ready marker. Readers hold a shared lock long enough to copy the header and requested record, then release it before checksum validation, JSON decoding, or consumer work. A reader cannot observe a partially copied batch through the protocol. A process killed during publication leaves an invalid marker until recovery.

When the bounded cache fills, the writer starts a new window. It does not wait for readers to acknowledge old batches. Oversized batches bypass the cache but remain accessible in the journal. No mmap flush participates in acceptance: only the journal is authoritative.

The writer still uses synchronous file locks and fsync. A reader stopped inside its copy can delay publication; there is no lock-free, wait-free, or bounded latency guarantee. These are advisory locks for trusted local processes on a local Unix filesystem. External truncation can invalidate mmap safety and cause SIGBUS. Network filesystems and malicious processes bypassing the protocol are unsupported.

## Reading, catch-up, and restart

`StreamReader::open` takes journal path, stream path, and an optional `ReaderCheckpoint`. With no checkpoint it starts at sequence 1 and byte 8. `next_batch` returns a complete command batch or `None` at the published end.

A checkpoint contains journal device/inode, next sequence, and next byte offset. On open, the reader scans from the beginning to validate that the checkpoint is a real complete-command boundary. It refuses a checkpoint from another journal, a sequence/offset mismatch, or one ahead of committed history. This scan is intentionally simple and O(history); indexing and snapshots are deferred.

If the next batch is cached, it is copied from mmap. Otherwise, the reader reads that framed record from a separate read-only journal handle. This handle never invokes recovery or truncation. The read is bounded by the published byte watermark, not the current file length, so a record saved but not yet published remains hidden while the writer is live.

Both routes validate framing, CRC, JSON, command shape, and contiguous envelope sequence before advancing the same cursor. There is no separate subscribe-after-replay switch that can miss an event. Slow readers can recover overwritten batches, catch the writer, and continue from mmap. Separate readers do not share progress.

The cursor advances when a batch is returned. A real consumer must save its projection state and checkpoint atomically, or make effects idempotent. The transport alone does not guarantee exactly-once external effects after consumer failure.

## Failure behavior

| Failure point | Behavior |
|---|---|
| Preparation/internal failure | No core commit or publication; internal faults halt the worker. |
| Journal append/sync failure | No live core commit, callback, or stream advance; stop and recover uncertain disk outcome. |
| Process dies after durable append, before publication | Startup replays and publishes the recovered prefix, including the saved command. |
| Publication returns an error after core commit | Stop worker and return unavailable; command is already durable and may appear on recovery. |
| Process dies during mmap copy | Reader refuses invalid ready marker; restart repairs publication from journal. |
| Reader falls behind window | Read missing complete records from the committed journal prefix. |
| Reader detects CRC/gap/identity/boundary error | Return an error without silently skipping or advancing past the bad batch. |
| Reader dies after processing but before checkpoint | Its last batch may repeat; projection/checkpoint coordination is the consumer's responsibility. |

No success response is sent before publication. An unavailable response after a storage/publication fault is not a normal business rejection. Existing order-id duplicate checks help order retries; this milestone does not add general command idempotency. Local callbacks are not durable subscribers and are not replayed on restart.

## Running the diagnostic consumer

Start the exchange normally so the journal and stream exist. In another terminal:

```sh
cargo run -- --event-probe exchange-events.log exchange-events.log.mmap /tmp/stock-reader.json --once
```

One JSON array is printed per command. Omit `--once` to follow live events with 10 ms idle polling. Omit the checkpoint argument to read from the beginning. The probe runs before database setup and therefore does not need PostgreSQL or authentication credentials. It is a trusted internal diagnostic, not a public market-data endpoint: events include account and order details.

After flushing each output batch, the probe writes its optional checkpoint using a private temporary file, sync, rename, and directory sync. Output and checkpoint are not a single transaction, so output may repeat after a crash. Use a different checkpoint path for each consumer. Journal/stream destinations are rejected as checkpoint paths. Errors exit nonzero rather than repairing data.

If a writer restarts using the same files, attached readers can continue. If the cache disappears or becomes incompatible, stop all participating processes, remove only the disposable cache if needed, restart the writer, and reopen readers using their checkpoints. Never remove or replace the journal to fix a cache problem. A copied/replaced journal has a different identity and needs an explicit consumer reset/migration. Machine power loss may damage or remove the cache; no cache durability is assumed.

## Verification

`cargo fmt -- --check` and `cargo test` pass: 92 unit tests and 2 executable integration tests. Tests cover accepted orders, fills, rejections, and cancellations appearing exactly as runtime history; no publication on failed durable append; worker shutdown after publication failure; recovery of saved but unpublished commands; independent fast/slow readers; cache rollover and oversized records; corrupt bytes; sequence/command shape validation; checkpoint boundary/identity checks; competing writers; and startup refusal before publishing invalid replay history.

OS-process tests use real mapped files and file locks. A separate child reader receives 100 concurrently published batches. Other child writers are killed after durable append and while holding the publication lock with an invalid marker. Restart recovers the command and continues with the next batch. Executable integration tests invoke the actual `stock --event-probe` binary without database configuration, validate complete JSON output, resume a saved checkpoint, and reject unsafe/invalid arguments.

These are local process-crash tests. They do not establish performance targets, machine-power-loss behavior, deployment readiness, or a new full HTTP/database smoke test. Clippy runs successfully without treating warnings as errors; existing repository-wide dead-code/compatibility and style warnings remain.

## Decisions and remaining work

A bounded window plus durable fallback was chosen over an unbounded mapped file so live cache size is fixed and slow readers have an explicit recovery path. A lock-free ring was deferred because it requires a separate overwrite/reclamation and publication proof; replacing these short locks is a performance milestone, not a claim made by this change.

Directly exposing mutable core state was rejected because it would break single ownership and make reader speed part of trading correctness. Publishing before fsync/core commit was rejected because readers could consume events not yet committed. Treating mmap as the recovery source was rejected because `/dev/shm` is volatile and mapping alone supplies no persistence protocol.

Gateway ingress remains Tokio mpsc. HTTP reads still query the worker. No market-data publisher, candles, reporting projection, hot-warm engine, cross-host replication, snapshots, group commit, ring buffer, CPU pinning, or benchmark claims were added. Choose and discuss the next business subscriber separately.

API references used for the safety/synchronization design: [memmap2 raw mappings](https://docs.rs/memmap2/latest/memmap2/struct.MmapRaw.html) and [Rust File locking](https://doc.rust-lang.org/std/fs/struct.File.html#method.lock).
