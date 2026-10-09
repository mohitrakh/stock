//! Linux/Unix, same-host committed-event transport. The journal is authoritative; this bounded
//! mmap window is disposable. All participants must use this locking protocol and must never
//! truncate, replace, or modify either backing file while it is in use.

use std::{
    fmt,
    fs::{File, OpenOptions},
    io,
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use memmap2::{MmapOptions, MmapRaw};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::event_store::{
    JOURNAL_HEADER_LEN, MAX_RECORD_LEN, RECORD_HEADER_LEN, crc32, journal_id_of,
};
use crate::types::exchange_event::{EventEnvelope, ExchangeEvent};

/// Bytes 8..24 of the header hold the journal id; until milestone 23 they held the journal file's
/// device and inode, which a copy on another machine does not share.
const MAGIC: &[u8; 8] = b"EXCHBUS2";
const HEADER: usize = 80;
const READY: u64 = 1;
pub const DEFAULT_CAPACITY: usize = 4 * 1024 * 1024;
const MAX_CAPACITY: usize = 64 * 1024 * 1024;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// A writer died between clearing the ready marker and setting it again: a reader holding the
/// shared lock never sees a live writer's publication half done. The journal is intact, and a
/// restarted writer repairs the stream.
#[derive(Debug)]
struct PublicationInterrupted;

impl fmt::Display for PublicationInterrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stream publication interrupted; restart the writer to recover")
    }
}

impl std::error::Error for PublicationInterrupted {}

/// Whether a reader's error is an interrupted publication rather than a damaged stream.
pub(crate) fn is_publication_interrupted(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<PublicationInterrupted>())
}

fn word(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn journal_id_at(bytes: &[u8]) -> Uuid {
    Uuid::from_bytes(bytes[8..24].try_into().unwrap())
}

/// A consumer should save this together with its own processed state. Merely returning a batch
/// cannot guarantee exactly-once external side effects across a consumer crash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderCheckpoint {
    journal_id: Uuid,
    pub next_sequence: u64,
    byte_offset: u64,
}

impl ReaderCheckpoint {
    /// The id of the journal this checkpoint was taken against. A warm promotion passes it to
    /// `EventStore::open_suffix`, which compares it the moment it holds the writer lock — before
    /// it reads or repairs anything in the file.
    pub(crate) fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    /// The journal byte where the next unread command record starts.
    pub(crate) fn byte_offset(&self) -> u64 {
        self.byte_offset
    }
}

struct Unlock<'a>(&'a File);
impl Drop for Unlock<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

struct Mapping {
    file: File,
    map: MmapRaw,
    capacity: usize,
}

impl Mapping {
    // Raw mappings avoid creating long-lived Rust slices over externally mutable memory.
    // SAFETY CONTRACT: a private, fixed-size file; one journal owner; all copies protected by
    // file locks. No mapped pointer/reference escapes this module. Operators must not bypass
    // the protocol (in particular, truncation could cause SIGBUS).
    fn copy(&self, offset: usize, len: usize) -> Vec<u8> {
        assert!(offset <= self.map.len() && len <= self.map.len() - offset);
        let mut bytes = vec![0; len];
        // SAFETY: bounds checked above; caller holds the shared or exclusive file lock.
        unsafe {
            std::ptr::copy_nonoverlapping(self.map.as_ptr().add(offset), bytes.as_mut_ptr(), len);
        }
        bytes
    }

    fn write(&self, offset: usize, bytes: &[u8]) {
        assert!(offset <= self.map.len() && bytes.len() <= self.map.len() - offset);
        // SAFETY: bounds checked above; caller holds the exclusive file lock on a writable map.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.map.as_mut_ptr().add(offset),
                bytes.len(),
            );
        }
    }

    fn ready(&self) -> &AtomicU64 {
        // SAFETY: mmap is page aligned; offset 72 is aligned and within the fixed header. This
        // field is accessed exclusively as an atomic, including in other cooperating processes.
        unsafe { &*self.map.as_ptr().add(72).cast::<AtomicU64>() }
    }

    /// The journal id and published end in the header, by the writer's own rule: any header with
    /// a valid checksum, even while a publication is interrupted, since every record before the
    /// end was synced first. `None` if the header is torn, or at once while a writer holds the
    /// stream: only a live writer can, and then the journal's writer lock is taken too.
    fn published_end(&self) -> Option<(Uuid, u64)> {
        self.file.try_lock_shared().ok()?;
        let _unlock = Unlock(&self.file);
        let header = self.copy(0, 68);
        (crc32(&header[..64]) == u32::from_le_bytes(header[64..68].try_into().unwrap()))
            .then(|| (journal_id_at(&header), word(&header, 32)))
    }

    fn snapshot(&self, requested_offset: Option<u64>) -> io::Result<Snapshot> {
        self.file.lock_shared()?;
        let _unlock = Unlock(&self.file);
        if self.ready().load(Ordering::Acquire) != READY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PublicationInterrupted,
            ));
        }
        let header = self.copy(0, 68);
        if &header[..8] != MAGIC
            || word(&header, 24) != self.capacity as u64
            || crc32(&header[..64]) != u32::from_le_bytes(header[64..68].try_into().unwrap())
        {
            return Err(invalid("invalid stream header"));
        }
        let end = word(&header, 32);
        let start = word(&header, 48);
        let len = word(&header, 56);
        if len > self.capacity as u64
            || start < JOURNAL_HEADER_LEN as u64
            || start.checked_add(len) != Some(end)
        {
            return Err(invalid("invalid stream window bounds"));
        }
        let cache = if let Some(offset) =
            requested_offset.filter(|&offset| offset >= start && offset < end)
        {
            if end - offset < RECORD_HEADER_LEN as u64 {
                return Err(invalid("incomplete cached record header"));
            }
            let location = HEADER + (offset - start) as usize;
            let length = record_length(&self.copy(location, RECORD_HEADER_LEN))?;
            if length as u64 > end - offset {
                return Err(invalid("incomplete cached record"));
            }
            self.copy(location, length)
        } else {
            Vec::new()
        };
        Ok(Snapshot {
            journal_id: journal_id_at(&header),
            end,
            last_sequence: word(&header, 40),
            cache,
        })
    }
}

struct Snapshot {
    journal_id: Uuid,
    end: u64,
    last_sequence: u64,
    cache: Vec<u8>,
}

pub struct StreamWriter {
    mapping: Mapping,
    journal_id: Uuid,
    end: u64,
    last_sequence: u64,
    cache: Vec<u8>,
    #[cfg(test)]
    fail_publish: bool,
}

impl StreamWriter {
    /// The caller owns the journal's lifetime writer lock and has completed deterministic replay.
    /// Recovery publishes the entire validated journal as readable, with an initially empty cache.
    /// Production goes through `open_at`, which can also hold records back.
    #[cfg(test)]
    pub(super) fn open(
        path: impl AsRef<Path>,
        journal: &File,
        last_sequence: u64,
        capacity: usize,
    ) -> io::Result<Self> {
        let end = journal.metadata()?.len();
        Self::open_at(path, journal, last_sequence, (end, last_sequence), capacity)
    }

    /// Like `open`, but publishes only up to `published`: the journal end, and the last sequence
    /// before it, that readers may see. A replicated journal holds back the records its replica
    /// has not confirmed yet, and publishes them later with `publish_through`. `last_sequence` is
    /// the journal's own, for the check against what the stream already published.
    pub(super) fn open_at(
        path: impl AsRef<Path>,
        journal: &File,
        last_sequence: u64,
        published: (u64, u64),
        capacity: usize,
    ) -> io::Result<Self> {
        if !(1..=MAX_CAPACITY).contains(&capacity) {
            return Err(invalid("stream capacity must be between 1 byte and 64 MiB"));
        }
        let metadata = journal.metadata()?;
        let journal_id = journal_id_of(journal)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        // Reject aliases of the journal before acquiring the stream lock (which would deadlock).
        let bus_meta = file.metadata()?;
        if bus_meta.dev() == metadata.dev() && bus_meta.ino() == metadata.ino() {
            return Err(invalid("stream path aliases the durable journal"));
        }
        file.lock()?;
        let unlock = Unlock(&file);
        // Another journal may have tried to initialize this path while we waited for the lock.
        // Re-read length under the lock and install identity before releasing it.
        let existing_len = file.metadata()?.len();
        let new = existing_len == 0;
        if new {
            file.set_len((HEADER + capacity) as u64)?;
        } else if existing_len != (HEADER + capacity) as u64 {
            return Err(invalid("stream size changed; use its original capacity"));
        }
        // MmapRaw deliberately exposes only raw pointers; all accesses are synchronized above.
        let map = MmapOptions::new().map_raw(&file)?;
        if new {
            let mut prefix = MAGIC.to_vec();
            prefix.extend_from_slice(journal_id.as_bytes());
            prefix.extend_from_slice(&(capacity as u64).to_le_bytes());
            // SAFETY: exclusive lock; the new file has fixed size; destination is disjoint.
            unsafe {
                std::ptr::copy_nonoverlapping(prefix.as_ptr(), map.as_mut_ptr(), prefix.len());
            }
        } else {
            let mut header = [0; 68];
            // SAFETY: exclusive file lock, fixed-size mapping, local nonoverlapping destination.
            unsafe {
                std::ptr::copy_nonoverlapping(map.as_ptr(), header.as_mut_ptr(), header.len());
            }
            if &header[..8] != MAGIC
                || journal_id_at(&header) != journal_id
                || word(&header, 24) != capacity as u64
            {
                return Err(invalid("stream belongs to a different journal or format"));
            }
            // A record is published only after its sync, so what this stream published can never
            // be ahead of the recovered journal. If it is, the journal lost committed history:
            // an older copy of it, with the same id, was put in its place, or the disk lost
            // writes it had reported synced. Trading on it would lose acknowledged commands and
            // reuse offsets that readers already consumed. A torn header (bad checksum) proves
            // nothing either way.
            if crc32(&header[..64]) == u32::from_le_bytes(header[64..68].try_into().unwrap())
                && (word(&header, 32) > metadata.len() || word(&header, 40) > last_sequence)
            {
                return Err(invalid(
                    "the journal ends before what its stream already published: acknowledged commands are missing (an older copy of the journal, or a disk that lost synced writes)",
                ));
            }
        }
        drop(unlock);
        if published.0 < JOURNAL_HEADER_LEN as u64
            || published.0 > metadata.len()
            || published.1 > last_sequence
        {
            return Err(invalid("cannot publish beyond the journal"));
        }
        let mut writer = Self {
            mapping: Mapping {
                file,
                map,
                capacity,
            },
            journal_id,
            end: published.0,
            last_sequence: published.1,
            cache: Vec::new(),
            #[cfg(test)]
            fail_publish: false,
        };
        writer.publish(None)?;
        Ok(writer)
    }

    /// Publishes the journal up to `end` without appending a record: readers read what lies
    /// between from the journal. Used by a replicated primary once its replica confirms what it
    /// held back, and by the replica, which keeps no records in memory.
    pub(super) fn publish_through(&mut self, end: u64, last_sequence: u64) -> io::Result<()> {
        if end < self.end || last_sequence < self.last_sequence {
            return Err(invalid("stream publication cannot move backwards"));
        }
        self.end = end;
        self.last_sequence = last_sequence;
        // The cache window must end where the published journal ends.
        self.cache.clear();
        self.publish(None)
    }

    /// Writes the header to disk. A stream is a cache and is otherwise never synced; the replica
    /// syncs its own, because what it published is the committed end it reports to the primary.
    pub(super) fn sync_header(&self) -> io::Result<()> {
        self.mapping.map.flush_range(0, HEADER)
    }

    /// Called only AFTER durable append and core commit. Oversized batches bypass the cache;
    /// their committed bytes remain available through the journal, without splitting a command.
    pub(super) fn append(&mut self, record: &[u8], last_sequence: u64) -> io::Result<()> {
        if last_sequence <= self.last_sequence {
            return Err(invalid("stream publication sequence did not advance"));
        }
        self.end = self
            .end
            .checked_add(record.len() as u64)
            .ok_or_else(|| invalid("journal offset overflow"))?;
        self.last_sequence = last_sequence;
        if record.len() > self.mapping.capacity {
            self.cache.clear();
        } else {
            if self.cache.len() + record.len() > self.mapping.capacity {
                self.cache.clear();
            }
            self.cache.extend_from_slice(record);
        }
        self.publish(Some(record))
    }

    fn publish(&mut self, record: Option<&[u8]>) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_publish {
            return Err(io::Error::other("injected stream publication failure"));
        }
        let mut header = Vec::with_capacity(68);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(self.journal_id.as_bytes());
        for value in [
            self.mapping.capacity as u64,
            self.end,
            self.last_sequence,
            self.end - self.cache.len() as u64,
            self.cache.len() as u64,
        ] {
            header.extend_from_slice(&value.to_le_bytes());
        }
        header.extend_from_slice(&crc32(&header).to_le_bytes());
        self.mapping.file.lock()?;
        let _unlock = Unlock(&self.mapping.file);
        // SeqCst invalidation must precede any overwrite, even if the process dies mid-copy.
        self.mapping.ready().store(0, Ordering::SeqCst);
        if let Some(record) = record.filter(|record| record.len() <= self.mapping.capacity) {
            self.mapping
                .write(HEADER + self.cache.len() - record.len(), record);
        }
        self.mapping.write(0, &header);
        self.mapping.ready().store(READY, Ordering::Release);
        // No msync: the mmap cache is transport, never the durability boundary.
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn fail_publication_for_test(&mut self) {
        self.fail_publish = true;
    }
}

/// An independent consumer. When it reaches the last committed end it validated, `next_batch`
/// reads the stream header, and the next record from the cache window if it is there, under a
/// short shared lock; records before that end come straight from the journal. Validation and
/// deserializing happen outside the lock, and consumer processing never holds a writer-blocking
/// lock.
pub struct StreamReader {
    mapping: Mapping,
    journal: File,
    cursor: ReaderCheckpoint,
    /// The committed end and last sequence of the last stream header this reader validated.
    /// Everything before that end is committed and never changes.
    last_watermark: u64,
    last_sequence: u64,
    journal_only: bool,
}

impl StreamReader {
    pub fn checkpoint_from_parts(
        journal_id: Uuid,
        next_sequence: u64,
        byte_offset: u64,
    ) -> ReaderCheckpoint {
        ReaderCheckpoint {
            journal_id,
            next_sequence,
            byte_offset,
        }
    }

    pub fn open(
        journal_path: impl AsRef<Path>,
        stream_path: impl AsRef<Path>,
        checkpoint: Option<ReaderCheckpoint>,
    ) -> io::Result<Self> {
        let journal = File::open(journal_path)?;
        let journal_id = journal_id_of(&journal)?;
        let file = File::open(stream_path)?;
        let size = file.metadata()?.len();
        if size <= HEADER as u64 || size > (HEADER + MAX_CAPACITY) as u64 {
            return Err(invalid("invalid stream file size"));
        }
        let map = MmapOptions::new().map_raw_read_only(&file)?;
        let mapping = Mapping {
            file,
            map,
            capacity: size as usize - HEADER,
        };
        let snapshot = mapping.snapshot(None)?;
        if snapshot.journal_id != journal_id {
            return Err(invalid("stream and journal identities differ"));
        }
        let cursor = checkpoint.unwrap_or(ReaderCheckpoint {
            journal_id,
            next_sequence: 1,
            byte_offset: JOURNAL_HEADER_LEN as u64,
        });
        if cursor.journal_id != journal_id
            || cursor.byte_offset < JOURNAL_HEADER_LEN as u64
            || cursor.next_sequence == 0
        {
            return Err(invalid(
                "checkpoint belongs to a different journal or is invalid",
            ));
        }
        // A checkpoint is checked where it points, never by re-reading the history before it, so
        // a restart costs the same however long the journal is: the record there must be complete
        // and begin with the checkpoint's next sequence. A checkpoint at the committed end is
        // checked against the published last sequence by `validate_snapshot`.
        if cursor.byte_offset < snapshot.end {
            read_record(&journal, cursor.byte_offset, snapshot.end)
                .and_then(|bytes| decode_batch(&bytes, cursor.next_sequence))
                .map_err(|_| invalid("checkpoint is not at a complete command boundary"))?;
        }
        let reader = Self {
            mapping,
            journal,
            cursor,
            last_watermark: snapshot.end,
            last_sequence: snapshot.last_sequence,
            journal_only: false,
        };
        reader.validate_snapshot(&snapshot)?;
        Ok(reader)
    }

    pub fn checkpoint(&self) -> ReaderCheckpoint {
        self.cursor.clone()
    }

    /// Reads every batch from the durable journal and uses the mmap stream only for its committed
    /// watermark. The cache copy is faster, but a structurally valid cache can still disagree with
    /// the journal, and a consumer whose state must be as trustworthy as journal recovery — the
    /// snapshot-writing warm replica — must never build it from the cache.
    pub(crate) fn journal_only(mut self) -> Self {
        self.journal_only = true;
        self
    }

    /// The journal file this reader reads, as opened: a promotion takes over only this file.
    pub(crate) fn journal(&self) -> &File {
        &self.journal
    }

    /// The journal end the stream has published, which a promotion's journal must still reach:
    /// every record before it was synced first, and readers may already have consumed it. It never
    /// waits for a writer. If the header cannot be read now, the last end this reader validated.
    pub(crate) fn published_end(&self) -> u64 {
        match self.mapping.published_end() {
            Some((journal_id, end)) if journal_id == self.cursor.journal_id => {
                end.max(self.last_watermark)
            }
            _ => self.last_watermark,
        }
    }

    fn validate_snapshot(&self, snapshot: &Snapshot) -> io::Result<()> {
        if snapshot.journal_id != self.cursor.journal_id
            || snapshot.end < self.last_watermark
            || snapshot.end < self.cursor.byte_offset
            || self.journal.metadata()?.len() < snapshot.end
        {
            return Err(invalid(
                "journal identity changed or committed history regressed",
            ));
        }
        let next = snapshot
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        if self.cursor.next_sequence > next
            || (self.cursor.byte_offset == snapshot.end) != (self.cursor.next_sequence == next)
        {
            return Err(invalid("published sequence and byte position disagree"));
        }
        Ok(())
    }

    pub fn next_batch(&mut self) -> io::Result<Option<Vec<EventEnvelope>>> {
        // The stream is consulted only once the reader reaches the last end it validated. Before
        // that, records are committed and never change, so a reader that is behind reads them
        // straight from the journal: two reads per record instead of six system calls.
        let mut cache = Vec::new();
        if self.cursor.byte_offset == self.last_watermark {
            let cached = (!self.journal_only).then_some(self.cursor.byte_offset);
            let snapshot = self.mapping.snapshot(cached)?;
            self.validate_snapshot(&snapshot)?;
            self.last_watermark = snapshot.end;
            self.last_sequence = snapshot.last_sequence;
            if self.cursor.byte_offset == snapshot.end {
                return Ok(None);
            }
            cache = snapshot.cache;
        }
        let bytes = if !cache.is_empty() {
            cache
        } else {
            read_record(&self.journal, self.cursor.byte_offset, self.last_watermark)?
        };
        let batch = decode_batch(&bytes, self.cursor.next_sequence)?;
        let next_sequence = batch
            .last()
            .unwrap()
            .seq_num
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        let end = self.cursor.byte_offset + bytes.len() as u64;
        if end > self.last_watermark
            || next_sequence > self.last_sequence + 1
            || (end == self.last_watermark) != (next_sequence == self.last_sequence + 1)
        {
            return Err(invalid("batch disagrees with committed watermark"));
        }
        // Advance only after the entire batch has passed validation.
        self.cursor.byte_offset = end;
        self.cursor.next_sequence = next_sequence;
        Ok(Some(batch))
    }
}

/// The journal end, and the last sequence before it, that the stream file at `path` already
/// published for this journal: a whole header with a valid checksum. Otherwise nothing beyond
/// the journal header. Read without the stream lock: only the journal's writer, who holds the
/// journal's writer lock and calls this, ever writes the stream.
pub(crate) fn already_published(path: impl AsRef<Path>, journal: &File) -> io::Result<(u64, u64)> {
    let journal_id = journal_id_of(journal)?;
    let nothing = (JOURNAL_HEADER_LEN as u64, 0);
    let mut header = [0; 68];
    let Ok(file) = File::open(path) else {
        return Ok(nothing);
    };
    if file.read_exact_at(&mut header, 0).is_err()
        || &header[..8] != MAGIC
        || journal_id_at(&header) != journal_id
        || crc32(&header[..64]) != u32::from_le_bytes(header[64..68].try_into().unwrap())
    {
        return Ok(nothing);
    }
    Ok((word(&header, 32), word(&header, 40)))
}

pub(super) fn record_length(header: &[u8]) -> io::Result<usize> {
    if header.len() < RECORD_HEADER_LEN {
        return Err(invalid("incomplete record header"));
    }
    let len = u32::from_le_bytes(header[..4].try_into().unwrap());
    if len > MAX_RECORD_LEN {
        return Err(invalid("record exceeds size limit"));
    }
    Ok(RECORD_HEADER_LEN + len as usize)
}

pub(super) fn read_record(file: &File, offset: u64, committed_end: u64) -> io::Result<Vec<u8>> {
    if offset
        .checked_add(RECORD_HEADER_LEN as u64)
        .is_none_or(|end| end > committed_end)
    {
        return Err(invalid("record header exceeds committed prefix"));
    }
    let mut header = [0; RECORD_HEADER_LEN];
    file.read_exact_at(&mut header, offset)?;
    let len = record_length(&header)?;
    if offset
        .checked_add(len as u64)
        .is_none_or(|end| end > committed_end)
    {
        return Err(invalid("record exceeds committed prefix"));
    }
    let mut bytes = vec![0; len];
    bytes[..RECORD_HEADER_LEN].copy_from_slice(&header);
    file.read_exact_at(
        &mut bytes[RECORD_HEADER_LEN..],
        offset + RECORD_HEADER_LEN as u64,
    )?;
    Ok(bytes)
}

pub(super) fn decode_batch(bytes: &[u8], first_sequence: u64) -> io::Result<Vec<EventEnvelope>> {
    if record_length(bytes)? != bytes.len()
        || crc32(&bytes[8..]) != u32::from_le_bytes(bytes[4..8].try_into().unwrap())
    {
        return Err(invalid("record checksum or length mismatch"));
    }
    let batch: Vec<EventEnvelope> = serde_json::from_slice(&bytes[8..])
        .map_err(|_| invalid("record contains invalid event JSON"))?;
    if batch.len() < 2 || !matches!(batch[0].event, ExchangeEvent::Input(_)) {
        return Err(invalid("record must contain one input and its outputs"));
    }
    for (index, envelope) in batch.iter().enumerate() {
        if first_sequence.checked_add(index as u64) != Some(envelope.seq_num)
            || (index > 0 && !matches!(envelope.event, ExchangeEvent::Output(_)))
        {
            return Err(invalid(
                "event sequence gap, duplicate, or invalid command boundary",
            ));
        }
    }
    Ok(batch)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{
        exchange::event_store::{EventStore, encode_record},
        types::exchange_event::{ExchangeInputEvent, ExchangeOutputEvent},
    };
    use std::{
        path::PathBuf,
        time::{Duration, Instant},
    };

    pub struct Fixture {
        pub dir: PathBuf,
        pub log: PathBuf,
        pub bus: PathBuf,
    }
    impl Fixture {
        pub fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("stock-stream-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            Self {
                log: dir.join("events.log"),
                bus: dir.join("events.mmap"),
                dir,
            }
        }
        pub fn start(&self, capacity: usize) -> (EventStore, StreamWriter) {
            let (store, events) = EventStore::open(&self.log).unwrap();
            let stream = StreamWriter::open(
                &self.bus,
                store.file(),
                events.last().map_or(0, |e| e.seq_num),
                capacity,
            )
            .unwrap();
            (store, stream)
        }
        pub fn reader(&self) -> StreamReader {
            StreamReader::open(&self.log, &self.bus, None).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    pub fn batch(seq: u64) -> Vec<EventEnvelope> {
        vec![
            EventEnvelope {
                seq_num: seq,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
            EventEnvelope {
                seq_num: seq + 1,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
        ]
    }
    fn append(store: &mut EventStore, writer: &mut StreamWriter, seq: u64) {
        let record = encode_record(&batch(seq)).unwrap();
        store.append_record(&record).unwrap();
        writer.append(&record, seq + 1).unwrap();
    }

    #[test]
    fn independent_readers_catch_up_after_window_overwrite_and_follow_live() {
        let fixture = Fixture::new();
        let size = encode_record(&batch(1)).unwrap().len();
        let (mut store, mut writer) = fixture.start(size * 2);
        let mut slow = fixture.reader();
        let mut fast = fixture.reader();
        assert!(fast.next_batch().unwrap().is_none());
        for seq in (1..=19).step_by(2) {
            append(&mut store, &mut writer, seq);
            assert_eq!(fast.next_batch().unwrap().unwrap(), batch(seq));
            assert!(fast.next_batch().unwrap().is_none());
        }
        for seq in (1..=19).step_by(2) {
            assert_eq!(slow.next_batch().unwrap().unwrap(), batch(seq));
        }
        assert_eq!(slow.checkpoint(), fast.checkpoint());
        append(&mut store, &mut writer, 21);
        assert_eq!(slow.next_batch().unwrap().unwrap(), batch(21));
        assert_eq!(fast.next_batch().unwrap().unwrap(), batch(21));
    }

    #[test]
    fn oversize_batches_and_recovered_history_use_journal_then_return_to_mmap() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(1);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
        let checkpoint = reader.checkpoint();
        drop(writer);
        drop(store);
        let (mut store, mut writer) = fixture.start(1);
        let mut resumed = StreamReader::open(&fixture.log, &fixture.bus, Some(checkpoint)).unwrap();
        assert!(resumed.next_batch().unwrap().is_none());
        append(&mut store, &mut writer, 3);
        assert_eq!(resumed.next_batch().unwrap().unwrap(), batch(3));
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(3));
    }

    #[test]
    fn durable_but_unpublished_record_stays_hidden_until_recovery() {
        let fixture = Fixture::new();
        let (mut store, writer) = fixture.start(4096);
        let mut reader = fixture.reader();
        store.append(&batch(1)).unwrap();
        assert!(reader.next_batch().unwrap().is_none());
        drop(writer);
        drop(store);
        let (mut store, mut writer) = fixture.start(4096);
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
        append(&mut store, &mut writer, 3);
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(3));
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn incomplete_publication_is_refused_and_repaired_on_restart() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        writer.mapping.file.lock().unwrap();
        writer.mapping.ready().store(0, Ordering::SeqCst);
        writer.mapping.file.unlock().unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.checkpoint().next_sequence, 1);
        drop(writer);
        drop(store);
        let (_store, _writer) = fixture.start(4096);
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
    }

    #[test]
    fn corrupt_cache_or_journal_never_advances_the_reader() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        writer.mapping.file.lock().unwrap();
        writer.mapping.write(HEADER + 10, &[0xff]);
        writer.mapping.file.unlock().unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.checkpoint().next_sequence, 1);
        // It had validated the committed end before the damaged copy was refused, so it reads
        // that record from the journal now, which is intact.
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
        let mut reader = fixture.reader();
        OpenOptions::new()
            .write(true)
            .open(&fixture.log)
            .unwrap()
            .write_all_at(&[0xff], JOURNAL_HEADER_LEN as u64 + 12)
            .unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.checkpoint().next_sequence, 1);
    }

    /// Behind the last committed end it validated, a reader reads records from the journal
    /// without consulting the stream. They are committed and never change, so it delivers them
    /// even when the stream breaks meanwhile, and notices the break once it reaches that end.
    #[test]
    fn a_reader_behind_reads_committed_records_without_consulting_the_stream() {
        let fixture = Fixture::new();
        // A one-byte window caches nothing, so every batch is read from the journal.
        let (mut store, mut writer) = fixture.start(1);
        for seq in [1, 3, 5] {
            append(&mut store, &mut writer, seq);
        }
        let mut reader = fixture.reader();
        writer.mapping.file.lock().unwrap();
        writer.mapping.ready().store(0, Ordering::SeqCst);
        writer.mapping.file.unlock().unwrap();

        for seq in [1, 3, 5] {
            assert_eq!(reader.next_batch().unwrap().unwrap(), batch(seq));
        }
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.checkpoint().next_sequence, 7);
    }

    /// What a promotion must still find in the journal: the end in any header with a valid
    /// checksum, even mid-publication, as the writer itself trusts it. Never waiting for a writer,
    /// and falling back to the last end the reader validated when the header cannot be used.
    #[test]
    fn the_published_end_is_read_as_the_writer_trusts_it_and_never_waits() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        let published = store.file().metadata().unwrap().len();
        let validated = JOURNAL_HEADER_LEN as u64;
        assert_eq!(reader.published_end(), published);

        // An interrupted publication clears the ready marker, but the header is whole.
        writer.mapping.file.lock().unwrap();
        writer.mapping.ready().store(0, Ordering::SeqCst);
        assert_eq!(
            reader.published_end(),
            validated,
            "a writer holds the stream"
        );
        writer.mapping.file.unlock().unwrap();
        assert_eq!(reader.published_end(), published);

        // A torn header proves nothing.
        writer.mapping.file.lock().unwrap();
        let checksum_byte = writer.mapping.copy(64, 1)[0];
        writer.mapping.write(64, &[!checksum_byte]);
        writer.mapping.file.unlock().unwrap();
        assert_eq!(reader.published_end(), validated);
    }

    /// A checkpoint is checked where it points: a restart reads the record there, never the
    /// history before it, so it costs the same however long the journal is. Damage earlier in
    /// the journal is therefore invisible to it, while a reader starting from the beginning is
    /// refused at that damage.
    #[test]
    fn a_checkpoint_is_checked_where_it_points_without_rereading_history() {
        let fixture = Fixture::new();
        // A one-byte window caches nothing, so every batch is read from the journal.
        let (mut store, mut writer) = fixture.start(1);
        let mut reader = fixture.reader();
        for seq in [1, 3, 5] {
            append(&mut store, &mut writer, seq);
        }
        reader.next_batch().unwrap().unwrap();
        reader.next_batch().unwrap().unwrap();
        let checkpoint = reader.checkpoint();
        OpenOptions::new()
            .write(true)
            .open(&fixture.log)
            .unwrap()
            .write_all_at(&[0xff], JOURNAL_HEADER_LEN as u64 + 12)
            .unwrap();

        let mut resumed = StreamReader::open(&fixture.log, &fixture.bus, Some(checkpoint)).unwrap();
        assert_eq!(resumed.next_batch().unwrap().unwrap(), batch(5));
        assert!(StreamReader::open(&fixture.log, &fixture.bus, None).is_err());
    }

    /// The journal id travels with the bytes, so a checkpoint is valid on a byte-identical copy
    /// of the journal at another path, as it will be on another machine.
    #[test]
    fn a_checkpoint_is_valid_on_a_copy_of_the_journal() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(1);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        append(&mut store, &mut writer, 3);
        reader.next_batch().unwrap().unwrap();
        let copy = fixture.dir.join("copy.log");
        std::fs::copy(&fixture.log, &copy).unwrap();

        let mut on_copy =
            StreamReader::open(&copy, &fixture.bus, Some(reader.checkpoint())).unwrap();
        assert_eq!(on_copy.next_batch().unwrap().unwrap(), batch(3));
        assert_eq!(on_copy.checkpoint().journal_id(), store.journal_id());
    }

    #[test]
    fn checkpoint_must_match_journal_identity_and_complete_batch_boundary() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        append(&mut store, &mut writer, 3);
        reader.next_batch().unwrap();
        // In the middle of the journal, the record at the checkpoint is what proves it.
        let middle = reader.checkpoint();
        assert_eq!(
            StreamReader::open(&fixture.log, &fixture.bus, Some(middle.clone()))
                .unwrap()
                .next_batch()
                .unwrap()
                .unwrap(),
            batch(3)
        );
        // At the committed end, the published last sequence is.
        reader.next_batch().unwrap();
        let end = reader.checkpoint();
        assert!(
            StreamReader::open(&fixture.log, &fixture.bus, Some(end.clone()))
                .unwrap()
                .next_batch()
                .unwrap()
                .is_none()
        );
        for bad in [
            ReaderCheckpoint {
                next_sequence: 4,
                ..middle.clone()
            },
            ReaderCheckpoint {
                byte_offset: middle.byte_offset - 1,
                ..middle.clone()
            },
            ReaderCheckpoint {
                byte_offset: middle.byte_offset + 1,
                ..middle.clone()
            },
            ReaderCheckpoint {
                journal_id: Uuid::new_v4(),
                ..middle.clone()
            },
            ReaderCheckpoint {
                next_sequence: 6,
                ..end.clone()
            },
            ReaderCheckpoint {
                byte_offset: end.byte_offset + 1,
                ..end.clone()
            },
        ] {
            assert!(StreamReader::open(&fixture.log, &fixture.bus, Some(bad)).is_err());
        }
    }

    #[test]
    fn competing_writer_and_foreign_stream_are_refused_without_modification() {
        let fixture = Fixture::new();
        let (store, _writer) = fixture.start(4096);
        assert!(EventStore::open(&fixture.log).is_err());
        assert!(StreamWriter::open(&fixture.log, store.file(), 0, 4096).is_err());
        assert_eq!(
            std::fs::read(&fixture.log).unwrap().len(),
            JOURNAL_HEADER_LEN
        );
        let other = Fixture::new();
        let (other_store, _) = EventStore::open(&other.log).unwrap();
        let before = std::fs::read(&fixture.bus).unwrap();
        assert!(StreamWriter::open(&fixture.bus, other_store.file(), 0, 4096).is_err());
        assert_eq!(std::fs::read(&fixture.bus).unwrap(), before);
        assert!(StreamReader::open(&other.log, &fixture.bus, None).is_err());
    }

    /// An older copy of the journal keeps its id, so only the stream's watermark can show that it
    /// is behind: the writer refuses to start on it, and leaves the stream as it was. A journal
    /// behind in length alone, or in last sequence alone, is refused too.
    #[test]
    fn a_journal_behind_what_its_stream_published_is_refused() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        append(&mut store, &mut writer, 1);
        let first_end = store.file().metadata().unwrap().len() as usize;
        append(&mut store, &mut writer, 3);
        drop(writer);
        drop(store);
        let journal = std::fs::read(&fixture.log).unwrap();
        let stream_before = std::fs::read(&fixture.bus).unwrap();

        // (journal, recovered last sequence): behind in length, in sequence, in both.
        for (bytes, last_sequence) in [
            (&journal[..first_end], 4),
            (&journal[..], 2),
            (&journal[..first_end], 2),
        ] {
            std::fs::write(&fixture.log, bytes).unwrap();
            let (store, _) = EventStore::open(&fixture.log).unwrap();
            assert!(StreamWriter::open(&fixture.bus, store.file(), last_sequence, 4096).is_err());
            assert_eq!(std::fs::read(&fixture.bus).unwrap(), stream_before);
        }

        // The whole journal, with its real last sequence, still starts.
        std::fs::write(&fixture.log, &journal).unwrap();
        let (store, _) = EventStore::open(&fixture.log).unwrap();
        assert!(StreamWriter::open(&fixture.bus, store.file(), 4, 4096).is_ok());
    }

    #[test]
    fn batch_validation_rejects_gaps_duplicates_and_partial_commands() {
        let mut events = batch(1);
        events[1].seq_num = 1;
        assert!(decode_batch(&encode_record(&events).unwrap(), 1).is_err());
        assert!(decode_batch(&encode_record(&batch(3)).unwrap(), 1).is_err());
        assert!(decode_batch(&encode_record(&batch(1)[..1]).unwrap(), 1).is_err());
        assert!(decode_batch(&encode_record(&[batch(1), batch(3)].concat()).unwrap(), 1).is_err());
    }

    // Invoked in a different OS process by the following parent test.
    #[test]
    fn reader_process_helper() {
        let Ok(dir) = std::env::var("STOCK_STREAM_TEST_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let mut reader =
            StreamReader::open(dir.join("events.log"), dir.join("events.mmap"), None).unwrap();
        std::fs::write(dir.join("ready"), b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        for seq in (1..=199).step_by(2) {
            loop {
                if let Some(actual) = reader.next_batch().unwrap() {
                    assert_eq!(actual, batch(seq));
                    break;
                }
                assert!(Instant::now() < deadline, "reader timed out");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn separate_process_reads_concurrent_publication_without_loss() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(1024);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "exchange::event_stream::tests::reader_process_helper",
                "--nocapture",
            ])
            .env("STOCK_STREAM_TEST_DIR", &fixture.dir)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !fixture.dir.join("ready").exists() {
            if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("reader failed to start");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        for seq in (1..=199).step_by(2) {
            append(&mut store, &mut writer, seq);
        }
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn crashing_writer_process_helper() {
        let Ok(dir) = std::env::var("STOCK_CRASH_TEST_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let (mut store, _) = EventStore::open(dir.join("events.log")).unwrap();
        let writer = StreamWriter::open(dir.join("events.mmap"), store.file(), 0, 4096).unwrap();
        store.append(&batch(1)).unwrap();
        if std::env::var("STOCK_CRASH_DURING_COPY").is_ok() {
            writer.mapping.file.lock().unwrap();
            writer.mapping.ready().store(0, Ordering::SeqCst);
            writer.mapping.write(HEADER, b"incomplete cache bytes");
            // Deliberately leave the exclusive lock held until the parent kills this process.
        }
        std::fs::write(dir.join("ready"), b"durable").unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[test]
    fn sigkill_after_append_and_during_publication_recovers_without_loss() {
        for mid_copy in [false, true] {
            let fixture = Fixture::new();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "exchange::event_stream::tests::crashing_writer_process_helper",
                ])
                .env("STOCK_CRASH_TEST_DIR", &fixture.dir);
            if mid_copy {
                command.env("STOCK_CRASH_DURING_COPY", "1");
            }
            let mut child = command.spawn().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !fixture.dir.join("ready").exists() {
                if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("writer failed to start");
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            if mid_copy {
                assert!(StreamReader::open(&fixture.log, &fixture.bus, None).is_err());
            } else {
                assert!(fixture.reader().next_batch().unwrap().is_none());
            }
            let (mut store, mut writer) = fixture.start(4096);
            let mut reader = fixture.reader();
            assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
            append(&mut store, &mut writer, 3);
            assert_eq!(reader.next_batch().unwrap().unwrap(), batch(3));
            assert!(reader.next_batch().unwrap().is_none());
        }
    }
}
