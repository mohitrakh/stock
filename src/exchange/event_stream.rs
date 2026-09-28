//! Linux/Unix, same-host committed-event transport. The journal is authoritative; this bounded
//! mmap window is disposable. All participants must use this locking protocol and must never
//! truncate, replace, or modify either backing file while it is in use.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use memmap2::{MmapOptions, MmapRaw};
use serde::{Deserialize, Serialize};

use super::event_store::{FILE_MAGIC, MAX_RECORD_LEN, RECORD_HEADER_LEN, crc32};
use crate::types::exchange_event::{EventEnvelope, ExchangeEvent};

const MAGIC: &[u8; 8] = b"EXCHBUS1";
const HEADER: usize = 80;
const READY: u64 = 1;
pub const DEFAULT_CAPACITY: usize = 4 * 1024 * 1024;
const MAX_CAPACITY: usize = 64 * 1024 * 1024;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn word(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

/// A consumer should save this together with its own processed state. Merely returning a batch
/// cannot guarantee exactly-once external side effects across a consumer crash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderCheckpoint {
    device: u64,
    inode: u64,
    pub next_sequence: u64,
    byte_offset: u64,
}

impl ReaderCheckpoint {
    /// Checks that a writer-owned journal is still the file this checkpoint describes. A warm
    /// replica uses this after it has acquired the writer lock, before allowing the hand-off to
    /// a primary factory that will fully replay the authoritative journal.
    pub(crate) fn matches_journal_identity(&self, device: u64, inode: u64) -> bool {
        self.device == device && self.inode == inode
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

    fn snapshot(&self, requested_offset: Option<u64>) -> io::Result<Snapshot> {
        self.file.lock_shared()?;
        let _unlock = Unlock(&self.file);
        if self.ready().load(Ordering::Acquire) != READY {
            return Err(invalid(
                "stream publication interrupted; restart the writer to recover",
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
        if len > self.capacity as u64 || start < 8 || start.checked_add(len) != Some(end) {
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
            device: word(&header, 8),
            inode: word(&header, 16),
            end,
            last_sequence: word(&header, 40),
            cache,
        })
    }
}

struct Snapshot {
    device: u64,
    inode: u64,
    end: u64,
    last_sequence: u64,
    cache: Vec<u8>,
}

pub struct StreamWriter {
    mapping: Mapping,
    device: u64,
    inode: u64,
    end: u64,
    last_sequence: u64,
    cache: Vec<u8>,
    #[cfg(test)]
    fail_publish: bool,
}

impl StreamWriter {
    /// The caller owns the journal's lifetime writer lock and has completed deterministic replay.
    /// Recovery publishes the entire validated journal as readable, with an initially empty cache.
    pub(super) fn open(
        path: impl AsRef<Path>,
        journal: &File,
        last_sequence: u64,
        capacity: usize,
    ) -> io::Result<Self> {
        if !(1..=MAX_CAPACITY).contains(&capacity) {
            return Err(invalid("stream capacity must be between 1 byte and 64 MiB"));
        }
        let metadata = journal.metadata()?;
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
            for value in [metadata.dev(), metadata.ino(), capacity as u64] {
                prefix.extend_from_slice(&value.to_le_bytes());
            }
            // SAFETY: exclusive lock; the new file has fixed size; destination is disjoint.
            unsafe {
                std::ptr::copy_nonoverlapping(prefix.as_ptr(), map.as_mut_ptr(), prefix.len());
            }
        } else {
            let mut prefix = [0; 32];
            // SAFETY: exclusive file lock, fixed-size mapping, local nonoverlapping destination.
            unsafe {
                std::ptr::copy_nonoverlapping(map.as_ptr(), prefix.as_mut_ptr(), prefix.len());
            }
            if &prefix[..8] != MAGIC
                || word(&prefix, 8) != metadata.dev()
                || word(&prefix, 16) != metadata.ino()
                || word(&prefix, 24) != capacity as u64
            {
                return Err(invalid("stream belongs to a different journal or format"));
            }
        }
        drop(unlock);
        let mut writer = Self {
            mapping: Mapping {
                file,
                map,
                capacity,
            },
            device: metadata.dev(),
            inode: metadata.ino(),
            end: metadata.len(),
            last_sequence,
            cache: Vec::new(),
            #[cfg(test)]
            fail_publish: false,
        };
        writer.publish(None)?;
        Ok(writer)
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
        for value in [
            self.device,
            self.inode,
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

/// An independent consumer. `next_batch` copies under a short shared lock, then validates and
/// deserializes outside it. Consumer processing never holds a writer-blocking lock.
pub struct StreamReader {
    mapping: Mapping,
    journal: File,
    cursor: ReaderCheckpoint,
    last_watermark: u64,
}

impl StreamReader {
    pub fn checkpoint_from_parts(
        device: u64,
        inode: u64,
        next_sequence: u64,
        byte_offset: u64,
    ) -> ReaderCheckpoint {
        ReaderCheckpoint {
            device,
            inode,
            next_sequence,
            byte_offset,
        }
    }

    pub fn open(
        journal_path: impl AsRef<Path>,
        stream_path: impl AsRef<Path>,
        checkpoint: Option<ReaderCheckpoint>,
    ) -> io::Result<Self> {
        let mut journal = File::open(journal_path)?;
        let meta = journal.metadata()?;
        let mut magic = [0; 8];
        journal.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid("invalid journal magic"));
        }
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
        if snapshot.device != meta.dev() || snapshot.inode != meta.ino() {
            return Err(invalid("stream and journal identities differ"));
        }
        let cursor = checkpoint.unwrap_or(ReaderCheckpoint {
            device: meta.dev(),
            inode: meta.ino(),
            next_sequence: 1,
            byte_offset: 8,
        });
        if cursor.device != meta.dev()
            || cursor.inode != meta.ino()
            || cursor.byte_offset < 8
            || cursor.next_sequence == 0
        {
            return Err(invalid(
                "checkpoint belongs to a different journal or is invalid",
            ));
        }
        // Validate an externally supplied checkpoint against actual record boundaries. Restart
        // is O(history); snapshots/indexed seeking are deliberately outside this milestone.
        let mut offset = 8;
        let mut next = 1;
        while offset < cursor.byte_offset {
            let bytes = read_record(&mut journal, offset, snapshot.end)?;
            let batch = decode_batch(&bytes, next)?;
            next = batch
                .last()
                .unwrap()
                .seq_num
                .checked_add(1)
                .ok_or_else(|| invalid("sequence overflow"))?;
            offset += bytes.len() as u64;
        }
        if offset != cursor.byte_offset || next != cursor.next_sequence {
            return Err(invalid("checkpoint is not at a complete command boundary"));
        }
        let reader = Self {
            mapping,
            journal,
            cursor,
            last_watermark: snapshot.end,
        };
        reader.validate_snapshot(&snapshot)?;
        Ok(reader)
    }

    pub fn checkpoint(&self) -> ReaderCheckpoint {
        self.cursor.clone()
    }

    fn validate_snapshot(&self, snapshot: &Snapshot) -> io::Result<()> {
        if snapshot.device != self.cursor.device
            || snapshot.inode != self.cursor.inode
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
        let snapshot = self.mapping.snapshot(Some(self.cursor.byte_offset))?;
        self.validate_snapshot(&snapshot)?;
        self.last_watermark = snapshot.end;
        if self.cursor.byte_offset == snapshot.end {
            return Ok(None);
        }
        let bytes = if !snapshot.cache.is_empty() {
            snapshot.cache
        } else {
            read_record(&mut self.journal, self.cursor.byte_offset, snapshot.end)?
        };
        let batch = decode_batch(&bytes, self.cursor.next_sequence)?;
        let next_sequence = batch
            .last()
            .unwrap()
            .seq_num
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        let end = self.cursor.byte_offset + bytes.len() as u64;
        if end > snapshot.end
            || next_sequence > snapshot.last_sequence + 1
            || (end == snapshot.end) != (next_sequence == snapshot.last_sequence + 1)
        {
            return Err(invalid("batch disagrees with committed watermark"));
        }
        // Advance only after the entire batch has passed validation.
        self.cursor.byte_offset = end;
        self.cursor.next_sequence = next_sequence;
        Ok(Some(batch))
    }
}

fn record_length(header: &[u8]) -> io::Result<usize> {
    if header.len() < RECORD_HEADER_LEN {
        return Err(invalid("incomplete record header"));
    }
    let len = u32::from_le_bytes(header[..4].try_into().unwrap());
    if len > MAX_RECORD_LEN {
        return Err(invalid("record exceeds size limit"));
    }
    Ok(RECORD_HEADER_LEN + len as usize)
}

fn read_record(file: &mut File, offset: u64, committed_end: u64) -> io::Result<Vec<u8>> {
    if offset
        .checked_add(RECORD_HEADER_LEN as u64)
        .is_none_or(|end| end > committed_end)
    {
        return Err(invalid("record header exceeds committed prefix"));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0; RECORD_HEADER_LEN];
    file.read_exact(&mut header)?;
    let len = record_length(&header)?;
    if offset
        .checked_add(len as u64)
        .is_none_or(|end| end > committed_end)
    {
        return Err(invalid("record exceeds committed prefix"));
    }
    let mut bytes = vec![0; len];
    bytes[..RECORD_HEADER_LEN].copy_from_slice(&header);
    file.read_exact(&mut bytes[RECORD_HEADER_LEN..])?;
    Ok(bytes)
}

fn decode_batch(bytes: &[u8], first_sequence: u64) -> io::Result<Vec<EventEnvelope>> {
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
        drop(writer);
        drop(store);
        let (_store, _writer) = fixture.start(4096);
        // Recovery clears the damaged cache; journal is read only up to its committed boundary.
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch(1));
        let mut reader = fixture.reader();
        use std::os::unix::fs::FileExt;
        OpenOptions::new()
            .write(true)
            .open(&fixture.log)
            .unwrap()
            .write_all_at(&[0xff], 20)
            .unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.checkpoint().next_sequence, 1);
    }

    #[test]
    fn checkpoint_must_match_journal_identity_and_complete_batch_boundary() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut reader = fixture.reader();
        append(&mut store, &mut writer, 1);
        reader.next_batch().unwrap();
        let good = reader.checkpoint();
        assert!(
            StreamReader::open(&fixture.log, &fixture.bus, Some(good.clone()))
                .unwrap()
                .next_batch()
                .unwrap()
                .is_none()
        );
        for bad in [
            ReaderCheckpoint {
                next_sequence: 2,
                ..good.clone()
            },
            ReaderCheckpoint {
                byte_offset: good.byte_offset - 1,
                ..good.clone()
            },
            ReaderCheckpoint {
                inode: good.inode + 1,
                ..good.clone()
            },
            ReaderCheckpoint {
                byte_offset: good.byte_offset + 1,
                ..good.clone()
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
        assert_eq!(std::fs::read(&fixture.log).unwrap(), FILE_MAGIC);
        let other = Fixture::new();
        let (other_store, _) = EventStore::open(&other.log).unwrap();
        let before = std::fs::read(&fixture.bus).unwrap();
        assert!(StreamWriter::open(&fixture.bus, other_store.file(), 0, 4096).is_err());
        assert_eq!(std::fs::read(&fixture.bus).unwrap(), before);
        assert!(StreamReader::open(&other.log, &fixture.bus, None).is_err());
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
