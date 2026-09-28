use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use crate::types::exchange_event::EventEnvelope;

/// Marks the file as an exchange event log and pins the on-disk format. A format change bumps the
/// trailing digit, so an old file is rejected with a clear message instead of failing somewhere
/// deep in a JSON parse.
pub(super) const FILE_MAGIC: &[u8; 8] = b"EXCHLOG1";

/// `[len: u32 LE][crc: u32 LE]` ahead of every payload.
pub(super) const RECORD_HEADER_LEN: usize = 8;

/// Refuses a length field that could only come from corruption, before it is used to size an
/// allocation.
pub(super) const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum EventStoreError {
    Io(std::io::Error),
    /// The file exists but is not an event log, or not one this build can read.
    BadMagic,
    /// Damage that is not a torn tail: a bad checksum or unreadable payload with more records
    /// after it. Recovery refuses rather than guessing which part is trustworthy.
    Corrupt(String),
    /// A warm process found a different journal at its configured path after it took the writer
    /// lock. This is checked before recovery can truncate a torn tail in the wrong file.
    JournalIdentityMismatch {
        expected_device: u64,
        expected_inode: u64,
        actual_device: u64,
        actual_inode: u64,
    },
}

impl std::fmt::Display for EventStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "event log I/O error: {}", err),
            Self::BadMagic => write!(
                f,
                "file is not an exchange event log (expected magic {:?})",
                String::from_utf8_lossy(FILE_MAGIC)
            ),
            Self::Corrupt(detail) => write!(f, "event log is corrupt: {}", detail),
            Self::JournalIdentityMismatch {
                expected_device,
                expected_inode,
                actual_device,
                actual_inode,
            } => write!(
                f,
                "event log identity changed (expected {expected_device}:{expected_inode}, found {actual_device}:{actual_inode})"
            ),
        }
    }
}

impl From<std::io::Error> for EventStoreError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// CRC-32 (IEEE 802.3, reflected) over `data`.
///
/// Hand-rolled rather than pulled in as a dependency: it is ten lines, and `crc32_matches_known_vector`
/// pins it to the standard check value so a mistake here cannot go unnoticed.
pub(super) fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;

    for &byte in data {
        crc ^= byte as u32;

        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }

    !crc
}

/// An append-only file of durable records, one record per processed command.
///
/// A record holds the input event and every output event that command generated, framed together,
/// so a crash leaves either the whole command or none of it. That is what lets replay trust the
/// file: it can never find an input whose outputs went missing.
pub struct EventStore {
    file: File,
}

impl Drop for EventStore {
    fn drop(&mut self) {
        // Explicitly release ownership before closing. A concurrent fork/exec can briefly
        // inherit the descriptor even with CLOEXEC, otherwise retaining this process's lock
        // after the owner is dropped and spuriously preventing its immediate restart.
        let _ = self.file.unlock();
    }
}

impl EventStore {
    #[cfg(test)]
    pub(crate) fn open_read_only_for_test(path: impl AsRef<Path>) -> Result<Self, EventStoreError> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self { file })
    }

    /// Opens (or creates) the log at `path`, recovers the events already in it, and truncates any
    /// torn record left by a crash so the next append starts from clean history.
    ///
    /// Returns the store and the recovered envelopes, in order. They come back together because
    /// the truncation has to happen before anything is appended — handing out a store that has not
    /// been recovered yet would let a caller append after a torn tail.
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        Self::open_inner(path, true, None)
    }

    /// Opens and fully validates an already-existing journal under the exclusive writer lock.
    /// This is for a warm promotion: it must never create a fresh file if the journal it followed
    /// disappeared, because its mmap-delivered in-memory state is not the durability authority.
    pub(crate) fn open_existing(
        path: impl AsRef<Path>,
    ) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        Self::open_inner(path, false, None)
    }

    /// Opens an already-existing journal for warm promotion only when it is the exact file the
    /// follower observed. The identity comparison happens after the exclusive lock and before
    /// any recovery read or torn-tail truncation, so a swapped path cannot mutate another log.
    pub(crate) fn open_existing_matching(
        path: impl AsRef<Path>,
        expected_device: u64,
        expected_inode: u64,
    ) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        Self::open_inner(
            path,
            false,
            Some((expected_device, expected_inode)),
        )
    }

    fn open_inner(
        path: impl AsRef<Path>,
        allow_initialize_empty: bool,
        expected_identity: Option<(u64, u64)>,
    ) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false).mode(0o600);
        if allow_initialize_empty {
            options.create(true);
        }
        let mut file = options.open(&path)?;

        // One writer owns recovery/truncation as well as appends. Independent stream readers
        // never take this lifetime lock; they read only the published, immutable prefix.
        file.try_lock().map_err(std::io::Error::from)?;

        if let Some((expected_device, expected_inode)) = expected_identity {
            let metadata = file.metadata()?;
            if metadata.dev() != expected_device || metadata.ino() != expected_inode {
                return Err(EventStoreError::JournalIdentityMismatch {
                    expected_device,
                    expected_inode,
                    actual_device: metadata.dev(),
                    actual_inode: metadata.ino(),
                });
            }
        }

        // ponytail: reads the whole log into memory. Fine while history is small; stream it, or
        // add snapshots, when startup time actually starts to hurt.
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        if bytes.is_empty() {
            if !allow_initialize_empty {
                return Err(EventStoreError::Corrupt(
                    "existing journal is empty".to_string(),
                ));
            }
            file.write_all(FILE_MAGIC)?;
            file.sync_all()?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;

            return Ok((Self { file }, Vec::new()));
        }

        if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
            return Err(EventStoreError::BadMagic);
        }

        let (events, decoded_len) = decode_records(&bytes[FILE_MAGIC.len()..], FILE_MAGIC.len())?;
        let good_len = FILE_MAGIC.len() + decoded_len;

        // Drop a torn tail so the next append cannot be written after damaged bytes.
        if good_len < bytes.len() {
            file.set_len(good_len as u64)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::End(0))?;

        Ok((Self { file }, events))
    }

    /// Opens the writer-owned journal at an already committed command boundary and recovers only
    /// records after it. A validated core snapshot supplies the skipped prefix; the journal still
    /// owns truncation of a torn suffix and remains the only authoritative history.
    pub(crate) fn open_suffix(
        path: impl AsRef<Path>,
        boundary: u64,
    ) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;
        file.try_lock().map_err(std::io::Error::from)?;

        let file_len = file.metadata()?.len();
        if boundary < FILE_MAGIC.len() as u64 || boundary > file_len {
            return Err(EventStoreError::Corrupt(format!(
                "snapshot boundary {} is outside the journal length {}",
                boundary, file_len
            )));
        }

        let mut magic = [0; FILE_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(EventStoreError::BadMagic);
        }

        file.seek(SeekFrom::Start(boundary))?;
        let mut suffix = Vec::new();
        file.read_to_end(&mut suffix)?;
        let (events, good_suffix_len) = decode_records(&suffix, boundary as usize)?;
        let good_len = boundary
            .checked_add(good_suffix_len as u64)
            .ok_or_else(|| EventStoreError::Corrupt("journal length overflow".to_string()))?;

        if good_len < file_len {
            file.set_len(good_len)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((Self { file }, events))
    }

    /// Writes one command's envelopes as a single framed record and synchronizes it to disk.
    ///
    /// Returns after synchronization. The runtime then commits its prepared core transition.
    /// Any error is fatal; an I/O failure has an ambiguous disk outcome and must not be retried
    /// in this writer. Recovery decides whether a complete record survived.
    #[cfg(test)]
    pub fn append(&mut self, envelopes: &[EventEnvelope]) -> Result<(), EventStoreError> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let record = encode_record(envelopes)?;
        self.append_record(&record)
    }

    pub(super) fn append_record(&mut self, record: &[u8]) -> Result<(), EventStoreError> {
        // One write_all, so a partial write can only ever truncate the tail of this record —
        // never interleave with the next one.
        self.file.write_all(record)?;
        // ponytail: one fsync per command. Group-commit several records behind one sync when the
        // worker ever has a batch to commit; today it processes one command at a time.
        self.file.sync_all()?;

        Ok(())
    }

    pub(super) fn file(&self) -> &File {
        &self.file
    }

    /// Identity of the writer-locked journal. Consumers use this only after taking the writer
    /// lock, to prove that their in-memory checkpoint still describes this exact file.
    pub(crate) fn journal_identity(&self) -> Result<(u64, u64), EventStoreError> {
        let metadata = self.file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
}

pub(super) fn encode_record(envelopes: &[EventEnvelope]) -> Result<Vec<u8>, EventStoreError> {
    let payload = serde_json::to_vec(envelopes)
        .map_err(|err| EventStoreError::Corrupt(format!("could not serialize record: {err}")))?;
    if payload.len() > MAX_RECORD_LEN as usize {
        return Err(EventStoreError::Corrupt("record exceeds size limit".into()));
    }
    let mut record = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc32(&payload).to_le_bytes());
    record.extend_from_slice(&payload);
    Ok(record)
}

/// Walks the framed records after the magic header.
///
/// Returns the decoded envelopes and the byte length of the trustworthy prefix. A record that runs
/// off the end of the file is a torn tail from a crash: decoding stops and the caller truncates.
/// A checksum failure is different — the bytes are all there but wrong — so it is reported as
/// corruption rather than silently dropped.
fn decode_records(
    bytes: &[u8],
    first_offset: usize,
) -> Result<(Vec<EventEnvelope>, usize), EventStoreError> {
    let mut events = Vec::new();
    let mut offset = 0;

    loop {
        if offset == bytes.len() {
            return Ok((events, offset));
        }

        if bytes.len() - offset < RECORD_HEADER_LEN {
            return Ok((events, offset)); // torn header
        }

        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let expected_crc = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());

        if len > MAX_RECORD_LEN {
            return Err(EventStoreError::Corrupt(format!(
                "record at byte {} claims {} bytes, above the {} byte ceiling",
                first_offset + offset,
                len,
                MAX_RECORD_LEN
            )));
        }

        let payload_start = offset + RECORD_HEADER_LEN;
        let payload_end = payload_start + len as usize;

        if payload_end > bytes.len() {
            return Ok((events, offset)); // torn payload
        }

        let payload = &bytes[payload_start..payload_end];

        if crc32(payload) != expected_crc {
            return Err(EventStoreError::Corrupt(format!(
                "checksum mismatch in the record at byte {}",
                first_offset + offset
            )));
        }

        let batch: Vec<EventEnvelope> = serde_json::from_slice(payload).map_err(|err| {
            EventStoreError::Corrupt(format!(
                "record at byte {} passed its checksum but did not parse: {}",
                first_offset + offset,
                err
            ))
        })?;

        use crate::types::exchange_event::ExchangeEvent;
        if batch.len() < 2
            || !matches!(batch[0].event, ExchangeEvent::Input(_))
            || batch[1..]
                .iter()
                .any(|event| !matches!(event.event, ExchangeEvent::Output(_)))
        {
            return Err(EventStoreError::Corrupt(format!(
                "record at byte {} is not one complete input/output batch",
                first_offset + offset
            )));
        }

        events.extend(batch);
        offset = payload_end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::exchange_event::{ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent};
    use std::{os::unix::fs::MetadataExt, path::PathBuf};

    fn deposit_batch(seq: u64, amount: u64) -> Vec<EventEnvelope> {
        vec![
            EventEnvelope {
                seq_num: seq,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount,
                }),
            },
            EventEnvelope {
                seq_num: seq + 1,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".to_string(),
                    amount,
                }),
            },
        ]
    }

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("exchange-test-{}-{}.log", name, std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn crc32_matches_known_vector() {
        // The canonical CRC-32/ISO-HDLC check value; matches zlib.crc32 exactly.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn recovery_refuses_records_that_split_one_command_across_frames() {
        let path = temp_path("split-batch");
        let (mut store, _) = EventStore::open(&path).unwrap();
        let batch = deposit_batch(1, 10);
        store.append(&batch[..1]).unwrap();
        store.append(&batch[1..]).unwrap();
        drop(store);
        assert!(matches!(
            EventStore::open(&path),
            Err(EventStoreError::Corrupt(_))
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn appended_records_survive_reopen() {
        let path = temp_path("reopen");

        {
            let (mut store, recovered) = EventStore::open(&path).unwrap();
            assert!(recovered.is_empty());

            store.append(&deposit_batch(1, 1_000)).unwrap();
            store.append(&deposit_batch(3, 250)).unwrap();
        }

        let (_store, recovered) = EventStore::open(&path).unwrap();

        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered[0].seq_num, 1);
        assert_eq!(recovered[3].seq_num, 4);
        assert_eq!(
            recovered,
            [deposit_batch(1, 1_000), deposit_batch(3, 250)].concat()
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn promotion_open_existing_never_creates_or_initializes_a_missing_or_empty_journal() {
        let missing = temp_path("promotion-missing");
        assert!(matches!(
            EventStore::open_existing(&missing),
            Err(EventStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
        ));
        assert!(!missing.exists());

        let empty = temp_path("promotion-empty");
        std::fs::File::create(&empty).unwrap();
        assert!(matches!(
            EventStore::open_existing(&empty),
            Err(EventStoreError::Corrupt(detail)) if detail == "existing journal is empty"
        ));
        assert_eq!(std::fs::metadata(&empty).unwrap().len(), 0);
        std::fs::remove_file(empty).unwrap();
    }

    #[test]
    fn promotion_checks_identity_before_it_repairs_a_foreign_torn_tail() {
        let expected = temp_path("promotion-expected-identity");
        let foreign = temp_path("promotion-foreign-identity");
        {
            let (mut store, _) = EventStore::open(&expected).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
        }
        let expected_metadata = std::fs::metadata(&expected).unwrap();

        {
            let (mut store, _) = EventStore::open(&foreign).unwrap();
            store.append(&deposit_batch(1, 20)).unwrap();
        }
        let foreign_metadata = std::fs::metadata(&foreign).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&foreign)
            .unwrap()
            .set_len(foreign_metadata.len() - 4)
            .unwrap();
        let foreign_before = std::fs::read(&foreign).unwrap();

        assert!(matches!(
            EventStore::open_existing_matching(
                &foreign,
                expected_metadata.dev(),
                expected_metadata.ino(),
            ),
            Err(EventStoreError::JournalIdentityMismatch { .. })
        ));
        assert_eq!(std::fs::read(&foreign).unwrap(), foreign_before);

        let (store, recovered) = EventStore::open_existing_matching(
            &foreign,
            foreign_metadata.dev(),
            foreign_metadata.ino(),
        )
        .unwrap();
        assert!(recovered.is_empty());
        drop(store);
        assert_eq!(std::fs::metadata(&foreign).unwrap().len(), FILE_MAGIC.len() as u64);

        std::fs::remove_file(expected).unwrap();
        std::fs::remove_file(foreign).unwrap();
    }

    #[test]
    fn a_torn_tail_is_discarded_and_the_log_keeps_working() {
        let path = temp_path("torn");

        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 1_000)).unwrap();
            store.append(&deposit_batch(3, 250)).unwrap();
        }

        // Simulate a crash midway through the second record's payload.
        let full = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full - 12).unwrap();
        drop(file);

        let (mut store, recovered) = EventStore::open(&path).unwrap();

        // The torn command vanished whole — its input did not survive without its output.
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered, deposit_batch(1, 1_000));

        // And the truncated file is still appendable.
        store.append(&deposit_batch(3, 77)).unwrap();
        drop(store);

        let (_store, reread) = EventStore::open(&path).unwrap();
        assert_eq!(reread.len(), 4);
        assert_eq!(reread[2..], deposit_batch(3, 77)[..]);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_flipped_byte_is_refused_rather_than_skipped() {
        let path = temp_path("corrupt");

        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 1_000)).unwrap();
            store.append(&deposit_batch(3, 250)).unwrap();
        }

        // Flip a byte inside the first record's payload. All bytes are present, so this is real
        // damage rather than a torn tail, and it must not be silently dropped.
        let mut bytes = std::fs::read(&path).unwrap();
        let victim = FILE_MAGIC.len() + RECORD_HEADER_LEN + 4;
        bytes[victim] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            EventStore::open(&path),
            Err(EventStoreError::Corrupt(_))
        ));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_foreign_file_is_refused() {
        let path = temp_path("foreign");
        std::fs::write(&path, b"this is somebody else's file, not an event log").unwrap();

        assert!(matches!(
            EventStore::open(&path),
            Err(EventStoreError::BadMagic)
        ));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_failed_append_reports_an_error_instead_of_claiming_durability() {
        let path = temp_path("readonly");

        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 1_000)).unwrap();
        }

        // A real write failure: a handle that cannot write. The runtime treats this as fatal.
        let read_only = OpenOptions::new().read(true).open(&path).unwrap();
        let mut store = EventStore { file: read_only };

        assert!(store.append(&deposit_batch(3, 250)).is_err());

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn suffix_recovery_starts_at_a_committed_record_boundary() {
        let path = temp_path("suffix");
        let boundary;
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            boundary = store.file.metadata().unwrap().len();
            store.append(&deposit_batch(3, 20)).unwrap();
        }

        let (_store, suffix) = EventStore::open_suffix(&path, boundary).unwrap();
        assert_eq!(suffix, deposit_batch(3, 20));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn suffix_recovery_drops_only_a_torn_suffix_tail() {
        let path = temp_path("suffix-torn-tail");
        let boundary;
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            boundary = store.file.metadata().unwrap().len();
            store.append(&deposit_batch(3, 20)).unwrap();
        }
        let full = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(full - 4)
            .unwrap();

        let (_store, suffix) = EventStore::open_suffix(&path, boundary).unwrap();
        assert!(suffix.is_empty());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), boundary);
        std::fs::remove_file(path).unwrap();
    }
}
