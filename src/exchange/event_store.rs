use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use uuid::Uuid;

use crate::types::exchange_event::EventEnvelope;

/// Marks the file as an exchange event log and pins the on-disk format. A format change bumps the
/// trailing digit, so an old file is rejected with a clear message instead of failing somewhere
/// deep in a JSON parse.
pub(super) const FILE_MAGIC: &[u8; 8] = b"EXCHLOG2";

/// The format before milestone 23, whose journals had no id.
const OLD_FILE_MAGIC: &[u8; 8] = b"EXCHLOG1";

/// The magic, then the journal id: 16 random bytes chosen when the journal is created. The id is
/// what names a journal, so a byte-identical copy on another machine is the same journal, and the
/// first record starts right after it.
pub(crate) const JOURNAL_HEADER_LEN: usize = 24;

/// Journal syncs since the process started. `--bench` divides orders by this to show how many
/// commands shared one sync.
pub(crate) static JOURNAL_SYNCS: AtomicU64 = AtomicU64::new(0);

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
    /// A journal from before milestone 23, which has no journal id.
    OldFormat,
    /// Damage that is not a torn tail: a bad checksum or unreadable payload with more records
    /// after it. Recovery refuses rather than guessing which part is trustworthy.
    Corrupt(String),
    /// The journal at the path is not the one expected, found after taking the writer lock and
    /// before recovery can truncate a torn tail in the wrong file.
    JournalIdentityMismatch {
        expected: Uuid,
        actual: Uuid,
    },
    /// The journal is shorter than the bytes already published or applied from it: by the stream,
    /// a warm replica, or into a snapshot. It lost committed history, most likely because an older
    /// copy with the same id replaced it.
    ShorterThanApplied {
        length: u64,
        applied: u64,
    },
    /// At promotion, the journal path names a different file from the one the warm replica read,
    /// even if it is a copy: the warm replica's core vouches only for the file it read.
    NotTheFollowedFile,
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
            Self::OldFormat => f.write_str(OLD_FORMAT),
            Self::Corrupt(detail) => write!(f, "event log is corrupt: {}", detail),
            Self::JournalIdentityMismatch { expected, actual } => write!(
                f,
                "event log identity changed (expected journal {expected}, found {actual})"
            ),
            Self::ShorterThanApplied { length, applied } => write!(
                f,
                "event log is {length} bytes, shorter than the {applied} bytes already published or applied from it: it may be an older copy of the journal"
            ),
            Self::NotTheFollowedFile => f.write_str(
                "the event log path names a different file from the one this warm replica followed; start a warm replica on it, then promote that one",
            ),
        }
    }
}

const OLD_FORMAT: &str = "event log is in the EXCHLOG1 format from before milestone 23, which has no journal id; start a new journal";

impl From<std::io::Error> for EventStoreError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// The journal id in a header, or why the bytes are not a journal header this build can read.
pub(super) fn parse_header(header: &[u8]) -> Result<Uuid, EventStoreError> {
    if header.starts_with(OLD_FILE_MAGIC) {
        return Err(EventStoreError::OldFormat);
    }
    if !header.starts_with(FILE_MAGIC) {
        return Err(EventStoreError::BadMagic);
    }
    if header.len() < JOURNAL_HEADER_LEN {
        return Err(EventStoreError::Corrupt(
            "incomplete journal header".to_string(),
        ));
    }
    Ok(Uuid::from_bytes(
        header[FILE_MAGIC.len()..JOURNAL_HEADER_LEN]
            .try_into()
            .unwrap(),
    ))
}

/// Refuses an open file that is not the expected journal. It reads only the header, so nothing is
/// read or repaired in a journal that is not the one expected.
fn check_journal_id(file: &File, expected: Uuid) -> Result<(), EventStoreError> {
    let mut header = [0; JOURNAL_HEADER_LEN];
    let read = file.read_at(&mut header, 0)?;
    let actual = parse_header(&header[..read])?;
    if actual != expected {
        return Err(EventStoreError::JournalIdentityMismatch { expected, actual });
    }
    Ok(())
}

/// Syncs the directory holding `path`, so that a file just created there survives a power loss.
pub(super) fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

/// Reads the id of an open journal from its header, without moving the file's cursor.
pub(crate) fn journal_id_of(file: &File) -> io::Result<Uuid> {
    let mut header = [0; JOURNAL_HEADER_LEN];
    file.read_exact_at(&mut header, 0).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            io::Error::new(io::ErrorKind::InvalidData, "incomplete journal header")
        } else {
            error
        }
    })?;
    parse_header(&header)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

/// CRC-32 (IEEE 802.3, reflected, as zlib computes it) over `data`.
///
/// The `crc` crate, which sqlx already brings in, with its 16-table implementation. It replaced a
/// hand-rolled loop that went bit by bit: at about 200 MB/s it was a quarter of the time of
/// writing a snapshot, and a seventh of a warm replica's replay (milestone 23 part 3).
/// `crc32_matches_known_vector` pins the standard check value.
pub(super) fn crc32(data: &[u8]) -> u32 {
    CRC32.checksum(data)
}

const CRC32: crc::Crc<u32, crc::Table<16>> =
    crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISO_HDLC);

/// The same checksum over the file's bytes `from..to`, read a megabyte at a time: a long stretch of
/// journal never has to fit in memory.
pub(super) fn crc32_of_file(file: &File, mut from: u64, to: u64) -> io::Result<u32> {
    let mut digest = CRC32.digest();
    let mut chunk = vec![0; 1024 * 1024];
    while from < to {
        let len = (to - from).min(chunk.len() as u64) as usize;
        file.read_exact_at(&mut chunk[..len], from)?;
        digest.update(&chunk[..len]);
        from += len as u64;
    }
    Ok(digest.finalize())
}

/// An append-only file of durable records, one record per processed command.
///
/// A record holds the input event and every output event that command generated, framed together,
/// so a crash leaves either the whole command or none of it. That is what lets replay trust the
/// file: it can never find an input whose outputs went missing.
pub struct EventStore {
    file: File,
    journal_id: Uuid,
    /// Syncs through this handle, so a test can prove that a group shares one.
    #[cfg(test)]
    pub(crate) syncs: u64,
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
    fn from_file(file: File, journal_id: Uuid) -> Self {
        Self {
            file,
            journal_id,
            #[cfg(test)]
            syncs: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn open_read_only_for_test(path: impl AsRef<Path>) -> Result<Self, EventStoreError> {
        let file = OpenOptions::new().read(true).open(path)?;
        let journal_id = journal_id_of(&file)?;
        Ok(Self::from_file(file, journal_id))
    }

    /// The id this journal was created with.
    pub(crate) fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    /// Opens (or creates) the log at `path`, recovers the events already in it, and truncates any
    /// torn record left by a crash so the next append starts from clean history.
    ///
    /// Returns the store and the recovered envelopes, in order. They come back together because
    /// the truncation has to happen before anything is appended — handing out a store that has not
    /// been recovered yet would let a caller append after a torn tail.
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;

        // One writer owns recovery/truncation as well as appends. Independent stream readers
        // never take this lifetime lock; they read only the published, immutable prefix.
        file.try_lock().map_err(std::io::Error::from)?;

        // ponytail: reads the whole log into memory. Fine while history is small; stream it, or
        // add snapshots, when startup time actually starts to hurt.
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        if bytes.is_empty() {
            // A new journal names itself once, here, for its whole life.
            let journal_id = Uuid::new_v4();
            let mut header = FILE_MAGIC.to_vec();
            header.extend_from_slice(journal_id.as_bytes());
            file.write_all(&header)?;
            file.sync_all()?;
            sync_parent_dir(path)?;

            return Ok((Self::from_file(file, journal_id), Vec::new()));
        }

        let journal_id = parse_header(&bytes)?;
        let (events, decoded_len) =
            decode_records(&bytes[JOURNAL_HEADER_LEN..], JOURNAL_HEADER_LEN)?;
        let good_len = JOURNAL_HEADER_LEN + decoded_len;

        // Drop a torn tail so the next append cannot be written after damaged bytes.
        if good_len < bytes.len() {
            file.set_len(good_len as u64)?;
        }
        // Synced even when nothing was cut: a process that died before its sync leaves records
        // in the page cache only, and recovery is about to serve and publish them as durable.
        file.sync_all()?;

        file.seek(SeekFrom::End(0))?;

        Ok((Self::from_file(file, journal_id), events))
    }

    /// Opens the writer-owned journal at an already committed command boundary and recovers only
    /// the records after it: the boundary of a validated core snapshot at startup, or the position
    /// a warm replica had applied at promotion. That core supplies the skipped prefix; the journal
    /// still owns truncation of a torn suffix and remains the only authoritative history.
    ///
    /// Everything is checked while holding the writer lock and before anything is read or
    /// repaired, so a file that is not the expected one is left untouched:
    /// - `followed`, at promotion, is the warm replica's own handle on the journal, and the end the
    ///   stream published. Promotion takes over only the file the warm replica read: its core,
    ///   with the snapshot it started from, stands for that file's first `boundary` bytes, which
    ///   nothing rewrites, while another file at the same path, even a copy, could differ there.
    /// - The id must be the expected journal's.
    /// - The journal must still reach the boundary and, at promotion, the published end. Every
    ///   published record was synced first, and readers may already have consumed it, so a
    ///   shorter journal lost committed history, most likely because an older copy replaced it.
    pub(crate) fn open_suffix(
        path: impl AsRef<Path>,
        expected: Uuid,
        boundary: u64,
        followed: Option<(&File, u64)>,
    ) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        file.try_lock().map_err(std::io::Error::from)?;
        if let Some((followed, _)) = followed {
            let (locked, followed) = (file.metadata()?, followed.metadata()?);
            if (locked.dev(), locked.ino()) != (followed.dev(), followed.ino()) {
                return Err(EventStoreError::NotTheFollowedFile);
            }
        }
        check_journal_id(&file, expected)?;

        let file_len = file.metadata()?.len();
        if boundary < JOURNAL_HEADER_LEN as u64 {
            return Err(EventStoreError::Corrupt(format!(
                "boundary {boundary} is inside the journal header"
            )));
        }
        let must_reach = followed.map_or(boundary, |(_, published)| published.max(boundary));
        if must_reach > file_len {
            return Err(EventStoreError::ShorterThanApplied {
                length: file_len,
                applied: must_reach,
            });
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
        }
        // As in `open`: what was recovered may still be in the page cache only.
        file.sync_all()?;
        file.seek(SeekFrom::End(0))?;
        Ok((Self::from_file(file, expected), events))
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

    /// Appends one or more complete framed records — a whole group-commit group — with ONE write
    /// and ONE sync. The runtime publishes the group and releases its replies only after this
    /// returns. Any error is fatal; recovery decides which complete records survived.
    pub(super) fn append_record(&mut self, records: &[u8]) -> Result<(), EventStoreError> {
        self.write_records(records)?;
        self.sync()
    }

    /// The first half of `append_record`: the records are written but not synced yet, so a
    /// replication sender can read them back and ship them while `sync` runs.
    pub(super) fn write_records(&mut self, records: &[u8]) -> Result<(), EventStoreError> {
        // One write_all of back-to-back records, so a process crash can only cut the tail of the
        // group: every record before the cut is complete, and recovery drops the torn one. After a
        // power loss, unsynced bytes can survive out of order; a complete record with a bad
        // checksum still refuses startup, exactly as a torn single record always could.
        self.file.write_all(records)?;
        Ok(())
    }

    pub(super) fn sync(&mut self) -> Result<(), EventStoreError> {
        self.file.sync_all()?;
        JOURNAL_SYNCS.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        {
            self.syncs += 1;
        }
        Ok(())
    }

    /// Where the next record will start: the journal's length.
    pub(super) fn end(&self) -> Result<u64, EventStoreError> {
        Ok(self.file.metadata()?.len())
    }

    pub(super) fn file(&self) -> &File {
        &self.file
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
    use std::path::PathBuf;

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
        // Long enough for the 16-bytes-at-a-time loop, not only its tail.
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn a_file_checksum_read_in_chunks_equals_the_checksum_of_its_bytes() {
        let path = temp_path("crc-of-file");
        let bytes: Vec<u8> = (0..2_500_000u32).map(|i| (i * 7 + i / 255) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let file = File::open(&path).unwrap();
        let (from, to) = (5, bytes.len() - 3);
        assert_eq!(
            crc32_of_file(&file, from as u64, to as u64).unwrap(),
            crc32(&bytes[from..to])
        );
        assert_eq!(crc32_of_file(&file, 9, 9).unwrap(), 0);
        let _ = std::fs::remove_file(&path);
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

    /// Startup from a snapshot and promotion open an existing journal; neither may start a new one.
    #[test]
    fn the_suffix_opener_never_creates_or_initializes_a_missing_or_empty_journal() {
        let boundary = JOURNAL_HEADER_LEN as u64;
        let missing = temp_path("suffix-missing");
        assert!(matches!(
            EventStore::open_suffix(&missing, Uuid::new_v4(), boundary, None),
            Err(EventStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
        ));
        assert!(!missing.exists());

        let empty = temp_path("suffix-empty");
        std::fs::File::create(&empty).unwrap();
        assert!(matches!(
            EventStore::open_suffix(&empty, Uuid::new_v4(), boundary, None),
            Err(EventStoreError::BadMagic)
        ));
        assert_eq!(std::fs::metadata(&empty).unwrap().len(), 0);
        std::fs::remove_file(empty).unwrap();
    }

    #[test]
    fn the_suffix_opener_checks_identity_before_it_repairs_a_foreign_torn_tail() {
        let expected = temp_path("promotion-expected-identity");
        let foreign = temp_path("promotion-foreign-identity");
        let expected_id = {
            let (mut store, _) = EventStore::open(&expected).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            store.journal_id()
        };
        let foreign_id = {
            let (mut store, _) = EventStore::open(&foreign).unwrap();
            store.append(&deposit_batch(1, 20)).unwrap();
            store.journal_id()
        };
        let foreign_len = std::fs::metadata(&foreign).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&foreign)
            .unwrap()
            .set_len(foreign_len - 4)
            .unwrap();
        let foreign_before = std::fs::read(&foreign).unwrap();
        let boundary = JOURNAL_HEADER_LEN as u64;

        assert!(matches!(
            EventStore::open_suffix(&foreign, expected_id, boundary, None),
            Err(EventStoreError::JournalIdentityMismatch { expected, actual })
                if expected == expected_id && actual == foreign_id
        ));
        assert_eq!(std::fs::read(&foreign).unwrap(), foreign_before);

        let (store, recovered) =
            EventStore::open_suffix(&foreign, foreign_id, boundary, None).unwrap();
        assert!(recovered.is_empty());
        drop(store);
        assert_eq!(
            std::fs::metadata(&foreign).unwrap().len(),
            JOURNAL_HEADER_LEN as u64
        );

        std::fs::remove_file(expected).unwrap();
        std::fs::remove_file(foreign).unwrap();
    }

    /// The id travels with the bytes: a copy of the journal at another path, as on another
    /// machine, opens as the same journal, and only a journal with a different id is refused.
    #[test]
    fn a_journal_keeps_its_id_and_a_copy_is_the_same_journal() {
        let path = temp_path("journal-id");
        let copy = temp_path("journal-id-copy");
        let journal_id = {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            store.journal_id()
        };
        let (store, _) = EventStore::open(&path).unwrap();
        assert_eq!(store.journal_id(), journal_id);
        assert_eq!(journal_id_of(store.file()).unwrap(), journal_id);
        drop(store);

        std::fs::copy(&path, &copy).unwrap();
        let boundary = JOURNAL_HEADER_LEN as u64;
        let (store, recovered) =
            EventStore::open_suffix(&copy, journal_id, boundary, None).unwrap();
        assert_eq!(recovered, deposit_batch(1, 10));
        drop(store);

        // Another id is refused before anything is read or repaired: the torn tail stays.
        let full_len = std::fs::metadata(&copy).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&copy)
            .unwrap()
            .set_len(full_len - 4)
            .unwrap();
        let torn = std::fs::read(&copy).unwrap();
        assert!(matches!(
            EventStore::open_suffix(&copy, Uuid::new_v4(), boundary, None),
            Err(EventStoreError::JournalIdentityMismatch { .. })
        ));
        assert_eq!(std::fs::read(&copy).unwrap(), torn);

        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(copy).unwrap();
    }

    /// An older copy of the journal keeps its id, so a journal shorter than what was already
    /// applied from it is refused too, and left as it was found.
    #[test]
    fn a_journal_shorter_than_what_was_applied_is_refused_untouched() {
        let path = temp_path("shorter-than-applied");
        let (journal_id, applied) = {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            store.append(&deposit_batch(3, 20)).unwrap();
            (store.journal_id(), store.file.metadata().unwrap().len())
        };
        let older = std::fs::read(&path).unwrap()[..applied as usize - 4].to_vec();
        std::fs::write(&path, &older).unwrap();

        assert!(matches!(
            EventStore::open_suffix(&path, journal_id, applied, None),
            Err(EventStoreError::ShorterThanApplied { length, applied: a })
                if length == applied - 4 && a == applied
        ));
        assert_eq!(std::fs::read(&path).unwrap(), older);
        std::fs::remove_file(path).unwrap();
    }

    /// Promotion takes over only the file the warm replica read. Even a byte-identical copy put
    /// in its place is refused, before its torn tail is repaired, while the followed file opens.
    #[test]
    fn promotion_opens_only_the_file_the_warm_replica_followed() {
        let path = temp_path("followed");
        let moved = temp_path("followed-moved");
        let (journal_id, applied) = {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            (store.journal_id(), store.file.metadata().unwrap().len())
        };
        let followed = File::open(&path).unwrap();

        std::fs::rename(&path, &moved).unwrap();
        let mut copy = std::fs::read(&moved).unwrap();
        copy.extend_from_slice(&encode_record(&deposit_batch(3, 20)).unwrap()[..10]);
        std::fs::write(&path, &copy).unwrap();
        assert!(matches!(
            EventStore::open_suffix(&path, journal_id, applied, Some((&followed, applied))),
            Err(EventStoreError::NotTheFollowedFile)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), copy);

        std::fs::rename(&moved, &path).unwrap();
        let (_store, suffix) =
            EventStore::open_suffix(&path, journal_id, applied, Some((&followed, applied)))
                .unwrap();
        assert!(suffix.is_empty());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_journal_from_before_journal_ids_is_refused_untouched_with_its_reason() {
        let path = temp_path("old-format");
        let mut old = OLD_FILE_MAGIC.to_vec();
        old.extend_from_slice(&encode_record(&deposit_batch(1, 10)).unwrap());
        std::fs::write(&path, &old).unwrap();

        assert!(matches!(
            EventStore::open(&path),
            Err(EventStoreError::OldFormat)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), old);
        std::fs::remove_file(path).unwrap();
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
        let victim = JOURNAL_HEADER_LEN + RECORD_HEADER_LEN + 4;
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
        let journal_id = journal_id_of(&read_only).unwrap();
        let mut store = EventStore::from_file(read_only, journal_id);

        assert!(store.append(&deposit_batch(3, 250)).is_err());

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn suffix_recovery_starts_at_a_committed_record_boundary() {
        let path = temp_path("suffix");
        let (journal_id, boundary);
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            boundary = store.file.metadata().unwrap().len();
            store.append(&deposit_batch(3, 20)).unwrap();
            journal_id = store.journal_id();
        }

        let (_store, suffix) = EventStore::open_suffix(&path, journal_id, boundary, None).unwrap();
        assert_eq!(suffix, deposit_batch(3, 20));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn suffix_recovery_drops_only_a_torn_suffix_tail() {
        let path = temp_path("suffix-torn-tail");
        let (journal_id, boundary);
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store.append(&deposit_batch(1, 10)).unwrap();
            boundary = store.file.metadata().unwrap().len();
            store.append(&deposit_batch(3, 20)).unwrap();
            journal_id = store.journal_id();
        }
        let full = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(full - 4)
            .unwrap();

        let (_store, suffix) = EventStore::open_suffix(&path, journal_id, boundary, None).unwrap();
        assert!(suffix.is_empty());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), boundary);
        std::fs::remove_file(path).unwrap();
    }
}
