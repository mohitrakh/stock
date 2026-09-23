use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::types::exchange_event::EventEnvelope;

/// Marks the file as an exchange event log and pins the on-disk format. A format change bumps the
/// trailing digit, so an old file is rejected with a clear message instead of failing somewhere
/// deep in a JSON parse.
const FILE_MAGIC: &[u8; 8] = b"EXCHLOG1";

/// `[len: u32 LE][crc: u32 LE]` ahead of every payload.
const RECORD_HEADER_LEN: usize = 8;

/// Refuses a length field that could only come from corruption, before it is used to size an
/// allocation.
const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum EventStoreError {
    Io(std::io::Error),
    /// The file exists but is not an event log, or not one this build can read.
    BadMagic,
    /// Damage that is not a torn tail: a bad checksum or unreadable payload with more records
    /// after it. Recovery refuses rather than guessing which part is trustworthy.
    Corrupt(String),
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
fn crc32(data: &[u8]) -> u32 {
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

impl EventStore {
    /// Opens (or creates) the log at `path`, recovers the events already in it, and truncates any
    /// torn record left by a crash so the next append starts from clean history.
    ///
    /// Returns the store and the recovered envelopes, in order. They come back together because
    /// the truncation has to happen before anything is appended — handing out a store that has not
    /// been recovered yet would let a caller append after a torn tail.
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<EventEnvelope>), EventStoreError> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // ponytail: reads the whole log into memory. Fine while history is small; stream it, or
        // add snapshots, when startup time actually starts to hurt.
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        if bytes.is_empty() {
            file.write_all(FILE_MAGIC)?;
            file.sync_all()?;

            return Ok((Self { file }, Vec::new()));
        }

        if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
            return Err(EventStoreError::BadMagic);
        }

        let (events, good_len) = decode_records(&bytes)?;

        // Drop a torn tail so the next append cannot be written after damaged bytes.
        if good_len < bytes.len() {
            file.set_len(good_len as u64)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::End(0))?;

        Ok((Self { file }, events))
    }

    /// Writes one command's envelopes as a single framed record and synchronizes it to disk.
    ///
    /// Returns only once the bytes are durable, so a caller may reply to a client the moment this
    /// returns `Ok`. A failure means the record is not durable and must be treated as fatal by the
    /// caller: the core has already moved on in memory, and there is no rollback.
    pub fn append(&mut self, envelopes: &[EventEnvelope]) -> Result<(), EventStoreError> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let payload = serde_json::to_vec(envelopes).map_err(|err| {
            EventStoreError::Corrupt(format!("could not serialize record: {}", err))
        })?;

        let mut record = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(&crc32(&payload).to_le_bytes());
        record.extend_from_slice(&payload);

        // One write_all, so a partial write can only ever truncate the tail of this record —
        // never interleave with the next one.
        self.file.write_all(&record)?;
        // ponytail: one fsync per command. Group-commit several records behind one sync when the
        // worker ever has a batch to commit; today it processes one command at a time.
        self.file.sync_all()?;

        Ok(())
    }
}

/// Walks the framed records after the magic header.
///
/// Returns the decoded envelopes and the byte length of the trustworthy prefix. A record that runs
/// off the end of the file is a torn tail from a crash: decoding stops and the caller truncates.
/// A checksum failure is different — the bytes are all there but wrong — so it is reported as
/// corruption rather than silently dropped.
fn decode_records(bytes: &[u8]) -> Result<(Vec<EventEnvelope>, usize), EventStoreError> {
    let mut events = Vec::new();
    let mut offset = FILE_MAGIC.len();

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
                offset, len, MAX_RECORD_LEN
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
                offset
            )));
        }

        let batch: Vec<EventEnvelope> = serde_json::from_slice(payload).map_err(|err| {
            EventStoreError::Corrupt(format!(
                "record at byte {} passed its checksum but did not parse: {}",
                offset, err
            ))
        })?;

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
}
