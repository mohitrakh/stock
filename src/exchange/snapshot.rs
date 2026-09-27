//! Atomic, journal-bound snapshots of deterministic exchange-core state.
//!
//! The durable journal remains authoritative. A snapshot is only a validated checkpoint that lets
//! startup rebuild the core by replaying the later journal suffix; it never permits journal
//! truncation, replacement, or a silent fresh start.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use serde::{Deserialize, Serialize};

use super::{core::CoreSnapshot, event_store::crc32};

const MAGIC: &[u8; 8] = b"EXCHSNP1";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 20; // magic + payload length + CRC-32
const MAX_PAYLOAD_LEN: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotBoundary {
    pub(crate) journal_device: u64,
    pub(crate) journal_inode: u64,
    /// The first byte of the next complete command record in the durable journal.
    pub(crate) byte_offset: u64,
    /// The first envelope sequence that is not represented by `core`.
    pub(crate) next_event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoadedSnapshot {
    pub(crate) boundary: SnapshotBoundary,
    pub(crate) core: CoreSnapshot,
}

#[derive(Serialize, Deserialize)]
struct SnapshotPayload {
    version: u32,
    boundary: SnapshotBoundary,
    core: CoreSnapshot,
}

fn invalid(message: impl Into<String>) -> String {
    message.into()
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn aliases(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

/// A missing snapshot is normal. A present but invalid one is an error for the caller to report
/// before falling back to full journal replay.
pub(crate) fn load(
    path: impl AsRef<Path>,
    journal_path: impl AsRef<Path>,
) -> Result<Option<LoadedSnapshot>, String> {
    let path = path.as_ref();
    let journal_path = journal_path.as_ref();
    let snapshot_meta = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("could not inspect snapshot: {error}")),
    };
    let journal_meta = fs::metadata(journal_path)
        .map_err(|error| format!("could not inspect journal for snapshot: {error}"))?;
    if aliases(&snapshot_meta, &journal_meta) {
        return Err(invalid("snapshot path aliases the durable journal"));
    }
    if snapshot_meta.len() < HEADER_LEN as u64
        || snapshot_meta.len() > HEADER_LEN as u64 + MAX_PAYLOAD_LEN
    {
        return Err(invalid("snapshot has an invalid file size"));
    }

    let bytes = fs::read(path).map_err(|error| format!("could not read snapshot: {error}"))?;
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Err(invalid("snapshot has invalid magic"));
    }
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let end = HEADER_LEN
        .checked_add(
            usize::try_from(payload_len)
                .map_err(|_| invalid("snapshot payload length does not fit this platform"))?,
        )
        .ok_or_else(|| invalid("snapshot payload length overflows"))?;
    if payload_len > MAX_PAYLOAD_LEN || end != bytes.len() {
        return Err(invalid("snapshot payload length does not match file size"));
    }
    let payload_bytes = &bytes[HEADER_LEN..];
    if crc32(payload_bytes) != expected_crc {
        return Err(invalid("snapshot checksum mismatch"));
    }
    let payload: SnapshotPayload = serde_json::from_slice(payload_bytes)
        .map_err(|error| format!("snapshot contains invalid JSON: {error}"))?;
    if payload.version != VERSION {
        return Err(format!(
            "snapshot format version {} is not supported",
            payload.version
        ));
    }
    if payload.boundary.journal_device != journal_meta.dev()
        || payload.boundary.journal_inode != journal_meta.ino()
    {
        return Err(invalid("snapshot belongs to a different journal"));
    }
    if payload.boundary.byte_offset < 8
        || payload.boundary.byte_offset > journal_meta.len()
        || payload.boundary.next_event_sequence == 0
    {
        return Err(invalid("snapshot has an invalid journal boundary"));
    }

    Ok(Some(LoadedSnapshot {
        boundary: payload.boundary,
        core: payload.core,
    }))
}

/// Publishes a complete checkpoint by replacing the old snapshot only after the new bytes have
/// been fully synchronized. If this returns an error before `rename`, the prior checkpoint stays
/// usable and the authoritative journal already contains the command that triggered the attempt.
pub(crate) fn write(
    path: impl AsRef<Path>,
    journal: &File,
    protected_path: impl AsRef<Path>,
    boundary: SnapshotBoundary,
    core: CoreSnapshot,
) -> Result<(), String> {
    write_inner(path, journal, protected_path, boundary, core, false)
}

fn write_inner(
    path: impl AsRef<Path>,
    journal: &File,
    protected_path: impl AsRef<Path>,
    boundary: SnapshotBoundary,
    core: CoreSnapshot,
    fail_before_rename: bool,
) -> Result<(), String> {
    let path = path.as_ref();
    let protected_path = protected_path.as_ref();
    let journal_meta = journal
        .metadata()
        .map_err(|error| format!("could not inspect journal for snapshot: {error}"))?;
    if boundary.journal_device != journal_meta.dev()
        || boundary.journal_inode != journal_meta.ino()
        || boundary.byte_offset < 8
        || boundary.byte_offset > journal_meta.len()
        || boundary.next_event_sequence == 0
    {
        return Err(invalid(
            "refusing to write a snapshot with an invalid journal boundary",
        ));
    }

    if let Ok(existing) = fs::metadata(path) {
        if aliases(&existing, &journal_meta) {
            return Err(invalid("snapshot path aliases the durable journal"));
        }
        if let Ok(protected) = fs::metadata(protected_path)
            && aliases(&existing, &protected)
        {
            return Err(invalid("snapshot path aliases the mmap stream"));
        }
    }

    let payload = serde_json::to_vec(&SnapshotPayload {
        version: VERSION,
        boundary,
        core,
    })
    .map_err(|error| format!("could not serialize snapshot: {error}"))?;
    if payload.len() as u64 > MAX_PAYLOAD_LEN {
        return Err(invalid("snapshot exceeds the 512 MiB size limit"));
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&crc32(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid("snapshot path has no valid file name"))?;
    let temporary = parent(path).join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| format!("could not create snapshot temporary file: {error}"))?;
        file.write_all(&bytes)
            .map_err(|error| format!("could not write snapshot: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("could not synchronize snapshot: {error}"))?;
        drop(file);
        if fail_before_rename {
            return Err(invalid("injected snapshot replacement failure"));
        }
        fs::rename(&temporary, path)
            .map_err(|error| format!("could not publish snapshot: {error}"))?;
        File::open(parent(path))
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("could not synchronize snapshot directory: {error}"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{exchange::core::ExchangeCore, exchange::event_store::EventStore};

    fn paths(
        name: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let dir =
            std::env::temp_dir().join(format!("stock-snapshot-{name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        (
            dir.clone(),
            dir.join("events.log"),
            dir.join("events.mmap"),
            dir.join("events.snapshot"),
        )
    }

    fn boundary(store: &EventStore) -> SnapshotBoundary {
        let meta = store.file().metadata().unwrap();
        SnapshotBoundary {
            journal_device: meta.dev(),
            journal_inode: meta.ino(),
            byte_offset: meta.len(),
            next_event_sequence: 1,
        }
    }

    #[test]
    fn failed_replacement_keeps_the_previous_snapshot_intact() {
        let (dir, journal, stream, snapshot) = paths("atomic-replace");
        fs::write(&stream, b"stream").unwrap();
        let (store, _) = EventStore::open(&journal).unwrap();
        let core = ExchangeCore::new();
        let boundary = boundary(&store);
        write(
            &snapshot,
            store.file(),
            &stream,
            boundary.clone(),
            core.snapshot(),
        )
        .unwrap();
        let before = fs::read(&snapshot).unwrap();

        let mut changed = ExchangeCore::new();
        changed.deposit("trader".to_string(), 1).unwrap();
        assert!(
            write_inner(
                &snapshot,
                store.file(),
                &stream,
                boundary.clone(),
                changed.snapshot(),
                true,
            )
            .is_err()
        );
        assert_eq!(fs::read(&snapshot).unwrap(), before);
        assert_eq!(
            load(&snapshot, &journal).unwrap().unwrap().boundary,
            boundary
        );

        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn snapshot_is_bound_to_its_exact_journal_identity() {
        let (dir, journal, stream, snapshot) = paths("journal-identity");
        fs::write(&stream, b"stream").unwrap();
        let (store, _) = EventStore::open(&journal).unwrap();
        let core = ExchangeCore::new();
        write(
            &snapshot,
            store.file(),
            &stream,
            boundary(&store),
            core.snapshot(),
        )
        .unwrap();
        let other_journal = dir.join("other.log");
        let (other_store, _) = EventStore::open(&other_journal).unwrap();
        assert!(load(&snapshot, &other_journal).is_err());

        drop(other_store);
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }
}
