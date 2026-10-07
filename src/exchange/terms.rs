//! The terms a journal holds (milestone 23, part 5).
//!
//! Each promotion starts a primary term. It journals a `TermStarted` record with the next epoch
//! before anything else it writes; the first primary's term is epoch 0 and has no record. Every
//! copy of a journal keeps an index of its terms beside it, `<journal>.terms`, so that the
//! replication handshake can compare epochs without reading the journal: each term's epoch, the
//! sequence of its `TermStarted` input, and the offset of that record.
//!
//! The index is saved before the record it lists is written, so a term start in the journal is
//! always in it. An entry beyond the journal's end is dropped when the index is loaded. An entry
//! exactly at the end is a term begun but never journaled: a primary that restarts journals it
//! then, byte for byte as it would have, and a replica drops it, since its primary sends it again.
//! The index also keeps the highest epoch it has ever listed, which no cut or drop lowers: a
//! promotion takes the epoch after it, because a term start indexed and never written may be
//! another primary's.
//! An index that is missing, names another journal, or lists a record that is not the term start
//! it says is rebuilt by one pass over the journal. An index that misses a term the journal holds
//! is not noticed: the index is saved before every term start, so only a copy of an older index
//! put in its place could.

use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    event_store::{JOURNAL_HEADER_LEN, RECORD_HEADER_LEN, journal_id_of, sync_parent_dir},
    event_stream::{decode_batch, read_record, record_length},
};
use crate::types::exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent};

/// Where a primary term starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Term {
    pub(crate) epoch: u64,
    /// The sequence of its `TermStarted` input.
    pub(crate) first_sequence: u64,
    /// The offset of that record.
    pub(crate) offset: u64,
}

#[derive(Serialize, Deserialize)]
struct IndexFile {
    journal_id: Uuid,
    highest_epoch: u64,
    terms: Vec<Term>,
}

/// A journal's terms, in order, and the file that keeps them.
pub(crate) struct Terms {
    path: PathBuf,
    /// None while the journal is empty: a new copy is named by its first bytes.
    journal_id: Option<Uuid>,
    terms: Vec<Term>,
    /// The highest epoch ever listed, including terms since cut or dropped.
    highest_epoch: u64,
}

impl Terms {
    /// No term yet, for a journal still being opened. The index sits beside the journal's real
    /// path, so every process reaching the journal through a link finds the same one.
    pub(crate) fn empty(journal_path: &Path) -> Self {
        let mut path =
            OsString::from(fs::canonicalize(journal_path).unwrap_or(journal_path.to_path_buf()));
        path.push(".terms");
        Self {
            path: PathBuf::from(path),
            journal_id: None,
            terms: Vec::new(),
            highest_epoch: 0,
        }
    }

    /// The index of the journal at `journal_path`, open as `journal`, checked against it.
    pub(crate) fn load(journal_path: &Path, journal: &File) -> io::Result<Self> {
        let path = Self::empty(journal_path).path;
        let len = journal.metadata()?.len();
        if len < JOURNAL_HEADER_LEN as u64 {
            return Ok(Self::empty(journal_path));
        }
        let journal_id = journal_id_of(journal)?;
        let listed = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<IndexFile>(&bytes).ok())
            .filter(|index| index.journal_id == journal_id);
        let (terms, highest_epoch, changed) = match listed {
            Some(index) => {
                let listed = index.terms.len();
                let kept: Vec<Term> = index
                    .terms
                    .into_iter()
                    .filter(|term| term.offset <= len)
                    .collect();
                // A term begun at the end has no record yet; every other must have its own.
                let checked = kept
                    .iter()
                    .filter(|term| term.offset < len)
                    .all(|term| term_at(journal, term.offset, len) == Some(*term));
                if checked {
                    let changed = kept.len() < listed;
                    (kept, index.highest_epoch, changed)
                } else {
                    eprintln!("terms: the index disagrees with the journal; rebuilding it");
                    (scan(journal, len)?, index.highest_epoch, true)
                }
            }
            None => (scan(journal, len)?, 0, true),
        };
        let latest = terms.last().map_or(0, |term| term.epoch);
        let index = Self {
            path,
            journal_id: Some(journal_id),
            terms,
            highest_epoch: highest_epoch.max(latest),
        };
        if changed || highest_epoch < latest {
            index.save()?;
        }
        Ok(index)
    }

    /// The latest epoch the journal holds: 0 before any promotion.
    pub(crate) fn epoch(&self) -> u64 {
        self.terms.last().map_or(0, |term| term.epoch)
    }

    /// The epoch a new term takes: after every epoch this index has listed, including a term start
    /// cut or dropped because its record was never written, which may be another primary's.
    pub(crate) fn next_epoch(&self) -> u64 {
        self.highest_epoch + 1
    }

    pub(crate) fn list(&self) -> &[Term] {
        &self.terms
    }

    /// A term begun where the journal ends, whose record was never written.
    pub(crate) fn pending(&self, end: u64) -> Option<Term> {
        self.terms.last().copied().filter(|term| term.offset == end)
    }

    /// Adds a term whose record is about to be written, saving the index first.
    pub(crate) fn add(&mut self, term: Term) -> io::Result<()> {
        if term.epoch <= self.epoch() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "term {} does not come after term {}",
                    term.epoch,
                    self.epoch()
                ),
            ));
        }
        self.terms.push(term);
        self.highest_epoch = self.highest_epoch.max(term.epoch);
        self.save()
    }

    /// Forgets the terms whose records a cut back to `end` removed.
    pub(crate) fn cut(&mut self, end: u64) -> io::Result<()> {
        let before = self.terms.len();
        self.terms.retain(|term| term.offset < end);
        if self.terms.len() < before {
            self.save()?;
        }
        Ok(())
    }

    /// A new copy is named once its header is written; it holds no term yet.
    pub(crate) fn name(&mut self, journal_id: Uuid) {
        self.journal_id = Some(journal_id);
        self.terms.clear();
        self.highest_epoch = 0;
    }

    fn save(&self) -> io::Result<()> {
        let Some(journal_id) = self.journal_id else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(&IndexFile {
            journal_id,
            highest_epoch: self.highest_epoch,
            terms: self.terms.clone(),
        })?;
        let mut temporary = self.path.clone().into_os_string();
        temporary.push(format!(".{}.tmp", Uuid::new_v4()));
        let temporary = PathBuf::from(temporary);
        let written = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            sync_parent_dir(&self.path)
        })();
        if written.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        written
    }
}

/// The term the record at `offset` starts, if it is a whole `TermStarted` record.
fn term_at(journal: &File, offset: u64, len: u64) -> Option<Term> {
    let record = read_record(journal, offset, len).ok()?;
    term_in(&record, offset)
}

fn term_in(record: &[u8], offset: u64) -> Option<Term> {
    let batch: Vec<EventEnvelope> =
        serde_json::from_slice(record.get(RECORD_HEADER_LEN..)?).ok()?;
    let first_sequence = batch.first()?.seq_num;
    // The checksum, the sequences and the shape of a command, as recovery checks them.
    let batch = decode_batch(record, first_sequence).ok()?;
    match batch[0].event {
        ExchangeEvent::Input(ExchangeInputEvent::TermStarted { epoch }) => Some(Term {
            epoch,
            first_sequence,
            offset,
        }),
        _ => None,
    }
}

/// Every term start in the journal, by one pass that reads the first bytes of each record, and the
/// whole record only where they name a term start.
fn scan(journal: &File, len: u64) -> io::Result<Vec<Term>> {
    eprintln!("terms: building the index from the journal");
    let needle = b"\"term_started\"";
    let mut terms = Vec::new();
    let mut head = [0; RECORD_HEADER_LEN + 128];
    let mut at = JOURNAL_HEADER_LEN as u64;
    while at + RECORD_HEADER_LEN as u64 <= len {
        let read = journal.read_at(&mut head, at)?;
        let record_len = record_length(&head[..read])? as u64;
        if at + record_len > len {
            break; // a torn tail, which recovery cuts
        }
        if head[..read]
            .windows(needle.len())
            .any(|window| window == needle)
            && let Some(term) = term_at(journal, at, len)
        {
            terms.push(term);
        }
        at += record_len;
    }
    Ok(terms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exchange::event_store::{EventStore, encode_record},
        types::exchange_event::ExchangeOutputEvent,
    };

    fn term_batch(seq: u64, epoch: u64) -> Vec<EventEnvelope> {
        vec![
            EventEnvelope {
                seq_num: seq,
                event: ExchangeEvent::Input(ExchangeInputEvent::TermStarted { epoch }),
            },
            EventEnvelope {
                seq_num: seq + 1,
                event: ExchangeEvent::Output(ExchangeOutputEvent::TermStarted { epoch }),
            },
        ]
    }

    fn deposit_batch(seq: u64) -> Vec<EventEnvelope> {
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

    #[test]
    fn the_index_is_rebuilt_from_the_journal_and_kept_in_step_with_it() {
        let dir = std::env::temp_dir().join(format!("stock-terms-{}", Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("events.log");
        let (mut store, _) = EventStore::open(&path).unwrap();
        store.append(&deposit_batch(1)).unwrap();
        let first = store.end().unwrap();
        store.append(&term_batch(3, 1)).unwrap();
        store.append(&deposit_batch(5)).unwrap();
        let second = store.end().unwrap();
        store.append(&term_batch(7, 2)).unwrap();
        let journal = File::open(&path).unwrap();

        // No index yet: one pass over the journal finds both term starts.
        let terms = Terms::load(&path, &journal).unwrap();
        let expected = [
            Term {
                epoch: 1,
                first_sequence: 3,
                offset: first,
            },
            Term {
                epoch: 2,
                first_sequence: 7,
                offset: second,
            },
        ];
        assert_eq!(terms.list(), expected);
        assert_eq!(terms.epoch(), 2);
        assert_eq!(terms.next_epoch(), 3);

        // Reached through a link, the journal keeps one index, beside its real path.
        let link = dir.join("link.log");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(Terms::load(&link, &journal).unwrap().list(), expected);
        assert!(!dir.join("link.log.terms").exists());

        // Saved before its record is written. At the journal's end, it is a term begun but never
        // journaled; beyond the end, it is dropped.
        let mut terms = Terms::load(&path, &journal).unwrap();
        let end = fs::metadata(&path).unwrap().len();
        let begun = Term {
            epoch: 3,
            first_sequence: 9,
            offset: end,
        };
        terms.add(begun).unwrap();
        assert!(
            terms
                .add(Term {
                    epoch: 3,
                    first_sequence: 11,
                    offset: end + 10,
                })
                .is_err()
        );
        let mut terms = Terms::load(&path, &journal).unwrap();
        assert_eq!(terms.pending(end), Some(begun));
        assert_eq!(terms.list()[..2], expected);
        terms.cut(end).unwrap();
        terms
            .add(Term {
                epoch: 4,
                offset: end + 10,
                ..begun
            })
            .unwrap();
        let terms = Terms::load(&path, &journal).unwrap();
        assert_eq!(terms.list(), expected);
        // Dropped, but its epoch is never taken again.
        assert_eq!(terms.next_epoch(), 5);

        // A cut forgets the terms it removed, but not their epochs.
        let mut terms = Terms::load(&path, &journal).unwrap();
        terms.cut(second).unwrap();
        assert_eq!(terms.epoch(), 1);
        assert_eq!(Terms::load(&path, &journal).unwrap().next_epoch(), 5);

        // An index that names a record that is not a term start is rebuilt, still above the highest
        // epoch it listed.
        let wrong = IndexFile {
            journal_id: store.journal_id(),
            highest_epoch: 7,
            terms: vec![Term {
                epoch: 1,
                first_sequence: 1,
                offset: JOURNAL_HEADER_LEN as u64,
            }],
        };
        fs::write(
            dir.join("events.log.terms"),
            serde_json::to_vec(&wrong).unwrap(),
        )
        .unwrap();
        let terms = Terms::load(&path, &journal).unwrap();
        assert_eq!(terms.list(), expected);
        assert_eq!(terms.next_epoch(), 8);

        // A record whose payload merely mentions a term start is not one.
        let mut decoy = deposit_batch(9);
        if let ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested { user_id, .. }) =
            &mut decoy[0].event
        {
            *user_id = "term_started".into();
        }
        assert!(
            String::from_utf8_lossy(&encode_record(&decoy).unwrap()).contains("\"term_started\"")
        );
        store.append(&decoy).unwrap();
        fs::remove_file(dir.join("events.log.terms")).unwrap();
        assert_eq!(Terms::load(&path, &journal).unwrap().list(), expected);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }
}
