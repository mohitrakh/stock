//! `--replica`: the second machine's copy of the journal (milestone 23, part 4).
//!
//! It keeps a byte-identical copy of the primary's journal, and its own stream, which a warm
//! replica on this machine follows. It dials the primary and says how far its copy is committed
//! and what it holds beyond that. It appends exactly the records the primary sends, after checking
//! each one: framing, checksum and sequence. It syncs, then confirms how far it is durable. It
//! publishes on its stream only the records the primary says both disks hold, so its readers never
//! see a record that might be cut. It keeps only the end and last sequence of each unpublished
//! record in memory; its readers read the records from the journal.
//!
//! The primary may tell it to cut its copy back to its committed end, when the primary does not
//! hold the same bytes beyond it: the primary never synced those, so it never acknowledged them.
//! The committed end is what its stream published. A stream is otherwise never synced, so this one
//! is synced within about two seconds of moving: after a power loss the committed end is at most
//! that old.
//!
//! A failed write or sync of its own files stops the replica, which could otherwise confirm what
//! is not on its disk. It holds the journal's writer lock, so nothing else writes the copy.

use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io,
    net::{TcpStream, ToSocketAddrs},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use uuid::Uuid;

use super::{
    event_store::{
        JOURNAL_HEADER_LEN, RECORD_HEADER_LEN, crc32_of_file, journal_id_of, parse_header,
        sync_parent_dir,
    },
    event_stream::{
        DEFAULT_CAPACITY, StreamWriter, already_published, decode_batch, record_length,
    },
    replication::{
        Hello, LINK_TIMEOUT, MAX_ANSWER, MAX_BODY, Position, RECORDS, REFUSE, SYNCED, WELCOME,
        invalid, read_frame, u64_at, write_frame,
    },
};

const RECONNECT_AFTER: Duration = Duration::from_secs(1);
/// A dial that gets no answer is given up after this long, rather than the minutes the system's
/// own retries take, so a replica comes back soon after the network does.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the replica waits for the primary's answer to its hello. The primary may first have
/// to compare a long stretch of journal, when this copy's stream was lost.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(300);
/// Once the stream has moved, its header is synced if this long has passed since the last time.
const STREAM_SYNC_EVERY: Duration = Duration::from_secs(1);

/// How a session with the primary ended.
#[derive(Debug)]
pub(crate) enum Ended {
    /// The primary refused this replica; reconnecting would not help.
    Refused(String),
    /// The link broke or fell silent, or a record did not check out: reconnect.
    Lost(io::Error),
    /// Reading, writing or syncing this machine's own files failed. Going on could confirm what is
    /// not on its disk, so the replica stops.
    Failed(io::Error),
}

impl From<io::Error> for Ended {
    fn from(error: io::Error) -> Self {
        Self::Lost(error)
    }
}

/// A record on this machine that is not published yet: where it ends, and its last sequence.
struct Unpublished {
    end: u64,
    last_sequence: u64,
}

pub(crate) struct Replica {
    journal: File,
    stream_path: PathBuf,
    stream: Option<StreamWriter>,
    journal_id: Option<Uuid>,
    /// Durable length, and the next sequence after the last complete record.
    end: u64,
    next_sequence: u64,
    /// What the stream published: an end and the last sequence before it.
    published: (u64, u64),
    unpublished: VecDeque<Unpublished>,
    /// When the stream's header was last synced, and the end it then held.
    stream_synced: (Instant, u64),
    /// The last hello, reused while the copy's committed end and length are unchanged, and it was
    /// not cut since: without its stream, its checksum covers the whole copy, and the replica may
    /// dial many times before the primary answers.
    last_hello: Option<Hello>,
}

impl Replica {
    /// Opens this machine's copy, taking its writer lock, and recovers it: a torn tail is cut,
    /// and the records after what the stream published are counted so they can be published, or
    /// cut, later.
    pub(crate) fn open(
        journal_path: impl AsRef<Path>,
        stream_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let journal_path = journal_path.as_ref();
        let journal = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(journal_path)?;
        journal.try_lock().map_err(io::Error::from)?;
        let mut replica = Self {
            journal,
            stream_path: stream_path.as_ref().to_path_buf(),
            stream: None,
            journal_id: None,
            end: 0,
            next_sequence: 1,
            published: (0, 0),
            unpublished: VecDeque::new(),
            stream_synced: (Instant::now(), 0),
            last_hello: None,
        };
        let len = replica.journal.metadata()?.len();
        if len < JOURNAL_HEADER_LEN as u64 {
            // New, empty, or a header torn while its first bytes were written. The directory entry
            // is synced too, so that the copy itself survives a power loss.
            replica.journal.set_len(0)?;
            replica.journal.sync_all()?;
            sync_parent_dir(journal_path)?;
            return Ok(replica);
        }
        replica.journal_id = Some(journal_id_of(&replica.journal)?);
        let (published_end, published_sequence) =
            already_published(&replica.stream_path, &replica.journal)?;
        if published_end > len {
            return Err(invalid(
                "the stream published more than this replica's journal holds",
            ));
        }
        replica.published = (published_end, published_sequence);
        replica.end = published_end;
        replica.next_sequence = published_sequence + 1;
        // The records after what the stream published, one at a time: there may be many. A torn
        // record at the end is cut.
        let mut header = [0; RECORD_HEADER_LEN];
        while replica.end + RECORD_HEADER_LEN as u64 <= len {
            replica.journal.read_exact_at(&mut header, replica.end)?;
            let record_len = record_length(&header)?;
            if replica.end + record_len as u64 > len {
                break;
            }
            let mut record = vec![0; record_len];
            replica.journal.read_exact_at(&mut record, replica.end)?;
            replica.accept(&record)?;
        }
        if replica.end < len {
            replica.journal.set_len(replica.end)?;
        }
        replica.journal.sync_all()?;
        replica.open_stream()?;
        replica.stream_synced.1 = replica.published.0;
        Ok(replica)
    }

    fn open_stream(&mut self) -> io::Result<()> {
        self.stream = Some(StreamWriter::open_at(
            &self.stream_path,
            &self.journal,
            self.next_sequence - 1,
            self.published,
            DEFAULT_CAPACITY,
        )?);
        Ok(())
    }

    /// Checks one whole record, which starts at this copy's end, and counts it as unpublished.
    fn accept(&mut self, record: &[u8]) -> io::Result<()> {
        let batch = decode_batch(record, self.next_sequence)?;
        let last_sequence = batch.last().unwrap().seq_num;
        self.end += record.len() as u64;
        self.next_sequence = last_sequence + 1;
        self.unpublished.push_back(Unpublished {
            end: self.end,
            last_sequence,
        });
        Ok(())
    }

    /// Checks the records the primary sent, which must be whole.
    fn accept_records(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let record = bytes
                .get(..record_length(bytes)?)
                .ok_or_else(|| invalid("the primary sent an incomplete record"))?;
            self.accept(record)?;
            bytes = &bytes[record.len()..];
        }
        Ok(())
    }

    /// What this copy says when it connects: how far it is committed, which is on both disks, and
    /// what it holds beyond that, by its checksum.
    fn hello(&mut self) -> io::Result<Hello> {
        let unchanged =
            |hello: &Hello| (hello.committed, hello.end) == (self.published.0, self.end);
        if let Some(hello) = self.last_hello.filter(unchanged) {
            return Ok(hello);
        }
        let hello = Hello {
            journal_id: self.journal_id,
            committed: self.published.0,
            committed_next: self.published.1 + 1,
            end: self.end,
            end_next: self.next_sequence,
            tail_crc: crc32_of_file(&self.journal, self.published.0, self.end)?,
        };
        self.last_hello = Some(hello);
        Ok(hello)
    }

    /// Cuts this copy back to what is committed, because the primary does not hold the same
    /// records beyond it: the primary never synced them, so they were never acknowledged. What
    /// is published is never cut.
    fn cut_back(&mut self) -> io::Result<()> {
        let committed = self.published.0;
        self.journal.set_len(committed)?;
        self.journal.sync_all()?;
        eprintln!(
            "replica: cut {} bytes the primary does not hold",
            self.end - committed
        );
        self.unpublished.clear();
        self.end = committed;
        self.next_sequence = self.published.1 + 1;
        // Refilled to the same length, the copy may hold different bytes: describe it afresh.
        self.last_hello = None;
        Ok(())
    }

    /// Appends records the primary sent at this copy's end, syncs, and keeps them unpublished.
    /// Records that do not check out end the session; a failed write or sync stops the replica.
    fn append(&mut self, offset: u64, mut bytes: &[u8]) -> Result<(), Ended> {
        if offset != self.end {
            return Err(invalid("the primary sent records for another position").into());
        }
        if self.end == 0 {
            // A new copy starts with the primary's journal header, which names the journal.
            let header = bytes
                .get(..JOURNAL_HEADER_LEN)
                .ok_or_else(|| invalid("the primary sent an incomplete journal header"))?;
            let journal_id = parse_header(header).map_err(|error| invalid(error.to_string()))?;
            self.journal
                .write_all_at(header, 0)
                .map_err(Ended::Failed)?;
            self.journal_id = Some(journal_id);
            self.end = JOURNAL_HEADER_LEN as u64;
            self.published = (self.end, 0);
            bytes = &bytes[JOURNAL_HEADER_LEN..];
        }
        let checked_from = self.end;
        let previous_sequence = self.next_sequence;
        let unpublished = self.unpublished.len();
        if let Err(error) = self.accept_records(bytes) {
            self.end = checked_from;
            self.next_sequence = previous_sequence;
            self.unpublished.truncate(unpublished);
            return Err(error.into());
        }
        self.journal
            .write_all_at(bytes, checked_from)
            .map_err(Ended::Failed)?;
        self.journal.sync_all().map_err(Ended::Failed)?;
        if self.stream.is_none() {
            self.open_stream().map_err(Ended::Failed)?;
        }
        Ok(())
    }

    /// Publishes the records both disks hold. Readers read them from the journal.
    fn publish(&mut self, commit: u64) -> io::Result<()> {
        let count = self
            .unpublished
            .partition_point(|record| record.end <= commit);
        let Some(last) = count.checked_sub(1).map(|index| &self.unpublished[index]) else {
            return Ok(());
        };
        let published = (last.end, last.last_sequence);
        self.stream
            .as_mut()
            .ok_or_else(|| invalid("no stream to publish on"))?
            .publish_through(published.0, published.1)?;
        self.published = published;
        self.unpublished.drain(..count);
        Ok(())
    }

    /// Syncs the stream's header once it has moved and a second has passed since the last time.
    /// What it published is the committed end this copy reports to the primary: after a power
    /// loss it must not be much older than it was.
    fn sync_stream(&mut self) -> io::Result<()> {
        let (at, end) = self.stream_synced;
        if self.published.0 != end && at.elapsed() >= STREAM_SYNC_EVERY {
            if let Some(stream) = &self.stream {
                stream.sync_header()?;
            }
            self.stream_synced = (Instant::now(), self.published.0);
        }
        Ok(())
    }

    /// One session with the primary: hello, where to resume, then records until the link breaks
    /// or falls silent.
    pub(crate) fn follow(&mut self, primary: &str) -> Result<(), Ended> {
        // Before connecting: without its stream this reads the whole copy, and the primary waits
        // only a few seconds for the hello.
        let hello = self.hello().map_err(Ended::Failed)?;
        let mut socket = connect(primary)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(ANSWER_TIMEOUT))?;
        hello.send(&mut socket)?;
        let (kind, body) = read_frame(&mut socket, MAX_ANSWER)?;
        if kind == REFUSE {
            return Err(Ended::Refused(String::from_utf8_lossy(&body).into_owned()));
        }
        if kind != WELCOME {
            return Err(invalid("expected the primary's welcome").into());
        }
        let resume = Position::parse(&body)?;
        if self.journal_id.is_some() && resume.journal_id != self.journal_id {
            return Err(Ended::Refused(
                "the primary holds another journal than this replica".to_string(),
            ));
        }
        // The primary keeps this copy whole, or cuts it back to what is committed.
        let whole = (resume.end, resume.next_sequence) == (self.end, self.next_sequence);
        let committed = self.journal_id.is_some()
            && (resume.end, resume.next_sequence) == (self.published.0, self.published.1 + 1);
        if !whole {
            if !committed {
                return Err(invalid("the primary resumes where this copy cannot").into());
            }
            self.cut_back().map_err(Ended::Failed)?;
        }
        // From now on the primary sends something at least every second: silence means it, or
        // the network, is gone.
        socket.set_read_timeout(Some(LINK_TIMEOUT))?;
        eprintln!(
            "replica: following {primary} from journal byte {}",
            self.end
        );
        loop {
            let (kind, body) = read_frame(&mut socket, MAX_BODY)?;
            if kind != RECORDS {
                return Err(invalid("expected records from the primary").into());
            }
            let offset = u64_at(&body, 0)?;
            let commit = u64_at(&body, 8)?;
            let records = &body[16..];
            if !records.is_empty() {
                self.append(offset, records)?;
            }
            // Every frame is answered, so the primary hears from this replica just as often.
            write_frame(&mut socket, SYNCED, &[&self.end.to_le_bytes()])?;
            if commit > self.end {
                return Err(invalid("the primary committed beyond this replica's journal").into());
            }
            self.publish(commit).map_err(Ended::Failed)?;
            self.sync_stream().map_err(Ended::Failed)?;
        }
    }
}

fn connect(primary: &str) -> io::Result<TcpStream> {
    let mut failed = io::Error::new(io::ErrorKind::NotFound, "the primary's address is empty");
    for address in primary.to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            Ok(socket) => return Ok(socket),
            Err(error) => failed = error,
        }
    }
    Err(failed)
}

/// `stock --replica PRIMARY_ADDR JOURNAL STREAM`: follows the primary for good, reconnecting
/// whenever the link breaks, until the primary refuses this replica or its own disk fails.
pub fn run(args: &[String]) -> Result<(), String> {
    let [primary, journal, stream] = args else {
        return Err("usage: stock --replica PRIMARY_ADDR JOURNAL STREAM".to_string());
    };
    let mut replica = Replica::open(journal, stream)
        .map_err(|error| format!("could not open the replica's journal: {error}"))?;
    loop {
        match replica.follow(primary) {
            Ok(()) => {}
            Err(Ended::Refused(reason)) => return Err(format!("the primary refused: {reason}")),
            Err(Ended::Failed(error)) => {
                return Err(format!("this machine's copy failed: {error}"));
            }
            Err(Ended::Lost(error)) => {
                eprintln!("replica: link to {primary} lost: {error}; reconnecting")
            }
        }
        thread::sleep(RECONNECT_AFTER);
    }
}
