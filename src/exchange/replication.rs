//! Synchronous replication of the journal to a replica on another machine (milestone 23, part 4).
//!
//! The replica dials the primary's replication port. It says which journal it holds, how far its
//! copy is committed, and what it holds beyond that. Committed records are on both disks, so they
//! are identical on both machines. What lies beyond them the replica keeps only if the primary
//! holds the same bytes; otherwise it cuts back to its committed end. Anything the primary lacks
//! there it never synced, so it was never acknowledged. The primary then sends, straight from its
//! journal file, whatever the replica lacks: first the catch-up, then each group as the worker
//! writes it. The replica checks each record, appends exactly those bytes, syncs, and confirms how
//! far it is durable.
//!
//! The worker answers and publishes a group only once both disks hold it, so nothing that exists
//! on one machine only is ever visible. It ships the group while it syncs its own disk, then waits
//! for the confirmation. The primary also sends the commit point, the end both disks hold, and the
//! replica publishes on its own stream only up to it.
//!
//! When no replica confirms, the worker waits: commands queue and nothing is acknowledged. The
//! operator can tell the primary to run alone. It returns to synchronous mode by itself once a
//! replica holds everything the primary has synced. The operator must never both promote a replica
//! and let the primary run alone.
//!
//! Epochs (part 5) fence primaries that were replaced. Each promotion starts a term with the next
//! epoch (see `terms`), and both sides say their latest epoch. A primary refuses a replica that has
//! seen a later term than its own: it was replaced, and must never be confirmed again. A replica
//! from an earlier term must have committed nothing past the point where its term ended in this
//! journal; otherwise it acknowledged commands after the promotion, which means an old primary ran
//! alone while another took over, and the two histories split. What it holds beyond that point it
//! never acknowledged, and the usual cut back drops it.
//!
//! The primary sends the commit point at least every second, and the replica answers every frame.
//! Either side drops a link that stays silent for 10 s, as when the other machine lost power or
//! the network was cut without a word, and the replica dials again.
//!
//! Frames are `[body length: u32 LE][kind: u8][body]`:
//! - `HELLO`, replica to primary: journal id (16 bytes, zero for an empty journal); its committed
//!   end and the next sequence there; its length and the next sequence there (u64 each); the
//!   checksum of the bytes between them (u32); its latest epoch (u64);
//! - `WELCOME`, primary to replica: journal id, where the replica resumes (u64), and the next
//!   sequence there (u64): its length, or its committed end if it must cut back; the primary's
//!   epoch (u64);
//! - `REFUSE`, primary to replica: the reason, as UTF-8;
//! - `RECORDS`, primary to replica: offset (u64), commit point (u64), then whole journal records,
//!   possibly none;
//! - `SYNCED`, replica to primary, answering every `RECORDS`: the end its journal is durable to
//!   (u64).

use std::{
    fs::File,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    os::unix::fs::FileExt,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use uuid::Uuid;

use super::{
    event_store::{JOURNAL_HEADER_LEN, MAX_RECORD_LEN, RECORD_HEADER_LEN, crc32_of_file},
    event_stream::{decode_batch, read_record, record_length},
    terms::Term,
};

pub(super) const HELLO: u8 = 1;
pub(super) const WELCOME: u8 = 2;
pub(super) const REFUSE: u8 = 3;
pub(super) const RECORDS: u8 = 4;
pub(super) const SYNCED: u8 = 5;

/// Records sent in one frame: at most this much, unless a single record is bigger.
const CHUNK: usize = 1024 * 1024;
/// The largest body: offset and commit point, then one whole record of the largest size.
pub(super) const MAX_BODY: usize = 16 + RECORD_HEADER_LEN + MAX_RECORD_LEN as usize;
/// The hello's body, and the largest answer to it: a welcome, or a refusal and its reason.
pub(super) const HELLO_LEN: usize = 60;
pub(super) const MAX_ANSWER: usize = 4096;
/// A replica must say hello within this long after connecting.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// A worker waiting this long for a connected replica counts as paused.
const PAUSE_AFTER: Duration = Duration::from_secs(1);
/// The primary sends a frame at least this often, and the replica answers each one. Tests use
/// shorter times, to see a silent link dropped quickly.
const HEARTBEAT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(1)
};
/// A link silent this long, on either side, is dropped.
pub(super) const LINK_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(3)
} else {
    Duration::from_secs(10)
};

pub(super) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(super) fn write_frame(stream: &mut impl Write, kind: u8, parts: &[&[u8]]) -> io::Result<()> {
    let len = parts.iter().map(|part| part.len()).sum::<usize>();
    if len > MAX_BODY {
        return Err(invalid("replication frame too large"));
    }
    let mut frame = Vec::with_capacity(5 + len);
    frame.extend_from_slice(&(len as u32).to_le_bytes());
    frame.push(kind);
    for part in parts {
        frame.extend_from_slice(part);
    }
    stream.write_all(&frame)
}

/// Reads one frame whose body is at most `limit` bytes, the most the frame expected here can
/// hold. A longer one is refused before anything is allocated for it.
pub(super) fn read_frame(stream: &mut impl Read, limit: usize) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0; 5];
    stream.read_exact(&mut header).map_err(silence)?;
    let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    if len > limit {
        return Err(invalid("replication frame too large"));
    }
    let mut body = vec![0; len];
    stream.read_exact(&mut body).map_err(silence)?;
    Ok((header[4], body))
}

/// A read timeout, which the socket reports as "would block", said plainly.
fn silence(error: io::Error) -> io::Error {
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => io::Error::new(
            io::ErrorKind::TimedOut,
            "nothing heard from the other side in time",
        ),
        _ => error,
    }
}

pub(super) fn u64_at(body: &[u8], offset: usize) -> io::Result<u64> {
    body.get(offset..offset + 8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
        .ok_or_else(|| invalid("replication frame too short"))
}

/// What a replica says when it connects: the journal it holds (none while empty), how far its copy
/// is committed, how long it is, the next sequence at each, the checksum of the bytes between, and
/// the latest epoch it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Hello {
    pub(super) journal_id: Option<Uuid>,
    pub(super) committed: u64,
    pub(super) committed_next: u64,
    pub(super) end: u64,
    pub(super) end_next: u64,
    pub(super) tail_crc: u32,
    pub(super) epoch: u64,
}

impl Hello {
    pub(super) fn send(&self, stream: &mut impl Write) -> io::Result<()> {
        let id = self.journal_id.map_or([0; 16], |id| *id.as_bytes());
        write_frame(
            stream,
            HELLO,
            &[
                &id,
                &self.committed.to_le_bytes(),
                &self.committed_next.to_le_bytes(),
                &self.end.to_le_bytes(),
                &self.end_next.to_le_bytes(),
                &self.tail_crc.to_le_bytes(),
                &self.epoch.to_le_bytes(),
            ],
        )
    }

    pub(super) fn parse(body: &[u8]) -> io::Result<Self> {
        if body.len() != HELLO_LEN {
            return Err(invalid("the replica's hello has the wrong length"));
        }
        let id = Uuid::from_bytes(body[..16].try_into().unwrap());
        Ok(Self {
            journal_id: (!id.is_nil()).then_some(id),
            committed: u64_at(body, 16)?,
            committed_next: u64_at(body, 24)?,
            end: u64_at(body, 32)?,
            end_next: u64_at(body, 40)?,
            tail_crc: u32::from_le_bytes(body[48..52].try_into().unwrap()),
            epoch: u64_at(body, 52)?,
        })
    }
}

/// Where a replica resumes: the journal, the end, the next sequence there, and the primary's epoch.
/// Sent as `WELCOME`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Position {
    pub(super) journal_id: Option<Uuid>,
    pub(super) end: u64,
    pub(super) next_sequence: u64,
    pub(super) epoch: u64,
}

impl Position {
    pub(super) fn send(&self, stream: &mut impl Write, kind: u8) -> io::Result<()> {
        let id = self.journal_id.map_or([0; 16], |id| *id.as_bytes());
        write_frame(
            stream,
            kind,
            &[
                &id,
                &self.end.to_le_bytes(),
                &self.next_sequence.to_le_bytes(),
                &self.epoch.to_le_bytes(),
            ],
        )
    }

    pub(super) fn parse(body: &[u8]) -> io::Result<Self> {
        if body.len() != 40 {
            return Err(invalid("replication position has the wrong length"));
        }
        let id = Uuid::from_bytes(body[..16].try_into().unwrap());
        Ok(Self {
            journal_id: (!id.is_nil()).then_some(id),
            end: u64_at(body, 16)?,
            next_sequence: u64_at(body, 24)?,
            epoch: u64_at(body, 32)?,
        })
    }
}

/// What the operator port shows, and `/health` reads.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicationStatus {
    /// "synchronous", "paused" (no replica, or the worker has waited for it over a second), or
    /// "running alone".
    pub mode: &'static str,
    /// This primary's term.
    pub epoch: u64,
    pub replica_connected: bool,
    pub journal_end: u64,
    pub replica_end: Option<u64>,
    /// How long the worker has been waiting for the replica, if it is.
    pub waiting_ms: Option<u64>,
}

/// The primary's side of the link. The worker reports each group it writes and syncs; the link
/// ships it and makes the worker wait for the replica's confirmation.
pub struct Replication {
    shared: Arc<Shared>,
    address: SocketAddr,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// A read handle on the journal: the sender reads back what the worker wrote.
    journal: File,
    journal_id: Uuid,
    /// The journal's terms; the last is this primary's. A promotion adds its own before it
    /// listens, so they never change while it does.
    terms: Vec<Term>,
    stopped: AtomicBool,
}

impl Shared {
    fn epoch(&self) -> u64 {
        self.terms.last().map_or(0, |term| term.epoch)
    }
}

struct State {
    /// The journal's end as the worker wrote it, and the next sequence after it.
    written: u64,
    next_sequence: u64,
    /// The end the worker has synced.
    synced: u64,
    replica: Option<Link>,
    connections: u64,
    run_alone: bool,
    waiting_since: Option<Instant>,
}

/// The connected replica: which connection it is, how far it confirmed, how far it was sent, and
/// its socket, to cut it when another replica connects.
struct Link {
    connection: u64,
    confirmed: u64,
    sent: u64,
    socket: TcpStream,
}

impl State {
    fn confirmed(&self) -> u64 {
        self.replica.as_ref().map_or(0, |link| link.confirmed)
    }

    /// Running alone ends once the replica holds everything this primary has synced: everything
    /// acknowledged is then on both disks again, and the next group waits for the replica.
    fn rejoin(&mut self) {
        if self.run_alone && self.replica.is_some() && self.confirmed() >= self.synced {
            self.run_alone = false;
            eprintln!("replication: the replica caught up; synchronous again");
        }
    }

    /// What both disks hold: the replica may publish up to here.
    fn commit_point(&self) -> u64 {
        self.synced.min(self.confirmed())
    }

    fn current(&mut self, connection: u64) -> Option<&mut Link> {
        self.replica
            .as_mut()
            .filter(|link| link.connection == connection)
    }

    fn status(&self, epoch: u64) -> ReplicationStatus {
        let waited = self.waiting_since.map(|since| since.elapsed());
        let mode = if self.run_alone {
            "running alone"
        } else if self.replica.is_none() || waited.is_some_and(|waited| waited >= PAUSE_AFTER) {
            "paused"
        } else {
            "synchronous"
        };
        ReplicationStatus {
            mode,
            epoch,
            replica_connected: self.replica.is_some(),
            journal_end: self.written,
            replica_end: self.replica.as_ref().map(|link| link.confirmed),
            waiting_ms: waited.map(|waited| waited.as_millis() as u64),
        }
    }
}

impl Replication {
    /// Listens for the replica on `address`. `journal` is a read handle on the journal, whose
    /// complete records end at `end`, followed by `next_sequence`; all of it is synced. `terms`
    /// are its terms, the last being this primary's.
    pub(crate) fn listen(
        address: SocketAddr,
        journal: File,
        journal_id: Uuid,
        end: u64,
        next_sequence: u64,
        terms: Vec<Term>,
    ) -> io::Result<Arc<Self>> {
        let listener = TcpListener::bind(address)?;
        let address = listener.local_addr()?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                written: end,
                next_sequence,
                synced: end,
                replica: None,
                connections: 0,
                run_alone: false,
                waiting_since: None,
            }),
            changed: Condvar::new(),
            journal,
            journal_id,
            terms,
            stopped: AtomicBool::new(false),
        });
        let accepting = Arc::clone(&shared);
        thread::Builder::new()
            .name("replication-listener".into())
            .spawn(move || accept(accepting, listener))?;
        Ok(Arc::new(Self { shared, address }))
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.state.lock().unwrap()
    }

    /// The worker wrote records up to `end`, followed by `next_sequence`, but has not synced
    /// them yet: the sender ships them now, while the worker syncs.
    pub(crate) fn written(&self, end: u64, next_sequence: u64) {
        let mut state = self.lock();
        state.written = end;
        state.next_sequence = next_sequence;
        self.shared.changed.notify_all();
    }

    /// The worker synced up to `end`. Returns once the replica has confirmed it too, or the
    /// primary runs alone; until then the worker waits, and so does every command behind it.
    /// True when it was let through by running alone, before the replica held it.
    pub(crate) fn confirm(&self, end: u64) -> bool {
        let mut state = self.lock();
        state.synced = state.synced.max(end);
        self.shared.changed.notify_all();
        if state.confirmed() < end && !state.run_alone {
            state.waiting_since = Some(Instant::now());
            while state.confirmed() < end && !state.run_alone {
                state = self.shared.changed.wait(state).unwrap();
            }
            state.waiting_since = None;
        }
        state.confirmed() < end
    }

    pub fn status(&self) -> ReplicationStatus {
        self.lock().status(self.shared.epoch())
    }

    /// Nothing can be acknowledged now: no replica, or the worker has waited too long for it,
    /// and the primary does not run alone.
    pub fn paused(&self) -> bool {
        self.status().mode == "paused"
    }

    /// The operator's switch: the worker stops waiting for the replica, and the recovery point
    /// of zero no longer holds until a replica has caught up again, which turns it back off.
    pub fn run_alone(&self) -> ReplicationStatus {
        let mut state = self.lock();
        if !state.run_alone {
            eprintln!(
                "replication: running alone at the operator's request; acknowledged commands exist on this machine only until a replica catches up"
            );
        }
        state.run_alone = true;
        self.shared.changed.notify_all();
        state.status(self.shared.epoch())
    }

    /// Cuts the link to the replica, as a network fault would, and keeps listening.
    #[cfg(test)]
    pub(crate) fn drop_replica(&self) {
        if let Some(link) = self.lock().replica.take() {
            let _ = link.socket.shutdown(Shutdown::Both);
        }
        self.shared.changed.notify_all();
    }

    /// Stops listening and drops the replica: for tests, which simulate the primary going away.
    #[cfg(test)]
    pub(crate) fn shutdown(&self) {
        self.shared.stopped.store(true, Ordering::Release);
        if let Some(link) = self.lock().replica.take() {
            let _ = link.socket.shutdown(Shutdown::Both);
        }
        self.shared.changed.notify_all();
        let _ = TcpStream::connect(self.address);
    }
}

fn accept(shared: Arc<Shared>, listener: TcpListener) {
    for socket in listener.incoming() {
        if shared.stopped.load(Ordering::Acquire) {
            return;
        }
        let Ok(socket) = socket else { continue };
        // Each handshake on its own thread: a slow peer, or a long comparison, holds up no other.
        let attaching = Arc::clone(&shared);
        let spawned = thread::Builder::new()
            .name("replication-handshake".into())
            .spawn(move || {
                if let Err(error) = attach(&attaching, socket) {
                    eprintln!("replication: replica not attached: {error}");
                }
            });
        if let Err(error) = spawned {
            eprintln!("replication: replica not attached: {error}");
        }
    }
}

/// Checks the replica's hello, tells it where it resumes, and makes it the replica.
fn attach(shared: &Arc<Shared>, mut socket: TcpStream) -> io::Result<()> {
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    let (kind, body) = read_frame(&mut socket, HELLO_LEN)?;
    if kind != HELLO {
        return Err(invalid("expected the replica's hello"));
    }
    let hello = Hello::parse(&body)?;
    // What the worker wrote never changes, so the comparison, which may read a long stretch of
    // journal, runs without holding up the worker.
    let written = {
        let state = shared.state.lock().unwrap();
        (state.written, state.next_sequence)
    };
    let resume = match resume_point(shared, written, hello) {
        Ok(resume) => resume,
        Err(reason) => {
            write_frame(&mut socket, REFUSE, &[reason.as_bytes()])?;
            return Err(invalid(reason));
        }
    };
    resume.send(&mut socket, WELCOME)?;
    // From now on the replica answers at least every second: silence means it, or the network,
    // is gone.
    socket.set_read_timeout(Some(LINK_TIMEOUT))?;
    let (receiver, link_socket) = (socket.try_clone()?, socket.try_clone()?);

    let mut state = shared.state.lock().unwrap();
    state.connections += 1;
    let connection = state.connections;
    if let Some(old) = state.replica.take() {
        let _ = old.socket.shutdown(Shutdown::Both);
    }
    state.replica = Some(Link {
        connection,
        confirmed: resume.end,
        sent: resume.end,
        socket: link_socket,
    });
    state.rejoin();
    shared.changed.notify_all();
    drop(state);
    eprintln!(
        "replication: replica attached, resuming at journal byte {}",
        resume.end
    );

    let receiving = Arc::clone(shared);
    let sending = Arc::clone(shared);
    let spawned = thread::Builder::new()
        .name("replication-receiver".into())
        .spawn(move || {
            if let Err(error) = receive(&receiving, connection, receiver) {
                detach(&receiving, connection, &error);
            }
        })
        .and_then(|_| {
            thread::Builder::new()
                .name("replication-sender".into())
                .spawn(move || {
                    if let Err(error) = send(&sending, connection, socket, resume.end) {
                        detach(&sending, connection, &error);
                    }
                })
        });
    if let Err(error) = spawned {
        // Without both threads the link would never move: drop it, and the replica dials again.
        detach(shared, connection, &error);
        return Err(error);
    }
    Ok(())
}

/// Where the replica described by `hello` resumes, or why it cannot be this primary's replica.
///
/// Its committed end is on both disks, so this journal must reach it, as a command boundary with
/// the same next sequence: a shorter journal lost committed history. Beyond it, the replica keeps
/// what it holds only if this journal holds the same bytes, ending at a command boundary; any
/// difference means this primary never synced those bytes, so the replica cuts back. `written` is
/// this journal's end and the next sequence there.
///
/// The epochs come first. A replica that has seen a later term than this primary's means this
/// primary was replaced: it is refused, so nothing confirms this primary again. A replica from an
/// earlier term must have committed only records from before the next term started here.
fn resume_point(shared: &Shared, written: (u64, u64), hello: Hello) -> Result<Position, String> {
    let epoch = shared.epoch();
    let resume = |end, next_sequence| Position {
        journal_id: Some(shared.journal_id),
        end,
        next_sequence,
        epoch,
    };
    let Some(journal_id) = hello.journal_id else {
        return if hello.end == 0 {
            Ok(resume(0, 1))
        } else {
            Err("the replica's journal has no id but is not empty".to_string())
        };
    };
    if journal_id != shared.journal_id {
        return Err(format!(
            "the replica holds journal {journal_id}, not this primary's journal {}",
            shared.journal_id
        ));
    }
    if hello.epoch > epoch {
        return Err(format!(
            "the replica has seen term {}, later than this primary's term {epoch}: this primary was replaced",
            hello.epoch
        ));
    }
    if let Some(next) = shared.terms.iter().find(|term| term.epoch > hello.epoch)
        && hello.committed_next > next.first_sequence
    {
        return Err(format!(
            "the replica committed records after its term {} ended here, at sequence {}: it acknowledged commands while another primary took over, so the two histories have split",
            hello.epoch, next.first_sequence
        ));
    }
    if hello.committed < JOURNAL_HEADER_LEN as u64 || hello.committed > hello.end {
        return Err("the replica described an impossible position".to_string());
    }
    if hello.committed > written.0 {
        return Err(
            "the replica holds committed records this journal lacks: this journal may be an older copy"
                .to_string(),
        );
    }
    if !is_boundary(shared, written, hello.committed, hello.committed_next) {
        return Err(
            "the replica's committed end is not a command boundary of this journal".to_string(),
        );
    }
    let same_tail = hello.end > hello.committed
        && hello.end <= written.0
        && crc32_of_file(&shared.journal, hello.committed, hello.end)
            .is_ok_and(|crc| crc == hello.tail_crc)
        && is_boundary(shared, written, hello.end, hello.end_next);
    if same_tail {
        return Ok(resume(hello.end, hello.end_next));
    }
    Ok(resume(hello.committed, hello.committed_next))
}

/// Whether a command record of this journal starts at `at` with `next_sequence`, or this journal
/// ends there followed by it.
fn is_boundary(shared: &Shared, written: (u64, u64), at: u64, next_sequence: u64) -> bool {
    if at == written.0 {
        return next_sequence == written.1;
    }
    read_record(&shared.journal, at, written.0)
        .and_then(|record| decode_batch(&record, next_sequence))
        .is_ok()
}

/// Reads the replica's confirmations.
fn receive(shared: &Shared, connection: u64, mut socket: TcpStream) -> io::Result<()> {
    loop {
        let (kind, body) = read_frame(&mut socket, 8)?;
        if kind != SYNCED {
            return Err(invalid("expected the replica's confirmation"));
        }
        let end = u64_at(&body, 0)?;
        let mut state = shared.state.lock().unwrap();
        let Some(link) = state.current(connection) else {
            return Ok(());
        };
        // It can confirm only what this link sent it, and never less than before.
        if end < link.confirmed || end > link.sent {
            return Err(invalid("the replica confirmed an impossible end"));
        }
        link.confirmed = end;
        state.rejoin();
        shared.changed.notify_all();
    }
}

/// Ships what the replica lacks, and the commit point whenever it moves, or at least every
/// heartbeat.
fn send(shared: &Shared, connection: u64, mut socket: TcpStream, mut sent: u64) -> io::Result<()> {
    let mut commit_sent = None;
    loop {
        let (written, commit) = {
            let mut state = shared.state.lock().unwrap();
            let heartbeat = Instant::now() + HEARTBEAT;
            loop {
                if state.current(connection).is_none() {
                    return Ok(());
                }
                let commit = state.commit_point();
                let now = Instant::now();
                if state.written > sent || commit_sent != Some(commit) || now >= heartbeat {
                    break (state.written, commit);
                }
                state = shared
                    .changed
                    .wait_timeout(state, heartbeat - now)
                    .unwrap()
                    .0;
            }
        };
        let records = if written > sent {
            whole_records(&shared.journal, sent, written)?
        } else {
            Vec::new()
        };
        if !records.is_empty() {
            // Counted before it is sent: the replica's confirmation may come back at once.
            match shared.state.lock().unwrap().current(connection) {
                Some(link) => link.sent = sent + records.len() as u64,
                None => return Ok(()),
            }
        }
        write_frame(
            &mut socket,
            RECORDS,
            &[&sent.to_le_bytes(), &commit.to_le_bytes(), &records],
        )?;
        sent += records.len() as u64;
        commit_sent = Some(commit);
    }
}

/// Whole records from `from`, up to `CHUNK` bytes of them, or the one record there if it is
/// bigger. Everything before `to` was written by the worker, and `to` is a record boundary. A new
/// replica first gets the journal header alone, which names the journal.
fn whole_records(journal: &File, from: u64, to: u64) -> io::Result<Vec<u8>> {
    if from == 0 {
        let mut header = vec![0; JOURNAL_HEADER_LEN];
        journal.read_exact_at(&mut header, 0)?;
        return Ok(header);
    }
    let mut bytes = vec![0; (to - from).min(CHUNK as u64) as usize];
    journal.read_exact_at(&mut bytes, from)?;
    let mut end = 0;
    while end + RECORD_HEADER_LEN <= bytes.len() {
        let len = record_length(&bytes[end..])?;
        if end + len > bytes.len() {
            break;
        }
        end += len;
    }
    if end == 0 {
        // The first record alone is bigger than a chunk.
        return read_record(journal, from, to);
    }
    bytes.truncate(end);
    Ok(bytes)
}

fn detach(shared: &Shared, connection: u64, error: &io::Error) {
    let mut state = shared.state.lock().unwrap();
    if state.current(connection).is_some() {
        if let Some(link) = state.replica.take() {
            let _ = link.socket.shutdown(Shutdown::Both);
        }
        eprintln!("replication: replica lost: {error}");
        shared.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        thread::JoinHandle,
    };

    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{
        exchange::{
            event_store::{EventStore, crc32, encode_record},
            event_stream::StreamReader,
            replica::{Ended, Replica},
            runtime::{recover_replicated_runtime, recover_runtime_with_stream},
            terms::{Term, Terms},
        },
        types::{
            exchange_event::{
                EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent,
            },
            types::ExchangeCommand,
        },
    };

    struct Machines {
        dir: PathBuf,
    }

    impl Machines {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("stock-replication-{}", Uuid::new_v4()));
            fs::create_dir(&dir).unwrap();
            Self { dir }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }

        /// The primary on its journal, replicated, with its worker running.
        fn primary(&self) -> (mpsc::Sender<ExchangeCommand>, Arc<Replication>) {
            let (tx, rx) = mpsc::channel(64);
            let mut runtime = recover_replicated_runtime(
                rx,
                self.path("primary.log"),
                self.path("primary.mmap"),
                self.path("primary.snapshot"),
            )
            .unwrap();
            let replication = runtime.replicate("127.0.0.1:0".parse().unwrap()).unwrap();
            thread::spawn(move || runtime.run());
            (tx, replication)
        }

        /// The replica's machine after a promotion: a primary on its copy that starts the next
        /// term before it listens, as `main` does.
        fn promoted(&self) -> (mpsc::Sender<ExchangeCommand>, Arc<Replication>) {
            let (tx, rx) = mpsc::channel(64);
            let mut runtime = recover_replicated_runtime(
                rx,
                self.path("primary.log"),
                self.path("primary.mmap"),
                self.path("primary.snapshot"),
            )
            .unwrap();
            runtime.begin_term().unwrap();
            let replication = runtime.replicate("127.0.0.1:0".parse().unwrap()).unwrap();
            thread::spawn(move || runtime.run());
            (tx, replication)
        }

        /// One session of the replica on this machine's copy, until the link ends.
        fn replica(&self, primary: &Replication) -> JoinHandle<Result<(), Ended>> {
            let journal = self.path("replica.log");
            let stream = self.path("replica.mmap");
            let address = primary.address().to_string();
            thread::spawn(move || Replica::open(journal, stream).unwrap().follow(&address))
        }

        fn same_journal(&self) -> bool {
            fs::read(self.path("primary.log")).unwrap()
                == fs::read(self.path("replica.log")).unwrap()
        }

        /// What the replica's stream published: its batches, read by an ordinary reader.
        fn replica_published(&self) -> usize {
            let mut reader =
                StreamReader::open(self.path("replica.log"), self.path("replica.mmap"), None)
                    .unwrap();
            let mut batches = 0;
            while reader.next_batch().unwrap().is_some() {
                batches += 1;
            }
            batches
        }
    }

    impl Drop for Machines {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn deposit(
        exchange: &mpsc::Sender<ExchangeCommand>,
        amount: u64,
    ) -> oneshot::Receiver<Result<(), String>> {
        let (respond_to, reply) = oneshot::channel();
        exchange
            .blocking_send(ExchangeCommand::Deposit {
                user_id: "buyer".into(),
                amount,
                respond_to,
            })
            .unwrap();
        reply
    }

    /// The reply, if it arrives within `wait`.
    fn reply_within<T>(reply: &mut oneshot::Receiver<T>, wait: Duration) -> Option<T> {
        let deadline = Instant::now() + wait;
        loop {
            if let Ok(value) = reply.try_recv() {
                return Some(value);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn eventually(what: &str, mut check: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !check() {
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn deposit_batch(seq: u64, amount: u64) -> Vec<EventEnvelope> {
        vec![
            EventEnvelope {
                seq_num: seq,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount,
                }),
            },
            EventEnvelope {
                seq_num: seq + 1,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount,
                }),
            },
        ]
    }

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

    /// A journal of deposits 1, 2, ... at `path`, published on its stream as a primary leaves it.
    fn journal_with(path: &Path, stream: &Path, deposits: u64) {
        let (_tx, rx) = mpsc::channel(1);
        let mut runtime = recover_runtime_with_stream(rx, path, stream).unwrap();
        for amount in 1..=deposits {
            runtime
                .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount,
                })
                .unwrap();
        }
    }

    #[test]
    fn a_command_is_answered_only_once_the_replica_holds_it() {
        let machines = Machines::new();
        let (exchange, replication) = machines.primary();

        let mut first = deposit(&exchange, 10);
        assert!(reply_within(&mut first, Duration::from_millis(300)).is_none());
        assert_eq!(replication.status().mode, "paused");
        assert!(replication.paused());

        let session = machines.replica(&replication);
        assert_eq!(
            reply_within(&mut first, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        let mut second = deposit(&exchange, 20);
        assert_eq!(
            reply_within(&mut second, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        let status = replication.status();
        assert_eq!(status.mode, "synchronous");
        assert_eq!(status.replica_end, Some(status.journal_end));

        // The replica publishes both once the primary tells it both disks hold them.
        eventually("the replica published both", || {
            machines.replica_published() == 2
        });
        replication.shutdown();
        assert!(matches!(session.join().unwrap(), Err(Ended::Lost(_))));
    }

    #[test]
    fn a_replica_that_loses_its_link_comes_back_and_the_primary_waits_meanwhile() {
        let machines = Machines::new();
        let (exchange, replication) = machines.primary();
        let first_session = machines.replica(&replication);
        let mut first = deposit(&exchange, 10);
        assert_eq!(
            reply_within(&mut first, Duration::from_secs(10)),
            Some(Ok(()))
        );
        replication.drop_replica();
        assert!(first_session.join().unwrap().is_err());

        let mut second = deposit(&exchange, 20);
        assert!(reply_within(&mut second, Duration::from_millis(300)).is_none());
        assert!(replication.paused());
        let second_session = machines.replica(&replication);
        assert_eq!(
            reply_within(&mut second, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(second_session.join().unwrap().is_err());
    }

    #[test]
    fn running_alone_releases_a_paused_primary_until_a_replica_catches_up() {
        let machines = Machines::new();
        let (exchange, replication) = machines.primary();
        let mut first = deposit(&exchange, 10);
        assert!(reply_within(&mut first, Duration::from_millis(300)).is_none());

        assert_eq!(replication.run_alone().mode, "running alone");
        assert_eq!(
            reply_within(&mut first, Duration::from_secs(10)),
            Some(Ok(()))
        );
        let mut second = deposit(&exchange, 20);
        assert_eq!(
            reply_within(&mut second, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(!replication.paused());
        // The worker learns that it answers on this machine only, and syncs its stream's end.
        assert!(replication.confirm(replication.status().journal_end));

        // A replica catches up on both, which turns running alone off by itself.
        let session = machines.replica(&replication);
        eventually("synchronous again", || {
            replication.status().mode == "synchronous"
        });
        assert!(!replication.confirm(replication.status().journal_end));
        let mut third = deposit(&exchange, 30);
        assert_eq!(
            reply_within(&mut third, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// The primary restarted without a record it had written and sent but never synced, so never
    /// acknowledged, and may already have written another in its place. The replica holds the old
    /// one: it cuts back to what is committed and takes the primary's records instead.
    #[test]
    fn a_replica_holding_what_a_restarted_primary_lost_takes_the_primarys_records() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            2,
        );
        let mut copy = fs::read(machines.path("primary.log")).unwrap();
        copy.extend_from_slice(&encode_record(&deposit_batch(5, 3)).unwrap());
        fs::write(machines.path("replica.log"), &copy).unwrap();

        let (exchange, replication) = machines.primary();
        let before = replication.status().journal_end;
        let mut reply = deposit(&exchange, 4);
        // The primary writes its deposit where the replica holds the lost one, with the same
        // length, before the replica connects: only the checksum tells them apart.
        eventually("the deposit is written", || {
            replication.status().journal_end > before
        });
        assert_eq!(
            fs::metadata(machines.path("replica.log")).unwrap().len(),
            replication.status().journal_end
        );
        let session = machines.replica(&replication);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// A replica that already holds records its stream never published, because the commit
    /// point did not reach it, keeps them: the primary has them too, and resumes after them.
    #[test]
    fn a_replica_keeps_what_it_holds_and_publishes_it_once_committed() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            3,
        );
        fs::copy(machines.path("primary.log"), machines.path("replica.log")).unwrap();
        let replica_before = fs::read(machines.path("replica.log")).unwrap();

        let (_exchange, replication) = machines.primary();
        let session = machines.replica(&replication);
        eventually("the replica published all three", || {
            machines.path("replica.mmap").exists() && machines.replica_published() == 3
        });
        assert_eq!(
            fs::read(machines.path("replica.log")).unwrap(),
            replica_before
        );
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    #[test]
    fn a_replica_of_another_journal_is_refused_and_left_untouched() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        {
            let (mut other, _) = EventStore::open(machines.path("replica.log")).unwrap();
            other.append(&deposit_batch(1, 7)).unwrap();
        }
        let before = fs::read(machines.path("replica.log")).unwrap();

        let (_exchange, replication) = machines.primary();
        let session = machines.replica(&replication);
        assert!(matches!(
            session.join().unwrap(),
            Err(Ended::Refused(reason)) if reason.contains("not this primary's journal")
        ));
        assert_eq!(fs::read(machines.path("replica.log")).unwrap(), before);
        replication.shutdown();
    }

    /// After a restart, the primary's journal may hold records its replica never confirmed. It
    /// publishes them, and serves anything, only once the replica holds them.
    #[test]
    fn a_restarted_primary_publishes_and_serves_nothing_before_its_replica_holds_it() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            2,
        );
        fs::remove_file(machines.path("primary.mmap")).unwrap();

        let (exchange, replication) = machines.primary();
        // Nor does it write its startup snapshot, which would stand beyond what it published.
        assert!(!machines.path("primary.snapshot").exists());
        let (respond_to, mut session) = oneshot::channel();
        exchange
            .blocking_send(ExchangeCommand::GetSession { respond_to })
            .unwrap();
        assert!(reply_within(&mut session, Duration::from_millis(300)).is_none());
        let mut reader = StreamReader::open(
            machines.path("primary.log"),
            machines.path("primary.mmap"),
            None,
        )
        .unwrap();
        assert!(reader.next_batch().unwrap().is_none());

        let link = machines.replica(&replication);
        assert!(reply_within(&mut session, Duration::from_secs(10)).is_some());
        assert_eq!(reader.next_batch().unwrap().unwrap(), deposit_batch(1, 1));
        assert_eq!(reader.next_batch().unwrap().unwrap(), deposit_batch(3, 2));
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(link.join().unwrap().is_err());
    }

    #[test]
    fn frames_are_bounded_and_positions_round_trip() {
        let position = Position {
            journal_id: Some(Uuid::new_v4()),
            end: 1234,
            next_sequence: 77,
            epoch: 3,
        };
        let mut bytes = Vec::new();
        position.send(&mut bytes, HELLO).unwrap();
        let (kind, body) = read_frame(&mut bytes.as_slice(), MAX_ANSWER).unwrap();
        assert_eq!((kind, Position::parse(&body).unwrap()), (HELLO, position));

        let hello = Hello {
            journal_id: None,
            committed: 24,
            committed_next: 1,
            end: 999,
            end_next: 9,
            tail_crc: 0xDEAD_BEEF,
            epoch: 2,
        };
        bytes.clear();
        hello.send(&mut bytes).unwrap();
        let (kind, body) = read_frame(&mut bytes.as_slice(), HELLO_LEN).unwrap();
        assert_eq!((kind, Hello::parse(&body).unwrap()), (HELLO, hello));
        // Longer than the frame expected is refused, whatever follows.
        assert!(read_frame(&mut bytes.as_slice(), HELLO_LEN - 1).is_err());

        // A length beyond the largest frame is refused before anything is allocated.
        let mut oversized = ((MAX_BODY + 1) as u32).to_le_bytes().to_vec();
        oversized.push(RECORDS);
        assert!(read_frame(&mut oversized.as_slice(), MAX_BODY).is_err());
    }

    /// The test plays the primary: the replica holds both records on its disk, but publishes each
    /// only once a commit point says the primary's disk holds it too.
    #[test]
    fn a_replica_publishes_only_up_to_the_commit_point() {
        fn ship(primary: &mut TcpStream, offset: u64, commit: u64, records: &[u8]) {
            write_frame(
                primary,
                RECORDS,
                &[&offset.to_le_bytes(), &commit.to_le_bytes(), records],
            )
            .unwrap();
            // Every frame is answered with how far the replica is durable.
            let (kind, body) = read_frame(primary, 8).unwrap();
            let durable = offset + records.len() as u64;
            assert_eq!((kind, u64_at(&body, 0).unwrap()), (SYNCED, durable));
        }

        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            2,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let header = JOURNAL_HEADER_LEN as u64;
        let first_end = header + record_length(&journal[JOURNAL_HEADER_LEN..]).unwrap() as u64;
        let end = journal.len() as u64;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (copy, stream) = (machines.path("replica.log"), machines.path("replica.mmap"));
        let session = thread::spawn(move || Replica::open(copy, stream).unwrap().follow(&address));
        let (mut primary, _) = listener.accept().unwrap();
        assert_eq!(read_frame(&mut primary, HELLO_LEN).unwrap().0, HELLO);
        Position {
            journal_id: Some(Uuid::from_bytes(journal[8..24].try_into().unwrap())),
            end: 0,
            next_sequence: 1,
            epoch: 0,
        }
        .send(&mut primary, WELCOME)
        .unwrap();

        ship(&mut primary, 0, 0, &journal[..JOURNAL_HEADER_LEN]);
        ship(&mut primary, header, header, &journal[JOURNAL_HEADER_LEN..]);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(machines.replica_published(), 0);
        ship(&mut primary, end, first_end, &[]);
        eventually("the first record is published", || {
            machines.replica_published() == 1
        });
        ship(&mut primary, end, end, &[]);
        eventually("both are published", || machines.replica_published() == 2);

        // A commit point beyond what the replica holds ends the session.
        ship(&mut primary, end, end + 1, &[]);
        assert!(matches!(
            session.join().unwrap(),
            Err(Ended::Lost(error)) if error.to_string().contains("committed beyond")
        ));
    }

    /// The handshake's decisions, with the test playing replicas that hold different things.
    #[test]
    fn the_handshake_keeps_an_identical_tail_cuts_a_different_one_and_refuses_lost_history() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            3,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let (_exchange, replication) = machines.primary();
        let journal_id = Some(Uuid::from_bytes(journal[8..24].try_into().unwrap()));
        let header = JOURNAL_HEADER_LEN as u64;
        let first_end = header + record_length(&journal[JOURNAL_HEADER_LEN..]).unwrap() as u64;
        let end = journal.len() as u64;
        // Where the primary tells a replica to resume, or why it refuses it. Deposits 1, 2 and 3
        // take sequences 1 to 6.
        let answer = |committed: u64, committed_next: u64, tail: &[u8]| {
            let mut replica = TcpStream::connect(replication.address()).unwrap();
            Hello {
                journal_id,
                committed,
                committed_next,
                end: committed + tail.len() as u64,
                end_next: 7,
                tail_crc: crc32(tail),
                epoch: 0,
            }
            .send(&mut replica)
            .unwrap();
            let (kind, body) = read_frame(&mut replica, MAX_ANSWER).unwrap();
            match kind {
                WELCOME => Ok(Position::parse(&body).unwrap().end),
                _ => Err(String::from_utf8(body).unwrap()),
            }
        };

        // What it holds beyond its committed end is kept when the primary holds the same bytes.
        assert_eq!(answer(header, 1, &journal[JOURNAL_HEADER_LEN..]), Ok(end));
        assert_eq!(
            answer(first_end, 3, &journal[first_end as usize..]),
            Ok(end)
        );
        // One byte different, or more than the primary holds: cut back to the committed end.
        let mut different = journal[JOURNAL_HEADER_LEN..].to_vec();
        different[20] ^= 1;
        assert_eq!(answer(header, 1, &different), Ok(header));
        let mut longer = journal[JOURNAL_HEADER_LEN..].to_vec();
        longer.extend_from_slice(&encode_record(&deposit_batch(7, 4)).unwrap());
        assert_eq!(answer(header, 1, &longer), Ok(header));
        // Committed records the primary lacks, or a committed end that is not one of its command
        // boundaries: refused.
        assert!(answer(end + 10, 9, &[]).unwrap_err().contains("lacks"));
        assert!(
            answer(header + 1, 1, &[])
                .unwrap_err()
                .contains("not a command boundary")
        );
        replication.shutdown();
    }

    /// The test plays a primary that welcomes the replica and then says nothing, as one whose
    /// machine lost power would, and then a replica that never answers: each side drops the link.
    #[test]
    fn a_silent_link_is_dropped_on_both_sides() {
        let machines = Machines::new();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (copy, stream) = (machines.path("replica.log"), machines.path("replica.mmap"));
        let session = thread::spawn(move || Replica::open(copy, stream).unwrap().follow(&address));
        let (mut silent_primary, _) = listener.accept().unwrap();
        assert_eq!(read_frame(&mut silent_primary, HELLO_LEN).unwrap().0, HELLO);
        Position {
            journal_id: Some(Uuid::new_v4()),
            end: 0,
            next_sequence: 1,
            epoch: 0,
        }
        .send(&mut silent_primary, WELCOME)
        .unwrap();
        let welcomed = Instant::now();
        assert!(matches!(session.join().unwrap(), Err(Ended::Lost(_))));
        assert!(welcomed.elapsed() >= LINK_TIMEOUT / 2);

        let (_exchange, replication) = machines.primary();
        let mut silent_replica = TcpStream::connect(replication.address()).unwrap();
        Hello {
            journal_id: None,
            committed: 0,
            committed_next: 1,
            end: 0,
            end_next: 1,
            tail_crc: 0,
            epoch: 0,
        }
        .send(&mut silent_replica)
        .unwrap();
        let (kind, _) = read_frame(&mut silent_replica, MAX_ANSWER).unwrap();
        assert_eq!(kind, WELCOME);
        eventually("the replica is attached", || {
            replication.status().replica_connected
        });
        eventually("the silent replica is dropped", || {
            !replication.status().replica_connected
        });
        replication.shutdown();
        drop(silent_primary);
    }

    #[test]
    fn a_replica_cuts_a_torn_tail_and_refuses_a_damaged_record() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            2,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let first_end = JOURNAL_HEADER_LEN + record_length(&journal[JOURNAL_HEADER_LEN..]).unwrap();
        let (copy, stream) = (machines.path("replica.log"), machines.path("replica.mmap"));

        // The second record cut short, as by a crash while it was written.
        fs::write(&copy, &journal[..journal.len() - 3]).unwrap();
        drop(Replica::open(&copy, &stream).unwrap());
        assert_eq!(fs::read(&copy).unwrap(), journal[..first_end].to_vec());

        // A complete record that does not check out is refused, and left as it is.
        let mut damaged = journal.clone();
        damaged[first_end + 20] ^= 1;
        fs::write(&copy, &damaged).unwrap();
        assert!(Replica::open(&copy, &stream).is_err());
        assert_eq!(fs::read(&copy).unwrap(), damaged);
    }

    /// A stream it cannot open, here another journal's, stops the replica instead of leaving it
    /// confirming records it can never publish.
    #[test]
    fn a_replica_whose_stream_belongs_to_another_journal_stops() {
        let machines = Machines::new();
        journal_with(
            &machines.path("other.log"),
            &machines.path("replica.mmap"),
            1,
        );
        let (_exchange, replication) = machines.primary();
        let session = machines.replica(&replication);
        assert!(matches!(
            session.join().unwrap(),
            Err(Ended::Failed(error)) if error.to_string().contains("different journal")
        ));
        replication.shutdown();
    }

    /// With nothing to send for longer than the link timeout, heartbeats keep the link up.
    #[test]
    fn an_idle_link_stays_up() {
        let machines = Machines::new();
        let (exchange, replication) = machines.primary();
        let session = machines.replica(&replication);
        eventually("the replica is attached", || {
            replication.status().replica_connected
        });
        thread::sleep(LINK_TIMEOUT + Duration::from_secs(1));
        assert!(replication.status().replica_connected);
        assert!(!session.is_finished());
        let mut reply = deposit(&exchange, 10);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// The test plays a primary that cuts the replica back, sends a record of the same length in
    /// place of the one cut, and loses the link before the commit point moves. Dialing again, the
    /// replica describes what it holds now: a hello kept from before the cut would get the record
    /// it confirmed cut too.
    #[test]
    fn a_replica_refilled_after_a_cut_describes_its_new_bytes() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let (header, first) = journal.split_at(JOURNAL_HEADER_LEN);
        let lost = encode_record(&deposit_batch(3, 3)).unwrap();
        let written = encode_record(&deposit_batch(3, 4)).unwrap();
        assert_eq!(lost.len(), written.len());
        let copy = [header, first, lost.as_slice()].concat();
        fs::write(machines.path("replica.log"), copy).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (copy, stream) = (machines.path("replica.log"), machines.path("replica.mmap"));
        let sessions = thread::spawn(move || {
            let mut replica = Replica::open(copy, stream).unwrap();
            let _ = replica.follow(&address);
            let _ = replica.follow(&address);
        });
        let hello = |primary: &mut TcpStream| {
            let (kind, body) = read_frame(primary, HELLO_LEN).unwrap();
            assert_eq!(kind, HELLO);
            Hello::parse(&body).unwrap()
        };

        let (mut primary, _) = listener.accept().unwrap();
        let held = [first, lost.as_slice()].concat();
        assert_eq!(hello(&mut primary).tail_crc, crc32(&held));
        let start = JOURNAL_HEADER_LEN as u64;
        Position {
            journal_id: Some(Uuid::from_bytes(header[8..24].try_into().unwrap())),
            end: start,
            next_sequence: 1,
            epoch: 0,
        }
        .send(&mut primary, WELCOME)
        .unwrap();
        let records = [first, written.as_slice()].concat();
        let frame: [&[u8]; 3] = [&start.to_le_bytes(), &start.to_le_bytes(), &records];
        write_frame(&mut primary, RECORDS, &frame).unwrap();
        assert_eq!(read_frame(&mut primary, 8).unwrap().0, SYNCED);
        drop(primary);

        let (mut primary, _) = listener.accept().unwrap();
        assert_eq!(hello(&mut primary).tail_crc, crc32(&records));
        drop(primary);
        sessions.join().unwrap();
    }

    /// A peer that connects and never says hello holds up only its own handshake.
    #[test]
    fn a_peer_that_never_says_hello_holds_up_no_replica() {
        let machines = Machines::new();
        let (exchange, replication) = machines.primary();
        let _silent = TcpStream::connect(replication.address()).unwrap();
        let session = machines.replica(&replication);
        let mut reply = deposit(&exchange, 10);
        assert_eq!(
            reply_within(&mut reply, HANDSHAKE_TIMEOUT - Duration::from_secs(1)),
            Some(Ok(()))
        );
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// The ends of a journal's records, in order.
    fn record_ends(journal: &[u8]) -> Vec<u64> {
        let mut ends = Vec::new();
        let mut at = JOURNAL_HEADER_LEN;
        while at < journal.len() {
            at += record_length(&journal[at..]).unwrap();
            ends.push(at as u64);
        }
        ends
    }

    /// The epochs in the handshake, with the test playing replicas of different terms against a
    /// primary in term 1, which began after two deposits.
    #[test]
    fn epochs_fence_a_replaced_primary_and_refuse_a_split_history() {
        let machines = Machines::new();
        {
            let (_tx, rx) = mpsc::channel(1);
            let mut runtime = recover_runtime_with_stream(
                rx,
                machines.path("primary.log"),
                machines.path("primary.mmap"),
            )
            .unwrap();
            let deposit = |amount| ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount,
            };
            for input in [
                deposit(1),
                deposit(2),
                ExchangeInputEvent::TermStarted { epoch: 1 },
                deposit(3),
            ] {
                runtime.record_input_for_test(input).unwrap();
            }
        }
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let journal_id = Some(Uuid::from_bytes(journal[8..24].try_into().unwrap()));
        // After the second deposit term 1 begins, at sequence 5; the third deposit ends the journal.
        let ends = record_ends(&journal);
        let (_exchange, replication) = machines.primary();
        assert_eq!(replication.status().epoch, 1);
        let answer = |epoch: u64, committed: u64, committed_next: u64| {
            let mut replica = TcpStream::connect(replication.address()).unwrap();
            Hello {
                journal_id,
                committed,
                committed_next,
                end: committed,
                end_next: committed_next,
                tail_crc: 0,
                epoch,
            }
            .send(&mut replica)
            .unwrap();
            let (kind, body) = read_frame(&mut replica, MAX_ANSWER).unwrap();
            match kind {
                WELCOME => Ok(Position::parse(&body).unwrap()),
                _ => Err(String::from_utf8(body).unwrap()),
            }
        };

        // A replica that has seen a later term: this primary was replaced, and is never confirmed.
        assert!(answer(2, ends[3], 9).unwrap_err().contains("was replaced"));
        // From term 0 it committed only what came before term 1 began: welcome, in term 1.
        let welcome = answer(0, ends[1], 5).unwrap();
        assert_eq!((welcome.end, welcome.epoch), (ends[1], 1));
        // From term 0 but committed past where term 1 began here: the histories split.
        assert!(answer(0, ends[3], 9).unwrap_err().contains("split"));
        // In term 1, anything this journal holds.
        assert_eq!(answer(1, ends[3], 9).unwrap().end, ends[3]);
        replication.shutdown();
    }

    /// The test plays a primary of term 0 that a replica holding term 1 dials: it refuses it.
    #[test]
    fn a_replica_refuses_a_primary_from_an_earlier_term() {
        let machines = Machines::new();
        {
            let (_tx, rx) = mpsc::channel(1);
            let mut runtime = recover_runtime_with_stream(
                rx,
                machines.path("replica.log"),
                machines.path("replica.mmap"),
            )
            .unwrap();
            runtime
                .record_input_for_test(ExchangeInputEvent::TermStarted { epoch: 1 })
                .unwrap();
        }
        let journal = fs::read(machines.path("replica.log")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (copy, stream) = (machines.path("replica.log"), machines.path("replica.mmap"));
        let session = thread::spawn(move || Replica::open(copy, stream).unwrap().follow(&address));
        let (mut primary, _) = listener.accept().unwrap();
        let (_, body) = read_frame(&mut primary, HELLO_LEN).unwrap();
        assert_eq!(Hello::parse(&body).unwrap().epoch, 1);
        Position {
            journal_id: Some(Uuid::from_bytes(journal[8..24].try_into().unwrap())),
            end: journal.len() as u64,
            next_sequence: 3,
            epoch: 0,
        }
        .send(&mut primary, WELCOME)
        .unwrap();
        assert!(matches!(
            session.join().unwrap(),
            Err(Ended::Refused(reason)) if reason.contains("was replaced")
        ));
    }

    /// A failover. The old primary acknowledged two deposits, which the replica holds, and wrote a
    /// third that was never confirmed. The replica's machine promotes its copy into term 1, and the
    /// old primary rejoins as its replica: it drops the third deposit, which it never acknowledged,
    /// and takes the new term's records.
    #[test]
    fn a_promoted_replica_starts_a_term_and_the_old_primary_rejoins_as_its_replica() {
        let machines = Machines::new();
        journal_with(&machines.path("old.log"), &machines.path("old.mmap"), 2);
        fs::copy(machines.path("old.log"), machines.path("primary.log")).unwrap();
        let mut unconfirmed = fs::OpenOptions::new()
            .append(true)
            .open(machines.path("old.log"))
            .unwrap();
        unconfirmed
            .write_all(&encode_record(&deposit_batch(5, 3)).unwrap())
            .unwrap();
        drop(unconfirmed);

        let (exchange, replication) = machines.promoted();
        assert_eq!(replication.status().epoch, 1);
        let (old, old_stream) = (machines.path("old.log"), machines.path("old.mmap"));
        let address = replication.address().to_string();
        let rejoined =
            thread::spawn(move || Replica::open(old, old_stream).unwrap().follow(&address));
        let mut reply = deposit(&exchange, 4);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        let promoted = fs::read(machines.path("primary.log")).unwrap();
        assert_eq!(fs::read(machines.path("old.log")).unwrap(), promoted);
        // Two deposits, the start of term 1, and the deposit of term 1.
        assert_eq!(record_ends(&promoted).len(), 4);
        let old = File::open(machines.path("old.log")).unwrap();
        assert_eq!(
            Terms::load(&machines.path("old.log"), &old)
                .unwrap()
                .epoch(),
            1
        );
        replication.shutdown();
        assert!(rejoined.join().unwrap().is_err());
    }

    /// A promotion that stopped after beginning its term and before journaling it, here before
    /// `run`: restarted as an ordinary primary, it journals that term first, byte for byte as it
    /// would have.
    #[test]
    fn a_term_begun_but_never_journaled_is_journaled_when_the_primary_restarts() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        {
            let (_tx, rx) = mpsc::channel(1);
            let mut runtime = recover_replicated_runtime(
                rx,
                machines.path("primary.log"),
                machines.path("primary.mmap"),
                machines.path("primary.snapshot"),
            )
            .unwrap();
            assert_eq!(runtime.begin_term().unwrap(), 1);
        }

        let (exchange, replication) = machines.primary();
        assert_eq!(replication.status().epoch, 1);
        let session = machines.replica(&replication);
        let mut reply = deposit(&exchange, 5);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        // The first deposit, the start of term 1, then the new deposit.
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let ends = record_ends(&journal);
        assert_eq!(ends.len(), 3);
        assert_eq!(
            journal[ends[0] as usize..ends[1] as usize],
            encode_record(&term_batch(3, 1)).unwrap()
        );
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// A replica that indexed the start of term 1 and stopped before writing its record holds no
    /// such term: it forgets the entry, follows the primary of term 0, and takes the term again only
    /// when a primary sends it. But its epoch may be another primary's: promoted later, this copy
    /// takes the epoch after it.
    #[test]
    fn a_replica_forgets_a_term_it_indexed_but_never_wrote() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        fs::write(machines.path("replica.log"), &journal).unwrap();
        let journal_id = Uuid::from_bytes(journal[8..24].try_into().unwrap());
        fs::write(
            machines.path("replica.log.terms"),
            format!(
                r#"{{"journal_id":"{journal_id}","highest_epoch":1,"terms":[{{"epoch":1,"first_sequence":3,"offset":{}}}]}}"#,
                journal.len()
            ),
        )
        .unwrap();

        let (exchange, replication) = machines.primary();
        let session = machines.replica(&replication);
        let mut reply = deposit(&exchange, 5);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        replication.shutdown();
        assert!(matches!(session.join().unwrap(), Err(Ended::Lost(_))));

        let (_tx, rx) = mpsc::channel(1);
        let mut promoted = recover_replicated_runtime(
            rx,
            machines.path("replica.log"),
            machines.path("replica.mmap"),
            machines.path("replica.snapshot"),
        )
        .unwrap();
        assert_eq!(promoted.begin_term().unwrap(), 2);
    }

    /// A copy whose index ends with a term start it never wrote, as a replica that indexed another
    /// primary's term start and then failed leaves it, is promoted: the new term takes the epoch
    /// after it, never that one.
    #[test]
    fn a_promotion_takes_the_epoch_after_a_term_its_copy_indexed_but_never_wrote() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let journal_id = Uuid::from_bytes(journal[8..24].try_into().unwrap());
        fs::write(
            machines.path("primary.log.terms"),
            format!(
                r#"{{"journal_id":"{journal_id}","highest_epoch":1,"terms":[{{"epoch":1,"first_sequence":3,"offset":{}}}]}}"#,
                journal.len()
            ),
        )
        .unwrap();

        let (exchange, replication) = machines.promoted();
        assert_eq!(replication.status().epoch, 2);
        let session = machines.replica(&replication);
        let mut reply = deposit(&exchange, 5);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let ends = record_ends(&journal);
        assert_eq!(
            journal[ends[0] as usize..ends[1] as usize],
            encode_record(&term_batch(3, 2)).unwrap()
        );
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }

    /// A replica holding the start of term 1, not even committed yet, dials a primary of term 0:
    /// refused, so nothing confirms a primary that was replaced.
    #[test]
    fn a_primary_refuses_a_replica_that_holds_a_later_term() {
        let machines = Machines::new();
        journal_with(
            &machines.path("primary.log"),
            &machines.path("primary.mmap"),
            1,
        );
        let mut copy = fs::read(machines.path("primary.log")).unwrap();
        copy.extend_from_slice(&encode_record(&term_batch(3, 1)).unwrap());
        fs::write(machines.path("replica.log"), copy).unwrap();

        let (_exchange, replication) = machines.primary();
        assert_eq!(replication.status().epoch, 0);
        let session = machines.replica(&replication);
        // The primary's own refusal, before the replica could refuse it in turn.
        assert!(matches!(
            session.join().unwrap(),
            Err(Ended::Refused(reason)) if reason.contains("later than this primary's term")
        ));
        replication.shutdown();
    }

    /// A replica whose tail holds the start of term 1 where the primary has another record: the
    /// cut back drops it from the replica's index too, and the replica takes term 1 where the
    /// primary started it.
    #[test]
    fn a_term_start_cut_back_is_forgotten_and_taken_again() {
        let machines = Machines::new();
        {
            let (_tx, rx) = mpsc::channel(1);
            let mut runtime = recover_runtime_with_stream(
                rx,
                machines.path("primary.log"),
                machines.path("primary.mmap"),
            )
            .unwrap();
            let deposit_input = |amount| ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount,
            };
            for input in [
                deposit_input(1),
                deposit_input(2),
                ExchangeInputEvent::TermStarted { epoch: 1 },
            ] {
                runtime.record_input_for_test(input).unwrap();
            }
        }
        let journal = fs::read(machines.path("primary.log")).unwrap();
        let ends = record_ends(&journal);
        let mut copy = journal[..ends[0] as usize].to_vec();
        copy.extend_from_slice(&encode_record(&term_batch(3, 1)).unwrap());
        fs::write(machines.path("replica.log"), &copy).unwrap();

        let (exchange, replication) = machines.primary();
        assert_eq!(replication.status().epoch, 1);
        let session = machines.replica(&replication);
        let mut reply = deposit(&exchange, 7);
        assert_eq!(
            reply_within(&mut reply, Duration::from_secs(10)),
            Some(Ok(()))
        );
        assert!(machines.same_journal());
        let copy = File::open(machines.path("replica.log")).unwrap();
        assert_eq!(
            Terms::load(&machines.path("replica.log"), &copy)
                .unwrap()
                .list(),
            [Term {
                epoch: 1,
                first_sequence: 5,
                offset: ends[1],
            }]
        );
        replication.shutdown();
        assert!(session.join().unwrap().is_err());
    }
}
