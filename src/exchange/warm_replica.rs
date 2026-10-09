//! Same-host warm replica.
//!
//! This process deterministically rebuilds the exchange core from complete committed batches but
//! deliberately owns no journal writer, mmap writer, command queue, customer HTTP routes, or
//! database connection. Its only listener is a loopback-only operator control endpoint. A manual
//! promotion first acquires the journal's exclusive writer lock, then hands its own core to the
//! new primary, which replays only the records this replica had not applied yet.
//!
//! It is also the exchange's snapshot writer. It reads every batch from the durable journal (the
//! mmap stream supplies only the committed watermark), checks each one by deterministic replay,
//! and writes the journal-bound core snapshot the primary loads on restart: right after each open,
//! and whenever the journal has grown by `snapshot_growth` times the last snapshot's size. The
//! primary's trading thread therefore never stops to serialize its whole state.

use std::{
    error::Error,
    fs::File,
    future::IntoFuture,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio::sync::{oneshot, watch};

use super::{
    core::ExchangeCore,
    event_store::{EventStore, EventStoreError, journal_id_of},
    event_stream::{
        ReaderCheckpoint, StreamReader, decode_batch, is_publication_interrupted, read_record,
    },
    runtime::{ReplayError, ReplicaCore},
    snapshot::{self, SnapshotBoundary},
};
use crate::types::exchange_event::{EventEnvelope, ExchangeEvent, ExchangeOutputEvent};

const DEFAULT_ADDR: &str = "127.0.0.1:4003";
const IDLE_POLL: Duration = Duration::from_millis(10);

type WarmResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// Whether `checkpoint` is a command boundary of the journal itself: the journal ends there, or a
/// complete record starts there with its next sequence. Read from the journal, never from the
/// stream, whose header a running primary may be rewriting.
fn is_journal_boundary(journal_path: &Path, checkpoint: &ReaderCheckpoint) -> bool {
    let Ok(journal) = File::open(journal_path) else {
        return false;
    };
    let Ok(len) = journal.metadata().map(|metadata| metadata.len()) else {
        return false;
    };
    let at = checkpoint.byte_offset();
    at == len
        || read_record(&journal, at, len)
            .and_then(|record| decode_batch(&record, checkpoint.next_sequence))
            .is_ok()
}

/// The fenced ownership hand-off returned to `main`, which creates the primary runtime from it:
/// the writer-locked journal, this replica's core, and the records after the core's position
/// that the replica had not applied yet (`suffix`, recovered from the same journal file).
pub(crate) struct WarmPromotion {
    pub(crate) store: EventStore,
    pub(crate) replica: ReplicaCore,
    pub(crate) suffix: Vec<EventEnvelope>,
    pub(crate) journal_path: PathBuf,
    pub(crate) stream_path: PathBuf,
}

/// The state which belongs exclusively to the follower thread. `applied_checkpoint` is separate
/// from `StreamReader::checkpoint()`: the reader moves its physical cursor before the core has
/// verified and committed that batch.
struct WarmReplica {
    reader: Option<StreamReader>,
    replica: Option<ReplicaCore>,
    applied_checkpoint: ReaderCheckpoint,
    journal_path: PathBuf,
    stream_path: PathBuf,
    snapshot_path: PathBuf,
    /// `None` when snapshots must not be written: an existing snapshot file was invalid, and it is
    /// preserved for diagnosis exactly as primary recovery preserves it.
    snapshots: Option<SnapshotWriter>,
    /// The stream's writer died while publishing, and has not repaired the stream since.
    stream_interrupted: bool,
}

/// Writes the journal-bound core snapshot from this follower's verified core, so the primary's
/// trading thread never has to.
struct SnapshotWriter {
    /// Read-only handle, used to bind each snapshot to the journal's identity and length.
    journal: File,
    /// A snapshot is due once the journal has grown, since the last one, by `growth` times that
    /// snapshot's size. Writing a snapshot costs about its size, so snapshots stay a fixed share
    /// of the replica's work however big the day's state gets. While they are written, a restart
    /// replays at most about `growth` snapshot sizes of journal past the last one, plus whatever
    /// this replica had not applied yet.
    growth: u64,
    /// The journal position of the last attempt, and the size that sets the next interval: the
    /// last snapshot's size, raised after a failed attempt.
    last_offset: u64,
    last_size: u64,
}

impl WarmReplica {
    fn open(
        journal_path: impl AsRef<Path>,
        stream_path: impl AsRef<Path>,
        snapshot_path: impl AsRef<Path>,
        snapshot_growth: u64,
    ) -> Result<Self, String> {
        let journal_path = journal_path.as_ref().to_path_buf();
        let stream_path = stream_path.as_ref().to_path_buf();
        let snapshot_path = snapshot_path.as_ref().to_path_buf();
        let snapshot_size = std::fs::metadata(&snapshot_path).map_or(0, |metadata| metadata.len());

        // The last value is the size of the snapshot the replica starts from: none here.
        let from_start =
            || -> Result<(ReplicaCore, StreamReader, ReaderCheckpoint, u64), String> {
                let reader = StreamReader::open(&journal_path, &stream_path, None)
                    .map_err(|error| format!("could not open committed stream: {error}"))?;
                let checkpoint = reader.checkpoint();
                Ok((ReplicaCore::new(), reader, checkpoint, 0))
            };

        let mut snapshot_is_safe_to_replace = true;
        // The offset of a snapshot found ahead of the stream, which stays the restart point.
        let mut ahead = None;
        let mut invalid_snapshot = |error: String| {
            eprintln!("ignoring snapshot and rebuilding warm replica from sequence 1: {error}");
            snapshot_is_safe_to_replace = false;
        };
        let (replica, reader, applied_checkpoint, start_size) =
            match snapshot::load(&snapshot_path, &journal_path) {
                Ok(Some(loaded)) => {
                    let checkpoint = StreamReader::checkpoint_from_parts(
                        loaded.boundary.journal_id,
                        loaded.boundary.next_event_sequence,
                        loaded.boundary.byte_offset,
                    );
                    match ExchangeCore::from_snapshot(loaded.core) {
                        Ok(core) => match StreamReader::open(
                            &journal_path,
                            &stream_path,
                            Some(checkpoint.clone()),
                        ) {
                            Ok(reader) => {
                                match ReplicaCore::from_snapshot(core, checkpoint.next_sequence) {
                                    Ok(replica) => (replica, reader, checkpoint, snapshot_size),
                                    Err(error) => {
                                        invalid_snapshot(format!("{error:?}"));
                                        from_start()?
                                    }
                                }
                            }
                            Err(error) => {
                                // A stale or malformed snapshot boundary is an optimization failure,
                                // not authority to invent a new exchange. Starting from the stream's
                                // validated beginning is safe; a bad stream still makes that fail.
                                // A snapshot at a command boundary of the journal that the stream
                                // cannot open is only ahead of it: a stream that a power loss left
                                // behind, or one whose replicated primary holds records back for
                                // its replica. It stays valid, and the restart point.
                                if is_journal_boundary(&journal_path, &checkpoint) {
                                    eprintln!(
                                        "snapshot ahead of the stream; rebuilding from sequence 1"
                                    );
                                    ahead = Some(checkpoint.byte_offset());
                                } else {
                                    invalid_snapshot(error.to_string());
                                }
                                from_start()?
                            }
                        },
                        Err(error) => {
                            invalid_snapshot(error);
                            from_start()?
                        }
                    }
                }
                Ok(None) => from_start()?,
                Err(error) => {
                    invalid_snapshot(error);
                    from_start()?
                }
            };

        let snapshots = if snapshot_is_safe_to_replace {
            // Rebuilding behind a snapshot found ahead of the stream, the replica writes none
            // until it has passed it, so the restart point never moves back.
            let (last_offset, last_size) = match ahead {
                Some(offset) => (offset, snapshot_size),
                None => (applied_checkpoint.byte_offset(), start_size),
            };
            Some(SnapshotWriter {
                journal: File::open(&journal_path)
                    .map_err(|error| format!("could not open journal for snapshots: {error}"))?,
                growth: snapshot_growth,
                last_offset,
                last_size,
            })
        } else {
            eprintln!(
                "preserving the invalid snapshot for diagnosis; remove it and restart the warm replica to resume snapshots"
            );
            None
        };

        Ok(Self {
            // Every batch comes from the journal, never the mmap cache, on every opening path:
            // this core becomes snapshots the primary trusts on restart, so it must be exactly
            // what journal recovery would build.
            reader: Some(reader.journal_only()),
            replica: Some(replica),
            applied_checkpoint,
            journal_path,
            stream_path,
            snapshot_path,
            snapshots,
            stream_interrupted: false,
        })
    }

    /// After each applied command, writes the snapshot at exactly the applied checkpoint when one
    /// is due: right after an open, or once the journal has grown by `growth` times the last
    /// snapshot's size. The state and the journal position it matches are taken together. An open
    /// has just cleared the previous day, so that snapshot is at its smallest and a restart
    /// replays only the new day.
    ///
    /// A failed attempt keeps the previous snapshot. It costs about as much as a written one, so
    /// the next attempt waits for `growth` times as much journal as this one did: each failure
    /// multiplies the wait, and a persistent failure costs a few attempts a day, not every command
    /// (unless `growth` is 0). After a failed attempt at an open, the next usually comes at the
    /// next open.
    fn maybe_write_snapshot(&mut self, opened: bool) {
        let (Some(writer), Some(replica)) = (self.snapshots.as_mut(), self.replica.as_ref()) else {
            return;
        };
        let offset = self.applied_checkpoint.byte_offset();
        // Never behind the last snapshot: only a replica rebuilding past one found ahead of the
        // stream is ever there.
        if offset < writer.last_offset {
            return;
        }
        let grown = offset - writer.last_offset;
        if !opened && grown < writer.growth.saturating_mul(writer.last_size) {
            return;
        }
        writer.last_offset = offset;
        let started = Instant::now();
        let journal_id = self.applied_checkpoint.journal_id();
        // If the journal at this path was moved aside and replaced by another journal, a snapshot
        // of the old one would sit on top of the new journal's valid snapshot and be refused at
        // every restart.
        match File::open(&self.journal_path).and_then(|journal| journal_id_of(&journal)) {
            Ok(id) if id == journal_id => {}
            _ => {
                writer.last_size = writer.last_size.max(grown);
                eprintln!(
                    "not writing a snapshot: the journal path no longer names the journal this warm replica follows"
                );
                return;
            }
        }
        let boundary = SnapshotBoundary {
            journal_id,
            byte_offset: offset,
            next_event_sequence: self.applied_checkpoint.next_sequence,
        };
        match snapshot::write(
            &self.snapshot_path,
            &writer.journal,
            &self.stream_path,
            boundary,
            replica.snapshot(),
        ) {
            Ok(size) => {
                writer.last_size = size;
                println!(
                    "warm replica wrote a snapshot through event sequence {} in {} ms",
                    self.applied_checkpoint.next_sequence - 1,
                    started.elapsed().as_millis()
                );
            }
            Err(error) => {
                writer.last_size = writer.last_size.max(grown);
                eprintln!("snapshot checkpoint was not updated: {error}");
            }
        }
    }

    fn follow_once(&mut self) -> Result<bool, String> {
        let next = self
            .reader
            .as_mut()
            .ok_or("warm replica was already promoted")?
            .next_batch();
        // A writer killed while publishing leaves the stream unreadable until it restarts, but
        // every record it published is in the journal. Waiting keeps this replica promotable: the
        // promotion fences the writer and reads the rest from the journal, never the stream.
        let interrupted = matches!(&next, Err(error) if is_publication_interrupted(error));
        if interrupted != self.stream_interrupted {
            self.stream_interrupted = interrupted;
            if interrupted {
                eprintln!(
                    "the stream's writer stopped while publishing; waiting at event sequence {} until it restarts or a promotion",
                    self.applied_checkpoint.next_sequence
                );
            } else {
                eprintln!("the stream's writer repaired the stream; following again");
            }
        }
        if interrupted {
            return Ok(false);
        }
        let batch = next.map_err(|error| format!("committed stream read failed: {error}"))?;
        let Some(batch) = batch else {
            return Ok(false);
        };

        self.replica
            .as_mut()
            .ok_or("warm replica was already promoted")?
            .apply_batch(&batch)
            .map_err(replay_message)?;

        // Only a completed deterministic core transition advances the promotion boundary.
        self.applied_checkpoint = self
            .reader
            .as_ref()
            .expect("reader exists while follower is active")
            .checkpoint();
        let opened = matches!(
            batch.get(1).map(|envelope| &envelope.event),
            Some(ExchangeEvent::Output(
                ExchangeOutputEvent::MarketOpened { .. }
            ))
        );
        self.maybe_write_snapshot(opened);
        Ok(true)
    }

    fn catch_up(&mut self) -> Result<(), String> {
        while self.follow_once()? {}
        Ok(())
    }

    fn next_event_sequence(&self) -> Result<u64, String> {
        let sequence = self
            .replica
            .as_ref()
            .ok_or("warm replica was already promoted")?
            .next_event_sequence();
        if sequence != self.applied_checkpoint.next_sequence {
            return Err("warm replica core and applied checkpoint disagree".to_string());
        }
        Ok(sequence)
    }

    /// Fence first. A `WouldBlock` result leaves every field intact, so the caught-up warm can
    /// continue following the old primary. Once the lock succeeds, only the journal after the
    /// applied checkpoint is read; the primary factory replays it into this replica's core. Any
    /// error after the fence is terminal and fails closed.
    fn try_promote(&mut self) -> Result<WarmPromotion, PromotionFailure> {
        let checkpoint = self.applied_checkpoint.clone();
        let (Some(reader), Some(replica)) = (self.reader.as_ref(), self.replica.as_ref()) else {
            return Err(PromotionFailure::Fatal(
                "warm replica was already promoted".into(),
            ));
        };
        if replica.next_event_sequence() != checkpoint.next_sequence {
            return Err(PromotionFailure::Fatal(
                "warm replica core and applied checkpoint disagree".into(),
            ));
        }

        // Fence, prove identity, then recover — strictly in that order. Recovery can repair a torn
        // tail, which truncates the file; if the path now names another file, that repair would
        // destroy history this follower never read. `open_suffix` refuses a file other than the
        // one this replica read, a different journal, or one shorter than what the stream
        // published or this replica applied, while holding the lock and before it reads anything
        // after the header.
        let (store, suffix) = EventStore::open_suffix(
            &self.journal_path,
            checkpoint.journal_id(),
            checkpoint.byte_offset(),
            Some((reader.journal(), reader.published_end())),
        )
        .map_err(PromotionFailure::from_store)?;
        // The lock is held and the journal after the checkpoint has been recovered. The core moves
        // to the primary factory, which replays `suffix` into it before anything is written.
        self.reader.take();
        Ok(WarmPromotion {
            store,
            replica: self.replica.take().expect("checked above"),
            suffix,
            journal_path: self.journal_path.clone(),
            stream_path: self.stream_path.clone(),
        })
    }
}

fn replay_message(error: ReplayError) -> String {
    format!("deterministic replay failed: {error:?}")
}

#[derive(Debug)]
enum PromotionFailure {
    Fenced,
    Fatal(String),
}

impl PromotionFailure {
    fn from_store(error: EventStoreError) -> Self {
        match error {
            EventStoreError::Io(error) if error.kind() == io::ErrorKind::WouldBlock => Self::Fenced,
            error => Self::Fatal(format!("could not acquire or recover journal: {error}")),
        }
    }
}

enum PromotionReply {
    Promoted,
    Fenced,
    Failed(String),
}

struct PromotionRequest {
    respond_to: oneshot::Sender<PromotionReply>,
}

#[derive(Clone)]
struct ManagementState {
    available: Arc<AtomicBool>,
    next_event_sequence: Arc<AtomicU64>,
    stream_interrupted: Arc<AtomicBool>,
    promotion_requests: mpsc::Sender<PromotionRequest>,
}

#[derive(Serialize)]
struct WarmStatus {
    role: &'static str,
    next_event_sequence: u64,
    stream_interrupted: bool,
}

async fn health(State(state): State<ManagementState>) -> impl IntoResponse {
    if state.available.load(Ordering::Acquire) {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "warm replica unavailable")
    }
}

async fn status(State(state): State<ManagementState>) -> Response {
    if !state.available.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "warm replica unavailable").into_response();
    }
    (
        StatusCode::OK,
        Json(WarmStatus {
            role: "warm-replica",
            next_event_sequence: state.next_event_sequence.load(Ordering::Acquire),
            stream_interrupted: state.stream_interrupted.load(Ordering::Acquire),
        }),
    )
        .into_response()
}

async fn promote(State(state): State<ManagementState>) -> Response {
    if !state.available.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "warm replica unavailable").into_response();
    }
    let (respond_to, response) = oneshot::channel();
    if state
        .promotion_requests
        .send(PromotionRequest { respond_to })
        .is_err()
    {
        state.available.store(false, Ordering::Release);
        return (StatusCode::SERVICE_UNAVAILABLE, "warm replica unavailable").into_response();
    }
    match response.await {
        Ok(PromotionReply::Promoted) => (
            StatusCode::ACCEPTED,
            "promotion fenced the old writer; starting the primary runtime",
        )
            .into_response(),
        Ok(PromotionReply::Fenced) => (
            StatusCode::CONFLICT,
            "promotion refused: the current primary still owns the journal writer lock",
        )
            .into_response(),
        Ok(PromotionReply::Failed(error)) => {
            state.available.store(false, Ordering::Release);
            (StatusCode::SERVICE_UNAVAILABLE, error).into_response()
        }
        Err(_) => {
            state.available.store(false, Ordering::Release);
            (StatusCode::SERVICE_UNAVAILABLE, "warm replica unavailable").into_response()
        }
    }
}

fn follow(
    mut warm: WarmReplica,
    requests: mpsc::Receiver<PromotionRequest>,
    available: Arc<AtomicBool>,
    next_event_sequence: Arc<AtomicU64>,
    stream_interrupted: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    promotion_result: oneshot::Sender<Result<WarmPromotion, String>>,
) {
    loop {
        if stop.load(Ordering::Acquire) {
            available.store(false, Ordering::Release);
            return;
        }
        match requests.try_recv() {
            Ok(request) => match warm.try_promote() {
                Ok(promotion) => {
                    available.store(false, Ordering::Release);
                    if promotion_result.send(Ok(promotion)).is_ok() {
                        let _ = request.respond_to.send(PromotionReply::Promoted);
                    } else {
                        let _ = request.respond_to.send(PromotionReply::Failed(
                            "promotion coordinator stopped before hand-off".into(),
                        ));
                    }
                    return;
                }
                Err(PromotionFailure::Fenced) => {
                    let _ = request.respond_to.send(PromotionReply::Fenced);
                }
                Err(PromotionFailure::Fatal(error)) => {
                    available.store(false, Ordering::Release);
                    let _ = request
                        .respond_to
                        .send(PromotionReply::Failed(error.clone()));
                    let _ = promotion_result.send(Err(error));
                    return;
                }
            },
            Err(mpsc::TryRecvError::Disconnected) => {
                available.store(false, Ordering::Release);
                return;
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }

        let followed = warm.follow_once();
        stream_interrupted.store(warm.stream_interrupted, Ordering::Release);
        match followed {
            Ok(true) => match warm.next_event_sequence() {
                Ok(sequence) => next_event_sequence.store(sequence, Ordering::Release),
                Err(error) => {
                    available.store(false, Ordering::Release);
                    let _ = promotion_result.send(Err(error));
                    return;
                }
            },
            Ok(false) => thread::sleep(IDLE_POLL),
            Err(error) => {
                available.store(false, Ordering::Release);
                let _ = promotion_result.send(Err(error));
                return;
            }
        }
    }
}

/// Runs a local, read-only warm replica until an operator POSTs `/promote`. The returned hand-off
/// has already fenced the prior writer; `main` must immediately build the normal primary runtime
/// from it. This function never opens PostgreSQL or the customer-facing exchange routes. While it
/// follows, it writes a core snapshot right after each open, and whenever the journal has grown
/// by `snapshot_growth` times the last snapshot's size.
pub async fn run(args: &[String], snapshot_growth: u64) -> WarmResult<WarmPromotion> {
    if !(3..=4).contains(&args.len()) {
        return Err(
            invalid("usage: stock --warm-replica JOURNAL STREAM SNAPSHOT [LISTEN_ADDR]").into(),
        );
    }
    let address: SocketAddr = args
        .get(3)
        .map(String::as_str)
        .unwrap_or(DEFAULT_ADDR)
        .parse()?;
    if !address.ip().is_loopback() {
        return Err(invalid("warm-replica management listener must use a loopback address").into());
    }

    let mut warm = WarmReplica::open(&args[0], &args[1], &args[2], snapshot_growth)
        .map_err(io::Error::other)?;
    warm.catch_up().map_err(io::Error::other)?;
    let initial_sequence = warm.next_event_sequence().map_err(io::Error::other)?;

    let available = Arc::new(AtomicBool::new(true));
    let next_event_sequence = Arc::new(AtomicU64::new(initial_sequence));
    let stream_interrupted = Arc::new(AtomicBool::new(warm.stream_interrupted));
    let (request_tx, request_rx) = mpsc::channel();
    let (promotion_tx, mut promotion_rx) = oneshot::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let follower_available = Arc::clone(&available);
    let follower_sequence = Arc::clone(&next_event_sequence);
    let follower_interrupted = Arc::clone(&stream_interrupted);
    let follower_stop = Arc::clone(&stop);
    let follower = thread::Builder::new()
        .name("warm-replica-follower".into())
        .spawn(move || {
            follow(
                warm,
                request_rx,
                follower_available,
                follower_sequence,
                follower_interrupted,
                follower_stop,
                promotion_tx,
            )
        })?;

    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/promote", post(promote))
        .with_state(ManagementState {
            available,
            next_event_sequence,
            stream_interrupted,
            promotion_requests: request_tx,
        });
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!(
        "Warm replica is following committed exchange events on {}",
        listener.local_addr()?
    );

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .into_future();
    tokio::pin!(server);

    let promotion = tokio::select! {
        outcome = &mut promotion_rx => match outcome {
            Ok(outcome) => outcome,
            Err(_) => Err("warm replica follower stopped without a promotion result".to_string()),
        },
        outcome = &mut server => {
            stop.store(true, Ordering::Release);
            let _ = follower.join();
            outcome?;
            return Err(io::Error::other("warm management server stopped before promotion").into());
        },
    };
    stop.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    server.await?;
    let _ = follower.join();

    promotion.map_err(|error| io::Error::other(error).into())
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, net::TcpListener, os::unix::fs::FileExt, time::Duration};

    use super::*;
    use crate::{
        exchange::{
            event_store::{EventStore, EventStoreError, JOURNAL_HEADER_LEN, encode_record},
            event_stream::{
                DEFAULT_CAPACITY, StreamReader, StreamWriter, record_length, tests::Fixture,
            },
            runtime::{
                promote_replica, recover_runtime_with_stream,
                recover_runtime_with_stream_and_snapshot, replay_event_log,
            },
        },
        types::{
            exchange_event::{ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
            types::Order,
        },
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn order(id: &str, user: &str, side: &str, price: u64, quantity: u32) -> Order {
        Order::new(
            id.to_string(),
            user.to_string(),
            "AAPL".to_string(),
            side,
            price,
            quantity,
            None,
            1.0,
            0,
        )
        .unwrap()
    }

    fn runtime_for(fixture: &Fixture) -> crate::exchange::runtime::ExchangeRuntime {
        let (_tx, rx) = tokio::sync::mpsc::channel(32);
        recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap()
    }

    /// Orders are refused until a trading day opens, so a history that trades starts with this.
    fn open_market() -> ExchangeInputEvent {
        ExchangeInputEvent::MarketOpenRequested {
            trading_day: chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
        }
    }

    fn unused_address() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    async fn http_request(
        address: &str,
        method: &str,
        path: &str,
    ) -> Result<(u16, String), String> {
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .map_err(|error| error.to_string())?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|error| error.to_string())?;
        let response = String::from_utf8(response).map_err(|error| error.to_string())?;
        let (headers, body) = response
            .split_once("\r\n\r\n")
            .ok_or_else(|| "missing HTTP response body separator".to_string())?;
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .ok_or_else(|| "missing HTTP response status".to_string())?
            .parse()
            .map_err(|error| format!("invalid HTTP response status: {error}"))?;
        Ok((status, body.to_string()))
    }

    async fn wait_for_http(address: &str, path: &str) -> (u16, String) {
        for _ in 0..250 {
            if let Ok(response) = http_request(address, "GET", path).await {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("warm replica did not bind its management listener")
    }

    #[test]
    fn warm_replica_catches_up_then_follows_the_journal_without_writing() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        for input in [
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 1_000,
            },
            ExchangeInputEvent::SharesDepositRequested {
                user_id: "seller".into(),
                symbol: "AAPL".into(),
                quantity: 10,
            },
            ExchangeInputEvent::RiskLimitSetRequested {
                user_id: "buyer".into(),
                symbol: "AAPL".into(),
                max_daily_quantity: 5,
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell", "seller", "SELL", 10, 10),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy", "buyer", "BUY", 10, 5),
            },
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "sell".into(),
                user_id: "seller".into(),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("risk-rejected", "buyer", "BUY", 10, 1),
            },
        ] {
            primary.record_input_for_test(input).unwrap();
        }

        // A primary restart republishes history with an empty mmap cache. The warm reads every
        // batch from the durable journal and uses the stream only for the committed watermark.
        drop(primary);
        let mut primary = runtime_for(&fixture);
        let journal_before = std::fs::read(&fixture.log).unwrap();
        let stream_before = std::fs::read(&fixture.bus).unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
        assert_eq!(
            warm.next_event_sequence().unwrap(),
            primary.next_event_sequence()
        );
        assert_eq!(std::fs::read(&fixture.log).unwrap(), journal_before);
        assert_eq!(std::fs::read(&fixture.bus).unwrap(), stream_before);

        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 25,
            })
            .unwrap();
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
        assert_eq!(
            warm.next_event_sequence().unwrap(),
            primary.next_event_sequence()
        );
    }

    #[test]
    fn warm_replica_rejects_wrong_output_without_advancing_its_applied_state() {
        let mut replica = ReplicaCore::new();
        let before = replica.snapshot();
        let batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 100,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 99,
                }),
            },
        ];

        assert!(matches!(
            replica.apply_batch(&batch),
            Err(ReplayError::OutputMismatch { .. })
        ));
        assert_eq!(replica.snapshot(), before);
        assert_eq!(replica.next_event_sequence(), 1);
    }

    #[test]
    fn semantic_replay_failure_keeps_the_warm_promotion_checkpoint_at_the_last_good_batch() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let (mut store, mut writer) =
            fixture.start(crate::exchange::event_stream::DEFAULT_CAPACITY);
        // Snapshot after every command: a batch that fails replay must not produce one.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        let checkpoint_before = warm.applied_checkpoint.clone();
        let core_before = warm.replica.as_ref().unwrap().snapshot();
        let batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 100,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 99,
                }),
            },
        ];
        let record = encode_record(&batch).unwrap();
        store.append_record(&record).unwrap();
        writer.append(&record, 2).unwrap();

        assert!(warm.follow_once().is_err());
        assert!(!snapshot.exists());
        assert_eq!(warm.applied_checkpoint, checkpoint_before);
        assert_eq!(warm.replica.as_ref().unwrap().snapshot(), core_before);
    }

    #[test]
    fn warm_follows_and_promotes_from_the_journal_not_a_differing_valid_mmap_cache() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let journal_batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
        ];
        let cache_batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 20,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 20,
                }),
            },
        ];
        let journal_record = encode_record(&journal_batch).unwrap();
        let cache_record = encode_record(&cache_batch).unwrap();
        assert_eq!(journal_record.len(), cache_record.len());
        let (mut store, mut writer) =
            fixture.start(crate::exchange::event_stream::DEFAULT_CAPACITY);
        // The production path: a primary always leaves a startup snapshot, so the warm opens from
        // one. Write the empty exchange's snapshot at the journal's first record boundary.
        snapshot::write(
            &snapshot,
            store.file(),
            &fixture.bus,
            SnapshotBoundary {
                journal_id: store.journal_id(),
                byte_offset: JOURNAL_HEADER_LEN as u64,
                next_event_sequence: 1,
            },
            ExchangeCore::new().snapshot(),
        )
        .unwrap();
        // Caught up with the empty journal, the warm replica stands at the stream's committed end,
        // where a reader is offered the next record from the stream's cache window.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        store.append_record(&journal_record).unwrap();
        writer.append(&journal_record, 2).unwrap();
        drop(writer);
        drop(store);

        // This simulates an impossible-under-the-cooperative-protocol but still structurally
        // valid cache disagreement. The warm reads batches from the journal, so the cache never
        // reaches its core — which is what makes that core safe to snapshot, and to promote.
        let stream = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fixture.bus)
            .unwrap();
        stream.lock().unwrap();
        stream.write_all_at(&cache_record, 80).unwrap();
        stream.unlock().unwrap();
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            replay_event_log(&journal_batch).unwrap().snapshot()
        );
        assert_ne!(
            replay_event_log(&cache_batch).unwrap().snapshot(),
            replay_event_log(&journal_batch).unwrap().snapshot()
        );

        let WarmPromotion {
            store,
            replica,
            suffix,
            stream_path,
            ..
        } = warm.try_promote().unwrap();
        assert!(suffix.is_empty());
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let promoted = promote_replica(rx, store, replica, suffix, &stream_path, false).unwrap();
        assert_eq!(
            promoted.core_snapshot_for_test(),
            replay_event_log(&journal_batch).unwrap().snapshot()
        );
    }

    #[tokio::test]
    async fn loopback_promote_endpoint_hands_the_locked_journal_and_the_core_to_the_primary() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let batch = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".into(),
                    amount: 10,
                }),
            },
        ];
        let record = encode_record(&batch).unwrap();
        let (mut store, mut writer) =
            fixture.start(crate::exchange::event_stream::DEFAULT_CAPACITY);
        store.append_record(&record).unwrap();
        writer.append(&record, 2).unwrap();
        drop(writer);
        drop(store);

        let address = unused_address();
        let args = vec![
            fixture.log.to_string_lossy().into_owned(),
            fixture.bus.to_string_lossy().into_owned(),
            snapshot.to_string_lossy().into_owned(),
            address.clone(),
        ];
        let handoff = tokio::spawn(async move { run(&args, u64::MAX).await });
        let (status, body) = wait_for_http(&address, "/status").await;
        assert_eq!(status, 200);
        assert!(body.contains("\"next_event_sequence\":3"));

        let (status, body) = http_request(&address, "POST", "/promote").await.unwrap();
        assert_eq!(status, 202);
        assert!(body.contains("promotion fenced the old writer"));
        let WarmPromotion {
            store,
            replica,
            suffix,
            stream_path,
            ..
        } = handoff.await.unwrap().unwrap();
        assert!(matches!(
            EventStore::open(&fixture.log),
            Err(EventStoreError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));

        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let promoted = promote_replica(rx, store, replica, suffix, &stream_path, false).unwrap();
        assert_eq!(promoted.next_event_sequence(), 3);
        assert_eq!(
            promoted.core_snapshot_for_test(),
            replay_event_log(&batch).unwrap().snapshot()
        );
    }

    #[test]
    fn warm_replica_writes_snapshots_that_the_primary_restarts_from() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut primary =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        let startup_snapshot = std::fs::read(&snapshot).unwrap();
        for input in [
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 100,
            },
            ExchangeInputEvent::SharesDepositRequested {
                user_id: "seller".into(),
                symbol: "AAPL".into(),
                quantity: 5,
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell", "seller", "SELL", 10, 5),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy", "buyer", "BUY", 10, 3),
            },
            ExchangeInputEvent::MarketCloseRequested,
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 7,
            },
        ] {
            primary.record_input_for_test(input).unwrap();
        }
        // Trading never touched the snapshot: only the warm replica writes them now.
        assert_eq!(std::fs::read(&snapshot).unwrap(), startup_snapshot);

        // A snapshot after every command; the warm replica has applied seven of them when the
        // eighth arrives.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        warm.catch_up().unwrap();
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 9,
            })
            .unwrap();

        // The last snapshot is at exactly the warm's applied position then: before the eighth.
        let loaded = snapshot::load(&snapshot, &fixture.log).unwrap().unwrap();
        let last_command_envelopes = 2;
        assert_eq!(
            loaded.boundary.next_event_sequence,
            primary.next_event_sequence() - last_command_envelopes
        );
        let live = primary.core_snapshot_for_test();
        drop(primary);

        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let restarted =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        // The restart loaded the warm's snapshot, closed session included, and replayed only the
        // last command.
        assert_eq!(restarted.event_log().len(), last_command_envelopes as usize);
        assert_eq!(restarted.core_snapshot_for_test(), live);
    }

    #[test]
    fn warm_replica_writes_a_snapshot_right_after_each_open() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut primary =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        for input in [
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 100,
            },
            ExchangeInputEvent::SharesDepositRequested {
                user_id: "seller".into(),
                symbol: "AAPL".into(),
                quantity: 5,
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell", "seller", "SELL", 10, 5),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy", "buyer", "BUY", 10, 3),
            },
            ExchangeInputEvent::MarketCloseRequested,
            ExchangeInputEvent::MarketOpenRequested {
                trading_day: chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(),
            },
        ] {
            primary.record_input_for_test(input).unwrap();
        }

        // No journal growth makes a snapshot due at this growth factor, yet the second open left
        // one.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        let loaded = snapshot::load(&snapshot, &fixture.log).unwrap().unwrap();
        assert_eq!(
            loaded.boundary.next_event_sequence,
            primary.next_event_sequence()
        );

        // A restart from it replays nothing and holds the same, cleared state.
        let live = primary.core_snapshot_for_test();
        drop(primary);
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let restarted =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        assert!(restarted.event_log().is_empty());
        assert_eq!(restarted.core_snapshot_for_test(), live);
    }

    #[test]
    fn warm_replica_preserves_an_invalid_snapshot_and_writes_none() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        for amount in [10, 20, 30] {
            primary
                .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount,
                })
                .unwrap();
        }
        let corrupt = b"not an exchange snapshot";
        std::fs::write(&snapshot, corrupt).unwrap();

        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        warm.catch_up().unwrap();

        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
        assert_eq!(std::fs::read(&snapshot).unwrap(), corrupt);
    }

    /// A snapshot at a command boundary of the journal, beyond what the stream has published yet,
    /// is ahead, not invalid: a power loss left the stream's header behind, or a replicated
    /// primary holds records back. The warm replica rebuilds from sequence 1, writes no snapshot
    /// behind it, and keeps writing snapshots after it.
    #[test]
    fn a_snapshot_ahead_of_the_stream_is_kept_and_snapshots_continue() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let deposit = |primary: &mut crate::exchange::runtime::ExchangeRuntime, amount| {
            primary
                .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount,
                })
                .unwrap();
        };
        let mut primary = runtime_for(&fixture);
        deposit(&mut primary, 10);
        deposit(&mut primary, 20);
        let mut first = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        first.catch_up().unwrap();
        drop(first);
        // A record follows the snapshot, so its checkpoint is where a complete record starts.
        deposit(&mut primary, 30);
        drop(primary);
        let ahead = snapshot::load(&snapshot, &fixture.log)
            .unwrap()
            .unwrap()
            .boundary
            .byte_offset;

        // The stream comes back publishing only the first deposit.
        let journal = std::fs::read(&fixture.log).unwrap();
        let first_end = JOURNAL_HEADER_LEN + record_length(&journal[JOURNAL_HEADER_LEN..]).unwrap();
        std::fs::remove_file(&fixture.bus).unwrap();
        let file = File::open(&fixture.log).unwrap();
        let published = (first_end as u64, 2);
        drop(StreamWriter::open_at(&fixture.bus, &file, 6, published, DEFAULT_CAPACITY).unwrap());

        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        assert_eq!(warm.applied_checkpoint.next_sequence, 1);
        assert!(warm.snapshots.is_some());
        // A snapshot is due after every command here, yet none is written behind the one found.
        warm.catch_up().unwrap();
        assert_eq!(warm.applied_checkpoint.next_sequence, 3);
        let kept = snapshot::load(&snapshot, &fixture.log).unwrap().unwrap();
        assert_eq!(kept.boundary.byte_offset, ahead);

        // The primary restarts and publishes everything: past the snapshot, snapshots continue.
        let mut primary = runtime_for(&fixture);
        deposit(&mut primary, 40);
        warm.catch_up().unwrap();
        let latest = snapshot::load(&snapshot, &fixture.log).unwrap().unwrap();
        assert_eq!(
            latest.boundary.byte_offset,
            std::fs::metadata(&fixture.log).unwrap().len()
        );
    }

    #[test]
    fn warm_replica_uses_a_valid_primary_snapshot_then_follows_later_batches() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut primary =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        // A first warm replica writes a snapshot after that command, past the empty startup one.
        let mut first = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 0).unwrap();
        first.catch_up().unwrap();
        drop(first);
        let loaded = snapshot::load(&snapshot, &fixture.log).unwrap().unwrap();
        assert_eq!(
            loaded.boundary.next_event_sequence,
            primary.next_event_sequence()
        );

        // A second one starts from that snapshot, not from sequence 1, and keeps following.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        assert_eq!(
            warm.applied_checkpoint.next_sequence,
            primary.next_event_sequence()
        );
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 20,
            })
            .unwrap();
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
    }

    #[test]
    fn promotion_is_refused_while_the_primary_still_owns_the_journal() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        let before = warm.next_event_sequence().unwrap();

        assert!(matches!(warm.try_promote(), Err(PromotionFailure::Fenced)));
        assert_eq!(warm.next_event_sequence().unwrap(), before);

        // The old primary still works, and the same warm continues to follow it after the failed
        // manual request. This proves a failed fence does not consume the follower state.
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 20,
            })
            .unwrap();
        warm.catch_up().unwrap();
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
    }

    #[test]
    fn promotion_refuses_a_swapped_journal_without_repairing_the_replacement() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        drop(primary);

        // An operator moves the followed journal aside, and a different journal — torn in the
        // middle of its only record — appears at the same path. The follower still holds the
        // original file open, so promotion sees that the path now names another file.
        std::fs::rename(&fixture.log, fixture.dir.join("events.moved")).unwrap();
        {
            let (mut replacement, _) = EventStore::open(&fixture.log).unwrap();
            replacement
                .append(&[
                    EventEnvelope {
                        seq_num: 1,
                        event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                            user_id: "someone-else".into(),
                            amount: 99,
                        }),
                    },
                    EventEnvelope {
                        seq_num: 2,
                        event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                            user_id: "someone-else".into(),
                            amount: 99,
                        }),
                    },
                ])
                .unwrap();
        }
        let torn_len = std::fs::metadata(&fixture.log).unwrap().len() - 4;
        OpenOptions::new()
            .write(true)
            .open(&fixture.log)
            .unwrap()
            .set_len(torn_len)
            .unwrap();
        let replacement_before = std::fs::read(&fixture.log).unwrap();

        assert!(matches!(
            warm.try_promote(),
            Err(PromotionFailure::Fatal(message)) if message.contains("different file")
        ));

        // The refusal must come before any recovery read or torn-tail repair. A journal that is
        // not the one this follower read is left byte-for-byte as the operator left it.
        assert_eq!(std::fs::read(&fixture.log).unwrap(), replacement_before);
    }

    #[test]
    fn promotion_refuses_an_older_copy_of_the_journal_it_followed() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        let deposit = |amount| ExchangeInputEvent::FundsDepositRequested {
            user_id: "buyer".into(),
            amount,
        };
        primary.record_input_for_test(deposit(10)).unwrap();
        let older = std::fs::read(&fixture.log).unwrap();
        primary.record_input_for_test(deposit(20)).unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        drop(primary);
        let current = std::fs::read(&fixture.log).unwrap();

        // A copy taken after the first deposit is put back in place. It has the same id. Renamed
        // into place, it is not the file this follower read; written over that file, it is
        // shorter than what the follower applied. Either way it lost the second deposit, and it
        // is refused untouched.
        let backup = fixture.dir.join("events.backup");
        std::fs::write(&backup, &older).unwrap();
        std::fs::rename(&backup, &fixture.log).unwrap();
        assert!(matches!(
            warm.try_promote(),
            Err(PromotionFailure::Fatal(message)) if message.contains("different file")
        ));
        assert_eq!(std::fs::read(&fixture.log).unwrap(), older);

        // A new warm replica cannot even follow the copy: the stream published more than it holds.
        assert!(WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).is_err());

        std::fs::write(&fixture.log, &current).unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        std::fs::write(&fixture.log, &older).unwrap();
        assert!(matches!(
            warm.try_promote(),
            Err(PromotionFailure::Fatal(message)) if message.contains("shorter than")
        ));
        assert_eq!(std::fs::read(&fixture.log).unwrap(), older);
    }

    /// A warm replica that lags has applied less than the stream published. An older copy written
    /// over the followed file can then reach what the replica applied and still lack records that
    /// were published, and that readers may already have consumed: promotion refuses it untouched.
    #[test]
    fn promotion_refuses_a_journal_shorter_than_what_the_stream_published() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        let deposit = |amount| ExchangeInputEvent::FundsDepositRequested {
            user_id: "buyer".into(),
            amount,
        };
        primary.record_input_for_test(deposit(10)).unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        primary.record_input_for_test(deposit(20)).unwrap();
        let older = std::fs::read(&fixture.log).unwrap();
        primary.record_input_for_test(deposit(30)).unwrap();
        drop(primary);

        // The replica applied only the first deposit; the copy holds two of the three published.
        std::fs::write(&fixture.log, &older).unwrap();
        assert!(matches!(
            warm.try_promote(),
            Err(PromotionFailure::Fatal(message)) if message.contains("shorter than")
        ));
        assert_eq!(std::fs::read(&fixture.log).unwrap(), older);
    }

    /// The file is the one the warm replica followed, but it now holds another journal: the id
    /// check on the promotion path refuses it untouched.
    #[test]
    fn promotion_refuses_another_journal_written_over_the_followed_file() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let other = fixture.dir.join("other.log");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        drop(primary);
        {
            let (mut store, _) = EventStore::open(&other).unwrap();
            for seq in [1, 3] {
                store
                    .append(&[
                        EventEnvelope {
                            seq_num: seq,
                            event: ExchangeEvent::Input(
                                ExchangeInputEvent::FundsDepositRequested {
                                    user_id: "someone-else".into(),
                                    amount: 99,
                                },
                            ),
                        },
                        EventEnvelope {
                            seq_num: seq + 1,
                            event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                                user_id: "someone-else".into(),
                                amount: 99,
                            }),
                        },
                    ])
                    .unwrap();
            }
        }
        let replacement = std::fs::read(&other).unwrap();
        std::fs::write(&fixture.log, &replacement).unwrap();

        assert!(matches!(
            warm.try_promote(),
            Err(PromotionFailure::Fatal(message)) if message.contains("identity changed")
        ));
        assert_eq!(std::fs::read(&fixture.log).unwrap(), replacement);
    }

    /// A snapshot that cannot be written costs about as much as one that is, so failed attempts
    /// back off: each waits for the growth factor times as much journal as the one before, rather
    /// than coming after every command. Both ways an attempt fails: the write itself, and a
    /// journal path that no longer names the journal the replica follows.
    #[test]
    fn failed_snapshots_are_retried_less_and_less_often() {
        for journal_replaced in [false, true] {
            let fixture = Fixture::new();
            let snapshot = if journal_replaced {
                fixture.dir.join("events.snapshot")
            } else {
                // Its directory does not exist, so every write fails.
                fixture.dir.join("missing").join("events.snapshot")
            };
            let mut primary = runtime_for(&fixture);
            let growth = 4;
            let mut warm =
                WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, growth).unwrap();
            if journal_replaced {
                // The primary and the replica keep their own handles on the journal they use.
                std::fs::rename(&fixture.log, fixture.dir.join("events.moved")).unwrap();
                drop(EventStore::open(&fixture.log).unwrap());
            }
            let mut attempts = vec![warm.snapshots.as_ref().unwrap().last_offset];
            for user in 0..200 {
                primary
                    .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                        user_id: format!("user-{user}"),
                        amount: 1,
                    })
                    .unwrap();
                warm.catch_up().unwrap();
                let attempted = warm.snapshots.as_ref().unwrap().last_offset;
                if attempts.last() != Some(&attempted) {
                    attempts.push(attempted);
                }
            }
            assert!(!snapshot.exists());
            assert!(
                (4..10).contains(&attempts.len()),
                "{journal_replaced}: {attempts:?}"
            );
            assert!(
                attempts
                    .windows(3)
                    .all(|gaps| gaps[2] - gaps[1] >= growth * (gaps[1] - gaps[0])),
                "{journal_replaced}: {attempts:?}"
            );
        }
    }

    /// The growth rule, command by command: a snapshot is written exactly when the journal has
    /// grown by the growth factor times the last snapshot's size. Every deposit adds a user, so
    /// the state, and with it the gap between snapshots, keeps growing.
    #[test]
    fn warm_replica_snapshots_once_the_journal_grows_by_the_factor_times_the_last_snapshot() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut primary =
            recover_runtime_with_stream_and_snapshot(rx, &fixture.log, &fixture.bus, &snapshot)
                .unwrap();
        let growth = 3;
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, growth).unwrap();
        let size = || std::fs::metadata(&snapshot).unwrap().len();
        let boundary = || {
            snapshot::load(&snapshot, &fixture.log)
                .unwrap()
                .unwrap()
                .boundary
                .byte_offset
        };
        let (mut last_offset, mut last_size) = (boundary(), size());
        let mut gaps = Vec::new();
        for user in 0..200 {
            primary
                .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                    user_id: format!("user-{user}"),
                    amount: 1,
                })
                .unwrap();
            warm.catch_up().unwrap();
            let offset = std::fs::metadata(&fixture.log).unwrap().len();
            let due = offset - last_offset >= growth * last_size;
            assert_eq!(boundary() == offset, due, "after deposit {user}");
            if due {
                gaps.push(offset - last_offset);
                (last_offset, last_size) = (offset, size());
            }
        }
        assert!(gaps.len() >= 3, "{gaps:?}");
        assert!(gaps.windows(2).all(|pair| pair[0] < pair[1]), "{gaps:?}");
    }

    #[test]
    fn promotion_reconciles_a_durable_batch_hidden_from_mmap_then_continues_sequences() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        let before_hidden_tail = warm.replica.as_ref().unwrap().snapshot();

        primary.fail_stream_publication_for_test();
        assert!(
            primary
                .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 20,
                })
                .is_err()
        );
        let expected_after_hidden_tail = primary.core_snapshot_for_test();
        assert!(!warm.follow_once().unwrap());
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            before_hidden_tail
        );
        assert_eq!(warm.next_event_sequence().unwrap(), 3);

        drop(primary);
        let WarmPromotion {
            store,
            replica,
            suffix,
            stream_path,
            ..
        } = warm.try_promote().unwrap();
        // Only the durable batch the warm replica never saw is read and replayed.
        assert_eq!(suffix.len(), 2);
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut promoted =
            promote_replica(rx, store, replica, suffix, &stream_path, false).unwrap();
        assert_eq!(
            promoted.core_snapshot_for_test(),
            expected_after_hidden_tail
        );
        assert_eq!(promoted.next_event_sequence(), 5);

        promoted
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 5,
            })
            .unwrap();
        assert_eq!(promoted.next_event_sequence(), 7);
        let mut reader = StreamReader::open(&fixture.log, &fixture.bus, None).unwrap();
        for expected_amount in [10, 20, 5] {
            let batch = reader.next_batch().unwrap().unwrap();
            assert!(matches!(
                &batch[1].event,
                ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited { amount, .. })
                    if *amount == expected_amount
            ));
        }
        assert!(reader.next_batch().unwrap().is_none());
    }

    /// What a writer killed while publishing leaves behind: the record is in the journal and in
    /// the stream, but the ready marker it cleared is never set again.
    fn interrupt_stream_publication(fixture: &Fixture) {
        OpenOptions::new()
            .write(true)
            .open(&fixture.bus)
            .unwrap()
            .write_all_at(&0u64.to_le_bytes(), 72)
            .unwrap();
    }

    /// The replica process on the second machine can be killed while it publishes, and the
    /// failover runbook kills it right before the promotion. The warm replica then waits where
    /// it is, and the promotion reads the rest from the journal and repairs the stream.
    #[test]
    fn an_interrupted_publication_leaves_the_warm_replica_waiting_and_promotable() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 20,
            })
            .unwrap();
        let expected = primary.core_snapshot_for_test();
        drop(primary);
        interrupt_stream_publication(&fixture);

        assert!(!warm.follow_once().unwrap());
        assert!(!warm.follow_once().unwrap());
        assert!(warm.stream_interrupted);
        assert_eq!(warm.next_event_sequence().unwrap(), 3);

        let WarmPromotion {
            store,
            replica,
            suffix,
            stream_path,
            ..
        } = warm.try_promote().unwrap();
        assert_eq!(suffix.len(), 2);
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut promoted =
            promote_replica(rx, store, replica, suffix, &stream_path, false).unwrap();
        assert_eq!(promoted.core_snapshot_for_test(), expected);
        promoted
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 5,
            })
            .unwrap();
        assert_eq!(promoted.next_event_sequence(), 7);
        let mut reader = StreamReader::open(&fixture.log, &fixture.bus, None).unwrap();
        for expected_amount in [10, 20, 5] {
            let batch = reader.next_batch().unwrap().unwrap();
            assert!(matches!(
                &batch[1].event,
                ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited { amount, .. })
                    if *amount == expected_amount
            ));
        }
        assert!(reader.next_batch().unwrap().is_none());
    }

    /// A writer that restarts repairs the stream, and the waiting warm replica follows it again.
    #[test]
    fn a_warm_replica_follows_again_once_a_restarted_writer_repairs_the_stream() {
        let fixture = Fixture::new();
        let snapshot = fixture.dir.join("events.snapshot");
        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            })
            .unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
        warm.catch_up().unwrap();
        drop(primary);
        interrupt_stream_publication(&fixture);
        assert!(!warm.follow_once().unwrap());
        assert!(warm.stream_interrupted);

        let mut primary = runtime_for(&fixture);
        primary
            .record_input_for_test(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 20,
            })
            .unwrap();
        warm.catch_up().unwrap();
        assert!(!warm.stream_interrupted);
        assert_eq!(warm.next_event_sequence().unwrap(), 5);
        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
    }
}
