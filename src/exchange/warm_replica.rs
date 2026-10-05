//! Same-host warm replica.
//!
//! This process deterministically rebuilds the exchange core from complete committed batches but
//! deliberately owns no journal writer, mmap writer, command queue, customer HTTP routes, or
//! database connection. Its only listener is a loopback-only operator control endpoint. A manual
//! promotion first acquires the journal's exclusive writer lock, then fully revalidates the
//! authoritative journal before creating a primary runtime.
//!
//! It is also the exchange's snapshot writer. It reads every batch from the durable journal (the
//! mmap stream supplies only the committed watermark), checks each one by deterministic replay, and
//! every `snapshot_every` commands, and right after each open, writes the journal-bound core
//! snapshot the primary loads on restart. The primary's trading thread therefore never stops to
//! serialize its whole state.

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
    event_store::{EventStore, EventStoreError},
    event_stream::{ReaderCheckpoint, StreamReader},
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

/// The fenced ownership hand-off returned to `main`, which creates the normal primary runtime.
/// It contains a complete writer-locked journal recovery, not the warm core: promotion rebuilds
/// from the whole journal. (Reusing the warm's own snapshot to promote faster is future work.)
pub(crate) struct WarmPromotion {
    pub(crate) store: EventStore,
    pub(crate) recovered: Vec<EventEnvelope>,
    pub(crate) journal_path: PathBuf,
    pub(crate) stream_path: PathBuf,
    pub(crate) snapshot_path: PathBuf,
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
}

/// Writes the journal-bound core snapshot from this follower's verified core, so the primary's
/// trading thread never has to.
struct SnapshotWriter {
    /// Read-only handle, used to bind each snapshot to the journal's identity and length.
    journal: File,
    every_commands: u64,
    commands_since: u64,
}

impl WarmReplica {
    fn open(
        journal_path: impl AsRef<Path>,
        stream_path: impl AsRef<Path>,
        snapshot_path: impl AsRef<Path>,
        snapshot_every: u64,
    ) -> Result<Self, String> {
        let journal_path = journal_path.as_ref().to_path_buf();
        let stream_path = stream_path.as_ref().to_path_buf();
        let snapshot_path = snapshot_path.as_ref().to_path_buf();

        let from_start = || -> Result<(ReplicaCore, StreamReader, ReaderCheckpoint), String> {
            let reader = StreamReader::open(&journal_path, &stream_path, None)
                .map_err(|error| format!("could not open committed stream: {error}"))?;
            let checkpoint = reader.checkpoint();
            Ok((ReplicaCore::new(), reader, checkpoint))
        };

        let mut snapshot_is_safe_to_replace = true;
        let mut invalid_snapshot = |error: String| {
            eprintln!("ignoring snapshot and rebuilding warm replica from sequence 1: {error}");
            snapshot_is_safe_to_replace = false;
        };
        let (replica, reader, applied_checkpoint) =
            match snapshot::load(&snapshot_path, &journal_path) {
                Ok(Some(loaded)) => {
                    let checkpoint = StreamReader::checkpoint_from_parts(
                        loaded.boundary.journal_device,
                        loaded.boundary.journal_inode,
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
                                    Ok(replica) => (replica, reader, checkpoint),
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
                                invalid_snapshot(error.to_string());
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
            Some(SnapshotWriter {
                journal: File::open(&journal_path)
                    .map_err(|error| format!("could not open journal for snapshots: {error}"))?,
                every_commands: snapshot_every,
                commands_since: 0,
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
        })
    }

    /// Counts one applied command and, every `every_commands` or right after an open, writes the
    /// snapshot at exactly the applied checkpoint: the state and the journal position it matches
    /// are taken together. An open has just cleared the previous day, so the snapshot is at its
    /// smallest and a restart replays only the new day. A failed write keeps the previous snapshot
    /// and is retried after another full interval.
    fn maybe_write_snapshot(&mut self, opened: bool) {
        let (Some(writer), Some(replica)) = (self.snapshots.as_mut(), self.replica.as_ref()) else {
            return;
        };
        writer.commands_since += 1;
        if !opened && writer.commands_since < writer.every_commands {
            return;
        }
        writer.commands_since = 0;
        let started = Instant::now();
        let (journal_device, journal_inode) = self.applied_checkpoint.journal_identity();
        use std::os::unix::fs::MetadataExt;
        // If the journal at this path was moved aside and replaced, a snapshot of the old one
        // would sit on top of the new journal's valid snapshot and be refused at every restart.
        match std::fs::metadata(&self.journal_path) {
            Ok(meta) if meta.dev() == journal_device && meta.ino() == journal_inode => {}
            _ => {
                eprintln!(
                    "not writing a snapshot: the journal path no longer names the journal this warm replica follows"
                );
                return;
            }
        }
        let boundary = SnapshotBoundary {
            journal_device,
            journal_inode,
            byte_offset: self.applied_checkpoint.byte_offset(),
            next_event_sequence: self.applied_checkpoint.next_sequence,
        };
        match snapshot::write(
            &self.snapshot_path,
            &writer.journal,
            &self.stream_path,
            boundary,
            replica.snapshot(),
        ) {
            Ok(()) => println!(
                "warm replica wrote a snapshot through event sequence {} in {} ms",
                self.applied_checkpoint.next_sequence - 1,
                started.elapsed().as_millis()
            ),
            Err(error) => eprintln!("snapshot checkpoint was not updated: {error}"),
        }
    }

    fn follow_once(&mut self) -> Result<bool, String> {
        let batch = self
            .reader
            .as_mut()
            .ok_or("warm replica was already promoted")?
            .next_batch()
            .map_err(|error| format!("committed stream read failed: {error}"))?;
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
    /// continue following the old primary. Once the lock succeeds, the entire durable journal is
    /// re-read and later deterministically replayed by the primary factory. Any error after the
    /// fence is terminal and fails closed.
    fn try_promote(&mut self) -> Result<WarmPromotion, PromotionFailure> {
        let checkpoint = self.applied_checkpoint.clone();
        let replica = self
            .replica
            .as_ref()
            .ok_or_else(|| PromotionFailure::Fatal("warm replica was already promoted".into()))?;
        if replica.next_event_sequence() != checkpoint.next_sequence {
            return Err(PromotionFailure::Fatal(
                "warm replica core and applied checkpoint disagree".into(),
            ));
        }

        // Fence, prove identity, then recover — strictly in that order. Recovery can repair a torn
        // tail, which truncates the file; if the path now names a different journal, that repair
        // would destroy history this follower never read. `open_existing_matching` refuses a
        // mismatched file while holding the lock and before its first read, leaving it untouched.
        let (device, inode) = checkpoint.journal_identity();
        let (store, recovered) =
            EventStore::open_existing_matching(&self.journal_path, device, inode)
                .map_err(PromotionFailure::from_store)?;
        // The lock is held and the full journal has been physically recovered. Drop the reader and
        // warm core: `ExchangeRuntime` will rebuild solely from `recovered` before it writes.
        self.reader.take();
        self.replica.take();
        Ok(WarmPromotion {
            store,
            recovered,
            journal_path: self.journal_path.clone(),
            stream_path: self.stream_path.clone(),
            snapshot_path: self.snapshot_path.clone(),
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
    promotion_requests: mpsc::Sender<PromotionRequest>,
}

#[derive(Serialize)]
struct WarmStatus {
    role: &'static str,
    next_event_sequence: u64,
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

        match warm.follow_once() {
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
/// follows, it writes a core snapshot every `snapshot_every` commands and right after each open.
pub async fn run(args: &[String], snapshot_every: u64) -> WarmResult<WarmPromotion> {
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

    let mut warm = WarmReplica::open(&args[0], &args[1], &args[2], snapshot_every)
        .map_err(io::Error::other)?;
    warm.catch_up().map_err(io::Error::other)?;
    let initial_sequence = warm.next_event_sequence().map_err(io::Error::other)?;

    let available = Arc::new(AtomicBool::new(true));
    let next_event_sequence = Arc::new(AtomicU64::new(initial_sequence));
    let (request_tx, request_rx) = mpsc::channel();
    let (promotion_tx, mut promotion_rx) = oneshot::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let follower_available = Arc::clone(&available);
    let follower_sequence = Arc::clone(&next_event_sequence);
    let follower_stop = Arc::clone(&stop);
    let follower = thread::Builder::new()
        .name("warm-replica-follower".into())
        .spawn(move || {
            follow(
                warm,
                request_rx,
                follower_available,
                follower_sequence,
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
    use std::{
        fs::OpenOptions,
        net::TcpListener,
        os::unix::fs::{FileExt, MetadataExt},
        time::Duration,
    };

    use super::*;
    use crate::{
        exchange::{
            event_store::{EventStore, EventStoreError, encode_record},
            event_stream::{StreamReader, tests::Fixture},
            runtime::{
                promote_replica_with_stream_and_snapshot, recover_runtime_with_stream,
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
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 1).unwrap();
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
    fn warm_follows_and_promotion_rebuilds_from_the_journal_not_a_differing_valid_mmap_cache() {
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
        let journal = std::fs::metadata(&fixture.log).unwrap();
        snapshot::write(
            &snapshot,
            store.file(),
            &fixture.bus,
            SnapshotBoundary {
                journal_device: journal.dev(),
                journal_inode: journal.ino(),
                byte_offset: 8,
                next_event_sequence: 1,
            },
            ExchangeCore::new().snapshot(),
        )
        .unwrap();
        store.append_record(&journal_record).unwrap();
        writer.append(&journal_record, 2).unwrap();
        drop(writer);
        drop(store);

        // This simulates an impossible-under-the-cooperative-protocol but still structurally
        // valid cache disagreement. The warm reads batches from the journal, so the cache never
        // reaches its core — which is what makes that core safe to snapshot — and a promotion
        // rebuilds from the journal regardless.
        let stream = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fixture.bus)
            .unwrap();
        stream.lock().unwrap();
        stream.write_all_at(&cache_record, 80).unwrap();
        stream.unlock().unwrap();
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, u64::MAX).unwrap();
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
            recovered,
            journal_path,
            stream_path,
            snapshot_path,
        } = warm.try_promote().unwrap();
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let promoted = promote_replica_with_stream_and_snapshot(
            rx,
            store,
            recovered,
            &journal_path,
            &stream_path,
            &snapshot_path,
        )
        .unwrap();
        assert_eq!(
            promoted.core_snapshot_for_test(),
            replay_event_log(&journal_batch).unwrap().snapshot()
        );
    }

    #[tokio::test]
    async fn loopback_promote_endpoint_hands_a_writer_locked_full_journal_to_the_primary_factory() {
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
            recovered,
            journal_path,
            stream_path,
            snapshot_path,
        } = handoff.await.unwrap().unwrap();
        let journal = std::fs::metadata(&fixture.log).unwrap();
        assert!(matches!(
            EventStore::open_existing_matching(&fixture.log, journal.dev(), journal.ino()),
            Err(EventStoreError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));

        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let promoted = promote_replica_with_stream_and_snapshot(
            rx,
            store,
            recovered,
            &journal_path,
            &stream_path,
            &snapshot_path,
        )
        .unwrap();
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
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 9,
            },
        ] {
            primary.record_input_for_test(input).unwrap();
        }
        // Trading never touched the snapshot: only the warm replica writes them now.
        assert_eq!(std::fs::read(&snapshot).unwrap(), startup_snapshot);

        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 2).unwrap();
        warm.catch_up().unwrap();

        // Eight commands: a snapshot right after the open, then one every two commands after it,
        // so the last was written after the seventh, at exactly the warm's applied position then.
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

        // Seven commands against an interval of 1,000, yet the second open left a snapshot.
        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 1_000).unwrap();
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

        let mut warm = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 1).unwrap();
        warm.catch_up().unwrap();

        assert_eq!(
            warm.replica.as_ref().unwrap().snapshot(),
            primary.core_snapshot_for_test()
        );
        assert_eq!(std::fs::read(&snapshot).unwrap(), corrupt);
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
        let mut first = WarmReplica::open(&fixture.log, &fixture.bus, &snapshot, 1).unwrap();
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
        // original file open by inode, so only its checkpoint identity can tell the two apart.
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
            Err(PromotionFailure::Fatal(_))
        ));

        // The refusal must come before any recovery read or torn-tail repair. A journal that is
        // not the one this follower read is left byte-for-byte as the operator left it.
        assert_eq!(std::fs::read(&fixture.log).unwrap(), replacement_before);
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
            recovered,
            journal_path,
            stream_path,
            snapshot_path,
        } = warm.try_promote().unwrap();
        assert_eq!(recovered.len(), 4);
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut promoted = promote_replica_with_stream_and_snapshot(
            rx,
            store,
            recovered,
            &journal_path,
            &stream_path,
            &snapshot_path,
        )
        .unwrap();
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
}
