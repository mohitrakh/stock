use std::{net::SocketAddr, path::Path, sync::Arc};

use crate::{
    exchange::{
        core::{
            CoreError, ExchangeCore, PreparedAddOrder, PreparedCancelOrder, PreparedExpiry,
            SessionError,
        },
        event_store::{EventStore, EventStoreError, MAX_RECORD_LEN, encode_record},
        event_stream::{DEFAULT_CAPACITY, StreamWriter, already_published},
        replication::Replication,
        snapshot::{self, SnapshotBoundary},
    },
    types::{
        exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::{ExchangeCommand, OrderView, SessionView},
    },
};

/// The rejection reason journaled for an order sent while the market is closed.
pub(crate) const MARKET_CLOSED: &str = "MarketClosed";

pub struct ExchangeRuntime {
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    core: ExchangeCore,
    /// Every durable envelope this runtime has seen, kept for tests to inspect. Production never
    /// read it, and it grew with every command for the life of the process; the journal already
    /// holds the complete history. See `docs/performance/03-no-history-in-ram.md`.
    #[cfg(test)]
    event_log: Vec<EventEnvelope>,
    next_event_seq: u64,
    /// `None` runs the exchange in memory only, which is what the unit tests and
    /// `ExchangeRuntime::new` want. The server always supplies a store.
    store: Option<EventStore>,
    stream: Option<StreamWriter>,
    /// The link to the replica on the other machine, when the journal is replicated. Every group
    /// then waits for the replica's confirmation before it is published or answered.
    replication: Option<Arc<Replication>>,
    /// With replication, startup publishes only what was already published, since the replica
    /// may not hold the rest. This is the journal's recovered end and last sequence, published,
    /// and served, once the replica confirms it.
    held: Option<(u64, u64)>,
}

/// How much the journal grows between core snapshots, as a multiple of the last snapshot's size.
/// Since milestone 20 the warm replica writes them, not this runtime: a snapshot serializes the
/// whole core, and doing that on the trading thread froze trading for seconds (see
/// `docs/performance/04-snapshots-off-the-trading-thread.md`). Since milestone 23 part 3 the rule
/// is journal growth rather than a fixed 10,000 commands, which made a day's snapshot work grow
/// with the square of its length (see `docs/performance/09-promotion-from-the-warm-replica.md`).
pub const DEFAULT_SNAPSHOT_GROWTH: u64 = 4;

/// Most commands that may share one journal sync. Below this cap a group is simply whatever was
/// already queued; the cap bounds how long the first command waits for the last to be prepared.
const MAX_GROUP: usize = 1_024;

/// Startup refuses untrustworthy history or an unusable committed-event stream.
#[derive(Debug)]
pub enum StartupError {
    Store(EventStoreError),
    Replay(ReplayError),
    Stream(std::io::Error),
}

#[derive(Debug)]
enum RuntimeFailure {
    Store(EventStoreError),
    Stream(std::io::Error),
    Internal(String),
}

impl std::fmt::Display for RuntimeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "event store failure: {}", error),
            Self::Stream(error) => write!(f, "committed event publication failed: {}", error),
            Self::Internal(reason) => write!(f, "exchange internal fault: {}", reason),
        }
    }
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(err) => write!(f, "{}", err),
            Self::Stream(err) => write!(f, "event stream: {}", err),
            Self::Replay(err) => write!(
                f,
                "stored history did not replay deterministically: {:?}",
                err
            ),
        }
    }
}

enum InputEventResult {
    Deposit(Result<(), String>),
    DepositShares(Result<(), String>),
    SetRiskLimit(Result<(), String>),
    PlaceOrder(Result<OrderView, String>),
    CancelOrder(Result<(), String>),
    Session(Result<SessionView, String>),
}
enum PreparedCommit {
    None,
    Deposit {
        user_id: String,
        amount: u64,
    },
    DepositShares {
        user_id: String,
        symbol: String,
        quantity: u64,
    },
    RiskLimit {
        user_id: String,
        symbol: String,
        limit: u64,
    },
    AddOrder(Box<PreparedAddOrder>),
    CancelOrder(Box<PreparedCancelOrder>),
    OpenMarket(chrono::NaiveDate),
    CloseMarket(Box<PreparedExpiry>),
}

struct PreparedInput {
    result: InputEventResult,
    output_events: Vec<ExchangeOutputEvent>,
    commit: PreparedCommit,
    executions: Vec<crate::types::types::Execution>,
}

#[derive(Debug, PartialEq)]
pub enum ReplayError {
    EventSequenceMismatch {
        expected: u64,
        actual: u64,
    },
    UnexpectedOutput {
        seq_num: u64,
        actual: ExchangeOutputEvent,
    },
    MissingOutput {
        seq_num: u64,
        expected: ExchangeOutputEvent,
    },
    OutputMismatch {
        seq_num: u64,
        expected: ExchangeOutputEvent,
        actual: ExchangeOutputEvent,
    },
    InternalFault(String),
}

/// A read-only deterministic follower of committed command batches.
///
/// The warm replica owns only an `ExchangeCore` and its next journal sequence. It has no journal
/// writer, mmap writer, command receiver, callbacks, or HTTP replies. Applying a batch uses the
/// same prepare/compare/commit path as normal recovery, so a follower cannot silently accept an
/// output the primary core would not have produced.
pub(crate) struct ReplicaCore {
    core: ExchangeCore,
    next_event_seq: u64,
}

impl ReplicaCore {
    pub(crate) fn new() -> Self {
        Self {
            core: ExchangeCore::new(),
            next_event_seq: 1,
        }
    }

    pub(crate) fn from_snapshot(
        core: ExchangeCore,
        next_event_seq: u64,
    ) -> Result<Self, ReplayError> {
        if next_event_seq == 0 {
            return Err(ReplayError::InternalFault(
                "replica snapshot has an invalid next event sequence".to_string(),
            ));
        }
        Ok(Self {
            core,
            next_event_seq,
        })
    }

    pub(crate) fn next_event_sequence(&self) -> u64 {
        self.next_event_seq
    }

    /// Validates one complete journal record and commits it only after every recorded output
    /// matches the deterministic prepared result. No local callback is published on this path.
    pub(crate) fn apply_batch(&mut self, batch: &[EventEnvelope]) -> Result<(), ReplayError> {
        self.next_event_seq = replay_committed_batch(&mut self.core, self.next_event_seq, batch)?;
        Ok(())
    }

    /// The normalized core state, as the warm replica writes it to a snapshot.
    pub(crate) fn snapshot(&self) -> crate::exchange::core::CoreSnapshot {
        self.core.snapshot()
    }

    /// The core and the next sequence, which a promotion hands to the new primary.
    pub(crate) fn into_parts(self) -> (ExchangeCore, u64) {
        (self.core, self.next_event_seq)
    }
}

impl InputEventResult {
    fn into_deposit_result(self) -> Result<(), String> {
        match self {
            Self::Deposit(result) => result,
            _ => unreachable!("expected deposit result"),
        }
    }

    fn into_deposit_shares_result(self) -> Result<(), String> {
        match self {
            Self::DepositShares(result) => result,
            _ => unreachable!("expected share deposit result"),
        }
    }

    fn into_set_risk_limit_result(self) -> Result<(), String> {
        match self {
            Self::SetRiskLimit(result) => result,
            _ => unreachable!("expected risk limit result"),
        }
    }

    fn into_place_order_result(self) -> Result<OrderView, String> {
        match self {
            Self::PlaceOrder(result) => result,
            _ => unreachable!("expected place order result"),
        }
    }

    fn into_cancel_order_result(self) -> Result<(), String> {
        match self {
            Self::CancelOrder(result) => result,
            _ => unreachable!("expected cancel order result"),
        }
    }

    fn into_session_result(self) -> Result<SessionView, String> {
        match self {
            Self::Session(result) => result,
            _ => unreachable!("expected session result"),
        }
    }
}

/// A refused open or close changes nothing; the refusal is journaled like any business rejection.
fn rejected_session(error: SessionError) -> PreparedInput {
    let reason = format!("{:?}", error);
    PreparedInput {
        result: InputEventResult::Session(Err(reason.clone())),
        output_events: vec![ExchangeOutputEvent::SessionRejected { reason }],
        commit: PreparedCommit::None,
        executions: Vec::new(),
    }
}

fn prepare_input_event(
    core: &ExchangeCore,
    event: ExchangeInputEvent,
) -> Result<PreparedInput, CoreError> {
    match event {
        ExchangeInputEvent::FundsDepositRequested { user_id, amount } => {
            match core.validate_deposit(amount) {
                Ok(()) => Ok(PreparedInput {
                    result: InputEventResult::Deposit(Ok(())),
                    output_events: vec![ExchangeOutputEvent::FundsDeposited {
                        user_id: user_id.clone(),
                        amount,
                    }],
                    commit: PreparedCommit::Deposit { user_id, amount },
                    executions: Vec::new(),
                }),
                Err(err) => {
                    let reason = format!("{:?}", err);
                    Ok(PreparedInput {
                        result: InputEventResult::Deposit(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::FundsDepositRejected {
                            user_id,
                            reason,
                        }],
                        commit: PreparedCommit::None,
                        executions: Vec::new(),
                    })
                }
            }
        }
        ExchangeInputEvent::RiskLimitSetRequested {
            user_id,
            symbol,
            max_daily_quantity,
        } => Ok(PreparedInput {
            result: InputEventResult::SetRiskLimit(Ok(())),
            output_events: vec![ExchangeOutputEvent::RiskLimitSet {
                user_id: user_id.clone(),
                symbol: symbol.clone(),
                max_daily_quantity,
            }],
            commit: PreparedCommit::RiskLimit {
                user_id,
                symbol,
                limit: max_daily_quantity,
            },
            executions: Vec::new(),
        }),
        ExchangeInputEvent::SharesDepositRequested {
            user_id,
            symbol,
            quantity,
        } => match core.validate_share_deposit(&symbol, quantity) {
            Ok(()) => Ok(PreparedInput {
                result: InputEventResult::DepositShares(Ok(())),
                output_events: vec![ExchangeOutputEvent::SharesDeposited {
                    user_id: user_id.clone(),
                    symbol: symbol.clone(),
                    quantity,
                }],
                commit: PreparedCommit::DepositShares {
                    user_id,
                    symbol,
                    quantity,
                },
                executions: Vec::new(),
            }),
            Err(err) => {
                let reason = format!("{:?}", err);
                Ok(PreparedInput {
                    result: InputEventResult::DepositShares(Err(reason.clone())),
                    output_events: vec![ExchangeOutputEvent::SharesDepositRejected {
                        user_id,
                        symbol,
                        reason,
                    }],
                    commit: PreparedCommit::None,
                    executions: Vec::new(),
                })
            }
        },
        // Checked here, the one entry point shared by live trading, replay and the warm replica,
        // so all three agree on which orders the session refused.
        ExchangeInputEvent::NewOrderRequested { order } if !core.is_market_open() => {
            Ok(PreparedInput {
                result: InputEventResult::PlaceOrder(Err(MARKET_CLOSED.to_string())),
                output_events: vec![ExchangeOutputEvent::OrderRejected {
                    order_id: order.order_id,
                    reason: MARKET_CLOSED.to_string(),
                }],
                commit: PreparedCommit::None,
                executions: Vec::new(),
            })
        }
        ExchangeInputEvent::NewOrderRequested { order } => {
            let order_id = order.order_id.clone();
            match core.prepare_add_order(order) {
                Ok(prepared) => {
                    let mut output_events = vec![ExchangeOutputEvent::OrderAccepted {
                        order_id: prepared.order.order_id.clone(),
                        seq_num: prepared.seq_num,
                    }];
                    output_events.extend(
                        prepared
                            .executions
                            .iter()
                            .cloned()
                            .map(|execution| ExchangeOutputEvent::ExecutionCreated { execution }),
                    );
                    let executions = prepared.executions.clone();
                    let view = prepared.view.clone();
                    Ok(PreparedInput {
                        result: InputEventResult::PlaceOrder(Ok(view)),
                        output_events,
                        commit: PreparedCommit::AddOrder(Box::new(prepared)),
                        executions,
                    })
                }
                Err(CoreError::Business(err)) => {
                    let reason = format!("{:?}", err);
                    Ok(PreparedInput {
                        result: InputEventResult::PlaceOrder(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::OrderRejected {
                            order_id,
                            reason,
                        }],
                        commit: PreparedCommit::None,
                        executions: Vec::new(),
                    })
                }
                Err(CoreError::Internal(reason)) => Err(CoreError::Internal(reason)),
            }
        }
        ExchangeInputEvent::CancelOrderRequested { order_id, user_id } => {
            match core.prepare_cancel_order(&order_id, &user_id) {
                Ok(prepared) => Ok(PreparedInput {
                    result: InputEventResult::CancelOrder(Ok(())),
                    output_events: vec![ExchangeOutputEvent::OrderCanceled {
                        order_id: prepared.order_id.clone(),
                        seq_num: prepared.seq_num,
                    }],
                    commit: PreparedCommit::CancelOrder(Box::new(prepared)),
                    executions: Vec::new(),
                }),
                Err(CoreError::Business(err)) => {
                    let reason = format!("{:?}", err);
                    Ok(PreparedInput {
                        result: InputEventResult::CancelOrder(Err(reason.clone())),
                        output_events: vec![ExchangeOutputEvent::CancelRejected {
                            order_id,
                            reason,
                        }],
                        commit: PreparedCommit::None,
                        executions: Vec::new(),
                    })
                }
                Err(CoreError::Internal(reason)) => Err(CoreError::Internal(reason)),
            }
        }
        ExchangeInputEvent::MarketOpenRequested { trading_day } => {
            Ok(match core.prepare_open_market(trading_day) {
                Ok(()) => PreparedInput {
                    result: InputEventResult::Session(Ok(SessionView {
                        trading_day: Some(trading_day),
                        open: true,
                    })),
                    output_events: vec![ExchangeOutputEvent::MarketOpened { trading_day }],
                    commit: PreparedCommit::OpenMarket(trading_day),
                    executions: Vec::new(),
                },
                Err(error) => rejected_session(error),
            })
        }
        ExchangeInputEvent::MarketCloseRequested => prepare_close(core, MAX_RECORD_LEN as usize),
    }
}

/// The close: every resting order expires, all in one record of at most `record_limit` payload
/// bytes (the journal's limit, except in tests).
fn prepare_close(core: &ExchangeCore, record_limit: usize) -> Result<PreparedInput, CoreError> {
    let trading_day = match core.prepare_close_market() {
        Ok(trading_day) => trading_day,
        Err(error) => return Ok(rejected_session(error)),
    };
    let expiry = core.prepare_expiry().map_err(CoreError::Internal)?;
    let mut output_events = Vec::with_capacity(1 + expiry.expired.len());
    output_events.push(ExchangeOutputEvent::MarketClosed { trading_day });
    output_events.extend(expiry.expired.iter().map(|(order_id, seq_num)| {
        ExchangeOutputEvent::OrderExpired {
            order_id: order_id.clone(),
            seq_num: *seq_num,
        }
    }));
    // The resting-order cap keeps the record small enough for orders with gateway-checked ids.
    // Should an order from any other entry point make it too large anyway, the close is refused
    // while nothing has changed, rather than halting the worker when the record cannot be written.
    if !fits_one_record(
        &ExchangeInputEvent::MarketCloseRequested,
        &output_events,
        record_limit,
    ) {
        return Ok(rejected_session(SessionError::TooManyRestingOrders(
            expiry.expired.len(),
        )));
    }
    Ok(PreparedInput {
        result: InputEventResult::Session(Ok(SessionView {
            trading_day: Some(trading_day),
            open: false,
        })),
        output_events,
        commit: PreparedCommit::CloseMarket(Box::new(expiry)),
        executions: Vec::new(),
    })
}

/// Whether a command's whole journal record fits in `limit` payload bytes. Every envelope's
/// sequence number is counted at its widest, so the answer depends only on the command, never on
/// where in the journal it lands, and replay always reaches the same decision.
fn fits_one_record(
    input: &ExchangeInputEvent,
    outputs: &[ExchangeOutputEvent],
    limit: usize,
) -> bool {
    let widest = |event| EventEnvelope {
        seq_num: u64::MAX,
        event,
    };
    let batch: Vec<_> = std::iter::once(widest(ExchangeEvent::Input(input.clone())))
        .chain(
            outputs
                .iter()
                .map(|output| widest(ExchangeEvent::Output(output.clone()))),
        )
        .collect();
    serde_json::to_vec(&batch).is_ok_and(|payload| payload.len() <= limit)
}

impl PreparedInput {
    fn commit(
        self,
        core: &mut ExchangeCore,
    ) -> (InputEventResult, Vec<crate::types::types::Execution>) {
        let executions = self.executions;
        match self.commit {
            PreparedCommit::None => {}
            PreparedCommit::Deposit { user_id, amount } => core.commit_deposit(user_id, amount),
            PreparedCommit::DepositShares {
                user_id,
                symbol,
                quantity,
            } => core.commit_share_deposit(&user_id, &symbol, quantity),
            PreparedCommit::RiskLimit {
                user_id,
                symbol,
                limit,
            } => core.commit_risk_limit(user_id, symbol, limit),
            PreparedCommit::AddOrder(prepared) => core.commit_add_order(*prepared),
            PreparedCommit::CancelOrder(prepared) => core.commit_cancel_order(*prepared),
            PreparedCommit::OpenMarket(trading_day) => core.commit_open_market(trading_day),
            PreparedCommit::CloseMarket(expiry) => core.commit_close_market(*expiry),
        }
        (self.result, executions)
    }
}

#[cfg(test)]
struct ProcessedInput {
    output_events: Vec<ExchangeOutputEvent>,
}

#[cfg(test)]
fn process_input_event(core: &mut ExchangeCore, event: ExchangeInputEvent) -> ProcessedInput {
    let prepared = prepare_input_event(core, event).expect("test input must prepare");
    let output_events = prepared.output_events.clone();
    let _ = prepared.commit(core);
    ProcessedInput { output_events }
}

pub fn replay_event_log(event_log: &[EventEnvelope]) -> Result<ExchangeCore, ReplayError> {
    replay_event_log_from_core(ExchangeCore::new(), 1, event_log)
}

fn replay_event_log_from_core(
    mut core: ExchangeCore,
    first_sequence: u64,
    event_log: &[EventEnvelope],
) -> Result<ExchangeCore, ReplayError> {
    let mut index = 0;
    while index < event_log.len() {
        let end = event_log[index + 1..]
            .iter()
            .position(|envelope| matches!(envelope.event, ExchangeEvent::Input(_)))
            .map(|offset| index + offset + 1)
            .unwrap_or(event_log.len());
        let expected_sequence = first_sequence
            .checked_add(index as u64)
            .ok_or_else(|| ReplayError::InternalFault("journal sequence overflow".to_string()))?;
        let next_sequence =
            replay_committed_batch(&mut core, expected_sequence, &event_log[index..end])?;
        let expected_next = first_sequence
            .checked_add(end as u64)
            .ok_or_else(|| ReplayError::InternalFault("journal sequence overflow".to_string()))?;
        if next_sequence != expected_next {
            return Err(ReplayError::InternalFault(
                "replay batch advanced to an unexpected sequence".to_string(),
            ));
        }
        index = end;
    }

    Ok(core)
}

/// Replays exactly one complete command record into an existing core. A `StreamReader` already
/// establishes physical record boundaries; this function establishes the business boundary and
/// keeps the core unchanged whenever the recorded outcome is malformed or disagrees.
pub(crate) fn replay_committed_batch(
    core: &mut ExchangeCore,
    first_sequence: u64,
    batch: &[EventEnvelope],
) -> Result<u64, ReplayError> {
    if batch.is_empty() {
        return Err(ReplayError::InternalFault(
            "committed batch is empty".to_string(),
        ));
    }
    for (index, envelope) in batch.iter().enumerate() {
        let expected = first_sequence
            .checked_add(index as u64)
            .ok_or_else(|| ReplayError::InternalFault("journal sequence overflow".to_string()))?;
        if envelope.seq_num != expected {
            return Err(ReplayError::EventSequenceMismatch {
                expected,
                actual: envelope.seq_num,
            });
        }
    }

    let input = match &batch[0].event {
        ExchangeEvent::Input(input) => input.clone(),
        ExchangeEvent::Output(actual) => {
            return Err(ReplayError::UnexpectedOutput {
                seq_num: batch[0].seq_num,
                actual: actual.clone(),
            });
        }
    };
    let processed = prepare_input_event(core, input).map_err(|error| match error {
        CoreError::Business(reason) => ReplayError::InternalFault(format!("{:?}", reason)),
        CoreError::Internal(reason) => ReplayError::InternalFault(reason),
    })?;

    for (offset, expected_output) in processed.output_events.iter().cloned().enumerate() {
        let index = offset + 1;
        let Some(output_envelope) = batch.get(index) else {
            return Err(ReplayError::MissingOutput {
                seq_num: first_sequence.checked_add(index as u64).ok_or_else(|| {
                    ReplayError::InternalFault("journal sequence overflow".to_string())
                })?,
                expected: expected_output,
            });
        };
        let actual_output = match &output_envelope.event {
            ExchangeEvent::Output(actual) => actual,
            ExchangeEvent::Input(_) => {
                return Err(ReplayError::MissingOutput {
                    seq_num: output_envelope.seq_num,
                    expected: expected_output,
                });
            }
        };
        if actual_output != &expected_output {
            return Err(ReplayError::OutputMismatch {
                seq_num: output_envelope.seq_num,
                expected: expected_output,
                actual: actual_output.clone(),
            });
        }
    }

    let expected_len = 1 + processed.output_events.len();
    if let Some(extra) = batch.get(expected_len) {
        return match &extra.event {
            ExchangeEvent::Output(actual) => Err(ReplayError::UnexpectedOutput {
                seq_num: extra.seq_num,
                actual: actual.clone(),
            }),
            ExchangeEvent::Input(_) => Err(ReplayError::InternalFault(
                "committed batch contains a second input".to_string(),
            )),
        };
    }

    let _ = processed.commit(core);
    first_sequence
        .checked_add(batch.len() as u64)
        .ok_or_else(|| ReplayError::InternalFault("journal sequence overflow".to_string()))
}

impl ExchangeRuntime {
    pub fn new(rx: tokio::sync::mpsc::Receiver<ExchangeCommand>) -> Self {
        Self {
            rx,
            core: ExchangeCore::new(),
            #[cfg(test)]
            event_log: Vec::new(),
            next_event_seq: 1,
            store: None,
            stream: None,
            replication: None,
            held: None,
        }
    }

    pub fn from_event_log(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        event_log: Vec<EventEnvelope>,
    ) -> Result<Self, ReplayError> {
        Self::rebuild(rx, event_log, None)
    }

    /// Rebuilds from validated history and keeps writing to the store the history came from.
    pub fn from_store(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        store: EventStore,
        event_log: Vec<EventEnvelope>,
    ) -> Result<Self, ReplayError> {
        Self::rebuild(rx, event_log, Some(store))
    }

    fn rebuild(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        event_log: Vec<EventEnvelope>,
        store: Option<EventStore>,
    ) -> Result<Self, ReplayError> {
        let core = replay_event_log(&event_log)?;

        let next_event_seq = event_log
            .last()
            .map(|envelope| envelope.seq_num + 1)
            .unwrap_or(1);

        Ok(Self {
            rx,
            core,
            #[cfg(test)]
            event_log,
            next_event_seq,
            store,
            stream: None,
            replication: None,
            held: None,
        })
    }

    fn from_snapshot_suffix(
        rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
        store: EventStore,
        core: ExchangeCore,
        #[cfg_attr(not(test), allow(unused_variables))] suffix: Vec<EventEnvelope>,
        next_event_seq: u64,
    ) -> Self {
        Self {
            rx,
            core,
            // History before the snapshot is still available in the authoritative journal.
            #[cfg(test)]
            event_log: suffix,
            next_event_seq,
            store: Some(store),
            stream: None,
            replication: None,
            held: None,
        }
    }

    fn write_snapshot(&self, path: &Path, stream_path: &Path) -> Result<(), String> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| "cannot snapshot an in-memory-only runtime".to_string())?;
        let metadata = store
            .file()
            .metadata()
            .map_err(|error| format!("could not inspect journal for snapshot: {error}"))?;
        let boundary = SnapshotBoundary {
            journal_id: store.journal_id(),
            byte_offset: metadata.len(),
            next_event_sequence: self.next_event_seq,
        };
        snapshot::write(
            path,
            store.file(),
            stream_path,
            boundary,
            self.core.snapshot(),
        )
        .map(|_| ())
    }

    /// Replicates the journal from now on: listens on `address` for the replica on the other
    /// machine. Called before `run`; every group then waits for the replica's confirmation.
    pub fn replicate(&mut self, address: SocketAddr) -> std::io::Result<Arc<Replication>> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| std::io::Error::other("an in-memory exchange has no journal"))?;
        let end = store
            .end()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let replication = Replication::listen(
            address,
            store.file().try_clone()?,
            store.journal_id(),
            end,
            self.next_event_seq,
        )?;
        self.replication = Some(Arc::clone(&replication));
        Ok(replication)
    }

    /// Startup's last step with replication: waits until the replica holds the whole recovered
    /// journal, then publishes what was held back. No command is served before.
    fn release_held(&mut self) -> Result<(), RuntimeFailure> {
        let Some((end, last_sequence)) = self.held.take() else {
            return Ok(());
        };
        if let Some(replication) = &self.replication {
            replication.confirm(end);
        }
        self.stream
            .as_mut()
            .expect("a held runtime has a stream")
            .publish_through(end, last_sequence)
            .map_err(RuntimeFailure::Stream)
    }

    pub fn run(mut self) {
        if let Err(err) = self.release_held() {
            eprintln!("exchange worker halted: {err}. No further commands accepted.");
            return;
        }
        let mut group = Vec::with_capacity(MAX_GROUP);
        while let Some(first) = self.rx.blocking_recv() {
            // Natural batching: whatever queued up while the previous group was syncing becomes
            // the next group. A quiet exchange gets groups of one; a busy one shares each sync.
            group.push(first);
            while group.len() < MAX_GROUP {
                match self.rx.try_recv() {
                    Ok(command) => group.push(command),
                    Err(_) => break,
                }
            }
            if let Err(err) = self.handle_group(group.drain(..)) {
                // Fail closed. A store or preparation error stops the worker before anything in
                // the group is visible. A publication error happens AFTER the sync: recovery
                // publishes that group on restart.
                eprintln!(
                    "exchange worker halted: {}. No further commands accepted.",
                    err
                );
                break;
            }
        }
    }

    #[cfg(test)]
    pub fn event_log(&self) -> &[EventEnvelope] {
        &self.event_log
    }

    #[cfg(test)]
    pub(crate) fn core_snapshot_for_test(&self) -> crate::exchange::core::CoreSnapshot {
        self.core.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn record_input_for_test(
        &mut self,
        event: ExchangeInputEvent,
    ) -> Result<(), String> {
        self.record_and_process_input_event(event)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn fail_stream_publication_for_test(&mut self) {
        self.stream
            .as_mut()
            .expect("test runtime has a committed stream")
            .fail_publication_for_test();
    }

    /// The sequence to assign to the next durable envelope. This is the durable journal's
    /// high-water mark even when snapshot recovery keeps only its replayed suffix in memory.
    pub fn next_event_sequence(&self) -> u64 {
        self.next_event_seq
    }

    /// Stages one command: a write is prepared, encoded, and committed in memory; a read is
    /// answered from the core at its place in the queue. Either way the reply is held until the
    /// group is durable, so a read can never show state that has not been synced. A command that
    /// cannot be staged is answered with the halt message at once and stops the group.
    fn stage_command(
        &mut self,
        command: ExchangeCommand,
        staged: &mut Vec<Staged>,
    ) -> Result<HeldReply, RuntimeFailure> {
        match command {
            ExchangeCommand::Deposit {
                user_id,
                amount,
                respond_to,
            } => self.stage_write(
                ExchangeInputEvent::FundsDepositRequested { user_id, amount },
                staged,
                respond_to,
                InputEventResult::into_deposit_result,
            ),
            ExchangeCommand::DepositShares {
                user_id,
                symbol,
                quantity,
                respond_to,
            } => self.stage_write(
                ExchangeInputEvent::SharesDepositRequested {
                    user_id,
                    symbol,
                    quantity,
                },
                staged,
                respond_to,
                InputEventResult::into_deposit_shares_result,
            ),
            ExchangeCommand::SetRiskLimit {
                user_id,
                symbol,
                max_daily_quantity,
                respond_to,
            } => self.stage_write(
                ExchangeInputEvent::RiskLimitSetRequested {
                    user_id,
                    symbol,
                    max_daily_quantity,
                },
                staged,
                respond_to,
                InputEventResult::into_set_risk_limit_result,
            ),
            ExchangeCommand::PlaceOrder { order, respond_to } => self.stage_write(
                ExchangeInputEvent::NewOrderRequested { order },
                staged,
                respond_to,
                InputEventResult::into_place_order_result,
            ),
            ExchangeCommand::CancelOrder {
                order_id,
                user_id,
                respond_to,
            } => self.stage_write(
                ExchangeInputEvent::CancelOrderRequested { order_id, user_id },
                staged,
                respond_to,
                InputEventResult::into_cancel_order_result,
            ),

            // Reads never reach the event log: they mutate nothing, and logging them would make
            // every future replay longer for no change in outcome.
            ExchangeCommand::GetBalance {
                user_id,
                respond_to,
            } => Ok(hold_read(respond_to, self.core.balance_view(&user_id))),
            ExchangeCommand::GetPositions {
                user_id,
                respond_to,
            } => Ok(hold_read(respond_to, self.core.position_views(&user_id))),
            ExchangeCommand::GetExecutions {
                user_id,
                symbol,
                order_id,
                start_time,
                end_time,
                respond_to,
            } => Ok(hold_read(
                respond_to,
                self.core.execution_views(
                    &user_id,
                    symbol.as_deref(),
                    order_id.as_deref(),
                    start_time,
                    end_time,
                ),
            )),
            ExchangeCommand::GetRiskLimit {
                user_id,
                symbol,
                respond_to,
            } => Ok(hold_read(
                respond_to,
                self.core.risk_limit_view(&user_id, &symbol),
            )),
            ExchangeCommand::GetOrder {
                order_id,
                user_id,
                respond_to,
            } => Ok(hold_read(
                respond_to,
                self.core.order_view(&order_id, &user_id),
            )),
            ExchangeCommand::OpenMarket {
                trading_day,
                respond_to,
            } => self.stage_write(
                ExchangeInputEvent::MarketOpenRequested { trading_day },
                staged,
                respond_to,
                InputEventResult::into_session_result,
            ),
            ExchangeCommand::CloseMarket { respond_to } => self.stage_write(
                ExchangeInputEvent::MarketCloseRequested,
                staged,
                respond_to,
                InputEventResult::into_session_result,
            ),
            ExchangeCommand::GetSession { respond_to } => {
                Ok(hold_read(respond_to, self.core.session_view()))
            }
        }
    }

    fn stage_write<T: 'static>(
        &mut self,
        event: ExchangeInputEvent,
        staged: &mut Vec<Staged>,
        respond_to: tokio::sync::oneshot::Sender<Result<T, String>>,
        into_result: fn(InputEventResult) -> Result<T, String>,
    ) -> Result<HeldReply, RuntimeFailure> {
        match self.stage_input_event(event, staged) {
            Ok(result) => Ok(hold(respond_to, into_result(result))),
            Err(err) => {
                let _ = respond_to.send(Err(halted_message(&err)));
                Err(err)
            }
        }
    }

    /// Prepares one input without mutating the live core, encodes its complete batch, and commits
    /// it in memory, so the next command in the group sees it. The record becomes durable later,
    /// in `flush`, together with the rest of its group.
    fn stage_input_event(
        &mut self,
        event: ExchangeInputEvent,
        staged: &mut Vec<Staged>,
    ) -> Result<InputEventResult, RuntimeFailure> {
        let prepared =
            prepare_input_event(&self.core, event.clone()).map_err(|error| match error {
                CoreError::Business(reason) => RuntimeFailure::Internal(format!("{:?}", reason)),
                CoreError::Internal(reason) => RuntimeFailure::Internal(reason),
            })?;

        let mut seq_num = self.next_event_seq;
        let mut batch = Vec::with_capacity(1 + prepared.output_events.len());

        batch.push(EventEnvelope {
            seq_num,
            event: ExchangeEvent::Input(event),
        });
        seq_num += 1;

        for output_event in prepared.output_events.iter().cloned() {
            batch.push(EventEnvelope {
                seq_num,
                event: ExchangeEvent::Output(output_event),
            });
            seq_num += 1;
        }

        // Serialize once; these exact framed bytes go to disk and then to mmap.
        let record = encode_record(&batch).map_err(RuntimeFailure::Store)?;
        let (result, executions) = prepared.commit(&mut self.core);
        self.next_event_seq = seq_num;
        staged.push(Staged {
            record,
            last_sequence: seq_num - 1,
            executions,
            #[cfg(test)]
            batch,
        });

        Ok(result)
    }

    /// Makes a group durable with ONE journal write and ONE sync, then publishes each command's
    /// batch in order and runs its callbacks. Nothing here is reachable before the sync returns.
    fn flush(&mut self, staged: Vec<Staged>) -> Result<(), RuntimeFailure> {
        if staged.is_empty() {
            return Ok(()); // a group of reads changed nothing
        }
        if let Some(store) = self.store.as_mut() {
            let records: Vec<&[u8]> = staged.iter().map(|s| s.record.as_slice()).collect();
            let records = records.concat();
            if let Some(replication) = &self.replication {
                // The group goes to the replica while this disk syncs, then waits for both:
                // nothing that exists on one machine only is ever published or answered.
                store
                    .write_records(&records)
                    .map_err(RuntimeFailure::Store)?;
                let end = store.end().map_err(RuntimeFailure::Store)?;
                let next_sequence = staged.last().expect("a group").last_sequence + 1;
                replication.written(end, next_sequence);
                store.sync().map_err(RuntimeFailure::Store)?;
                replication.confirm(end);
            } else {
                store
                    .append_record(&records)
                    .map_err(RuntimeFailure::Store)?;
            }
        }
        for command in staged {
            // Only now is the command part of history and visible to callbacks.
            #[cfg(test)]
            self.event_log.extend(command.batch);
            if let Some(stream) = self.stream.as_mut() {
                stream
                    .append(&command.record, command.last_sequence)
                    .map_err(RuntimeFailure::Stream)?;
            }
            self.core.notify_executions(&command.executions);
        }
        Ok(())
    }

    /// Group commit. Every command already waiting is staged in queue order, then one sync makes
    /// the whole group durable, and only then are replies released — reads included. Until the
    /// sync returns, the in-memory core is ahead of the disk, but nothing outside this worker can
    /// observe it: no reply, no mmap batch, no callback, no snapshot. If the sync fails the worker
    /// halts, and the restart rebuilds from the journal, discarding the unsynced state.
    fn handle_group(
        &mut self,
        commands: impl Iterator<Item = ExchangeCommand>,
    ) -> Result<(), RuntimeFailure> {
        let mut staged = Vec::new();
        let mut replies = Vec::new();
        let mut fault = None;
        for command in commands {
            match self.stage_command(command, &mut staged) {
                Ok(reply) => replies.push(reply),
                Err(err) => {
                    // That command is already answered. The valid commands before it still go
                    // through the sync below; those queued after it are dropped unanswered, which
                    // the gateway reports as unavailable.
                    fault = Some(err);
                    break;
                }
            }
        }
        let flushed = self.flush(staged);
        let failure = flushed.as_ref().err().map(halted_message);
        for reply in replies {
            reply(failure.as_deref());
        }
        flushed?;
        fault.map_or(Ok(()), Err)
    }

    /// One command as a group of one. Tests drive the runtime through these directly.
    #[cfg(test)]
    fn handle_command(&mut self, command: ExchangeCommand) -> Result<(), RuntimeFailure> {
        self.handle_group(std::iter::once(command))
    }

    #[cfg(test)]
    fn record_and_process_input_event(
        &mut self,
        event: ExchangeInputEvent,
    ) -> Result<InputEventResult, RuntimeFailure> {
        let mut staged = Vec::new();
        let result = self.stage_input_event(event, &mut staged)?;
        self.flush(staged)?;
        Ok(result)
    }
}

/// A command committed in memory whose journal record has not been synced yet.
struct Staged {
    record: Vec<u8>,
    last_sequence: u64,
    executions: Vec<crate::types::types::Execution>,
    #[cfg(test)]
    batch: Vec<EventEnvelope>,
}

/// A reply held back until its group is durable and published. It receives `None` on success, or
/// the halt message if the group failed.
type HeldReply = Box<dyn FnOnce(Option<&str>)>;

fn hold<T: 'static>(
    respond_to: tokio::sync::oneshot::Sender<Result<T, String>>,
    result: Result<T, String>,
) -> HeldReply {
    Box::new(move |failure| {
        let _ = respond_to.send(match failure {
            None => result,
            Some(message) => Err(message.to_string()),
        });
    })
}

/// A read has no error channel. On failure it is dropped, which the gateway reports as
/// unavailable: it must not show state that never became durable.
fn hold_read<T: 'static>(respond_to: tokio::sync::oneshot::Sender<T>, value: T) -> HeldReply {
    Box::new(move |failure| {
        if failure.is_none() {
            let _ = respond_to.send(value);
        }
    })
}

fn halted_message(err: &RuntimeFailure) -> String {
    format!("exchange unavailable: {}", err)
}

/// Opens the event log at `path`, replays whatever history it holds, and returns a runtime ready to
/// continue it. An absent or empty log starts a new exchange; anything unreadable, out of sequence,
/// or failing deterministic replay is an error, never a silent fresh start.
pub fn recover_runtime(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    path: impl AsRef<Path>,
) -> Result<ExchangeRuntime, StartupError> {
    let (store, recovered) = EventStore::open(path).map_err(StartupError::Store)?;

    ExchangeRuntime::from_store(rx, store, recovered).map_err(StartupError::Replay)
}

/// Production startup: validate/replay first, then publish the recovered committed prefix.
/// Never publish merely because a record parsed: replay must also prove its business outputs.
#[cfg(test)]
pub fn recover_runtime_with_stream(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    journal_path: impl AsRef<Path>,
    stream_path: impl AsRef<Path>,
) -> Result<ExchangeRuntime, StartupError> {
    let mut runtime = recover_runtime(rx, journal_path)?;
    attach_stream(&mut runtime, stream_path.as_ref(), false)?;
    Ok(runtime)
}

/// Production recovery with a journal-bound authoritative-core snapshot. Snapshot problems are
/// deliberately non-fatal: the journal is still the source of truth, so startup logs the issue
/// and falls back to the existing full deterministic replay.
pub fn recover_runtime_with_stream_and_snapshot(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    journal_path: impl AsRef<Path>,
    stream_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
) -> Result<ExchangeRuntime, StartupError> {
    recover_from_files(
        rx,
        journal_path.as_ref(),
        stream_path.as_ref(),
        snapshot_path.as_ref(),
        false,
    )
}

/// The same for a replicated journal, except that it publishes nothing beyond what the stream
/// had already published: the replica may not hold the rest yet. `run` publishes it, and starts
/// serving, once the replica confirms it (attach the link with `replicate` first).
pub fn recover_replicated_runtime(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    journal_path: impl AsRef<Path>,
    stream_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
) -> Result<ExchangeRuntime, StartupError> {
    recover_from_files(
        rx,
        journal_path.as_ref(),
        stream_path.as_ref(),
        snapshot_path.as_ref(),
        true,
    )
}

fn recover_from_files(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    journal_path: &Path,
    stream_path: &Path,
    snapshot_path: &Path,
    hold: bool,
) -> Result<ExchangeRuntime, StartupError> {
    let journal_path = journal_path.to_path_buf();
    let stream_path = stream_path.to_path_buf();
    let snapshot_path = snapshot_path.to_path_buf();

    let (mut runtime, snapshot_is_safe_to_replace) =
        match snapshot::load(&snapshot_path, &journal_path) {
            Ok(Some(loaded)) => match recover_snapshot_state(&journal_path, loaded) {
                Ok((store, core, suffix, next_event_seq)) => (
                    ExchangeRuntime::from_snapshot_suffix(rx, store, core, suffix, next_event_seq),
                    true,
                ),
                Err(error) => {
                    eprintln!(
                        "ignoring snapshot and replaying the journal from sequence 1: {error}"
                    );
                    (recover_runtime(rx, &journal_path)?, false)
                }
            },
            Ok(None) => (recover_runtime(rx, &journal_path)?, true),
            Err(error) => {
                eprintln!("ignoring snapshot and replaying the journal from sequence 1: {error}");
                (recover_runtime(rx, &journal_path)?, false)
            }
        };

    attach_stream(&mut runtime, &stream_path, hold)?;
    // ONE fresh snapshot of the recovered core before the exchange accepts any command. From then
    // on the trading thread writes no snapshots; the warm replica keeps the checkpoint current.
    // None while records are held back: it would stand beyond what the stream published, and a
    // warm replica starting before the replica confirmed them would have to rebuild from
    // sequence 1.
    if snapshot_is_safe_to_replace
        && runtime.held.is_none()
        && let Err(error) = runtime.write_snapshot(&snapshot_path, &stream_path)
    {
        // This does not affect a durable, replayable exchange. An older snapshot, if any, stays.
        eprintln!("snapshot checkpoint was not updated during startup: {error}");
    }
    Ok(runtime)
}

/// Turns a read-only warm follower into the next primary, once the caller has fenced the old
/// writer and recovered the records the follower had not applied yet (`suffix`). The follower's
/// core is reused: it was built from this same journal file and checked output by output, on top
/// of the snapshot it started from, which is trusted exactly as a restart from that snapshot
/// trusts it. Promotion replays only the suffix and costs the follower's lag, not the journal's
/// history.
///
/// No startup snapshot is written: that would cost seconds late in a day. The warm replica's last
/// snapshot stays the restart point.
pub(crate) fn promote_replica(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    store: EventStore,
    replica: ReplicaCore,
    suffix: Vec<EventEnvelope>,
    stream_path: impl AsRef<Path>,
    hold: bool,
) -> Result<ExchangeRuntime, StartupError> {
    let (core, next_event_seq) = replica.into_parts();
    let (core, next_event_seq) =
        replay_suffix(core, next_event_seq, &suffix).map_err(StartupError::Replay)?;
    let mut runtime =
        ExchangeRuntime::from_snapshot_suffix(rx, store, core, suffix, next_event_seq);
    attach_stream(&mut runtime, stream_path.as_ref(), hold)?;
    Ok(runtime)
}

/// Opens the mmap stream at the recovered end. It refuses a journal that ends before what the
/// stream already published. With `hold`, for a replicated journal, it publishes only what the
/// stream had already published, and the runtime holds the rest until the replica confirms it.
fn attach_stream(
    runtime: &mut ExchangeRuntime,
    stream_path: &Path,
    hold: bool,
) -> Result<(), StartupError> {
    let journal = runtime.store.as_ref().unwrap().file();
    let last_sequence = runtime.next_event_seq - 1;
    let end = journal.metadata().map_err(StartupError::Stream)?.len();
    let published = if hold {
        already_published(stream_path, journal).map_err(StartupError::Stream)?
    } else {
        (end, last_sequence)
    };
    let stream = StreamWriter::open_at(
        stream_path,
        journal,
        last_sequence,
        published,
        DEFAULT_CAPACITY,
    )
    .map_err(StartupError::Stream)?;
    runtime.stream = Some(stream);
    if published.0 < end {
        runtime.held = Some((end, last_sequence));
    }
    Ok(())
}

/// Replays the records after a core's position, checking every output, and returns the core
/// with the next sequence to assign.
fn replay_suffix(
    core: ExchangeCore,
    next_event_seq: u64,
    suffix: &[EventEnvelope],
) -> Result<(ExchangeCore, u64), ReplayError> {
    let core = replay_event_log_from_core(core, next_event_seq, suffix)?;
    let next_event_seq = match suffix.last() {
        Some(envelope) => envelope
            .seq_num
            .checked_add(1)
            .ok_or_else(|| ReplayError::InternalFault("journal sequence overflow".to_string()))?,
        None => next_event_seq,
    };
    Ok((core, next_event_seq))
}

fn recover_snapshot_state(
    journal_path: &Path,
    loaded: snapshot::LoadedSnapshot,
) -> Result<(EventStore, ExchangeCore, Vec<EventEnvelope>, u64), String> {
    let core = ExchangeCore::from_snapshot(loaded.core)?;
    let (store, suffix) = EventStore::open_suffix(
        journal_path,
        loaded.boundary.journal_id,
        loaded.boundary.byte_offset,
        None,
    )
    .map_err(|error| error.to_string())?;
    let (core, next_event_seq) = replay_suffix(core, loaded.boundary.next_event_sequence, &suffix)
        .map_err(|error| format!("snapshot suffix did not replay deterministically: {error:?}"))?;
    Ok((store, core, suffix, next_event_seq))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::oneshot;

    use super::*;
    use crate::types::types::Order;

    fn runtime() -> ExchangeRuntime {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        ExchangeRuntime::new(rx)
    }

    fn trading_day(day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 10, day).unwrap()
    }

    /// Orders are refused until a trading day is open. A history that trades and is replayed
    /// starts with this journaled command.
    fn open_market() -> ExchangeInputEvent {
        ExchangeInputEvent::MarketOpenRequested {
            trading_day: trading_day(1),
        }
    }

    /// A runtime whose market is already open, for tests that never replay their history. Like
    /// the direct deposits those tests make, the open is applied to the core, not journaled.
    fn open_runtime() -> ExchangeRuntime {
        let mut runtime = runtime();
        runtime.core.open_market(trading_day(1)).unwrap();
        runtime
    }

    /// A sell order must now be backed by shares, so histories that contain one have to fund the
    /// seller first — and that funding has to be an event, or replay would rebuild a seller with
    /// no inventory and reject the sell it is meant to reproduce.
    fn share_deposit(user: &str, quantity: u64) -> ExchangeInputEvent {
        ExchangeInputEvent::SharesDepositRequested {
            user_id: user.to_string(),
            symbol: "AAPL".to_string(),
            quantity,
        }
    }

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

    #[test]
    fn append_failure_leaves_nothing_durable_or_visible() {
        let path = temp_log_path("atomic-append-failure");
        let (store, _) = EventStore::open(&path).unwrap();
        drop(store);

        let mut runtime = runtime();
        runtime.store = Some(EventStore::open_read_only_for_test(&path).unwrap());

        let result =
            runtime.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            });

        // Group commit stages a command in memory before its sync, so after a failed sync the
        // live core is ahead of the disk. It is never used again: the worker halts, and the only
        // way back is recovery from the journal, which holds nothing of the failed command.
        assert!(matches!(result, Err(RuntimeFailure::Store(_))));
        assert!(runtime.event_log.is_empty());
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered = recover_runtime(rx, &path).unwrap();
        assert_eq!(recovered.core.balance_view("buyer").balance, 0);
        assert_eq!(recovered.next_event_seq, 1);

        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn append_failure_does_not_publish_execution_callbacks() {
        let path = temp_log_path("atomic-callback-failure");
        let mut runtime = open_runtime();

        runtime
            .record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 100,
            })
            .unwrap();
        runtime
            .record_and_process_input_event(share_deposit("seller", 1))
            .unwrap();
        runtime
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("sell-1", "seller", "SELL", 10, 1),
            })
            .unwrap();

        let (store, _) = EventStore::open(&path).unwrap();
        drop(store);
        runtime.store = Some(EventStore::open_read_only_for_test(&path).unwrap());

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_for_subscriber = Arc::clone(&callback_count);
        runtime.core.subscribe(move |_| {
            callback_count_for_subscriber.fetch_add(1, Ordering::Relaxed);
        });
        let durable_history = runtime.event_log().len();

        let result =
            runtime.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("buy-1", "buyer", "BUY", 10, 1),
            });

        // The fill was staged in memory, but it never became durable, so nobody hears about it.
        assert!(matches!(result, Err(RuntimeFailure::Store(_))));
        assert_eq!(callback_count.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.event_log().len(), durable_history);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn deposit_appends_requested_and_deposited_events() {
        let mut runtime = runtime();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::Deposit {
                user_id: "buyer".to_string(),
                amount: 1_000,
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);
        assert_eq!(runtime.event_log[0].seq_num, 1);
        assert_eq!(runtime.event_log[1].seq_num, 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn place_order_appends_accepted_after_successful_core_processing() {
        let mut runtime = open_runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::NewOrderRequested { order }) => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 1);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn place_order_appends_rejected_after_failed_core_processing() {
        let mut runtime = open_runtime();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderRejected { order_id, reason }) => {
                assert_eq!(order_id, "buy-1");
                assert!(reason.contains("WalletRejected"));
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn cancel_order_appends_canceled_after_successful_core_processing() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "buy-1".to_string(),
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(user_id, "buyer");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 2);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn matching_order_appends_execution_created_events() {
        let mut runtime = open_runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime.core.deposit_shares("seller", "AAPL", 5).unwrap();
        runtime
            .core
            .add_order(order("sell-1", "seller", "SELL", 10, 5))
            .unwrap();
        let (respond_to, _response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 5),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 4);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::NewOrderRequested { order }) => {
                assert_eq!(order.order_id, "buy-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted { order_id, seq_num }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(*seq_num, 2);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        for event in &runtime.event_log[2..] {
            match &event.event {
                ExchangeEvent::Output(ExchangeOutputEvent::ExecutionCreated { execution }) => {
                    assert_eq!(execution.buy_order_id, "buy-1");
                    assert_eq!(execution.sell_order_id, "sell-1");
                    assert_eq!(execution.price.minor_units(), 10);
                    assert_eq!(execution.quantity, 5);
                }
                other => panic!("unexpected event: {:?}", other),
            }
        }
    }

    #[test]
    fn event_log_can_be_consumed_in_sequence() {
        let mut runtime = open_runtime();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::Deposit {
                user_id: "buyer".to_string(),
                amount: 1_000,
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::DepositShares {
                user_id: "seller".to_string(),
                symbol: "AAPL".to_string(),
                quantity: 5,
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("sell-1", "seller", "SELL", 10, 5),
                respond_to,
            })
            .unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 5),
                respond_to,
            })
            .unwrap();

        let consumed_events = runtime.event_log();
        assert_eq!(consumed_events.len(), 10);

        for (idx, envelope) in consumed_events.iter().enumerate() {
            assert_eq!(envelope.seq_num, (idx + 1) as u64);
        }

        match &consumed_events[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested { user_id, amount }) => {
                assert_eq!(user_id, "buyer");
                assert_eq!(*amount, 1_000);
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &consumed_events[9].event {
            ExchangeEvent::Output(ExchangeOutputEvent::ExecutionCreated { execution }) => {
                assert_eq!(execution.buy_order_id, "buy-1");
                assert_eq!(execution.sell_order_id, "sell-1");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }
    #[test]
    fn rejected_cancellation_is_recorded_and_returned() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime
            .core
            .add_order(order("buy-1", "buyer", "BUY", 10, 10))
            .unwrap();

        let (respond_to, response_rx) = oneshot::channel();

        runtime
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "buy-1".to_string(),
                user_id: "wrong-user".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(runtime.event_log.len(), 2);
        assert_eq!(runtime.event_log[0].seq_num, 1);
        assert_eq!(runtime.event_log[1].seq_num, 2);

        match &runtime.event_log[0].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "buy-1");
                assert_eq!(user_id, "wrong-user");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &runtime.event_log[1].event {
            ExchangeEvent::Output(ExchangeOutputEvent::CancelRejected { order_id, reason }) => {
                assert_eq!(order_id, "buy-1");
                assert!(reason.contains("Unauthorized"));
            }
            other => panic!("unexpected event: {:?}", other),
        }

        let response = response_rx.blocking_recv().unwrap();
        assert!(matches!(
            response,
            Err(reason) if reason.contains("Unauthorized")
        ));
    }
    #[test]
    fn same_input_sequence_produces_same_output_events() {
        let inputs = vec![
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            },
            share_deposit("seller", 5),
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell-1", "seller", "SELL", 10, 5),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy-1", "buyer", "BUY", 10, 5),
            },
        ];

        let mut first_core = ExchangeCore::new();
        let mut second_core = ExchangeCore::new();

        let mut first_outputs = Vec::new();
        let mut second_outputs = Vec::new();

        for input in inputs {
            let first_processed = process_input_event(&mut first_core, input.clone());
            let second_processed = process_input_event(&mut second_core, input);

            first_outputs.extend(first_processed.output_events);
            second_outputs.extend(second_processed.output_events);
        }

        assert_eq!(first_outputs.len(), 7);
        assert_eq!(first_outputs, second_outputs);
    }
    #[test]
    fn replay_rebuilds_matching_state_and_sequence() {
        let mut original = runtime();

        let _ = original.record_and_process_input_event(open_market());
        let _ =
            original.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            });

        let _ = original.record_and_process_input_event(share_deposit("seller", 10));

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("sell-1", "seller", "SELL", 10, 10),
        });

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("buy-1", "buyer", "BUY", 10, 5),
        });

        assert_eq!(original.event_log().len(), 12);

        let mut rebuilt_core = replay_event_log(original.event_log()).unwrap();

        let continuation = process_input_event(
            &mut rebuilt_core,
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
            },
        );

        assert_eq!(
            continuation.output_events,
            vec![ExchangeOutputEvent::OrderCanceled {
                order_id: "sell-1".to_string(),
                seq_num: 3,
            }]
        );
    }
    #[test]
    fn replay_rejects_event_sequence_gap() {
        let event_log = vec![EventEnvelope {
            seq_num: 2,
            event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::EventSequenceMismatch {
                expected: 1,
                actual: 2,
            })
        ));
    }

    #[test]
    fn replay_rejects_missing_output() {
        let event_log = vec![EventEnvelope {
            seq_num: 1,
            event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::MissingOutput {
                seq_num: 2,
                expected: ExchangeOutputEvent::FundsDeposited { .. },
            })
        ));
    }

    #[test]
    fn replay_rejects_unexpected_output() {
        let event_log = vec![EventEnvelope {
            seq_num: 1,
            event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                user_id: "buyer".to_string(),
                amount: 1_000,
            }),
        }];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::UnexpectedOutput { seq_num: 1, .. })
        ));
    }

    #[test]
    fn replay_rejects_modified_output() {
        let event_log = vec![
            EventEnvelope {
                seq_num: 1,
                event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                }),
            },
            EventEnvelope {
                seq_num: 2,
                event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                    user_id: "buyer".to_string(),
                    amount: 999,
                }),
            },
        ];

        assert!(matches!(
            replay_event_log(&event_log),
            Err(ReplayError::OutputMismatch { seq_num: 2, .. })
        ));
    }

    #[test]
    fn recovered_runtime_continues_event_and_matching_sequences() {
        let mut original = runtime();

        let _ = original.record_and_process_input_event(open_market());
        let _ =
            original.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".to_string(),
                amount: 1_000,
            });

        let _ = original.record_and_process_input_event(share_deposit("seller", 10));

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("sell-1", "seller", "SELL", 10, 10),
        });

        let _ = original.record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
            order: order("buy-1", "buyer", "BUY", 10, 5),
        });

        let recorded_log = original.event_log().to_vec();
        assert_eq!(recorded_log.len(), 12);

        let (_tx, rx) = tokio::sync::mpsc::channel(1);

        let mut recovered = ExchangeRuntime::from_event_log(rx, recorded_log).unwrap();

        let (respond_to, response_rx) = oneshot::channel();

        recovered
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(response_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(recovered.event_log().len(), 14);

        assert_eq!(recovered.event_log()[12].seq_num, 13);
        assert_eq!(recovered.event_log()[13].seq_num, 14);

        match &recovered.event_log()[12].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(user_id, "seller");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &recovered.event_log()[13].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(*seq_num, 3);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn queries_do_not_append_to_the_event_log() {
        let mut runtime = open_runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let length_before_reads = runtime.event_log().len();
        assert_eq!(length_before_reads, 2);

        let (respond_to, balance_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetBalance {
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        let (respond_to, order_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetOrder {
                order_id: "buy-1".to_string(),
                user_id: "buyer".to_string(),
                respond_to,
            })
            .unwrap();

        // The reads answered, and the log is exactly where it was.
        let balance = balance_rx.blocking_recv().unwrap();
        assert_eq!(balance.balance, 1_000);
        assert_eq!(balance.locked, 100);
        assert_eq!(balance.available, 900);

        assert!(order_rx.blocking_recv().unwrap().is_some());
        assert_eq!(runtime.event_log().len(), length_before_reads);
    }

    #[test]
    fn place_order_reply_reports_fill_state() {
        let mut runtime = open_runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();
        runtime.core.deposit_shares("seller", "AAPL", 4).unwrap();
        runtime
            .core
            .add_order(order("sell-1", "seller", "SELL", 10, 4))
            .unwrap();

        let (respond_to, response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let view = response_rx.blocking_recv().unwrap().unwrap();

        assert_eq!(view.order_id, "buy-1");
        assert_eq!(view.quantity, 10);
        assert_eq!(view.filled_quantity, 4);
        assert_eq!(view.remaining_quantity, 6);
        assert_eq!(view.status, "partially_filled");
        assert_eq!(view.price, 10);
    }

    fn temp_log_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "exchange-runtime-{}-{}.log",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn committed_stream_matches_runtime_history_and_survives_restart() {
        use crate::exchange::event_stream::tests::Fixture;
        let fixture = Fixture::new();
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let mut reader = fixture.reader();
        let mut expected = Vec::new();
        for input in [
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 1000,
            },
            share_deposit("seller", 10),
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell", "seller", "SELL", 10, 10),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy", "buyer", "BUY", 10, 5),
            },
            ExchangeInputEvent::NewOrderRequested {
                order: order("buy", "buyer", "BUY", 10, 5),
            },
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "sell".into(),
                user_id: "seller".into(),
            },
        ] {
            runtime.record_and_process_input_event(input).unwrap();
            let batch = reader.next_batch().unwrap().unwrap();
            assert!(matches!(batch[0].event, ExchangeEvent::Input(_)));
            expected.extend(batch);
            assert_eq!(expected, runtime.event_log());
            assert!(reader.next_batch().unwrap().is_none());
        }
        let checkpoint = reader.checkpoint();
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let mut resumed = crate::exchange::event_stream::StreamReader::open(
            &fixture.log,
            &fixture.bus,
            Some(checkpoint),
        )
        .unwrap();
        assert!(resumed.next_batch().unwrap().is_none());
        runtime
            .record_and_process_input_event(share_deposit("seller", 1))
            .unwrap();
        assert_eq!(resumed.next_batch().unwrap(), reader.next_batch().unwrap());
    }

    #[test]
    fn failed_durable_append_never_publishes_to_mmap() {
        use crate::exchange::event_stream::tests::Fixture;
        let fixture = Fixture::new();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let mut reader = fixture.reader();
        runtime.store = Some(EventStore::open_read_only_for_test(&fixture.log).unwrap());
        assert!(matches!(
            runtime.record_and_process_input_event(share_deposit("seller", 1)),
            Err(RuntimeFailure::Store(_))
        ));
        assert!(reader.next_batch().unwrap().is_none());
        assert!(runtime.event_log().is_empty());
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered = recover_runtime(rx, &fixture.log).unwrap();
        assert!(recovered.core.position_views("seller").is_empty());
    }

    #[test]
    fn publication_failure_halts_worker_but_recovers_the_committed_command() {
        use crate::exchange::event_stream::tests::Fixture;
        let fixture = Fixture::new();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        runtime.stream.as_mut().unwrap().fail_publication_for_test();
        let mut reader = fixture.reader();
        let (respond_to, reply) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::Deposit {
            user_id: "buyer".into(),
            amount: 10,
            respond_to,
        })
        .unwrap();
        runtime.run();
        assert!(
            reply
                .blocking_recv()
                .unwrap()
                .unwrap_err()
                .contains("publication")
        );
        assert!(tx.is_closed());
        assert!(reader.next_batch().unwrap().is_none());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        assert_eq!(runtime.core.balance_view("buyer").balance, 10);
        assert_eq!(reader.next_batch().unwrap().unwrap(), runtime.event_log());
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn a_durable_exchange_survives_a_restart_and_continues_both_sequences() {
        let path = temp_log_path("restart");

        // First run: fund a buyer, rest a sell, partially fill it.
        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();

            for command in [
                open_market(),
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-1", "seller", "SELL", 10, 10),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("buy-1", "buyer", "BUY", 10, 5),
                },
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }

            assert_eq!(runtime.event_log().len(), 12);
        }

        // Second run: same file, brand-new process state.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted = recover_runtime(rx, &path).unwrap();

        assert_eq!(restarted.event_log().len(), 12);
        // The session came back too: the market is still open for trading day 1.
        assert_eq!(
            restarted.core.session_view(),
            SessionView {
                trading_day: Some(trading_day(1)),
                open: true
            }
        );

        // Wallet, locks and order state all came back.
        let balance = restarted.core.balance_view("buyer");
        assert_eq!(balance.balance, 950);
        assert_eq!(balance.locked, 0);
        assert_eq!(restarted.core.balance_view("seller").balance, 50);

        // So did both share ledgers: the seller delivered 5 and still has 5 reserved behind the
        // resting half of the order, and the buyer is holding what it bought.
        let seller_position = &restarted.core.position_views("seller")[0];
        assert_eq!(seller_position.symbol, "AAPL");
        assert_eq!(seller_position.quantity, 5);
        assert_eq!(seller_position.locked, 5);
        assert_eq!(seller_position.available, 0);
        assert_eq!(restarted.core.position_views("buyer")[0].quantity, 5);

        let resting = restarted.core.order_view("sell-1", "seller").unwrap();
        assert_eq!(resting.status, "partially_filled");
        assert_eq!(resting.remaining_quantity, 5);

        // The order book survived too.
        let book = restarted.core.l2_snapshot("AAPL", 10).unwrap();
        assert_eq!(
            book.asks,
            vec![crate::types::types::L2Level {
                price: 10,
                quantity: 5
            }]
        );

        // Cancelling the resting quantity works, and both counters continue where they left off:
        // event sequences 13 and 14, matching sequence 3.
        let (respond_to, response_rx) = oneshot::channel();
        restarted
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(response_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(restarted.event_log().len(), 14);
        assert_eq!(restarted.event_log()[12].seq_num, 13);

        match &restarted.event_log()[13].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(*seq_num, 3);
            }
            other => panic!("unexpected event: {:?}", other),
        }
        drop(restarted);

        // And the cancellation itself is durable: a third start sees it, with the seller's
        // reservation released back to available.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let third = recover_runtime(rx, &path).unwrap();
        assert_eq!(third.event_log().len(), 14);
        assert_eq!(
            third.core.order_view("sell-1", "seller").unwrap().status,
            "canceled"
        );

        let seller_position = &third.core.position_views("seller")[0];
        assert_eq!(seller_position.quantity, 5);
        assert_eq!(seller_position.locked, 0);
        assert_eq!(seller_position.available, 5);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn snapshot_recovery_replays_only_the_suffix_and_continues_both_sequences() {
        let path = temp_log_path("snapshot-suffix");
        let stream = path.with_extension("mmap");
        let snapshot = path.with_extension("snapshot");
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&snapshot);

        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime =
                recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();
            for input in [
                open_market(),
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 100,
                },
                share_deposit("seller-a", 5),
                share_deposit("seller-b", 5),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-a", "seller-a", "SELL", 10, 5),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-b", "seller-b", "SELL", 10, 5),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("buy", "buyer", "BUY", 10, 7),
                },
            ] {
                runtime.record_and_process_input_event(input).unwrap();
            }
            runtime.write_snapshot(&snapshot, &stream).unwrap();
            runtime
                .record_and_process_input_event(ExchangeInputEvent::CancelOrderRequested {
                    order_id: "sell-b".into(),
                    user_id: "seller-b".into(),
                })
                .unwrap();
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted =
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();

        // The checkpoint already owns the first seven commands; only the cancellation is held and
        // replayed as the suffix. The full history remains in the journal for subscribers.
        assert_eq!(restarted.event_log().len(), 2);
        // The seven checkpointed commands produced eighteen envelopes: the crossing buy emits two
        // execution events for each fill as well as its accepted event. The cancellation begins
        // the two-envelope suffix at event sequence 19.
        assert_eq!(restarted.event_log()[0].seq_num, 19);
        assert!(restarted.core.is_market_open());
        assert_eq!(
            restarted
                .core
                .order_view("sell-b", "seller-b")
                .unwrap()
                .status,
            "canceled"
        );
        assert_eq!(restarted.core.balance_view("buyer").balance, 30);
        assert_eq!(
            restarted
                .core
                .execution_views("buyer", None, None, None, None)
                .len(),
            2
        );
        assert!(
            restarted
                .core
                .l2_snapshot("AAPL", 10)
                .unwrap()
                .asks
                .is_empty()
        );

        restarted
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("next", "buyer", "BUY", 9, 1),
            })
            .unwrap();
        match &restarted.event_log()[3].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderAccepted { seq_num, .. }) => {
                assert_eq!(*seq_num, 5);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        drop(restarted);
        let _ = std::fs::remove_file(&snapshot);
        let _ = std::fs::remove_file(&stream);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn corrupt_snapshot_is_preserved_and_startup_falls_back_to_full_replay() {
        let path = temp_log_path("corrupt-snapshot");
        let stream = path.with_extension("mmap");
        let snapshot = path.with_extension("snapshot");
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&snapshot);
        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();
            runtime
                .record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 10,
                })
                .unwrap();
        }
        let corrupt = b"not an exchange snapshot";
        std::fs::write(&snapshot, corrupt).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let restarted =
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();
        assert_eq!(restarted.event_log().len(), 2);
        assert_eq!(restarted.core.balance_view("buyer").balance, 10);
        assert_eq!(std::fs::read(&snapshot).unwrap(), corrupt);

        drop(restarted);
        let _ = std::fs::remove_file(&snapshot);
        let _ = std::fs::remove_file(&stream);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_append_cannot_advance_or_replace_a_snapshot() {
        let path = temp_log_path("snapshot-before-commit");
        let stream = path.with_extension("mmap");
        let snapshot = path.with_extension("snapshot");
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&snapshot);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime =
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        runtime.store = Some(EventStore::open_read_only_for_test(&path).unwrap());

        assert!(matches!(
            runtime.record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            }),
            Err(RuntimeFailure::Store(_))
        ));
        assert_eq!(std::fs::read(&snapshot).unwrap(), before);
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered = recover_runtime(rx, &path).unwrap();
        assert_eq!(recovered.core.balance_view("buyer").balance, 0);

        drop(recovered);
        let _ = std::fs::remove_file(&snapshot);
        let _ = std::fs::remove_file(&stream);
        std::fs::remove_file(path).unwrap();
    }

    /// Queues commands the way a burst of HTTP clients would, before the worker looks.
    fn queue<T>(
        commands: &mut Vec<ExchangeCommand>,
        make: impl FnOnce(oneshot::Sender<T>) -> ExchangeCommand,
    ) -> oneshot::Receiver<T> {
        let (respond_to, reply) = oneshot::channel();
        commands.push(make(respond_to));
        reply
    }

    #[test]
    fn a_queued_group_shares_one_sync_and_its_reads_see_earlier_writes() {
        let path = temp_log_path("group-commit");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime = recover_runtime(rx, &path).unwrap();
        let mut group = Vec::new();
        let open = queue(&mut group, |respond_to| ExchangeCommand::OpenMarket {
            trading_day: trading_day(1),
            respond_to,
        });
        let deposit = queue(&mut group, |respond_to| ExchangeCommand::Deposit {
            user_id: "buyer".into(),
            amount: 1_000,
            respond_to,
        });
        let shares = queue(&mut group, |respond_to| ExchangeCommand::DepositShares {
            user_id: "seller".into(),
            symbol: "AAPL".into(),
            quantity: 5,
            respond_to,
        });
        let sell = queue(&mut group, |respond_to| ExchangeCommand::PlaceOrder {
            order: order("sell-1", "seller", "SELL", 10, 5),
            respond_to,
        });
        let buy = queue(&mut group, |respond_to| ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 5),
            respond_to,
        });
        let balance = queue(&mut group, |respond_to| ExchangeCommand::GetBalance {
            user_id: "buyer".into(),
            respond_to,
        });

        runtime.handle_group(group.into_iter()).unwrap();

        // Five writes, one sync. The orders queued after the open see the market open.
        assert_eq!(runtime.store.as_ref().unwrap().syncs, 1);
        assert!(open.blocking_recv().unwrap().unwrap().open);
        deposit.blocking_recv().unwrap().unwrap();
        shares.blocking_recv().unwrap().unwrap();
        assert_eq!(sell.blocking_recv().unwrap().unwrap().status, "new");
        assert_eq!(buy.blocking_recv().unwrap().unwrap().status, "filled");
        // The read queued last sees the fill staged before it in the same group.
        assert_eq!(balance.blocking_recv().unwrap().balance, 950);

        let live = runtime.core_snapshot_for_test();
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered = recover_runtime(rx, &path).unwrap();
        assert_eq!(recovered.core_snapshot_for_test(), live);
        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_failed_group_sync_answers_every_command_unavailable_and_publishes_nothing() {
        use crate::exchange::event_stream::tests::Fixture;
        let fixture = Fixture::new();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let mut reader = fixture.reader();
        runtime.store = Some(EventStore::open_read_only_for_test(&fixture.log).unwrap());
        let mut group = Vec::new();
        let deposit = queue(&mut group, |respond_to| ExchangeCommand::Deposit {
            user_id: "buyer".into(),
            amount: 1_000,
            respond_to,
        });
        let balance = queue(&mut group, |respond_to| ExchangeCommand::GetBalance {
            user_id: "buyer".into(),
            respond_to,
        });

        assert!(matches!(
            runtime.handle_group(group.into_iter()),
            Err(RuntimeFailure::Store(_))
        ));

        assert!(
            deposit
                .blocking_recv()
                .unwrap()
                .unwrap_err()
                .starts_with("exchange unavailable:")
        );
        // The read would have shown a balance of 1,000 that never reached the disk. It is
        // dropped instead, which the gateway reports as unavailable.
        assert!(balance.blocking_recv().is_err());
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn a_fault_mid_group_still_syncs_the_commands_before_it_and_drops_those_after() {
        let path = temp_log_path("group-fault");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut runtime = recover_runtime(rx, &path).unwrap();
        for input in [
            open_market(),
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 10,
            },
            share_deposit("seller", 1),
            ExchangeInputEvent::NewOrderRequested {
                order: order("sell-1", "seller", "SELL", 10, 1),
            },
        ] {
            runtime.record_and_process_input_event(input).unwrap();
        }
        // No command can reach a balance that a fill overflows, so the live core is forced there.
        // The total cash is then past its limit, so the group's other commands deposit shares.
        runtime.core.force_balance_for_test("seller", u64::MAX);
        let mut group = Vec::new();
        let before = queue(&mut group, |respond_to| ExchangeCommand::DepositShares {
            user_id: "other".into(),
            symbol: "AAPL".into(),
            quantity: 7,
            respond_to,
        });
        // Crediting a seller who holds u64::MAX is an internal fault, not a rejection.
        let faulting = queue(&mut group, |respond_to| ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 1),
            respond_to,
        });
        let after = queue(&mut group, |respond_to| ExchangeCommand::DepositShares {
            user_id: "late".into(),
            symbol: "AAPL".into(),
            quantity: 3,
            respond_to,
        });

        assert!(matches!(
            runtime.handle_group(group.into_iter()),
            Err(RuntimeFailure::Internal(_))
        ));

        before.blocking_recv().unwrap().unwrap();
        assert!(
            faulting
                .blocking_recv()
                .unwrap()
                .unwrap_err()
                .starts_with("exchange unavailable:")
        );
        assert!(after.blocking_recv().is_err());
        drop(runtime);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered = recover_runtime(rx, &path).unwrap();
        assert_eq!(recovered.core.position_views("other")[0].quantity, 7);
        assert!(recovered.core.position_views("late").is_empty());
        assert!(recovered.core.order_view("buy-1", "buyer").is_none());
        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn risk_limits_and_executions_survive_a_restart() {
        let path = temp_log_path("risk-and-fills");

        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();

            for command in [
                open_market(),
                ExchangeInputEvent::RiskLimitSetRequested {
                    user_id: "buyer".to_string(),
                    symbol: "AAPL".to_string(),
                    max_daily_quantity: 8,
                },
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("sell-1", "seller", "SELL", 10, 10),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("buy-1", "buyer", "BUY", 10, 5),
                },
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let restarted = recover_runtime(rx, &path).unwrap();

        // The limit was set by an event, so replay rebuilds it — a limit read from configuration
        // could not have survived this, and worse, would have replayed differently elsewhere.
        let limit = restarted.core.risk_limit_view("buyer", "AAPL");
        assert_eq!(limit.max_daily_quantity, 8);
        assert_eq!(limit.used_today, 5);

        // Fills are rebuilt by settlement running again during replay, for both parties.
        let buyer_fills = restarted
            .core
            .execution_views("buyer", None, None, None, None);
        assert_eq!(buyer_fills.len(), 1);
        assert_eq!(buyer_fills[0].order_id, "buy-1");
        assert_eq!(buyer_fills[0].quantity, 5);

        assert_eq!(
            restarted
                .core
                .execution_views("seller", None, None, None, None)
                .len(),
            1
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn the_close_expires_resting_orders_and_a_restart_continues_the_next_day() {
        let path = temp_log_path("close-expiry");

        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime = recover_runtime(rx, &path).unwrap();
            for command in [
                ExchangeInputEvent::RiskLimitSetRequested {
                    user_id: "buyer".to_string(),
                    symbol: "AAPL".to_string(),
                    max_daily_quantity: 10,
                },
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".to_string(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
                open_market(),
                ExchangeInputEvent::NewOrderRequested {
                    order: order("resting-buy", "buyer", "BUY", 10, 10),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order("partial-sell", "seller", "SELL", 10, 4),
                },
                ExchangeInputEvent::MarketCloseRequested,
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }

            // The close's record: the close, then the buy's unfilled six expiring with the next
            // matching sequence after the two orders.
            let close = runtime.event_log().len() - 3;
            let outputs = outputs_of(&runtime);
            assert_eq!(
                outputs[outputs.len() - 2..],
                [
                    ExchangeOutputEvent::MarketClosed {
                        trading_day: trading_day(1)
                    },
                    ExchangeOutputEvent::OrderExpired {
                        order_id: "resting-buy".into(),
                        seq_num: 3
                    },
                ]
            );
            assert!(matches!(
                runtime.event_log()[close].event,
                ExchangeEvent::Input(ExchangeInputEvent::MarketCloseRequested)
            ));
            // Its collateral is free, and only the four that traded still count for the day.
            assert_eq!(runtime.core.balance_view("buyer").locked, 0);
            assert_eq!(runtime.core.risk_limit_view("buyer", "AAPL").used_today, 4);
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted = recover_runtime(rx, &path).unwrap();
        let expired = restarted.core.order_view("resting-buy", "buyer").unwrap();
        assert_eq!(
            (expired.status.as_str(), expired.remaining_quantity),
            ("expired", 6)
        );
        let refused = restarted
            .record_and_process_input_event(ExchangeInputEvent::CancelOrderRequested {
                order_id: "resting-buy".to_string(),
                user_id: "buyer".to_string(),
            })
            .unwrap()
            .into_cancel_order_result();
        assert!(refused.unwrap_err().contains("already Expired"));

        // The next day starts with the whole allowance and continues the matching sequence.
        restarted
            .record_and_process_input_event(ExchangeInputEvent::MarketOpenRequested {
                trading_day: trading_day(2),
            })
            .unwrap();
        assert_eq!(
            restarted.core.risk_limit_view("buyer", "AAPL").used_today,
            0
        );
        restarted
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("next-day", "buyer", "BUY", 10, 10),
            })
            .unwrap()
            .into_place_order_result()
            .unwrap();
        assert_eq!(
            outputs_of(&restarted).last(),
            Some(&ExchangeOutputEvent::OrderAccepted {
                order_id: "next-day".into(),
                seq_num: 4
            })
        );
        let live = restarted.core_snapshot_for_test();
        drop(restarted);

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered_again = recover_runtime(rx, &path).unwrap();
        assert_eq!(recovered_again.core_snapshot_for_test(), live);
        drop(recovered_again);
        std::fs::remove_file(&path).unwrap();
    }

    /// The size of a command's record payload in the journal's format, with every envelope sequence
    /// either at its widest or numbered from 1. It serializes directly rather than through
    /// `encode_record`, which refuses a payload over the limit, so a size test can see one.
    fn record_len(
        input: &ExchangeInputEvent,
        outputs: &[ExchangeOutputEvent],
        widest: bool,
    ) -> usize {
        let batch: Vec<_> = std::iter::once(ExchangeEvent::Input(input.clone()))
            .chain(outputs.iter().cloned().map(ExchangeEvent::Output))
            .enumerate()
            .map(|(index, event)| EventEnvelope {
                seq_num: if widest { u64::MAX } else { 1 + index as u64 },
                event,
            })
            .collect();
        serde_json::to_vec(&batch).unwrap().len()
    }

    #[test]
    fn a_client_order_id_returns_on_the_next_day_and_every_recovery_agrees() {
        let path = temp_log_path("ids-per-day");
        let stream = path.with_extension("mmap");
        let snapshot = path.with_extension("snapshot");
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&snapshot);
        let place = |runtime: &mut ExchangeRuntime, order: Order| {
            runtime
                .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested { order })
                .unwrap()
                .into_place_order_result()
        };

        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let mut runtime =
                recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();
            for input in [
                open_market(),
                ExchangeInputEvent::FundsDepositRequested {
                    user_id: "buyer".into(),
                    amount: 1_000,
                },
                share_deposit("seller", 10),
            ] {
                runtime.record_and_process_input_event(input).unwrap();
            }
            place(&mut runtime, order("same", "seller", "SELL", 10, 5)).unwrap();
            place(&mut runtime, order("buy", "buyer", "BUY", 10, 5)).unwrap();
            // Within the day the id is taken, even though its order has finished.
            let retry = place(&mut runtime, order("same", "seller", "SELL", 10, 1));
            assert!(retry.unwrap_err().contains("AlreadyExists"));

            runtime
                .record_and_process_input_event(ExchangeInputEvent::MarketCloseRequested)
                .unwrap();
            // A snapshot of the closed day: it still holds that day's orders, which the open
            // replayed after it must clear.
            runtime.write_snapshot(&snapshot, &stream).unwrap();
            runtime
                .record_and_process_input_event(ExchangeInputEvent::MarketOpenRequested {
                    trading_day: trading_day(2),
                })
                .unwrap();
            // On the next day it is free again.
            assert_eq!(
                place(&mut runtime, order("same", "seller", "SELL", 11, 1))
                    .unwrap()
                    .status,
                "new"
            );
            runtime
                .record_and_process_input_event(ExchangeInputEvent::CancelOrderRequested {
                    order_id: "same".into(),
                    user_id: "seller".into(),
                })
                .unwrap()
                .into_cancel_order_result()
                .unwrap();
        }

        // A restart from the snapshot replays only the open, the reused id and the cancellation,
        // and reaches the same state as replaying the whole journal.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let from_snapshot =
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot).unwrap();
        assert_eq!(from_snapshot.event_log().len(), 6);
        let restored = from_snapshot.core_snapshot_for_test();
        drop(from_snapshot);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let replayed = recover_runtime(rx, &path).unwrap();
        assert_eq!(replayed.core_snapshot_for_test(), restored);
        assert_eq!(
            replayed.core.order_view("same", "seller").unwrap().status,
            "canceled"
        );
        assert!(replayed.core.order_view("buy", "buyer").is_none());

        drop(replayed);
        let _ = std::fs::remove_file(&snapshot);
        let _ = std::fs::remove_file(&stream);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_close_too_large_for_one_record_is_refused_and_changes_nothing() {
        let mut core = ExchangeCore::new();
        core.open_market(trading_day(1)).unwrap();
        core.deposit_shares("seller", "AAPL", 3).unwrap();
        for id in ["a", "b", "c"] {
            core.add_order(order(id, "seller", "SELL", 10, 1)).unwrap();
        }
        let limit = MAX_RECORD_LEN as usize;
        let outputs = prepare_close(&core, limit).unwrap().output_events;
        assert_eq!(outputs.len(), 4);
        let close = ExchangeInputEvent::MarketCloseRequested;
        let widest = record_len(&close, &outputs, true);

        // The check is exact at the widest sequences, and a real record is always smaller, so a
        // close that passes it can be written wherever it lands in the journal.
        assert!(record_len(&close, &outputs, false) < widest);
        assert!(matches!(
            prepare_close(&core, widest).unwrap().commit,
            PreparedCommit::CloseMarket(_)
        ));
        let refused = prepare_close(&core, widest - 1).unwrap();
        assert_eq!(
            refused.output_events,
            [ExchangeOutputEvent::SessionRejected {
                reason: "TooManyRestingOrders(3)".into()
            }]
        );
        let before = core.snapshot();
        assert_eq!(
            refused.commit(&mut core).0.into_session_result(),
            Err("TooManyRestingOrders(3)".into())
        );
        assert_eq!(core.snapshot(), before);
        assert!(core.is_market_open());
    }

    /// The cap's promise at full scale: a book full of the longest ids the gateway allows, every
    /// byte of them escaped in JSON, still closes in one record. Heavy; run it in release:
    /// `cargo test --release --bin stock -- --ignored full_book`.
    #[test]
    #[ignore = "builds 200,000 resting orders; run in release"]
    fn a_full_book_of_the_longest_ids_closes_in_one_record() {
        use crate::types::{matching_engine::MAX_RESTING_ORDERS, order_manager::OrderManagerError};

        // 64 characters, each `"` or `\`, so each takes two bytes in JSON; the bits make it unique.
        let id = |n: usize| -> String {
            (0..64)
                .map(|bit| if (n >> bit) & 1 == 1 { '\\' } else { '"' })
                .collect()
        };
        let mut core = ExchangeCore::new();
        core.open_market(trading_day(1)).unwrap();
        core.deposit_shares("seller", "AAPL", MAX_RESTING_ORDERS as u64 + 1)
            .unwrap();
        for n in 0..MAX_RESTING_ORDERS {
            core.add_order(order(&id(n), "seller", "SELL", 10, 1))
                .unwrap();
        }
        assert!(matches!(
            core.add_order(order(&id(MAX_RESTING_ORDERS), "seller", "SELL", 10, 1)),
            Err(OrderManagerError::BookFull)
        ));

        let started = std::time::Instant::now();
        let prepared =
            prepare_input_event(&core, ExchangeInputEvent::MarketCloseRequested).unwrap();
        let prepared_in = started.elapsed();
        assert_eq!(prepared.output_events.len(), MAX_RESTING_ORDERS + 1);
        let close = ExchangeInputEvent::MarketCloseRequested;
        let real = record_len(&close, &prepared.output_events, false);
        let widest = record_len(&close, &prepared.output_events, true);
        println!(
            "close of {MAX_RESTING_ORDERS} worst-case orders: prepared in {prepared_in:?}, record {real} bytes ({widest} at the widest sequences, limit {MAX_RECORD_LEN})"
        );
        assert!(widest <= MAX_RECORD_LEN as usize);

        let _ = prepared.commit(&mut core);
        assert!(!core.is_market_open());
        assert!(core.l2_snapshot("AAPL", 1).unwrap().asks.is_empty());
    }

    /// The fill cap's promise: an order that trades against `MAX_FILLS_PER_ORDER` resting orders,
    /// with every field at its widest (the gateway's longest ids and symbol, every byte escaped in
    /// JSON, and the largest numbers), still fits in one record.
    #[test]
    fn the_largest_order_the_fill_cap_allows_fits_in_one_record() {
        use crate::types::{
            matching_engine::MAX_FILLS_PER_ORDER,
            types::{Execution, Price, Side},
        };

        // 64 characters that JSON escapes to two bytes each: the longest id the gateway lets in.
        let widest_id = "\"".repeat(64);
        let price = Price::new(u64::MAX).unwrap();
        // The longest number JSON prints for an `f64`.
        let timestamp = -f64::MIN_POSITIVE;
        let input = ExchangeInputEvent::NewOrderRequested {
            order: Order {
                order_id: widest_id.clone(),
                user_id: widest_id.clone(),
                symbol: widest_id.clone(),
                side: Side::Sell,
                price,
                quantity: u32::MAX,
                leaves_qty: u32::MAX,
                timestamp,
                seq_num: u64::MAX,
            },
        };
        let accepted = ExchangeOutputEvent::OrderAccepted {
            order_id: widest_id.clone(),
            seq_num: u64::MAX,
        };
        let execution = ExchangeOutputEvent::ExecutionCreated {
            execution: Execution {
                execution_id: format!("exec_{}", u64::MAX),
                buy_order_id: widest_id.clone(),
                sell_order_id: widest_id.clone(),
                symbol: widest_id,
                price,
                quantity: u32::MAX,
                timestamp,
            },
        };
        let outputs: Vec<_> = std::iter::once(accepted)
            .chain(std::iter::repeat_n(execution, 2 * MAX_FILLS_PER_ORDER))
            .collect();

        let widest = record_len(&input, &outputs, true);
        let one_trade = (widest - record_len(&input, &outputs[..1], true)) / MAX_FILLS_PER_ORDER;
        println!(
            "{MAX_FILLS_PER_ORDER} trades at the widest: record {widest} bytes, {one_trade} per trade (limit {MAX_RECORD_LEN})"
        );
        assert!(widest <= MAX_RECORD_LEN as usize);
    }

    /// Through the input path that live trading, replay and the warm replica share: an order that
    /// would take one resting order more than the cap is an ordinary rejection, recorded like any
    /// other, and changes nothing; an order at the cap trades.
    #[test]
    fn an_order_beyond_the_fill_cap_is_an_ordinary_rejection_and_changes_nothing() {
        use crate::types::matching_engine::MAX_FILLS_PER_ORDER;

        let resting = MAX_FILLS_PER_ORDER + 1;
        let mut core = ExchangeCore::new();
        core.open_market(trading_day(1)).unwrap();
        core.deposit_shares("seller", "AAPL", resting as u64)
            .unwrap();
        core.deposit("buyer".to_string(), 10 * resting as u64)
            .unwrap();
        for n in 0..resting {
            core.add_order(order(&format!("ask-{n}"), "seller", "SELL", 10, 1))
                .unwrap();
        }
        let before = core.snapshot();
        let buy = |id: &str, quantity: usize| ExchangeInputEvent::NewOrderRequested {
            order: order(id, "buyer", "BUY", 10, quantity as u32),
        };

        let refused = prepare_input_event(&core, buy("sweep", resting)).unwrap();
        assert_eq!(
            refused.output_events,
            [ExchangeOutputEvent::OrderRejected {
                order_id: "sweep".into(),
                reason: "TooManyFills".into()
            }]
        );
        assert_eq!(
            refused.commit(&mut core).0.into_place_order_result(),
            Err("TooManyFills".into())
        );
        assert_eq!(core.snapshot(), before);

        let at_cap = prepare_input_event(&core, buy("at-cap", MAX_FILLS_PER_ORDER)).unwrap();
        assert_eq!(at_cap.output_events.len(), 1 + 2 * MAX_FILLS_PER_ORDER);
    }

    #[test]
    fn a_rejected_order_is_recorded_so_replay_reproduces_the_rejection() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        runtime
            .record_and_process_input_event(open_market())
            .unwrap();
        runtime
            .record_and_process_input_event(ExchangeInputEvent::RiskLimitSetRequested {
                user_id: "buyer".to_string(),
                symbol: "AAPL".to_string(),
                max_daily_quantity: 3,
            })
            .unwrap();

        runtime
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order("too-big", "buyer", "BUY", 10, 4),
            })
            .unwrap();

        match &runtime.event_log()[5].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderRejected { order_id, reason }) => {
                assert_eq!(order_id, "too-big");
                assert!(reason.contains("RiskRejected"));
            }
            other => panic!("unexpected event: {:?}", other),
        }

        // The rejection is part of history, so replaying it regenerates the same refusal.
        replay_event_log(runtime.event_log()).unwrap();
    }

    #[test]
    fn startup_refuses_history_that_does_not_replay() {
        let path = temp_log_path("mismatch");

        // A structurally perfect record — correct framing, correct checksum — whose recorded
        // outcome disagrees with what the core regenerates: the deposit asks for 1000, the stored
        // outcome claims 999. Checksums cannot catch this; only deterministic replay can.
        {
            let (mut store, _) = EventStore::open(&path).unwrap();
            store
                .append(&[
                    EventEnvelope {
                        seq_num: 1,
                        event: ExchangeEvent::Input(ExchangeInputEvent::FundsDepositRequested {
                            user_id: "buyer".to_string(),
                            amount: 1_000,
                        }),
                    },
                    EventEnvelope {
                        seq_num: 2,
                        event: ExchangeEvent::Output(ExchangeOutputEvent::FundsDeposited {
                            user_id: "buyer".to_string(),
                            amount: 999,
                        }),
                    },
                ])
                .unwrap();
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        assert!(matches!(
            recover_runtime(rx, &path),
            Err(StartupError::Replay(_))
        ));

        let bus = path.with_extension("mmap");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        assert!(matches!(
            recover_runtime_with_stream(rx, &path, &bus),
            Err(StartupError::Replay(_))
        ));
        assert!(!bus.exists(), "unvalidated history must never be published");

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_users_order_is_not_readable_by_anyone_else() {
        let mut runtime = open_runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

        let (respond_to, _response_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::PlaceOrder {
                order: order("buy-1", "buyer", "BUY", 10, 10),
                respond_to,
            })
            .unwrap();

        let (respond_to, order_rx) = oneshot::channel();
        runtime
            .handle_command(ExchangeCommand::GetOrder {
                order_id: "buy-1".to_string(),
                user_id: "someone-else".to_string(),
                respond_to,
            })
            .unwrap();

        assert!(order_rx.blocking_recv().unwrap().is_none());
    }

    fn outputs_of(runtime: &ExchangeRuntime) -> Vec<ExchangeOutputEvent> {
        runtime
            .event_log()
            .iter()
            .filter_map(|envelope| match &envelope.event {
                ExchangeEvent::Output(output) => Some(output.clone()),
                ExchangeEvent::Input(_) => None,
            })
            .collect()
    }

    #[test]
    fn orders_are_refused_while_the_market_is_closed_and_replay_agrees() {
        let mut runtime = runtime();
        let place = |runtime: &mut ExchangeRuntime, id: &str| {
            runtime
                .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                    order: order(id, "buyer", "BUY", 10, 1),
                })
                .unwrap()
                .into_place_order_result()
                .map(|view| view.order_id)
        };

        // A new exchange has never opened. Cash moves at any time; orders do not.
        runtime
            .record_and_process_input_event(ExchangeInputEvent::FundsDepositRequested {
                user_id: "buyer".into(),
                amount: 100,
            })
            .unwrap()
            .into_deposit_result()
            .unwrap();
        assert_eq!(place(&mut runtime, "early"), Err(MARKET_CLOSED.to_string()));

        runtime
            .record_and_process_input_event(open_market())
            .unwrap();
        assert_eq!(place(&mut runtime, "during"), Ok("during".to_string()));

        runtime
            .record_and_process_input_event(ExchangeInputEvent::MarketCloseRequested)
            .unwrap();
        assert_eq!(place(&mut runtime, "late"), Err(MARKET_CLOSED.to_string()));

        let refused = |id: &str| ExchangeOutputEvent::OrderRejected {
            order_id: id.into(),
            reason: MARKET_CLOSED.into(),
        };
        // Deposited, refused, opened, accepted, closed with the accepted order expiring, refused.
        let outputs = outputs_of(&runtime);
        assert_eq!(outputs[1], refused("early"));
        assert_eq!(
            outputs[4..6],
            [
                ExchangeOutputEvent::MarketClosed {
                    trading_day: trading_day(1)
                },
                ExchangeOutputEvent::OrderExpired {
                    order_id: "during".into(),
                    seq_num: 2
                },
            ]
        );
        assert_eq!(outputs[6], refused("late"));

        // The refusals are history: replay regenerates exactly the same outcomes and state.
        let replayed = replay_event_log(runtime.event_log()).unwrap();
        assert_eq!(replayed.snapshot(), runtime.core_snapshot_for_test());
        assert!(!replayed.is_market_open());
    }

    #[test]
    fn refused_session_changes_are_journaled_and_change_nothing() {
        let mut runtime = runtime();
        let session = |runtime: &mut ExchangeRuntime, input| {
            runtime
                .record_and_process_input_event(input)
                .unwrap()
                .into_session_result()
        };
        let open_on = |day| ExchangeInputEvent::MarketOpenRequested {
            trading_day: trading_day(day),
        };

        assert_eq!(
            session(&mut runtime, ExchangeInputEvent::MarketCloseRequested),
            Err("AlreadyClosed".to_string())
        );
        assert!(session(&mut runtime, open_on(2)).is_ok());
        assert_eq!(
            session(&mut runtime, open_on(3)),
            Err("AlreadyOpen".to_string())
        );
        assert!(session(&mut runtime, ExchangeInputEvent::MarketCloseRequested).is_ok());
        let backwards = session(&mut runtime, open_on(1)).unwrap_err();
        assert!(backwards.starts_with("NotAfterLastTradingDay"));

        assert_eq!(
            outputs_of(&runtime)
                .iter()
                .filter(|output| matches!(output, ExchangeOutputEvent::SessionRejected { .. }))
                .count(),
            3
        );
        assert_eq!(
            runtime.core.session_view(),
            SessionView {
                trading_day: Some(trading_day(2)),
                open: false
            }
        );
        let replayed = replay_event_log(runtime.event_log()).unwrap();
        assert_eq!(replayed.session_view(), runtime.core.session_view());
    }
}
