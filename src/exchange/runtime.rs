use std::path::{Path, PathBuf};

use crate::{
    exchange::{
        core::{CoreError, ExchangeCore, PreparedAddOrder, PreparedCancelOrder},
        event_store::{EventStore, EventStoreError, encode_record},
        event_stream::{DEFAULT_CAPACITY, StreamWriter},
        snapshot::{self, SnapshotBoundary},
    },
    types::{
        exchange_event::{EventEnvelope, ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::{ExchangeCommand, OrderView},
    },
};

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
    snapshot: Option<SnapshotSchedule>,
}

/// Snapshotting is intentionally periodic: writing a whole authoritative-state checkpoint for
/// every order would turn a recovery optimization into a new trading-path bottleneck.
struct SnapshotSchedule {
    path: PathBuf,
    stream_path: PathBuf,
    every_commands: u64,
    commands_since_snapshot: u64,
}

pub const DEFAULT_SNAPSHOT_INTERVAL: u64 = 10_000;

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

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> crate::exchange::core::CoreSnapshot {
        self.core.snapshot()
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
}

fn prepare_input_event(
    core: &ExchangeCore,
    event: ExchangeInputEvent,
) -> Result<PreparedInput, CoreError> {
    match event {
        ExchangeInputEvent::FundsDepositRequested { user_id, amount } => {
            match core.validate_deposit(&user_id, amount) {
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
        } => match core.validate_share_deposit(&user_id, &symbol, quantity) {
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
    }
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
            snapshot: None,
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
            snapshot: None,
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
            snapshot: None,
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
            journal_device: {
                use std::os::unix::fs::MetadataExt;
                metadata.dev()
            },
            journal_inode: {
                use std::os::unix::fs::MetadataExt;
                metadata.ino()
            },
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
    }

    /// Counts a durable group of `commands` toward the schedule and writes at most one snapshot
    /// for it: every command in the group is already in the core, so one checkpoint covers them.
    fn maybe_write_snapshot(&mut self, commands: u64) {
        let Some((path, stream_path)) = self.snapshot.as_mut().and_then(|schedule| {
            schedule.commands_since_snapshot =
                schedule.commands_since_snapshot.saturating_add(commands);
            if schedule.commands_since_snapshot < schedule.every_commands {
                return None;
            }
            // A failed write cannot invalidate the journal or the previous snapshot. Retry after
            // another full interval rather than making a disk failure stall every later command.
            schedule.commands_since_snapshot = 0;
            Some((schedule.path.clone(), schedule.stream_path.clone()))
        }) else {
            return;
        };
        if let Err(error) = self.write_snapshot(&path, &stream_path) {
            eprintln!("snapshot checkpoint was not updated: {error}");
        }
    }

    pub fn run(mut self) {
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
            store
                .append_record(&records.concat())
                .map_err(RuntimeFailure::Store)?;
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
        let commands = staged.len();
        let flushed = self.flush(staged);
        let failure = flushed.as_ref().err().map(halted_message);
        for reply in replies {
            reply(failure.as_deref());
        }
        flushed?;
        self.maybe_write_snapshot(commands as u64);
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
        self.maybe_write_snapshot(1);
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
    let stream = StreamWriter::open(
        stream_path,
        runtime.store.as_ref().unwrap().file(),
        runtime.next_event_seq - 1,
        DEFAULT_CAPACITY,
    )
    .map_err(StartupError::Stream)?;
    runtime.stream = Some(stream);
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
    snapshot_interval: u64,
) -> Result<ExchangeRuntime, StartupError> {
    if snapshot_interval == 0 {
        return Err(StartupError::Replay(ReplayError::InternalFault(
            "snapshot interval must be at least one command".to_string(),
        )));
    }
    let journal_path = journal_path.as_ref().to_path_buf();
    let stream_path = stream_path.as_ref().to_path_buf();
    let snapshot_path = snapshot_path.as_ref().to_path_buf();

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

    attach_stream_and_snapshot(
        &mut runtime,
        stream_path,
        snapshot_path,
        snapshot_interval,
        snapshot_is_safe_to_replace,
    )?;
    Ok(runtime)
}

/// Turns a read-only warm follower into the next primary after the caller has acquired the
/// journal's exclusive writer lock and recovered the *entire* authoritative journal. The warm
/// core is useful as a live follower health check, but mmap-delivered state never becomes primary
/// authority: full durable recovery is repeated before this process can write or publish.
pub(crate) fn promote_replica_with_stream_and_snapshot(
    rx: tokio::sync::mpsc::Receiver<ExchangeCommand>,
    store: EventStore,
    recovered: Vec<EventEnvelope>,
    journal_path: impl AsRef<Path>,
    stream_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
    snapshot_interval: u64,
) -> Result<ExchangeRuntime, StartupError> {
    if snapshot_interval == 0 {
        return Err(StartupError::Replay(ReplayError::InternalFault(
            "snapshot interval must be at least one command".to_string(),
        )));
    }
    let journal_path = journal_path.as_ref().to_path_buf();
    let stream_path = stream_path.as_ref().to_path_buf();
    let snapshot_path = snapshot_path.as_ref().to_path_buf();
    let mut runtime =
        ExchangeRuntime::from_store(rx, store, recovered).map_err(StartupError::Replay)?;

    // Preserve a bad snapshot for diagnosis just as ordinary primary recovery does. A valid or
    // absent checkpoint may be refreshed after promotion because the full journal is still held.
    let snapshot_is_safe_to_replace = match snapshot::load(&snapshot_path, &journal_path) {
        Ok(_) => true,
        Err(error) => {
            eprintln!("preserving invalid snapshot during warm promotion: {error}");
            false
        }
    };
    attach_stream_and_snapshot(
        &mut runtime,
        stream_path,
        snapshot_path,
        snapshot_interval,
        snapshot_is_safe_to_replace,
    )?;
    Ok(runtime)
}

fn attach_stream_and_snapshot(
    runtime: &mut ExchangeRuntime,
    stream_path: PathBuf,
    snapshot_path: PathBuf,
    snapshot_interval: u64,
    snapshot_is_safe_to_replace: bool,
) -> Result<(), StartupError> {
    let stream = StreamWriter::open(
        &stream_path,
        runtime.store.as_ref().unwrap().file(),
        runtime.next_event_seq - 1,
        DEFAULT_CAPACITY,
    )
    .map_err(StartupError::Stream)?;
    runtime.stream = Some(stream);
    if snapshot_is_safe_to_replace {
        runtime.snapshot = Some(SnapshotSchedule {
            path: snapshot_path,
            stream_path,
            every_commands: snapshot_interval,
            commands_since_snapshot: 0,
        });
        if let Some(schedule) = &runtime.snapshot
            && let Err(error) = runtime.write_snapshot(&schedule.path, &schedule.stream_path)
        {
            // This does not affect a durable, replayable exchange. Retain an older snapshot if
            // one exists and retry after the configured number of later commits.
            eprintln!("snapshot checkpoint was not updated during startup: {error}");
        }
    }
    Ok(())
}

fn recover_snapshot_state(
    journal_path: &Path,
    loaded: snapshot::LoadedSnapshot,
) -> Result<(EventStore, ExchangeCore, Vec<EventEnvelope>, u64), String> {
    let core = ExchangeCore::from_snapshot(loaded.core)?;
    let (store, suffix) = EventStore::open_suffix(journal_path, loaded.boundary.byte_offset)
        .map_err(|error| error.to_string())?;
    let core = replay_event_log_from_core(core, loaded.boundary.next_event_sequence, &suffix)
        .map_err(|error| format!("snapshot suffix did not replay deterministically: {error:?}"))?;
    let next_event_seq = suffix
        .last()
        .map(|envelope| {
            envelope
                .seq_num
                .checked_add(1)
                .ok_or_else(|| "journal sequence overflow".to_string())
        })
        .transpose()?
        .unwrap_or(loaded.boundary.next_event_sequence);
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
        order_at(id, user, side, price, quantity, 1.0)
    }

    fn order_at(
        id: &str,
        user: &str,
        side: &str,
        price: u64,
        quantity: u32,
        timestamp: f64,
    ) -> Order {
        Order::new(
            id.to_string(),
            user.to_string(),
            "AAPL".to_string(),
            side,
            price,
            quantity,
            None,
            timestamp,
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
        let mut runtime = runtime();

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
        let mut runtime = runtime();
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
        let mut runtime = runtime();
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
        let mut runtime = runtime();
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
        let mut runtime = runtime();

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

        assert_eq!(first_outputs.len(), 6);
        assert_eq!(first_outputs, second_outputs);
    }
    #[test]
    fn replay_rebuilds_matching_state_and_sequence() {
        let mut original = runtime();

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

        assert_eq!(original.event_log().len(), 10);

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
        assert_eq!(recorded_log.len(), 10);

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
        assert_eq!(recovered.event_log().len(), 12);

        assert_eq!(recovered.event_log()[10].seq_num, 11);
        assert_eq!(recovered.event_log()[11].seq_num, 12);

        match &recovered.event_log()[10].event {
            ExchangeEvent::Input(ExchangeInputEvent::CancelOrderRequested {
                order_id,
                user_id,
            }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(user_id, "seller");
            }
            other => panic!("unexpected event: {:?}", other),
        }

        match &recovered.event_log()[11].event {
            ExchangeEvent::Output(ExchangeOutputEvent::OrderCanceled { order_id, seq_num }) => {
                assert_eq!(order_id, "sell-1");
                assert_eq!(*seq_num, 3);
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn queries_do_not_append_to_the_event_log() {
        let mut runtime = runtime();
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
        let mut runtime = runtime();
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

            assert_eq!(runtime.event_log().len(), 10);
        }

        // Second run: same file, brand-new process state.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted = recover_runtime(rx, &path).unwrap();

        assert_eq!(restarted.event_log().len(), 10);

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
        // event sequences 9 and 10, matching sequence 3.
        let (respond_to, response_rx) = oneshot::channel();
        restarted
            .handle_command(ExchangeCommand::CancelOrder {
                order_id: "sell-1".to_string(),
                user_id: "seller".to_string(),
                respond_to,
            })
            .unwrap();

        assert_eq!(response_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(restarted.event_log().len(), 12);
        assert_eq!(restarted.event_log()[10].seq_num, 11);

        match &restarted.event_log()[11].event {
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
        assert_eq!(third.event_log().len(), 12);
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
                recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot, u64::MAX)
                    .unwrap();
            for input in [
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
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot, u64::MAX)
                .unwrap();

        // The checkpoint already owns the first six commands; only the cancellation is held and
        // replayed as the suffix. The full history remains in the journal for subscribers.
        assert_eq!(restarted.event_log().len(), 2);
        // The six checkpointed commands produced sixteen envelopes: the crossing buy emits two
        // execution events for each fill as well as its accepted event. The cancellation begins
        // the two-envelope suffix at event sequence 17.
        assert_eq!(restarted.event_log()[0].seq_num, 17);
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
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot, 1).unwrap();
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
            recover_runtime_with_stream_and_snapshot(rx, &path, &stream, &snapshot, 1).unwrap();
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

        // Four writes, one sync.
        assert_eq!(runtime.store.as_ref().unwrap().syncs, 1);
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
            ExchangeInputEvent::FundsDepositRequested {
                user_id: "seller".into(),
                amount: u64::MAX,
            },
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
        let mut group = Vec::new();
        let before = queue(&mut group, |respond_to| ExchangeCommand::Deposit {
            user_id: "other".into(),
            amount: 7,
            respond_to,
        });
        // Crediting a seller who already holds u64::MAX is an internal fault, not a rejection.
        let faulting = queue(&mut group, |respond_to| ExchangeCommand::PlaceOrder {
            order: order("buy-1", "buyer", "BUY", 10, 1),
            respond_to,
        });
        let after = queue(&mut group, |respond_to| ExchangeCommand::Deposit {
            user_id: "late".into(),
            amount: 3,
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
        assert_eq!(recovered.core.balance_view("other").balance, 7);
        assert_eq!(recovered.core.balance_view("late").balance, 0);
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
    fn overnight_risk_usage_replays_and_continues_after_restart() {
        let path = temp_log_path("overnight-risk");

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
                ExchangeInputEvent::NewOrderRequested {
                    order: order_at("overnight", "buyer", "BUY", 10, 10, 3_600.0),
                },
                ExchangeInputEvent::NewOrderRequested {
                    order: order_at("day-two-sell", "seller", "SELL", 10, 4, 90_000.0),
                },
                ExchangeInputEvent::CancelOrderRequested {
                    order_id: "overnight".to_string(),
                    user_id: "buyer".to_string(),
                },
            ] {
                runtime.record_and_process_input_event(command).unwrap();
            }
            assert_eq!(runtime.core.risk_limit_view("buyer", "AAPL").used_today, 4);
        }

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut restarted = recover_runtime(rx, &path).unwrap();
        assert_eq!(
            restarted.core.risk_limit_view("buyer", "AAPL").used_today,
            4
        );

        let rejected = restarted
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order_at("too-many", "buyer", "BUY", 10, 7, 90_001.0),
            })
            .unwrap()
            .into_place_order_result();
        assert!(rejected.is_err());

        restarted
            .record_and_process_input_event(ExchangeInputEvent::NewOrderRequested {
                order: order_at("remaining", "buyer", "BUY", 10, 6, 90_002.0),
            })
            .unwrap()
            .into_place_order_result()
            .unwrap();
        assert_eq!(
            restarted.core.risk_limit_view("buyer", "AAPL").used_today,
            10
        );
        drop(restarted);

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let recovered_again = recover_runtime(rx, &path).unwrap();
        assert_eq!(
            recovered_again
                .core
                .risk_limit_view("buyer", "AAPL")
                .used_today,
            10
        );
        drop(recovered_again);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_rejected_order_is_recorded_so_replay_reproduces_the_rejection() {
        let mut runtime = runtime();
        runtime.core.deposit("buyer".to_string(), 1_000).unwrap();

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

        match &runtime.event_log()[3].event {
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
        let mut runtime = runtime();
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
}
