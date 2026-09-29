//! Independent L2 market-data projection built only from committed exchange batches.
//! The durable journal remains authoritative; this state is derived and can be rebuilt.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::{File, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};

use super::{
    candles::{Candle, CandleProjection},
    committed_batch::{self, CancelOutcome, CommittedCommand, NewOrderOutcome},
    event_stream::{ReaderCheckpoint, StreamReader},
};
use crate::types::{
    exchange_event::EventEnvelope,
    types::{Execution, L2Level, Order, OrderBookView, Side},
};

const STATE_VERSION: u32 = 2;
const DEFAULT_ADDR: &str = "127.0.0.1:4001";
const DEFAULT_DEPTH: usize = 10;
const MAX_DEPTH: usize = 50;
const IDLE_POLL: Duration = Duration::from_millis(10);
/// Longest the saved state may trail the served view while batches keep arriving. After a crash
/// the process replays at most about this much history; a shorter interval means more saves.
const SAVE_INTERVAL: Duration = Duration::from_secs(1);

type MdResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ProjectedOrder {
    order_id: String,
    symbol: String,
    side: Side,
    price: u64,
    remaining: u64,
}

#[derive(Debug, Clone, Default)]
struct ProjectedBook {
    bids: BTreeMap<Reverse<u64>, u64>,
    asks: BTreeMap<u64, u64>,
}

impl ProjectedBook {
    fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
struct MarketDataProjection {
    orders: HashMap<String, ProjectedOrder>,
    books: HashMap<String, ProjectedBook>,
}

impl MarketDataProjection {
    fn from_orders(orders: Vec<ProjectedOrder>) -> Result<Self, String> {
        let mut projection = Self::default();
        for order in orders {
            projection.insert_order(order)?;
        }
        Ok(projection)
    }

    fn persisted_orders(&self) -> Vec<ProjectedOrder> {
        let mut orders: Vec<_> = self.orders.values().cloned().collect();
        orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));
        orders
    }

    fn insert_order(&mut self, order: ProjectedOrder) -> Result<(), String> {
        if order.order_id.is_empty()
            || order.symbol.is_empty()
            || order.price == 0
            || order.remaining == 0
            || order.remaining > u32::MAX as u64
        {
            return Err("projected order has an invalid id, symbol, price, or quantity".into());
        }
        if self.orders.contains_key(&order.order_id) {
            return Err(format!("duplicate projected order {}", order.order_id));
        }
        self.add_level(&order.symbol, &order.side, order.price, order.remaining)?;
        self.orders.insert(order.order_id.clone(), order);
        Ok(())
    }

    fn add_level(
        &mut self,
        symbol: &str,
        side: &Side,
        price: u64,
        quantity: u64,
    ) -> Result<(), String> {
        let book = self.books.entry(symbol.to_string()).or_default();
        let level = match side {
            Side::Buy => book.bids.entry(Reverse(price)).or_default(),
            Side::Sell => book.asks.entry(price).or_default(),
        };
        *level = level
            .checked_add(quantity)
            .ok_or_else(|| format!("L2 aggregate overflow for {symbol} at {price}"))?;
        Ok(())
    }

    fn subtract_level(
        &mut self,
        symbol: &str,
        side: &Side,
        price: u64,
        quantity: u64,
    ) -> Result<(), String> {
        let book = self
            .books
            .get_mut(symbol)
            .ok_or_else(|| format!("missing projected book for {symbol}"))?;
        match side {
            Side::Buy => {
                let key = Reverse(price);
                let level = book
                    .bids
                    .get_mut(&key)
                    .ok_or_else(|| format!("missing bid level {symbol} {price}"))?;
                *level = level
                    .checked_sub(quantity)
                    .ok_or_else(|| format!("bid level underflow for {symbol} at {price}"))?;
                if *level == 0 {
                    book.bids.remove(&key);
                }
            }
            Side::Sell => {
                let level = book
                    .asks
                    .get_mut(&price)
                    .ok_or_else(|| format!("missing ask level {symbol} {price}"))?;
                *level = level
                    .checked_sub(quantity)
                    .ok_or_else(|| format!("ask level underflow for {symbol} at {price}"))?;
                if *level == 0 {
                    book.asks.remove(&price);
                }
            }
        }
        let empty = book.is_empty();
        if empty {
            self.books.remove(symbol);
        }
        Ok(())
    }

    fn reduce_order(&mut self, order_id: &str, quantity: u64) -> Result<(), String> {
        let order = self
            .orders
            .get(order_id)
            .cloned()
            .ok_or_else(|| format!("execution references unknown resting order {order_id}"))?;
        let remaining = order
            .remaining
            .checked_sub(quantity)
            .ok_or_else(|| format!("execution exceeds remaining quantity for order {order_id}"))?;
        self.subtract_level(&order.symbol, &order.side, order.price, quantity)?;
        if remaining == 0 {
            self.orders.remove(order_id);
        } else {
            self.orders
                .get_mut(order_id)
                .ok_or_else(|| format!("projected order disappeared during fill {order_id}"))?
                .remaining = remaining;
        }
        Ok(())
    }

    fn cancel_order(&mut self, order_id: &str) -> Result<(), String> {
        let order = self
            .orders
            .get(order_id)
            .cloned()
            .ok_or_else(|| format!("cancellation references unknown order {order_id}"))?;
        self.subtract_level(&order.symbol, &order.side, order.price, order.remaining)?;
        self.orders.remove(order_id);
        Ok(())
    }

    /// Applies one committed batch in place. An error can leave the projection partly updated;
    /// the follower then withdraws the whole view (`Follower::step`), so it is never served or saved.
    fn apply_batch(&mut self, batch: &[EventEnvelope]) -> Result<(), String> {
        match committed_batch::decode(batch)? {
            CommittedCommand::NewOrder { order, outcome } => self.apply_new_order(&order, outcome),
            CommittedCommand::Cancellation {
                order_id, outcome, ..
            } => {
                self.apply_cancellation(&order_id, outcome)
            }
            CommittedCommand::Other => Ok(()),
        }
    }

    fn apply_new_order(&mut self, order: &Order, outcome: NewOrderOutcome) -> Result<(), String> {
        let executions = match outcome {
            NewOrderOutcome::Rejected { .. } => return Ok(()),
            NewOrderOutcome::Accepted { executions, .. } => executions,
        };

        if order.order_id.is_empty() || order.symbol.is_empty() || order.price.minor_units() == 0 {
            return Err("accepted order has an invalid id, symbol, or price".into());
        }
        if self.orders.contains_key(&order.order_id) {
            return Err(format!(
                "accepted duplicate projected order {}",
                order.order_id
            ));
        }
        if order.leaves_qty == 0 || order.leaves_qty != order.quantity {
            return Err("accepted order has invalid initial remaining quantity".into());
        }
        let mut incoming_remaining = order.leaves_qty as u64;
        for pair in executions {
            self.apply_trade(order, &pair.first, &mut incoming_remaining)?;
        }

        if incoming_remaining > 0 {
            self.insert_order(ProjectedOrder {
                order_id: order.order_id.clone(),
                symbol: order.symbol.clone(),
                side: order.side.clone(),
                price: order.price.minor_units(),
                remaining: incoming_remaining,
            })?;
        }
        Ok(())
    }

    fn apply_trade(
        &mut self,
        incoming: &Order,
        execution: &Execution,
        incoming_remaining: &mut u64,
    ) -> Result<(), String> {
        let incoming_is_buy = matches!(incoming.side, Side::Buy);
        let incoming_id = &incoming.order_id;
        let correct_incoming_side = if incoming_is_buy {
            &execution.buy_order_id == incoming_id && &execution.sell_order_id != incoming_id
        } else {
            &execution.sell_order_id == incoming_id && &execution.buy_order_id != incoming_id
        };
        if !correct_incoming_side {
            return Err(
                "execution does not identify exactly one incoming order on its side".into(),
            );
        }
        if execution.symbol != incoming.symbol || execution.quantity == 0 {
            return Err("execution symbol or quantity is invalid for incoming order".into());
        }
        let quantity = execution.quantity as u64;
        *incoming_remaining = incoming_remaining
            .checked_sub(quantity)
            .ok_or("executions exceed incoming order quantity")?;

        let resting_id = if incoming_is_buy {
            &execution.sell_order_id
        } else {
            &execution.buy_order_id
        };
        let resting =
            self.orders.get(resting_id).cloned().ok_or_else(|| {
                format!("execution references unknown resting order {resting_id}")
            })?;
        let opposite = matches!(
            (&incoming.side, &resting.side),
            (Side::Buy, Side::Sell) | (Side::Sell, Side::Buy)
        );
        if !opposite || resting.symbol != incoming.symbol {
            return Err("execution references the wrong resting side or symbol".into());
        }
        if execution.price.minor_units() != resting.price {
            return Err("execution price is not the resting order price".into());
        }
        let crosses = if incoming_is_buy {
            incoming.price.minor_units() >= resting.price
        } else {
            incoming.price.minor_units() <= resting.price
        };
        if !crosses {
            return Err("execution price does not cross the incoming limit".into());
        }
        self.reduce_order(resting_id, quantity)
    }

    fn apply_cancellation(
        &mut self,
        requested_order_id: &str,
        outcome: CancelOutcome,
    ) -> Result<(), String> {
        match outcome {
            CancelOutcome::Canceled { .. } => self.cancel_order(requested_order_id),
            CancelOutcome::Rejected { .. } => Ok(()),
        }
    }

    fn view(&self, symbol: &str, depth: usize) -> Option<OrderBookView> {
        let book = self.books.get(symbol)?;
        let bids = book
            .bids
            .iter()
            .take(depth)
            .map(|(price, quantity)| L2Level {
                price: price.0,
                quantity: *quantity,
            })
            .collect();
        let asks = book
            .asks
            .iter()
            .take(depth)
            .map(|(price, quantity)| L2Level {
                price: *price,
                quantity: *quantity,
            })
            .collect();
        Some(OrderBookView {
            symbol: symbol.to_string(),
            bids,
            asks,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ProjectionFile {
    version: u32,
    checkpoint: ReaderCheckpoint,
    orders: Vec<ProjectedOrder>,
    candles: Vec<Candle>,
}

fn load_state(
    path: &Path,
) -> MdResult<(
    MarketDataProjection,
    CandleProjection,
    Option<ReaderCheckpoint>,
)> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let saved: ProjectionFile = serde_json::from_slice(&bytes)?;
            if saved.version != STATE_VERSION {
                return Err(invalid(format!(
                    "unsupported market-data state version {}",
                    saved.version
                ))
                .into());
            }
            let projection = MarketDataProjection::from_orders(saved.orders).map_err(invalid)?;
            let candles = CandleProjection::from_candles(saved.candles).map_err(invalid)?;
            Ok((projection, candles, Some(saved.checkpoint)))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok((
            MarketDataProjection::default(),
            CandleProjection::default(),
            None,
        )),
        Err(error) => Err(error.into()),
    }
}

fn projection_file(
    orders: &MarketDataProjection,
    candles: &CandleProjection,
    checkpoint: &ReaderCheckpoint,
) -> ProjectionFile {
    ProjectionFile {
        version: STATE_VERSION,
        checkpoint: checkpoint.clone(),
        orders: orders.persisted_orders(),
        candles: candles.persisted_candles(),
    }
}

/// Returns the number of bytes written.
fn save_state(path: &Path, file: &ProjectionFile) -> MdResult<usize> {
    save_state_with_rename(path, file, |from, to| std::fs::rename(from, to))
}

fn save_state_with_rename<F>(path: &Path, file: &ProjectionFile, rename: F) -> MdResult<usize>
where
    F: FnOnce(&Path, &Path) -> io::Result<()>,
{
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid("invalid market-data state path"))?;
    let temp = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    // One buffer, one write: serializing straight into the file was thousands of tiny writes.
    let bytes = serde_json::to_vec(file)?;
    let result: MdResult<()> = (|| {
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        out.write_all(&bytes)?;
        out.sync_all()?;
        rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result.map(|()| bytes.len())
}

fn ensure_safe_state_path(journal: &Path, stream: &Path, state: &Path) -> MdResult<()> {
    let journal_path = journal.canonicalize()?;
    let stream_path = stream.canonicalize()?;
    let parent = state
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()?;
    let state_name = state
        .file_name()
        .ok_or_else(|| invalid("invalid market-data state path"))?;
    let state_path = if state.exists() {
        state.canonicalize()?
    } else {
        parent.join(state_name)
    };
    if state_path == journal_path || state_path == stream_path {
        return Err(invalid("market-data state must not overwrite the journal or stream").into());
    }
    if state.exists() {
        let state_meta = state.metadata()?;
        for source in [journal, stream] {
            let source_meta = source.metadata()?;
            if state_meta.dev() == source_meta.dev() && state_meta.ino() == source_meta.ino() {
                return Err(
                    invalid("market-data state must not alias the journal or stream").into(),
                );
            }
        }
    }
    Ok(())
}

/// What the market-data process serves: the L2 book, the candles, and the reader position just
/// after the last batch applied to them, which is the checkpoint saved with them.
struct View {
    orders: MarketDataProjection,
    candles: CandleProjection,
    applied: ReaderCheckpoint,
}

impl View {
    fn apply_batch(
        &mut self,
        batch: &[EventEnvelope],
        after: ReaderCheckpoint,
    ) -> Result<(), String> {
        self.orders.apply_batch(batch)?;
        self.candles.apply_batch(batch)?;
        self.applied = after;
        Ok(())
    }
}

/// `None` means unavailable. Every route reads this one lock, so a view that a failed batch left
/// half-updated can never be served: the follower replaces it with `None` before letting go.
type Served = Arc<RwLock<Option<View>>>;

/// Applies committed batches to the served view in place and saves the view with its checkpoint
/// at most once per `save_interval`. See `docs/performance/05-market-data-keeps-up.md`.
struct Follower {
    reader: StreamReader,
    served: Served,
    state_path: PathBuf,
    save_interval: Duration,
    unsaved: u64,
    last_save: Instant,
}

impl Follower {
    fn new(
        reader: StreamReader,
        served: Served,
        state_path: PathBuf,
        save_interval: Duration,
    ) -> Self {
        Self {
            reader,
            served,
            state_path,
            save_interval,
            unsaved: 0,
            last_save: Instant::now(),
        }
    }

    /// Applies the next committed batch, if there is one; `false` means caught up. The batch is
    /// read outside the lock and applied in place under it — no copy of the state. If it fails,
    /// the view is withdrawn before the lock is released, and the error is terminal: the process
    /// then serves 503 and never saves that state; a restart rebuilds from the last saved one.
    fn step(&mut self) -> MdResult<bool> {
        let Some(batch) = self.reader.next_batch()? else {
            return Ok(false);
        };
        let after = self.reader.checkpoint();
        let mut served = self
            .served
            .write()
            .map_err(|_| invalid("market-data view lock poisoned"))?;
        let view = served
            .as_mut()
            .ok_or_else(|| invalid("market data is unavailable"))?;
        if let Err(error) = view.apply_batch(&batch, after) {
            *served = None;
            return Err(invalid(error).into());
        }
        drop(served);
        self.unsaved += 1;
        Ok(true)
    }

    /// Saves if batches are unsaved and `save_interval` has passed since the previous save
    /// *finished*, so a slow save can never make every batch trigger another one.
    fn maybe_save(&mut self) -> MdResult<()> {
        if self.unsaved > 0 && self.last_save.elapsed() >= self.save_interval {
            self.save()?;
        }
        Ok(())
    }

    /// Copies the view and its checkpoint under one read lock, then writes them outside it. The
    /// pair always matches: both come from the same moment, after a successfully applied batch.
    fn save(&mut self) -> MdResult<()> {
        let started = Instant::now();
        let file = {
            let served = self
                .served
                .read()
                .map_err(|_| invalid("market-data view lock poisoned"))?;
            let view = served
                .as_ref()
                .ok_or_else(|| invalid("market data is unavailable"))?;
            projection_file(&view.orders, &view.candles, &view.applied)
        };
        let bytes = save_state(&self.state_path, &file)?;
        println!(
            "market-data state saved through event sequence {} ({} batches, {} bytes) in {} ms",
            file.checkpoint.next_sequence - 1,
            self.unsaved,
            bytes,
            started.elapsed().as_millis()
        );
        self.unsaved = 0;
        self.last_save = Instant::now();
        Ok(())
    }

    fn follow(mut self) -> MdResult<()> {
        loop {
            if !self.step()? {
                self.maybe_save()?;
                thread::sleep(IDLE_POLL);
                continue;
            }
            self.maybe_save()?;
        }
    }
}

/// Withdraws the view when the follower thread ends for any reason, including a panic, so the
/// process never keeps serving a view that has stopped moving.
struct WithdrawOnExit(Served);

impl Drop for WithdrawOnExit {
    fn drop(&mut self) {
        let mut served = self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *served = None;
    }
}

#[derive(Clone)]
struct MarketDataState {
    served: Served,
}

#[derive(Deserialize)]
struct BookQuery {
    depth: Option<usize>,
}

#[derive(Deserialize)]
struct CandleQuery {
    symbol: Option<String>,
    start_time: Option<u64>,
    end_time: Option<u64>,
}

#[derive(Serialize)]
struct CandleView {
    symbol: String,
    candles: Vec<Candle>,
}

const UNAVAILABLE: (StatusCode, &str) =
    (StatusCode::SERVICE_UNAVAILABLE, "market data unavailable");

async fn health(State(state): State<MarketDataState>) -> impl IntoResponse {
    match state.served.read() {
        Ok(served) if served.is_some() => (StatusCode::OK, "OK"),
        _ => UNAVAILABLE,
    }
}

async fn order_book(
    State(state): State<MarketDataState>,
    AxumPath(symbol): AxumPath<String>,
    Query(query): Query<BookQuery>,
) -> Result<Json<OrderBookView>, (StatusCode, &'static str)> {
    let served = state.served.read().map_err(|_| UNAVAILABLE)?;
    let view = served.as_ref().ok_or(UNAVAILABLE)?;
    view.orders
        .view(
            &symbol,
            query.depth.unwrap_or(DEFAULT_DEPTH).clamp(1, MAX_DEPTH),
        )
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "market-data symbol not found"))
}

async fn candle_history(
    State(state): State<MarketDataState>,
    Query(query): Query<CandleQuery>,
) -> Result<Json<CandleView>, (StatusCode, &'static str)> {
    let served = state.served.read().map_err(|_| UNAVAILABLE)?;
    let view = served.as_ref().ok_or(UNAVAILABLE)?;
    let start_time = query
        .start_time
        .ok_or((StatusCode::BAD_REQUEST, "start_time is required"))?;
    let end_time = query
        .end_time
        .ok_or((StatusCode::BAD_REQUEST, "end_time is required"))?;
    if start_time > end_time {
        return Err((
            StatusCode::BAD_REQUEST,
            "start_time must not exceed end_time",
        ));
    }
    let symbol = query
        .symbol
        .filter(|symbol| !symbol.trim().is_empty())
        .ok_or((StatusCode::BAD_REQUEST, "symbol is required"))?;
    Ok(Json(CandleView {
        candles: view.candles.candles_in_range(&symbol, start_time, end_time),
        symbol,
    }))
}

pub async fn run(args: &[String]) -> MdResult<()> {
    if !(3..=4).contains(&args.len()) {
        return Err(
            invalid("usage: stock --market-data JOURNAL STREAM STATE_FILE [LISTEN_ADDR]").into(),
        );
    }
    let journal_path = PathBuf::from(&args[0]);
    let stream_path = PathBuf::from(&args[1]);
    let state_path = PathBuf::from(&args[2]);
    let address: SocketAddr = args
        .get(3)
        .map(String::as_str)
        .unwrap_or(DEFAULT_ADDR)
        .parse()?;

    ensure_safe_state_path(&journal_path, &stream_path, &state_path)?;
    let (orders, candles, checkpoint) = load_state(&state_path)?;
    let reader = StreamReader::open(&journal_path, &stream_path, checkpoint)?;
    let applied = reader.checkpoint();
    let served: Served = Arc::new(RwLock::new(Some(View {
        orders,
        candles,
        applied,
    })));
    let mut follower = Follower::new(reader, Arc::clone(&served), state_path, SAVE_INTERVAL);
    // Catch up before binding, saving on the usual cadence, then save once more so the state on
    // disk matches what the listener is about to serve.
    while follower.step()? {
        follower.maybe_save()?;
    }
    follower.save()?;

    let withdraw = WithdrawOnExit(Arc::clone(&served));
    thread::Builder::new()
        .name("market-data-follower".into())
        .spawn(move || {
            let _withdraw = withdraw;
            if let Err(error) = follower.follow() {
                eprintln!("market-data follower halted: {error}");
            }
        })?;

    let app = Router::new()
        .route("/health", get(health))
        .route("/marketdata/orderbook/{symbol}", get(order_book))
        .route("/marketdata/candles", get(candle_history))
        .with_state(MarketDataState { served });
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Market data is listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exchange::{
            core::ExchangeCore, event_stream::tests::Fixture, runtime::recover_runtime_with_stream,
        },
        types::exchange_event::{ExchangeEvent, ExchangeInputEvent, ExchangeOutputEvent},
        types::types::{ExchangeCommand, Price},
    };
    use tokio::sync::{mpsc, oneshot};

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

    fn envelopes(
        input: ExchangeInputEvent,
        outputs: Vec<ExchangeOutputEvent>,
    ) -> Vec<EventEnvelope> {
        std::iter::once(ExchangeEvent::Input(input))
            .chain(outputs.into_iter().map(ExchangeEvent::Output))
            .enumerate()
            .map(|(index, event)| EventEnvelope {
                seq_num: index as u64 + 1,
                event,
            })
            .collect()
    }

    fn accepted(order: Order, executions: Vec<Execution>) -> Vec<EventEnvelope> {
        let mut outputs = vec![ExchangeOutputEvent::OrderAccepted {
            order_id: order.order_id.clone(),
            seq_num: 1,
        }];
        outputs.extend(
            executions
                .into_iter()
                .map(|execution| ExchangeOutputEvent::ExecutionCreated { execution }),
        );
        envelopes(ExchangeInputEvent::NewOrderRequested { order }, outputs)
    }

    fn rejected(order: Order) -> Vec<EventEnvelope> {
        envelopes(
            ExchangeInputEvent::NewOrderRequested {
                order: order.clone(),
            },
            vec![ExchangeOutputEvent::OrderRejected {
                order_id: order.order_id,
                reason: "rejected".into(),
            }],
        )
    }

    fn canceled(order_id: &str) -> Vec<EventEnvelope> {
        envelopes(
            ExchangeInputEvent::CancelOrderRequested {
                order_id: order_id.to_string(),
                user_id: "owner".into(),
            },
            vec![ExchangeOutputEvent::OrderCanceled {
                order_id: order_id.to_string(),
                seq_num: 2,
            }],
        )
    }

    fn execution(id: &str, buy: &str, sell: &str, price: u64, quantity: u32) -> Execution {
        Execution {
            execution_id: id.to_string(),
            buy_order_id: buy.to_string(),
            sell_order_id: sell.to_string(),
            symbol: "AAPL".into(),
            price: Price::new(price).unwrap(),
            quantity,
            timestamp: 1.0,
        }
    }

    fn pair(prefix: &str, buy: &str, sell: &str, price: u64, quantity: u32) -> Vec<Execution> {
        vec![
            execution(&format!("{prefix}-buy"), buy, sell, price, quantity),
            execution(&format!("{prefix}-sell"), buy, sell, price, quantity),
        ]
    }

    #[test]
    fn accepted_rejected_and_canceled_orders_update_only_committed_l2() {
        let mut projection = MarketDataProjection::default();
        projection
            .apply_batch(&accepted(order("buy-1", "buyer", "BUY", 100, 7), vec![]))
            .unwrap();
        projection
            .apply_batch(&accepted(order("buy-2", "buyer", "BUY", 99, 3), vec![]))
            .unwrap();
        projection
            .apply_batch(&rejected(order("sell-rejected", "seller", "SELL", 101, 9)))
            .unwrap();

        assert_eq!(
            projection.view("AAPL", 1).unwrap().bids,
            vec![L2Level {
                price: 100,
                quantity: 7
            }]
        );
        projection.apply_batch(&canceled("buy-1")).unwrap();
        assert_eq!(
            projection.view("AAPL", 10).unwrap().bids,
            vec![L2Level {
                price: 99,
                quantity: 3
            }]
        );

        let cancel_rejected = envelopes(
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "missing".into(),
                user_id: "owner".into(),
            },
            vec![ExchangeOutputEvent::CancelRejected {
                order_id: "missing".into(),
                reason: "missing".into(),
            }],
        );
        projection.apply_batch(&cancel_rejected).unwrap();
        assert_eq!(projection.orders.len(), 1);
    }

    #[test]
    fn views_sort_each_side_apply_depth_and_hide_unknown_symbols() {
        let mut projection = MarketDataProjection::default();
        for resting in [
            order("bid-low", "buyer", "BUY", 99, 2),
            order("bid-high", "buyer", "BUY", 100, 3),
            order("ask-high", "seller", "SELL", 102, 4),
            order("ask-low", "seller", "SELL", 101, 5),
        ] {
            projection.apply_batch(&accepted(resting, vec![])).unwrap();
        }

        let view = projection.view("AAPL", 1).unwrap();
        assert_eq!(
            view.bids,
            vec![L2Level {
                price: 100,
                quantity: 3,
            }]
        );
        assert_eq!(
            view.asks,
            vec![L2Level {
                price: 101,
                quantity: 5,
            }]
        );
        assert!(projection.view("UNKNOWN", 10).is_none());
    }

    #[test]
    fn multi_fill_decrements_resting_orders_and_rests_the_incoming_remainder() {
        let mut projection = MarketDataProjection::default();
        projection
            .apply_batch(&accepted(order("sell-1", "s1", "SELL", 100, 3), vec![]))
            .unwrap();
        projection
            .apply_batch(&accepted(order("sell-2", "s2", "SELL", 101, 4), vec![]))
            .unwrap();
        let mut executions = pair("one", "buy-1", "sell-1", 100, 3);
        executions.extend(pair("two", "buy-1", "sell-2", 101, 4));
        projection
            .apply_batch(&accepted(
                order("buy-1", "buyer", "BUY", 102, 10),
                executions,
            ))
            .unwrap();

        assert_eq!(
            projection.view("AAPL", 10).unwrap(),
            OrderBookView {
                symbol: "AAPL".into(),
                bids: vec![L2Level {
                    price: 102,
                    quantity: 3,
                }],
                asks: vec![],
            }
        );
        assert!(!projection.orders.contains_key("sell-1"));
        assert!(!projection.orders.contains_key("sell-2"));
    }

    #[test]
    fn projection_matches_the_authoritative_core_including_self_trade_skip() {
        let mut core = ExchangeCore::new();
        core.deposit("alice".into(), 10_000).unwrap();
        core.deposit_shares("alice", "AAPL", 2).unwrap();
        core.deposit_shares("bob", "AAPL", 2).unwrap();
        let mut projection = MarketDataProjection::default();

        for resting in [
            order("alice-sell", "alice", "SELL", 100, 2),
            order("bob-sell", "bob", "SELL", 101, 2),
        ] {
            let outcome = core.add_order(resting.clone()).unwrap();
            projection
                .apply_batch(&accepted(resting, outcome.executions))
                .unwrap();
        }
        let incoming = order("alice-buy", "alice", "BUY", 101, 4);
        let outcome = core.add_order(incoming.clone()).unwrap();
        projection
            .apply_batch(&accepted(incoming, outcome.executions))
            .unwrap();

        assert_eq!(projection.view("AAPL", 10), core.l2_snapshot("AAPL", 10));

        let cancel_seq = core.cancel_order_for_user("alice-buy", "alice").unwrap();
        let cancel_batch = envelopes(
            ExchangeInputEvent::CancelOrderRequested {
                order_id: "alice-buy".into(),
                user_id: "alice".into(),
            },
            vec![ExchangeOutputEvent::OrderCanceled {
                order_id: "alice-buy".into(),
                seq_num: cancel_seq,
            }],
        );
        projection.apply_batch(&cancel_batch).unwrap();
        assert_eq!(projection.view("AAPL", 10), core.l2_snapshot("AAPL", 10));
    }

    #[test]
    fn l2_aggregation_exceeds_u32_without_wrapping() {
        let mut projection = MarketDataProjection::default();
        for id in ["one", "two"] {
            projection
                .apply_batch(&accepted(order(id, id, "BUY", 100, u32::MAX), vec![]))
                .unwrap();
        }
        assert_eq!(
            projection.view("AAPL", 10).unwrap().bids[0].quantity,
            u32::MAX as u64 * 2
        );

        let mut overflow = MarketDataProjection::default();
        overflow
            .add_level("AAPL", &Side::Buy, 100, u64::MAX)
            .unwrap();
        assert!(
            overflow
                .add_level("AAPL", &Side::Buy, 100, 1)
                .unwrap_err()
                .contains("overflow")
        );
    }

    #[test]
    fn malformed_execution_pairs_and_projection_references_are_rejected() {
        let incoming = order("buy", "buyer", "BUY", 100, 2);
        let one_execution = accepted(
            incoming.clone(),
            vec![execution("only", "buy", "sell", 100, 1)],
        );
        assert!(
            MarketDataProjection::default()
                .apply_batch(&one_execution)
                .unwrap_err()
                .contains("odd")
        );

        let duplicate_ids = accepted(
            incoming.clone(),
            vec![
                execution("same", "buy", "sell", 100, 1),
                execution("same", "buy", "sell", 100, 1),
            ],
        );
        assert!(
            MarketDataProjection::default()
                .apply_batch(&duplicate_ids)
                .unwrap_err()
                .contains("duplicate execution id")
        );

        let mut mismatched = pair("bad", "buy", "sell", 100, 1);
        mismatched[1].quantity = 2;
        assert!(
            MarketDataProjection::default()
                .apply_batch(&accepted(incoming.clone(), mismatched))
                .unwrap_err()
                .contains("inconsistent")
        );

        let mut duplicate_across_pairs = pair("first", "buy", "sell-1", 100, 1);
        let mut second_pair = pair("second", "buy", "sell-2", 100, 1);
        second_pair[0].execution_id = duplicate_across_pairs[0].execution_id.clone();
        duplicate_across_pairs.extend(second_pair);
        let mut projection = MarketDataProjection::default();
        projection
            .apply_batch(&accepted(
                order("sell-1", "seller-1", "SELL", 100, 1),
                vec![],
            ))
            .unwrap();
        projection
            .apply_batch(&accepted(
                order("sell-2", "seller-2", "SELL", 100, 1),
                vec![],
            ))
            .unwrap();
        assert!(
            projection
                .apply_batch(&accepted(incoming.clone(), duplicate_across_pairs))
                .unwrap_err()
                .contains("duplicate execution id")
        );

        let missing_resting = accepted(incoming, pair("missing", "buy", "sell", 100, 1));
        assert!(
            MarketDataProjection::default()
                .apply_batch(&missing_resting)
                .unwrap_err()
                .contains("unknown resting")
        );
    }

    #[test]
    fn wrong_side_symbol_price_and_quantity_are_rejected() {
        let base = order("resting", "seller", "SELL", 100, 1);
        let incoming = order("buy", "buyer", "BUY", 101, 2);

        let mut wrong_price = MarketDataProjection::default();
        wrong_price
            .apply_batch(&accepted(base.clone(), vec![]))
            .unwrap();
        assert!(
            wrong_price
                .apply_batch(&accepted(
                    incoming.clone(),
                    pair("price", "buy", "resting", 99, 1),
                ))
                .unwrap_err()
                .contains("resting order price")
        );

        let mut too_large = MarketDataProjection::default();
        too_large
            .apply_batch(&accepted(base.clone(), vec![]))
            .unwrap();
        assert!(
            too_large
                .apply_batch(&accepted(
                    incoming.clone(),
                    pair("large", "buy", "resting", 100, 2),
                ))
                .unwrap_err()
                .contains("remaining quantity")
        );

        let mut wrong_symbol = pair("symbol", "buy", "resting", 100, 1);
        for execution in &mut wrong_symbol {
            execution.symbol = "MSFT".into();
        }
        let mut projection = MarketDataProjection::default();
        projection.apply_batch(&accepted(base, vec![])).unwrap();
        assert!(
            projection
                .apply_batch(&accepted(incoming, wrong_symbol))
                .unwrap_err()
                .contains("symbol")
        );

        let mut wrong_side = MarketDataProjection::default();
        wrong_side
            .apply_batch(&accepted(
                order("resting-buy", "buyer-1", "BUY", 100, 1),
                vec![],
            ))
            .unwrap();
        assert!(
            wrong_side
                .apply_batch(&accepted(
                    order("incoming-buy", "buyer-2", "BUY", 101, 1),
                    pair("side", "incoming-buy", "resting-buy", 100, 1),
                ))
                .unwrap_err()
                .contains("wrong resting side")
        );
    }

    #[test]
    fn state_round_trips_and_invalid_state_refuses_startup() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let checkpoint = fixture.reader().checkpoint();
        let state_path = fixture.dir.join("market-data.json");
        let projection = MarketDataProjection::from_orders(vec![ProjectedOrder {
            order_id: "buy".into(),
            symbol: "AAPL".into(),
            side: Side::Buy,
            price: 100,
            remaining: 7,
        }])
        .unwrap();
        let candles = CandleProjection::from_candles(vec![Candle {
            symbol: "AAPL".into(),
            start_time: 60,
            open: 100,
            high: 101,
            low: 99,
            close: 101,
            volume: 7,
            trade_count: 2,
        }])
        .unwrap();
        save_state(
            &state_path,
            &projection_file(&projection, &candles, &checkpoint),
        )
        .unwrap();

        let (loaded, loaded_candles, loaded_checkpoint) = load_state(&state_path).unwrap();
        assert_eq!(loaded.persisted_orders(), projection.persisted_orders());
        assert_eq!(
            loaded_candles.persisted_candles(),
            candles.persisted_candles()
        );
        assert_eq!(loaded_checkpoint.unwrap(), checkpoint);

        std::fs::write(&state_path, b"not json").unwrap();
        assert!(load_state(&state_path).is_err());

        let unsupported = ProjectionFile {
            version: 99,
            checkpoint,
            orders: vec![],
            candles: vec![],
        };
        std::fs::write(&state_path, serde_json::to_vec(&unsupported).unwrap()).unwrap();
        assert!(
            load_state(&state_path)
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );
    }

    #[test]
    fn failed_state_replacement_preserves_the_previous_file() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let checkpoint = fixture.reader().checkpoint();
        let state_path = fixture.dir.join("market-data.json");
        let original = MarketDataProjection::from_orders(vec![ProjectedOrder {
            order_id: "original".into(),
            symbol: "AAPL".into(),
            side: Side::Buy,
            price: 100,
            remaining: 7,
        }])
        .unwrap();
        let candles = CandleProjection::default();
        save_state(
            &state_path,
            &projection_file(&original, &candles, &checkpoint),
        )
        .unwrap();
        let original_bytes = std::fs::read(&state_path).unwrap();

        let replacement = MarketDataProjection::from_orders(vec![ProjectedOrder {
            order_id: "replacement".into(),
            symbol: "AAPL".into(),
            side: Side::Sell,
            price: 101,
            remaining: 3,
        }])
        .unwrap();
        let result = save_state_with_rename(
            &state_path,
            &projection_file(&replacement, &candles, &checkpoint),
            |_temporary, _destination| Err(io::Error::other("injected rename failure")),
        );

        assert!(result.is_err());
        assert_eq!(std::fs::read(&state_path).unwrap(), original_bytes);
        assert_eq!(
            std::fs::read_dir(&fixture.dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
                .count(),
            0
        );
    }

    /// Gives a test batch the journal sequence numbers that follow `next`, as the exchange would.
    fn numbered(mut batch: Vec<EventEnvelope>, next: &mut u64) -> Vec<EventEnvelope> {
        for envelope in &mut batch {
            envelope.seq_num = *next;
            *next += 1;
        }
        batch
    }

    fn publish(
        store: &mut crate::exchange::event_store::EventStore,
        writer: &mut crate::exchange::event_stream::StreamWriter,
        batch: &[EventEnvelope],
    ) {
        let record = crate::exchange::event_store::encode_record(batch).unwrap();
        store.append_record(&record).unwrap();
        writer
            .append(&record, batch.last().unwrap().seq_num)
            .unwrap();
    }

    fn send(tx: &mpsc::Sender<ExchangeCommand>, command: ExchangeCommand) {
        tx.blocking_send(command).unwrap();
    }

    fn empty_view(reader: &StreamReader) -> Served {
        Arc::new(RwLock::new(Some(View {
            orders: MarketDataProjection::default(),
            candles: CandleProjection::default(),
            applied: reader.checkpoint(),
        })))
    }

    #[test]
    fn a_failed_batch_is_never_served_or_saved() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut next = 1;
        for batch in [
            accepted(order("sell-1", "seller-1", "SELL", 100, 1), vec![]),
            accepted(order("sell-2", "seller-2", "SELL", 100, 1), vec![]),
        ] {
            publish(&mut store, &mut writer, &numbered(batch, &mut next));
        }
        let mut executions = pair("first", "buy", "sell-1", 100, 1);
        let mut second_pair = pair("second", "buy", "sell-2", 100, 1);
        second_pair[0].execution_id = executions[0].execution_id.clone();
        executions.extend(second_pair);
        let malformed = accepted(order("buy", "buyer", "BUY", 100, 2), executions);
        publish(&mut store, &mut writer, &numbered(malformed, &mut next));

        let state_path = fixture.dir.join("market-data.json");
        let reader = fixture.reader();
        let served = empty_view(&reader);
        // A zero interval saves after every good batch, so any save of the bad one would show.
        let mut follower = Follower::new(
            reader,
            Arc::clone(&served),
            state_path.clone(),
            Duration::ZERO,
        );
        for _ in 0..2 {
            assert!(follower.step().unwrap());
            follower.maybe_save().unwrap();
        }
        let saved = std::fs::read(&state_path).unwrap();

        assert!(follower.step().is_err());
        // The view is withdrawn, so there is nothing left that could be saved.
        assert!(follower.save().is_err());
        assert!(served.read().unwrap().is_none());
        assert_eq!(std::fs::read(&state_path).unwrap(), saved);
    }

    #[test]
    fn unsaved_batches_wait_for_the_interval_and_the_final_save_catches_up() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut next = 1;
        for batch in [
            accepted(order("sell-1", "seller", "SELL", 100, 3), vec![]),
            accepted(order("sell-2", "seller", "SELL", 101, 4), vec![]),
        ] {
            publish(&mut store, &mut writer, &numbered(batch, &mut next));
        }
        let state_path = fixture.dir.join("market-data.json");
        let reader = fixture.reader();
        let served = empty_view(&reader);
        let mut follower = Follower::new(
            reader,
            Arc::clone(&served),
            state_path.clone(),
            Duration::from_secs(3600),
        );
        while follower.step().unwrap() {
            follower.maybe_save().unwrap();
        }
        // Both batches are served at once, but not yet saved.
        assert_eq!(
            served
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .orders
                .view("AAPL", 10)
                .unwrap()
                .asks
                .len(),
            2
        );
        assert!(!state_path.exists());

        follower.save().unwrap();
        let (orders, _, checkpoint) = load_state(&state_path).unwrap();
        assert_eq!(checkpoint.unwrap().next_sequence, next);
        assert_eq!(orders.view("AAPL", 10).unwrap().asks.len(), 2);
    }

    #[test]
    fn restarting_from_an_older_save_replays_to_the_same_view() {
        let fixture = Fixture::new();
        let (tx, rx) = mpsc::channel(16);
        let runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let worker = thread::spawn(move || runtime.run());
        let (respond_to, reply) = oneshot::channel();
        send(
            &tx,
            ExchangeCommand::DepositShares {
                user_id: "seller".into(),
                symbol: "AAPL".into(),
                quantity: 10,
                respond_to,
            },
        );
        reply.blocking_recv().unwrap().unwrap();
        let (respond_to, reply) = oneshot::channel();
        send(
            &tx,
            ExchangeCommand::Deposit {
                user_id: "buyer".into(),
                amount: 10_000,
                respond_to,
            },
        );
        reply.blocking_recv().unwrap().unwrap();
        for (id, user, side, price, quantity) in [
            ("sell-1", "seller", "SELL", 100, 5),
            ("sell-2", "seller", "SELL", 102, 5),
            ("buy-1", "buyer", "BUY", 101, 7),
        ] {
            let (respond_to, reply) = oneshot::channel();
            send(
                &tx,
                ExchangeCommand::PlaceOrder {
                    order: order(id, user, side, price, quantity),
                    respond_to,
                },
            );
            reply.blocking_recv().unwrap().unwrap();
        }
        drop(tx);
        worker.join().unwrap();

        // First run: save after the first batch only, then apply the rest without saving,
        // as a crash between two saves would leave it.
        let state_path = fixture.dir.join("market-data.json");
        let reader = fixture.reader();
        let served = empty_view(&reader);
        let mut first = Follower::new(
            reader,
            Arc::clone(&served),
            state_path.clone(),
            Duration::from_secs(3600),
        );
        assert!(first.step().unwrap());
        first.save().unwrap();
        while first.step().unwrap() {}
        let expected = {
            let served = served.read().unwrap();
            let view = served.as_ref().unwrap();
            (
                view.orders.view("AAPL", 10),
                view.candles.persisted_candles(),
            )
        };
        drop(first);

        // Restart from the older save and catch up.
        let (orders, candles, checkpoint) = load_state(&state_path).unwrap();
        let reader = StreamReader::open(&fixture.log, &fixture.bus, checkpoint).unwrap();
        let applied = reader.checkpoint();
        let served = Arc::new(RwLock::new(Some(View {
            orders,
            candles,
            applied,
        })));
        let mut second = Follower::new(
            reader,
            Arc::clone(&served),
            state_path,
            Duration::from_secs(3600),
        );
        while second.step().unwrap() {}

        let served = served.read().unwrap();
        let view = served.as_ref().unwrap();
        assert_eq!(
            (
                view.orders.view("AAPL", 10),
                view.candles.persisted_candles()
            ),
            expected
        );
        // One trade (5 at 100) made one candle; sell-2 still rests, and buy-1's remainder bids.
        let book = expected.0.unwrap();
        assert_eq!(
            (book.asks.len(), book.bids.len(), expected.1.len()),
            (1, 1, 1)
        );
    }

    #[test]
    fn state_path_alias_and_checkpoint_from_another_journal_are_refused() {
        let first = Fixture::new();
        let (_store, _writer) = first.start(4096);
        assert!(ensure_safe_state_path(&first.log, &first.bus, &first.log).is_err());
        let checkpoint = first.reader().checkpoint();

        let second = Fixture::new();
        let (_other_store, _other_writer) = second.start(4096);
        assert!(
            StreamReader::open(&second.log, &second.bus, Some(checkpoint)).is_err(),
            "a projection checkpoint must remain bound to its journal"
        );
    }

    #[test]
    fn duplicate_saved_orders_are_refused() {
        let duplicate = ProjectedOrder {
            order_id: "same".into(),
            symbol: "AAPL".into(),
            side: Side::Buy,
            price: 100,
            remaining: 1,
        };
        assert!(
            MarketDataProjection::from_orders(vec![duplicate.clone(), duplicate])
                .unwrap_err()
                .contains("duplicate")
        );

        let oversized = ProjectedOrder {
            order_id: "oversized".into(),
            symbol: "AAPL".into(),
            side: Side::Sell,
            price: 101,
            remaining: u32::MAX as u64 + 1,
        };
        assert!(MarketDataProjection::from_orders(vec![oversized]).is_err());
    }

    #[test]
    fn actual_exchange_runtime_publication_drives_and_recovers_the_projection() {
        let fixture = Fixture::new();
        let (tx, rx) = mpsc::channel(16);
        let runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let worker = thread::spawn(move || runtime.run());
        let mut reader = StreamReader::open(&fixture.log, &fixture.bus, None).unwrap();
        let mut projection = MarketDataProjection::default();

        let (respond_to, response) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::DepositShares {
            user_id: "seller".into(),
            symbol: "AAPL".into(),
            quantity: 5,
            respond_to,
        })
        .unwrap();
        assert_eq!(response.blocking_recv().unwrap(), Ok(()));

        let sell = order("sell", "seller", "SELL", 100, 5);
        let (respond_to, response) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::PlaceOrder {
            order: sell,
            respond_to,
        })
        .unwrap();
        assert!(response.blocking_recv().unwrap().is_ok());

        let (respond_to, response) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::Deposit {
            user_id: "buyer".into(),
            amount: 1_000,
            respond_to,
        })
        .unwrap();
        assert_eq!(response.blocking_recv().unwrap(), Ok(()));

        let buy = order("buy", "buyer", "BUY", 100, 2);
        let (respond_to, response) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::PlaceOrder {
            order: buy,
            respond_to,
        })
        .unwrap();
        assert!(response.blocking_recv().unwrap().is_ok());

        while let Some(batch) = reader.next_batch().unwrap() {
            projection.apply_batch(&batch).unwrap();
        }
        assert_eq!(
            projection.view("AAPL", 10).unwrap().asks,
            vec![L2Level {
                price: 100,
                quantity: 3,
            }]
        );
        let checkpoint = reader.checkpoint();

        drop(tx);
        worker.join().unwrap();

        let (tx, rx) = mpsc::channel(16);
        let runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let worker = thread::spawn(move || runtime.run());
        let mut resumed = StreamReader::open(&fixture.log, &fixture.bus, Some(checkpoint)).unwrap();
        let (respond_to, response) = oneshot::channel();
        tx.blocking_send(ExchangeCommand::CancelOrder {
            order_id: "sell".into(),
            user_id: "seller".into(),
            respond_to,
        })
        .unwrap();
        assert_eq!(response.blocking_recv().unwrap(), Ok(()));
        while let Some(batch) = resumed.next_batch().unwrap() {
            projection.apply_batch(&batch).unwrap();
        }
        assert!(projection.view("AAPL", 10).is_none());

        drop(tx);
        worker.join().unwrap();
    }
}
