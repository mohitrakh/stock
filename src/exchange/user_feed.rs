//! The private feed (milestone 26 part 2): each user's own fills and order changes, pushed over
//! WebSocket by its own process, `stock --user-feed`, which follows the committed stream as the
//! market-data process does.
//!
//! A fill names its buy and sell orders, not their users. The batch that fills a resting order
//! names the incoming order's user only, so this process keeps every open order's owner (the owner
//! map), saved with its stream checkpoint. User ids never enter the public market-data process.
//!
//! Who receives what (`docs/milestones/26-pushed-updates/01-theory.md` §7):
//! - an accepted order, its fills, its cancellation and its expiry go to its owner, from the map;
//!   a fill's incoming side is in the batch itself;
//! - a rejected order goes to the user named in that input, and never touches the map: order ids
//!   are chosen by clients, and the map may hold another user's live order under the same id;
//! - a refused cancellation goes to whoever asked, who may not be the owner.
//!
//! A client's first message must be `{"type":"auth","token":"<JWT>"}`, within `AUTH_DEADLINE`. The
//! token is checked like the gateway's (`verify_token`), and the connection closes when it expires.
//! The server then sends `{"type":"ready","seq":S}`: every later event (`order`, `fill`,
//! `cancel_rejected`) has a sequence after S; an `error` reply carries none. Nothing is replayed
//! (decision 5); a client fills the gap through the REST endpoints.

use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, RwLock},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use jsonwebtoken::DecodingKey;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc},
    time::timeout,
};

use super::{
    book_feed::{self, Close, Limits, SILENT_PINGS},
    committed_batch::{self, CancelOutcome, CommittedCommand, NewOrderOutcome},
    event_stream::{ReaderCheckpoint, StreamReader},
    market_data::{MdResult, ensure_safe_state_path, save_state},
};
use crate::{
    middleware::auth_middleware::{jwt_secret, verify_token},
    types::{exchange_event::EventEnvelope, types::Side},
};

const STATE_VERSION: u32 = 1;
const DEFAULT_ADDR: &str = "127.0.0.1:4006";
/// A client must authenticate within this long after connecting.
const AUTH_DEADLINE: Duration = Duration::from_secs(5);
/// The largest message or frame a client may send. The only request is `auth`, a few hundred
/// bytes; axum's default (64 MiB) would let an unauthenticated client make the server buffer that
/// much per connection.
const MAX_CLIENT_MESSAGE: usize = 16 * 1024;
/// The book feed's limits, with a smaller ring. One user's events are few, but tokio allocates a
/// ring's slots whole: about 60 bytes each (an estimate: a `Utf8Bytes` and the slot's own fields),
/// so 1,024 slots are about 60 KB per connected user, against about 240 KB at 4,096. A batch that
/// gives one user more events than the ring holds before the forwarder moves them on (an order
/// with over about 1,000 fills) closes that user's connection as a slow consumer (1013); the
/// client then reads REST.
const USER_LIMITS: Limits = Limits {
    ring: 1_024,
    ..Limits::DEFAULT
};
const IDLE_POLL: Duration = Duration::from_millis(10);
const SAVE_INTERVAL: Duration = Duration::from_secs(1);

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// One open order and its owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OpenOrder {
    order_id: String,
    user_id: String,
    symbol: String,
    side: Side,
    price: u64,
    quantity: u64,
    remaining: u64,
}

/// One message for one user. `seq` is the last envelope sequence of the batch it comes from.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    /// An order's state after a batch: `new`, `partially_filled`, `filled`, `canceled`, `expired`
    /// or `rejected` (with a reason).
    Order {
        seq: u64,
        order_id: String,
        symbol: String,
        side: Side,
        price: u64,
        quantity: u64,
        filled: u64,
        remaining: u64,
        status: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// One trade, as one side of it sees it. `execution_id` is the one REST returns for the trade
    /// (the first of the pair); with `symbol` it is unique, as execution ids count per symbol.
    Fill {
        seq: u64,
        execution_id: String,
        order_id: String,
        symbol: String,
        side: Side,
        price: u64,
        quantity: u64,
        remaining: u64,
    },
    /// A cancellation the exchange refused, told to whoever asked for it.
    CancelRejected {
        seq: u64,
        order_id: String,
        reason: String,
    },
}

impl OpenOrder {
    fn event(&self, seq: u64) -> Event {
        let status = match self.remaining {
            0 => "filled",
            remaining if remaining < self.quantity => "partially_filled",
            _ => "new",
        };
        self.event_with(seq, status)
    }

    fn event_with(&self, seq: u64, status: &'static str) -> Event {
        Event::Order {
            seq,
            order_id: self.order_id.clone(),
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            price: self.price,
            quantity: self.quantity,
            filled: self.quantity - self.remaining,
            remaining: self.remaining,
            status,
            reason: None,
        }
    }
}

/// The owner map: every open order, by id.
#[derive(Debug, Clone, Default)]
struct Owners {
    orders: HashMap<String, OpenOrder>,
}

impl Owners {
    fn from_orders(orders: Vec<OpenOrder>) -> Result<Self, String> {
        let mut owners = Self::default();
        for order in orders {
            if order.order_id.is_empty()
                || order.user_id.is_empty()
                || order.remaining == 0
                || order.remaining > order.quantity
            {
                return Err(format!("saved order {} is invalid", order.order_id));
            }
            if owners
                .orders
                .insert(order.order_id.clone(), order)
                .is_some()
            {
                return Err("duplicate saved order".into());
            }
        }
        Ok(owners)
    }

    fn persisted(&self) -> Vec<OpenOrder> {
        let mut orders: Vec<_> = self.orders.values().cloned().collect();
        orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));
        orders
    }

    /// Applies one committed batch and returns, in order, what each user must be told. An error
    /// means the map has drifted from the exchange; the caller then withdraws the whole feed.
    fn apply_batch(&mut self, batch: &[EventEnvelope]) -> Result<Vec<(String, Event)>, String> {
        let seq = batch.last().ok_or("empty committed batch")?.seq_num;
        match committed_batch::decode(batch)? {
            CommittedCommand::NewOrder {
                order,
                outcome: NewOrderOutcome::Rejected { reason },
            } => Ok(vec![(
                order.user_id.clone(),
                Event::Order {
                    seq,
                    order_id: order.order_id.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side.clone(),
                    price: order.price.minor_units(),
                    quantity: order.quantity as u64,
                    filled: 0,
                    remaining: 0,
                    status: "rejected",
                    reason: Some(reason),
                },
            )]),
            CommittedCommand::NewOrder {
                order,
                outcome: NewOrderOutcome::Accepted { executions, .. },
            } => {
                if self.orders.contains_key(&order.order_id) {
                    return Err(format!("accepted order {} is already open", order.order_id));
                }
                if order.leaves_qty == 0 || order.leaves_qty != order.quantity {
                    return Err("accepted order has an invalid initial quantity".into());
                }
                let mut incoming = OpenOrder {
                    order_id: order.order_id.clone(),
                    user_id: order.user_id.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side.clone(),
                    price: order.price.minor_units(),
                    quantity: order.quantity as u64,
                    remaining: order.quantity as u64,
                };
                let incoming_is_buy = matches!(order.side, Side::Buy);
                let mut told = Vec::new();
                for pair in executions {
                    let execution = &pair.first;
                    let (own, resting_id) = if incoming_is_buy {
                        (&execution.buy_order_id, &execution.sell_order_id)
                    } else {
                        (&execution.sell_order_id, &execution.buy_order_id)
                    };
                    if own != &order.order_id
                        || execution.symbol != order.symbol
                        || execution.quantity == 0
                    {
                        return Err("execution does not match its incoming order".into());
                    }
                    let quantity = execution.quantity as u64;
                    let price = execution.price.minor_units();
                    let resting = self
                        .orders
                        .get_mut(resting_id)
                        .ok_or_else(|| format!("execution names unknown order {resting_id}"))?;
                    if resting.symbol != order.symbol
                        || resting.side == order.side
                        || resting.price != price
                    {
                        return Err("execution names the wrong resting order".into());
                    }
                    incoming.remaining = incoming
                        .remaining
                        .checked_sub(quantity)
                        .ok_or("executions exceed the incoming order")?;
                    resting.remaining = resting
                        .remaining
                        .checked_sub(quantity)
                        .ok_or("execution exceeds the resting order")?;
                    told.push((
                        incoming.user_id.clone(),
                        Event::Fill {
                            seq,
                            execution_id: execution.execution_id.clone(),
                            order_id: incoming.order_id.clone(),
                            symbol: incoming.symbol.clone(),
                            side: incoming.side.clone(),
                            price,
                            quantity,
                            remaining: incoming.remaining,
                        },
                    ));
                    told.push((
                        resting.user_id.clone(),
                        Event::Fill {
                            seq,
                            execution_id: execution.execution_id.clone(),
                            order_id: resting.order_id.clone(),
                            symbol: resting.symbol.clone(),
                            side: resting.side.clone(),
                            price,
                            quantity,
                            remaining: resting.remaining,
                        },
                    ));
                    told.push((resting.user_id.clone(), resting.event(seq)));
                    if resting.remaining == 0 {
                        let resting_id = resting_id.clone();
                        self.orders.remove(&resting_id);
                    }
                }
                told.push((incoming.user_id.clone(), incoming.event(seq)));
                if incoming.remaining > 0 {
                    self.orders.insert(incoming.order_id.clone(), incoming);
                }
                Ok(told)
            }
            CommittedCommand::Cancellation {
                order_id,
                user_id,
                outcome: CancelOutcome::Canceled { .. },
            } => {
                let order = self
                    .orders
                    .remove(&order_id)
                    .ok_or_else(|| format!("cancellation of unknown order {order_id}"))?;
                if order.user_id != user_id {
                    return Err(format!("order {order_id} canceled by someone else"));
                }
                Ok(vec![(
                    order.user_id.clone(),
                    order.event_with(seq, "canceled"),
                )])
            }
            CommittedCommand::Cancellation {
                order_id,
                user_id,
                outcome: CancelOutcome::Rejected { reason },
            } => Ok(vec![(
                user_id,
                Event::CancelRejected {
                    seq,
                    order_id,
                    reason,
                },
            )]),
            CommittedCommand::MarketClosed { expired, .. } => {
                let mut told = Vec::with_capacity(expired.len());
                for expiry in expired {
                    let order = self
                        .orders
                        .remove(&expiry.order_id)
                        .ok_or_else(|| format!("expiry of unknown order {}", expiry.order_id))?;
                    told.push((order.user_id.clone(), order.event_with(seq, "expired")));
                }
                if !self.orders.is_empty() {
                    return Err(format!(
                        "{} orders are still open after the close",
                        self.orders.len()
                    ));
                }
                Ok(told)
            }
            CommittedCommand::MarketOpened { .. } | CommittedCommand::Other => Ok(Vec::new()),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    version: u32,
    checkpoint: ReaderCheckpoint,
    orders: Vec<OpenOrder>,
}

fn load_state(path: &Path) -> MdResult<(Owners, Option<ReaderCheckpoint>)> {
    match std::fs::read(path) {
        Ok(bytes) => {
            #[derive(Deserialize)]
            struct Version {
                version: u32,
            }
            let version = serde_json::from_slice::<Version>(&bytes)?.version;
            if version != STATE_VERSION {
                return Err(
                    invalid(format!("unsupported user-feed state version {version}")).into(),
                );
            }
            let saved: StateFile = serde_json::from_slice(&bytes)?;
            let owners = Owners::from_orders(saved.orders).map_err(invalid)?;
            Ok((owners, Some(saved.checkpoint)))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok((Owners::default(), None)),
        Err(error) => Err(error.into()),
    }
}

/// The per-user fan-out: one `broadcast` ring per connected user. Unlike the book feed, a served
/// connection always holds a receiver, so a halt reaches every connection by dropping the rings.
pub(super) struct UserHub {
    state: Mutex<HubState>,
    limits: Limits,
}

#[derive(Default)]
struct HubState {
    users: HashMap<String, broadcast::Sender<Utf8Bytes>>,
    halted: bool,
}

impl UserHub {
    pub(super) fn new(limits: Limits) -> Self {
        Self {
            state: Mutex::new(HubState::default()),
            limits,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HubState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Hands each event to its user's ring, if that user is connected. The follower calls this
    /// with the view's write lock held; it never waits for a client.
    fn deliver(&self, events: Vec<(String, Event)>) {
        let state = self.lock();
        for (user, event) in events {
            if let Some(sender) = state.users.get(&user).filter(|s| s.receiver_count() > 0) {
                let text = serde_json::to_string(&event).expect("an event always serializes");
                let _ = sender.send(text.into());
            }
        }
    }

    /// A receiver of every later event for `user`. The caller holds the view's read lock.
    fn join(&self, user: &str) -> Option<broadcast::Receiver<Utf8Bytes>> {
        let mut state = self.lock();
        if state.halted {
            return None;
        }
        let ring = self.limits.ring;
        Some(
            state
                .users
                .entry(user.to_string())
                .or_insert_with(|| broadcast::channel(ring).0)
                .subscribe(),
        )
    }

    /// Frees a user's ring once none of their connections is left.
    fn release(&self, user: &str) {
        let mut state = self.lock();
        if state
            .users
            .get(user)
            .is_some_and(|sender| sender.receiver_count() == 0)
        {
            state.users.remove(user);
        }
    }

    /// Refuses every later join, and closes every ring: each connection's forwarder then sees
    /// its ring closed and ends the connection with 1011.
    fn halt(&self) {
        let mut state = self.lock();
        state.halted = true;
        state.users.clear();
    }
}

struct View {
    owners: Owners,
    applied: ReaderCheckpoint,
}

type Served = Arc<RwLock<Option<View>>>;

struct Follower {
    reader: StreamReader,
    served: Served,
    hub: Arc<UserHub>,
    state_path: PathBuf,
    save_interval: Duration,
    unsaved: u64,
    last_save: Instant,
}

impl Follower {
    fn new(
        reader: StreamReader,
        served: Served,
        hub: Arc<UserHub>,
        state_path: PathBuf,
        save_interval: Duration,
    ) -> Self {
        Self {
            reader,
            served,
            hub,
            state_path,
            save_interval,
            unsaved: 0,
            last_save: Instant::now(),
        }
    }

    /// Applies the next committed batch, if there is one; `false` means caught up. Its events go
    /// out before the write lock is released, so a client's `ready` sequence and its join happen
    /// between two batches. On a failed batch the view is withdrawn and the error is terminal.
    fn step(&mut self) -> MdResult<bool> {
        let Some(batch) = self.reader.next_batch()? else {
            return Ok(false);
        };
        let after = self.reader.checkpoint();
        let mut served = self
            .served
            .write()
            .map_err(|_| invalid("user-feed view lock poisoned"))?;
        let view = served
            .as_mut()
            .ok_or_else(|| invalid("the user feed is unavailable"))?;
        match view.owners.apply_batch(&batch) {
            Ok(events) => {
                view.applied = after;
                self.hub.deliver(events);
            }
            Err(error) => {
                *served = None;
                return Err(invalid(error).into());
            }
        }
        drop(served);
        self.unsaved += 1;
        Ok(true)
    }

    fn maybe_save(&mut self) -> MdResult<()> {
        if self.unsaved > 0 && self.last_save.elapsed() >= self.save_interval {
            self.save()?;
        }
        Ok(())
    }

    /// The owner map and its checkpoint, copied under one read lock, written outside it. The file
    /// holds user ids: `save_state` creates it with mode 0600.
    fn save(&mut self) -> MdResult<()> {
        let file = {
            let served = self
                .served
                .read()
                .map_err(|_| invalid("user-feed view lock poisoned"))?;
            let view = served
                .as_ref()
                .ok_or_else(|| invalid("the user feed is unavailable"))?;
            StateFile {
                version: STATE_VERSION,
                checkpoint: view.applied.clone(),
                orders: view.owners.persisted(),
            }
        };
        save_state(&self.state_path, &file)?;
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

/// Withdraws the view and closes every connection when the follower thread ends for any reason.
struct WithdrawOnExit(Served, Arc<UserHub>);

impl Drop for WithdrawOnExit {
    fn drop(&mut self) {
        let mut served = self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *served = None;
        drop(served);
        self.1.halt();
    }
}

#[derive(Clone)]
struct FeedState {
    served: Served,
    hub: Arc<UserHub>,
    key: Arc<DecodingKey>,
}

/// Joins `user` at the view's current point: the last sequence applied, and a receiver of every
/// event after it. `None` when the feed is unavailable.
fn join_user(
    served: &Served,
    hub: &UserHub,
    user: &str,
) -> Option<(u64, broadcast::Receiver<Utf8Bytes>)> {
    let served = served.read().ok()?;
    let view = served.as_ref()?;
    let receiver = hub.join(user)?;
    Some((view.applied.next_sequence - 1, receiver))
}

/// The first text message, skipping pings and pongs; `None` when the client is gone or sends a
/// close, an empty string for a binary frame.
async fn first_text(socket: &mut WebSocket) -> Option<String> {
    loop {
        match socket.recv().await? {
            Ok(Message::Text(text)) => return Some(text.to_string()),
            Ok(Message::Binary(_)) => return Some(String::new()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Hello {
    Auth { token: String },
}

/// The user and the moment their token expires, or why the first message is refused.
fn authenticate(text: &str, key: &DecodingKey) -> Result<(String, SystemTime), &'static str> {
    let Ok(Hello::Auth { token }) = serde_json::from_str::<Hello>(text) else {
        return Err("expected {\"type\":\"auth\",\"token\":...} first");
    };
    let claims = verify_token(&token, key).ok_or("invalid or expired token")?;
    // `verify_token` allows the gateway's 60 s of leeway past `exp`. A connection lasts only until
    // `exp`, so a token already past it is refused here rather than served for no time at all.
    let expires = UNIX_EPOCH
        .checked_add(Duration::from_secs(claims.exp as u64))
        .filter(|expires| *expires > SystemTime::now())
        .ok_or("invalid or expired token")?;
    Ok((claims.sub, expires))
}

fn text(value: &impl Serialize) -> Message {
    Message::Text(
        serde_json::to_string(value)
            .expect("a feed message always serializes")
            .into(),
    )
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Notice<'a> {
    Ready { seq: u64 },
    Error { message: &'a str },
}

/// Serves one private connection: authentication, then the user's events until either side ends
/// it, the token expires, or the feed halts.
async fn serve<F>(mut socket: WebSocket, hub: Arc<UserHub>, key: Arc<DecodingKey>, join: F)
where
    F: Fn(&str) -> Option<(u64, broadcast::Receiver<Utf8Bytes>)>,
{
    let limits = hub.limits;
    let first = match timeout(AUTH_DEADLINE, first_text(&mut socket)).await {
        Ok(Some(first)) => first,
        Ok(None) => return,
        Err(_) => {
            book_feed::finish(&mut socket, Close::Policy("authenticate first"), limits).await;
            return;
        }
    };
    let (user, expires) = match authenticate(&first, &key) {
        Ok(authenticated) => authenticated,
        Err(message) => {
            let _ = book_feed::send(
                &mut socket,
                text(&Notice::Error { message }),
                limits.write_deadline,
            )
            .await;
            book_feed::finish(&mut socket, Close::Policy("authentication failed"), limits).await;
            return;
        }
    };
    let Some((seq, receiver)) = join(&user) else {
        book_feed::finish(&mut socket, Close::Unavailable, limits).await;
        return;
    };
    let (queue_tx, mut queue) = mpsc::channel::<Utf8Bytes>(limits.queue);
    let (closing_tx, mut closing) = mpsc::unbounded_channel::<Close>();
    let forwarder = tokio::spawn(book_feed::forward(receiver, queue_tx, closing_tx));
    let until_expiry = expires
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    let expiry = tokio::time::sleep(until_expiry);
    tokio::pin!(expiry);
    let mut ping = tokio::time::interval(limits.ping);
    ping.tick().await;
    let mut last_heard = tokio::time::Instant::now();

    let close = match book_feed::send(
        &mut socket,
        text(&Notice::Ready { seq }),
        limits.write_deadline,
    )
    .await
    {
        Err(close) => close,
        Ok(()) => loop {
            tokio::select! {
                incoming = socket.recv() => {
                    last_heard = tokio::time::Instant::now();
                    match incoming {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break None,
                        Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                            let reply = text(&Notice::Error { message: "this feed takes no requests after auth" });
                            if let Err(close) = book_feed::send(&mut socket, reply, limits.write_deadline).await {
                                break close;
                            }
                        }
                        Some(Ok(_)) => {}
                    }
                }
                Some(event) = queue.recv() => {
                    if let Err(close) = book_feed::send(&mut socket, Message::Text(event), limits.write_deadline).await {
                        break close;
                    }
                }
                Some(reason) = closing.recv() => break Some(reason),
                () = &mut expiry => break Some(Close::Policy("token expired")),
                _ = ping.tick() => {
                    if last_heard.elapsed() > limits.ping * SILENT_PINGS {
                        break Some(Close::Silent);
                    }
                    if let Err(close) = book_feed::send(&mut socket, Message::Ping(Default::default()), limits.write_deadline).await {
                        break close;
                    }
                }
            }
        },
    };
    forwarder.abort();
    let _ = forwarder.await;
    hub.release(&user);
    if let Some(close) = close {
        book_feed::finish(&mut socket, close, limits).await;
    }
}

const UNAVAILABLE: (StatusCode, &str) = (StatusCode::SERVICE_UNAVAILABLE, "user feed unavailable");

async fn health(State(state): State<FeedState>) -> impl IntoResponse {
    match state.served.read() {
        Ok(served) if served.is_some() => (StatusCode::OK, "OK"),
        _ => UNAVAILABLE,
    }
}

async fn user_socket(
    State(state): State<FeedState>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let upgrade = upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .max_frame_size(MAX_CLIENT_MESSAGE);
    upgrade.on_upgrade(move |socket| {
        let (hub, key) = (Arc::clone(&state.hub), Arc::clone(&state.key));
        serve(socket, hub, key, move |user| {
            join_user(&state.served, &state.hub, user)
        })
    })
}

fn router(served: Served, hub: Arc<UserHub>, key: Arc<DecodingKey>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/userfeed/ws", get(user_socket))
        .with_state(FeedState { served, hub, key })
}

pub async fn run(args: &[String]) -> MdResult<()> {
    if !(3..=4).contains(&args.len()) {
        return Err(
            invalid("usage: stock --user-feed JOURNAL STREAM STATE_FILE [LISTEN_ADDR]").into(),
        );
    }
    let secret = jwt_secret().ok_or_else(|| invalid("JWT_SECRET must be set"))?;
    let key = Arc::new(DecodingKey::from_secret(secret.as_bytes()));
    let journal_path = PathBuf::from(&args[0]);
    let stream_path = PathBuf::from(&args[1]);
    let state_path = PathBuf::from(&args[2]);
    let address: SocketAddr = args
        .get(3)
        .map(String::as_str)
        .unwrap_or(DEFAULT_ADDR)
        .parse()?;

    ensure_safe_state_path(&journal_path, &stream_path, &state_path)?;
    let (owners, checkpoint) = load_state(&state_path)?;
    let reader = StreamReader::open(&journal_path, &stream_path, checkpoint)?;
    let applied = reader.checkpoint();
    let served: Served = Arc::new(RwLock::new(Some(View { owners, applied })));
    let hub = Arc::new(UserHub::new(USER_LIMITS));
    let mut follower = Follower::new(
        reader,
        Arc::clone(&served),
        Arc::clone(&hub),
        state_path,
        SAVE_INTERVAL,
    );
    while follower.step()? {
        follower.maybe_save()?;
    }
    follower.save()?;

    let withdraw = WithdrawOnExit(Arc::clone(&served), Arc::clone(&hub));
    thread::Builder::new()
        .name("user-feed-follower".into())
        .spawn(move || {
            let _withdraw = withdraw;
            if let Err(error) = follower.follow() {
                eprintln!("user-feed follower halted: {error}");
            }
        })?;

    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("User feed is listening on {}", listener.local_addr()?);
    axum::serve(listener, router(served, hub, key)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        exchange::{
            core::ExchangeCore,
            event_stream::tests::Fixture,
            market_data::tests::{
                Client, accepted, canceled_by, close_code, closed, connect, envelopes, next_json,
                numbered, open_market, order, order_in, pair, parsed, publish, rejected, request,
                send,
            },
            runtime::recover_runtime_with_stream,
        },
        models::user::Claims,
        types::{
            exchange_event::{ExchangeInputEvent, ExchangeOutputEvent},
            types::{ExchangeCommand, Order, OrderView},
        },
    };
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::Value;
    use tokio::sync::{mpsc, oneshot};

    fn json(told: Vec<(String, Event)>) -> Vec<(String, Value)> {
        told.into_iter()
            .map(|(user, event)| (user, serde_json::to_value(event).unwrap()))
            .collect()
    }

    fn cancel_rejected(order_id: &str, user_id: &str, reason: &str) -> Vec<EventEnvelope> {
        envelopes(
            ExchangeInputEvent::CancelOrderRequested {
                order_id: order_id.to_string(),
                user_id: user_id.to_string(),
            },
            vec![ExchangeOutputEvent::CancelRejected {
                order_id: order_id.to_string(),
                reason: reason.to_string(),
            }],
        )
    }

    /// One user's open orders on one side of one book, as price levels, lowest price first.
    fn levels(owners: &Owners, symbol: &str, side: &Side) -> Vec<(u64, u64)> {
        let mut levels = BTreeMap::new();
        for open in owners.orders.values() {
            if open.symbol == symbol && &open.side == side {
                *levels.entry(open.price).or_default() += open.remaining;
            }
        }
        levels.into_iter().collect()
    }

    /// The oracle test: 3,000 random orders, cancellations, cancellations by someone else and
    /// reused order ids, from four users on two symbols, through the authoritative core, then the
    /// close and an id reused on the next day. After every batch:
    /// - exactly the expected messages were told, no more and no fewer: for each trade a `fill` to
    ///   each side's owner and an `order` to the resting side's owner, then an `order` to the
    ///   incoming user; one message for a cancellation, a refusal or an expiry;
    /// - every event names an order its receiver owns in the core, except a refusal, which goes
    ///   to whoever asked;
    /// - every order event matches the core's view of that order after the batch, and the fills
    ///   told so far add up to its filled quantity;
    /// - the owner map holds exactly the core's resting orders, each with its owner.
    #[test]
    fn every_event_reaches_only_its_owner_and_matches_the_core() {
        let symbols = ["AAPL", "MSFT"];
        let users = ["alice", "bob", "carol", "dave"];
        let mut core = ExchangeCore::new();
        for user in users {
            core.deposit(user.into(), 1_000_000_000).unwrap();
            for symbol in symbols {
                core.deposit_shares(user, symbol, 1_000_000).unwrap();
            }
        }
        let first_day = chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        core.open_market(first_day).unwrap();
        let mut owners = Owners::default();
        let mut resting: Vec<(String, String)> = Vec::new();
        // Every accepted order's user, by order id: ids are unique within the day.
        let mut owner_of: HashMap<String, String> = HashMap::new();
        let mut filled: HashMap<(String, String), u64> = HashMap::new();
        let mut counts: HashMap<String, usize> = HashMap::new();
        let (mut random, mut next) = (0x2545_F491_4F6C_DD1Du64, 1);
        for step in 0..3_000 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let r = random >> 33;
            let someone_else = |owner: &str| {
                let at = users.iter().position(|user| *user == owner).unwrap();
                users[(at + 1 + (r >> 13) as usize % 3) % 4]
            };
            // The batch, who made the request, and the messages expected, as
            // (user, message type, order id), in any order.
            let (batch, requester, mut expected) = match r % 10 {
                0 | 1 if !resting.is_empty() => {
                    let (id, user) = resting.swap_remove((r as usize / 10) % resting.len());
                    match core.cancel_order_for_user(&id, &user) {
                        Ok(seq) => {
                            let expected = vec![(user.clone(), "order", id.clone())];
                            (canceled_by(&id, &user, seq), user, expected)
                        }
                        Err(_) => continue, // filled since it rested
                    }
                }
                2 if !resting.is_empty() => {
                    let (id, owner) = resting[(r as usize / 10) % resting.len()].clone();
                    let other = someone_else(&owner);
                    assert!(core.cancel_order_for_user(&id, other).is_err());
                    let expected = vec![(other.to_string(), "cancel_rejected", id.clone())];
                    let batch = cancel_rejected(&id, other, "not yours");
                    (batch, other.to_string(), expected)
                }
                3 if !resting.is_empty() => {
                    let (id, owner) = resting[(r as usize / 10) % resting.len()].clone();
                    let other = someone_else(&owner);
                    let reused = order_in(symbols[0], &id, other, "BUY", 100, 1);
                    assert!(core.add_order(reused.clone()).is_err());
                    let expected = vec![(other.to_string(), "order", id.clone())];
                    (rejected(reused), other.to_string(), expected)
                }
                _ => {
                    let order = order_in(
                        symbols[(r % 2) as usize],
                        &format!("o{step}"),
                        users[((r >> 3) % 4) as usize],
                        if (r >> 5) % 2 == 0 { "BUY" } else { "SELL" },
                        95 + (r >> 7) % 11,
                        1 + ((r >> 11) % 9) as u32,
                    );
                    let user = order.user_id.clone();
                    let id = order.order_id.clone();
                    match core.add_order(order.clone()) {
                        Ok(outcome) => {
                            if outcome.view.remaining_quantity > 0 {
                                resting.push((id.clone(), user.clone()));
                            }
                            let mut expected = Vec::new();
                            // Each trade is a pair of execution events; its first names both
                            // orders.
                            for trade in outcome.executions.chunks(2) {
                                let execution = &trade[0];
                                let resting_id = if execution.buy_order_id == id {
                                    &execution.sell_order_id
                                } else {
                                    &execution.buy_order_id
                                };
                                let resting_owner = owner_of[resting_id].clone();
                                expected.push((user.clone(), "fill", id.clone()));
                                expected.push((resting_owner.clone(), "fill", resting_id.clone()));
                                expected.push((resting_owner, "order", resting_id.clone()));
                            }
                            expected.push((user.clone(), "order", id.clone()));
                            owner_of.insert(id, user.clone());
                            (accepted(order, outcome.executions), user, expected)
                        }
                        Err(_) => {
                            let expected = vec![(user.clone(), "order", id)];
                            (rejected(order), user, expected)
                        }
                    }
                }
            };
            let batch = numbered(batch, &mut next);
            let seq = batch.last().unwrap().seq_num;
            let told = json(owners.apply_batch(&batch).unwrap());
            let mut keys: Vec<_> = told
                .iter()
                .map(|(user, event)| {
                    let kind = event["type"].as_str().unwrap();
                    let kind = ["fill", "order", "cancel_rejected"]
                        .into_iter()
                        .find(|known| *known == kind)
                        .unwrap();
                    (
                        user.clone(),
                        kind,
                        event["order_id"].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            keys.sort();
            expected.sort();
            assert_eq!(keys, expected, "step {step}: the wrong set of messages");
            for (user, event) in told {
                assert_eq!(event["seq"], seq, "step {step}");
                let kind = event["type"].as_str().unwrap().to_string();
                let id = event["order_id"].as_str().unwrap();
                if kind == "cancel_rejected" || event["status"] == "rejected" {
                    assert_eq!(
                        user, requester,
                        "step {step}: a refusal sent to someone else"
                    );
                    *counts.entry(format!("refused {kind}")).or_default() += 1;
                    continue;
                }
                let view: OrderView = core.order_view(id, &user).unwrap_or_else(|| {
                    panic!("step {step}: {kind} of {id} sent to {user}, who does not own it")
                });
                if kind == "fill" {
                    *filled.entry((user, id.to_string())).or_default() +=
                        event["quantity"].as_u64().unwrap();
                } else {
                    assert_eq!(event["status"], view.status.as_str(), "step {step}, {id}");
                    assert_eq!(event["filled"], view.filled_quantity, "step {step}, {id}");
                    assert_eq!(event["remaining"], view.remaining_quantity, "step {step}");
                    let told = filled.get(&(user, id.to_string())).copied();
                    assert_eq!(
                        told.unwrap_or(0),
                        view.filled_quantity as u64,
                        "step {step}"
                    );
                    *counts
                        .entry(event["status"].as_str().unwrap().into())
                        .or_default() += 1;
                }
                *counts.entry(kind).or_default() += 1;
            }
            for open in owners.orders.values() {
                let view = core.order_view(&open.order_id, &open.user_id).unwrap();
                assert_eq!(
                    view.remaining_quantity as u64, open.remaining,
                    "step {step}"
                );
            }
            for symbol in symbols {
                let (bids, asks) = core
                    .l2_snapshot(symbol, usize::MAX)
                    .map_or_else(Default::default, |view| (view.bids, view.asks));
                let sorted = |levels: Vec<crate::types::types::L2Level>| {
                    let mut levels: Vec<_> = levels.iter().map(|l| (l.price, l.quantity)).collect();
                    levels.sort();
                    levels
                };
                assert_eq!(
                    levels(&owners, symbol, &Side::Buy),
                    sorted(bids),
                    "step {step}"
                );
                assert_eq!(
                    levels(&owners, symbol, &Side::Sell),
                    sorted(asks),
                    "step {step}"
                );
            }
        }

        // The close: every order still resting expires, to its owner, and the map empties.
        let expired = core.prepare_expiry().unwrap().expired;
        assert!(
            expired.len() > 10,
            "only {} orders rest at the close",
            expired.len()
        );
        core.close_market().unwrap();
        let expiries: Vec<(&str, u64)> = expired
            .iter()
            .map(|(id, seq)| (id.as_str(), *seq))
            .collect();
        let told = json(
            owners
                .apply_batch(&numbered(closed(&expiries), &mut next))
                .unwrap(),
        );
        assert_eq!(told.len(), expired.len());
        for ((user, event), (id, _)) in told.iter().zip(&expired) {
            assert_eq!(user, &owner_of[id]);
            assert_eq!(event["order_id"], id.as_str());
            let view = core.order_view(id, user).unwrap();
            assert_eq!(
                (view.status.as_str(), event["status"].as_str()),
                ("expired", Some("expired"))
            );
            assert_eq!(event["remaining"], view.remaining_quantity);
        }
        assert!(owners.orders.is_empty());

        // The next day, another user may take an id that expired: it is a new entry, theirs.
        let (reused, first_owner) = expired
            .iter()
            .map(|(id, _)| (id.clone(), owner_of[id].clone()))
            .next()
            .unwrap();
        let next_user = users.iter().find(|user| **user != first_owner).unwrap();
        core.open_market(first_day.succ_opt().unwrap()).unwrap();
        let order = order_in(symbols[0], &reused, next_user, "BUY", 1, 1);
        let outcome = core.add_order(order.clone()).unwrap();
        assert!(outcome.executions.is_empty());
        let told = json(
            owners
                .apply_batch(&numbered(accepted(order, vec![]), &mut next))
                .unwrap(),
        );
        assert_eq!(told.len(), 1);
        assert_eq!(
            (told[0].0.as_str(), &told[0].1["status"]),
            (*next_user, &Value::from("new"))
        );
        assert_eq!(owners.orders[&reused].user_id, *next_user);

        for (kind, at_least) in [
            ("fill", 1_000),
            ("partially_filled", 100),
            ("filled", 100),
            ("canceled", 100),
            ("refused cancel_rejected", 100),
            ("refused order", 100),
        ] {
            let seen = counts.get(kind).copied().unwrap_or(0);
            assert!(seen >= at_least, "only {seen} {kind} events: {counts:?}");
        }
    }

    /// Order ids are the clients' own. A second user reusing a live order's id is refused, and the
    /// refusal goes to them alone; so does someone else's cancellation. The first user's order and
    /// its owner stay in place, and its next fill still reaches them.
    #[test]
    fn refusals_go_to_whoever_asked_and_never_touch_the_owner_map() {
        let mut owners = Owners::default();
        let told = json(
            owners
                .apply_batch(&accepted(order("o1", "alice", "SELL", 100, 5), vec![]))
                .unwrap(),
        );
        assert_eq!(told.len(), 1);
        assert_eq!(told[0].0, "alice");
        assert_eq!(told[0].1["status"], "new");
        let original = owners.persisted();

        let told = json(
            owners
                .apply_batch(&rejected(order("o1", "mallory", "BUY", 100, 5)))
                .unwrap(),
        );
        assert_eq!(told.len(), 1);
        assert_eq!(told[0].0, "mallory");
        assert_eq!(told[0].1["status"], "rejected");
        assert_eq!(told[0].1["reason"], "rejected");
        assert_eq!(told[0].1["remaining"], 0);

        let told = owners
            .apply_batch(&cancel_rejected("o1", "mallory", "not yours"))
            .unwrap();
        assert_eq!(
            told,
            vec![(
                "mallory".to_string(),
                Event::CancelRejected {
                    seq: 2,
                    order_id: "o1".into(),
                    reason: "not yours".into(),
                }
            )]
        );
        assert_eq!(owners.persisted(), original);

        let told = json(
            owners
                .apply_batch(&accepted(
                    order("o2", "bob", "BUY", 101, 2),
                    pair("t1", "o2", "o1", 100, 2),
                ))
                .unwrap(),
        );
        let summary: Vec<_> = told
            .iter()
            .map(|(user, event)| {
                let kind = event["status"].as_str().unwrap_or("fill");
                (user.as_str(), kind, event["remaining"].as_u64().unwrap())
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("bob", "fill", 0),
                ("alice", "fill", 3),
                ("alice", "partially_filled", 3),
                ("bob", "filled", 0),
            ]
        );
        assert_eq!(told[0].1["execution_id"], "t1-buy");
        assert_eq!(told[1].1["execution_id"], "t1-buy");
        assert_eq!(told[0].1["price"], 100);
        assert_eq!(owners.orders["o1"].remaining, 3);
        assert!(!owners.orders.contains_key("o2"));
    }

    /// A batch the map cannot explain is drift, never a guess.
    #[test]
    fn a_batch_the_owner_map_cannot_explain_is_an_error() {
        let resting = || {
            let mut owners = Owners::default();
            owners
                .apply_batch(&accepted(order("o1", "alice", "SELL", 100, 5), vec![]))
                .unwrap();
            owners
        };
        let cases: Vec<(Vec<EventEnvelope>, &str)> = vec![
            (
                accepted(order("o1", "bob", "BUY", 99, 1), vec![]),
                "already open",
            ),
            (
                accepted(
                    order("o2", "bob", "BUY", 100, 1),
                    pair("t", "o2", "ghost", 100, 1),
                ),
                "unknown order ghost",
            ),
            (
                accepted(
                    order("o2", "bob", "BUY", 101, 1),
                    pair("t", "o2", "o1", 101, 1),
                ),
                "wrong resting order",
            ),
            (
                accepted(
                    order("o2", "bob", "BUY", 100, 9),
                    pair("t", "o2", "o1", 100, 6),
                ),
                "exceeds the resting order",
            ),
            (canceled_by("o1", "bob", 2), "canceled by someone else"),
            (canceled_by("ghost", "alice", 2), "unknown order ghost"),
            (closed(&[("ghost", 2)]), "expiry of unknown order"),
            (closed(&[]), "still open after the close"),
        ];
        for (batch, expected) in cases {
            let error = resting().apply_batch(&batch).unwrap_err();
            assert!(error.contains(expected), "{error:?}, expected {expected:?}");
        }
        let mut wrong_side = Owners::default();
        wrong_side
            .apply_batch(&accepted(order("b1", "alice", "BUY", 100, 1), vec![]))
            .unwrap();
        let error = wrong_side
            .apply_batch(&accepted(
                order("b2", "bob", "BUY", 100, 1),
                pair("t", "b2", "b1", 100, 1),
            ))
            .unwrap_err();
        assert!(error.contains("wrong resting order"), "{error:?}");
    }

    /// The close expires every open order to its owner and empties the map; the next day may reuse
    /// an id.
    #[test]
    fn the_close_tells_each_owner_and_the_next_day_starts_empty() {
        let mut owners = Owners::default();
        for open in [
            order("o1", "alice", "SELL", 101, 5),
            order("o2", "bob", "BUY", 99, 3),
        ] {
            owners.apply_batch(&accepted(open, vec![])).unwrap();
        }
        let told = json(
            owners
                .apply_batch(&closed(&[("o2", 3), ("o1", 4)]))
                .unwrap(),
        );
        let summary: Vec<_> = told
            .iter()
            .map(|(user, event)| {
                (
                    user.as_str(),
                    event["order_id"].as_str().unwrap(),
                    event["status"].as_str().unwrap(),
                    event["remaining"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [("bob", "o2", "expired", 3), ("alice", "o1", "expired", 5)]
        );
        assert!(owners.orders.is_empty());
        owners
            .apply_batch(&accepted(order("o1", "bob", "BUY", 99, 1), vec![]))
            .unwrap();
        assert_eq!(owners.orders["o1"].user_id, "bob");
    }

    #[test]
    fn state_round_trips_with_owner_only_permissions_and_bad_state_refuses_startup() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let checkpoint = fixture.reader().checkpoint();
        let state_path = fixture.dir.join("user-feed.json");
        let open = OpenOrder {
            order_id: "o1".into(),
            user_id: "alice".into(),
            symbol: "AAPL".into(),
            side: Side::Sell,
            price: 101,
            quantity: 5,
            remaining: 2,
        };
        let file = |orders: Vec<OpenOrder>, version| StateFile {
            version,
            checkpoint: checkpoint.clone(),
            orders,
        };
        save_state(&state_path, &file(vec![open.clone()], STATE_VERSION)).unwrap();
        let (loaded, loaded_checkpoint) = load_state(&state_path).unwrap();
        assert_eq!(loaded.persisted(), vec![open.clone()]);
        assert_eq!(loaded_checkpoint.unwrap(), checkpoint);
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&state_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        let refused = |file: StateFile| {
            std::fs::write(&state_path, serde_json::to_vec(&file).unwrap()).unwrap();
            load_state(&state_path).unwrap_err().to_string()
        };
        assert!(refused(file(vec![], 99)).contains("unsupported user-feed state version 99"));
        let duplicate = vec![open.clone(), open.clone()];
        assert!(refused(file(duplicate, STATE_VERSION)).contains("duplicate"));
        for bad in [
            OpenOrder {
                remaining: 0,
                ..open.clone()
            },
            OpenOrder {
                remaining: 6,
                ..open.clone()
            },
            OpenOrder {
                user_id: String::new(),
                ..open.clone()
            },
        ] {
            assert!(refused(file(vec![bad], STATE_VERSION)).contains("invalid"));
        }
        assert!(
            load_state(&fixture.dir.join("missing.json"))
                .unwrap()
                .1
                .is_none()
        );
    }

    /// A user's ring lives while any of their connections does and is freed when the last leaves;
    /// an event for a user with no connection allocates no ring; a halt closes the rings and
    /// refuses later joins.
    #[test]
    fn a_users_ring_is_freed_when_their_last_connection_leaves() {
        let hub = UserHub::new(Limits::DEFAULT);
        let event = |order_id: &str| Event::CancelRejected {
            seq: 1,
            order_id: order_id.into(),
            reason: "r".into(),
        };
        let mut first = hub.join("alice").unwrap();
        let mut second = hub.join("alice").unwrap();
        hub.deliver(vec![
            ("alice".into(), event("a")),
            ("bob".into(), event("b")),
        ]);
        assert!(!hub.lock().users.contains_key("bob"));
        for receiver in [&mut first, &mut second] {
            assert_eq!(parsed(&receiver.try_recv().unwrap())["order_id"], "a");
        }
        drop(first);
        hub.release("alice");
        assert!(hub.lock().users.contains_key("alice"));
        drop(second);
        hub.release("alice");
        assert!(hub.lock().users.is_empty());

        let mut third = hub.join("alice").unwrap();
        hub.halt();
        assert!(matches!(
            third.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        assert!(hub.join("alice").is_none());
    }

    fn reply<T>(
        tx: &mpsc::Sender<ExchangeCommand>,
        command: impl FnOnce(oneshot::Sender<T>) -> ExchangeCommand,
    ) -> T {
        let (respond_to, reply) = oneshot::channel();
        send(tx, command(respond_to));
        reply.blocking_recv().unwrap()
    }

    fn fund(tx: &mpsc::Sender<ExchangeCommand>, user: &str) {
        reply(tx, |respond_to| ExchangeCommand::Deposit {
            user_id: user.into(),
            amount: 1_000_000,
            respond_to,
        })
        .unwrap();
        reply(tx, |respond_to| ExchangeCommand::DepositShares {
            user_id: user.into(),
            symbol: "AAPL".into(),
            quantity: 100,
            respond_to,
        })
        .unwrap();
    }

    fn place(tx: &mpsc::Sender<ExchangeCommand>, order: Order) -> Result<OrderView, String> {
        reply(tx, |respond_to| {
            ExchangeCommand::place_order(order, respond_to)
        })
    }

    fn cancel(
        tx: &mpsc::Sender<ExchangeCommand>,
        order_id: &str,
        user: &str,
    ) -> Result<(), String> {
        reply(tx, |respond_to| ExchangeCommand::CancelOrder {
            order_id: order_id.into(),
            user_id: user.into(),
            respond_to,
        })
    }

    fn drain(receiver: &mut broadcast::Receiver<Utf8Bytes>) -> Vec<Value> {
        std::iter::from_fn(|| receiver.try_recv().ok())
            .map(|text| parsed(&text))
            .collect()
    }

    fn served_with(owners: Owners, reader: &StreamReader) -> Served {
        Arc::new(RwLock::new(Some(View {
            owners,
            applied: reader.checkpoint(),
        })))
    }

    fn feed_on_journal(fixture: &Fixture, limits: Limits) -> (Served, Arc<UserHub>, Follower) {
        let reader = fixture.reader();
        let served = served_with(Owners::default(), &reader);
        let hub = Arc::new(UserHub::new(limits));
        let follower = Follower::new(
            reader,
            Arc::clone(&served),
            Arc::clone(&hub),
            fixture.dir.join("user-feed.json"),
            Duration::from_secs(3600),
        );
        (served, hub, follower)
    }

    /// The real exchange drives the feed, and a restart from the saved map still knows who owns an
    /// order placed before it: the fill, the refused cancellation and the expiry that follow the
    /// restart each reach the right user.
    #[test]
    fn a_restart_from_the_saved_map_still_tells_the_owners_of_older_orders() {
        let fixture = Fixture::new();
        let (tx, rx) = mpsc::channel(16);
        let runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let worker = thread::spawn(move || runtime.run());
        open_market(&tx);
        fund(&tx, "alice");
        fund(&tx, "bob");
        place(&tx, order("sell-1", "alice", "SELL", 101, 5)).unwrap();

        let state_path = fixture.dir.join("user-feed.json");
        let (_served, _hub, mut first) = feed_on_journal(&fixture, Limits::DEFAULT);
        while first.step().unwrap() {}
        first.save().unwrap();
        drop(first);

        place(&tx, order("buy-1", "bob", "BUY", 102, 3)).unwrap();
        assert!(cancel(&tx, "sell-1", "carol").is_err());
        reply(&tx, |respond_to| ExchangeCommand::CloseMarket {
            respond_to,
        })
        .unwrap();
        drop(tx);
        worker.join().unwrap();

        let (owners, checkpoint) = load_state(&state_path).unwrap();
        assert_eq!(owners.persisted().len(), 1);
        let reader = StreamReader::open(&fixture.log, &fixture.bus, checkpoint).unwrap();
        let served = served_with(owners, &reader);
        let hub = Arc::new(UserHub::new(Limits::DEFAULT));
        let (_, mut alice) = join_user(&served, &hub, "alice").unwrap();
        let (_, mut bob) = join_user(&served, &hub, "bob").unwrap();
        let (_, mut carol) = join_user(&served, &hub, "carol").unwrap();
        let mut second = Follower::new(
            reader,
            Arc::clone(&served),
            Arc::clone(&hub),
            state_path,
            Duration::from_secs(3600),
        );
        while second.step().unwrap() {}

        let kinds = |events: &[Value]| -> Vec<(String, u64)> {
            events
                .iter()
                .map(|event| {
                    let kind = event["status"].as_str().or(event["type"].as_str());
                    (
                        kind.unwrap().to_string(),
                        event["remaining"].as_u64().unwrap_or(0),
                    )
                })
                .collect()
        };
        let alice = drain(&mut alice);
        assert_eq!(
            kinds(&alice),
            [
                ("fill".to_string(), 2),
                ("partially_filled".to_string(), 2),
                ("expired".to_string(), 2)
            ]
        );
        assert_eq!(alice[0]["price"], 101);
        assert_eq!(
            kinds(&drain(&mut bob)),
            [("fill".to_string(), 0), ("filled".to_string(), 0)]
        );
        let carol = drain(&mut carol);
        assert_eq!(carol.len(), 1);
        assert_eq!(carol[0]["type"], "cancel_rejected");
        assert_eq!(carol[0]["order_id"], "sell-1");
        assert!(
            served
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .owners
                .orders
                .is_empty()
        );
    }

    /// Durable before visible: while an order's journal sync is held, its owner hears nothing.
    #[test]
    fn nothing_reaches_a_user_before_its_sync() {
        let fixture = Fixture::new();
        let (tx, rx) = mpsc::channel(16);
        let mut runtime = recover_runtime_with_stream(rx, &fixture.log, &fixture.bus).unwrap();
        let (gate, arrived) = runtime.hold_syncs_for_test();
        let worker = thread::spawn(move || runtime.run());
        let synced = |command: ExchangeCommand| {
            send(&tx, command);
            arrived.recv().unwrap();
            gate.send(true).unwrap();
        };
        let (respond_to, opened) = oneshot::channel();
        synced(ExchangeCommand::OpenMarket {
            trading_day: chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
            respond_to,
        });
        opened.blocking_recv().unwrap().unwrap();
        let (respond_to, deposited) = oneshot::channel();
        synced(ExchangeCommand::DepositShares {
            user_id: "alice".into(),
            symbol: "AAPL".into(),
            quantity: 5,
            respond_to,
        });
        deposited.blocking_recv().unwrap().unwrap();

        let (served, hub, mut follower) = feed_on_journal(&fixture, Limits::DEFAULT);
        while follower.step().unwrap() {}
        let (_, mut receiver) = join_user(&served, &hub, "alice").unwrap();

        let (respond_to, placed) = oneshot::channel();
        send(
            &tx,
            ExchangeCommand::place_order(order("sell", "alice", "SELL", 101, 5), respond_to),
        );
        arrived.recv().unwrap(); // its sync is held
        assert!(!follower.step().unwrap(), "published before its sync");
        assert!(receiver.try_recv().is_err(), "an event before its sync");

        gate.send(true).unwrap();
        placed.blocking_recv().unwrap().unwrap();
        assert!(follower.step().unwrap());
        let event = parsed(&receiver.try_recv().unwrap());
        assert_eq!(
            (event["order_id"].as_str(), event["status"].as_str()),
            (Some("sell"), Some("new"))
        );
        drop(gate);
        drop(tx);
        worker.join().unwrap();
    }

    /// `ready`'s sequence and the join are taken together, under the view's read lock, while the
    /// follower delivers each batch under its write lock. So a user joining while batches arrive
    /// receives exactly the events of the batches after its sequence: none missed, none from
    /// before. The race is real but its outcome is timing: up to 100 joins race a follower
    /// applying 2,000 batches, and at least 3 must land mid-run.
    #[test]
    fn a_join_racing_the_follower_gets_exactly_the_events_after_its_sequence() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut next = 1;
        // Each batch is one rejected order of alice's: one message for her, nothing in the map.
        // The journal is synced once for all of them, then the stream publishes them.
        let mut records = Vec::new();
        let mut batch_seqs = Vec::new();
        for step in 0..2_000 {
            let batch = numbered(
                rejected(order(&format!("r{step}"), "alice", "BUY", 100, 1)),
                &mut next,
            );
            let record = crate::exchange::event_store::encode_record(&batch).unwrap();
            store.write_records(&record).unwrap();
            batch_seqs.push(batch.last().unwrap().seq_num);
            records.push(record);
        }
        store.sync().unwrap();
        for (record, seq) in records.iter().zip(&batch_seqs) {
            writer.append(record, *seq).unwrap();
        }
        let limits = Limits {
            ring: 4_096,
            ..USER_LIMITS
        };
        let (served, hub, mut follower) = feed_on_journal(&fixture, limits);
        let stepping = thread::spawn(move || while follower.step().unwrap() {});
        let mut joins = Vec::new();
        while !stepping.is_finished() && joins.len() < 100 {
            joins.push(join_user(&served, &hub, "alice").unwrap());
            thread::sleep(Duration::from_micros(50));
        }
        stepping.join().unwrap();
        let last = *batch_seqs.last().unwrap();
        let mid_run = joins
            .iter()
            .filter(|(seq, _)| *seq > 0 && *seq < last)
            .count();
        assert!(
            mid_run >= 3,
            "only {mid_run} of {} joins landed mid-run",
            joins.len()
        );
        for (seq, mut receiver) in joins {
            assert!(
                seq == 0 || batch_seqs.contains(&seq),
                "ready inside a batch: {seq}"
            );
            let received: Vec<u64> = drain(&mut receiver)
                .iter()
                .map(|event| event["seq"].as_u64().unwrap())
                .collect();
            let expected: Vec<u64> = batch_seqs.iter().copied().filter(|s| *s > seq).collect();
            assert_eq!(received, expected, "joined at {seq}");
        }
    }

    /// The same property, made deterministic: a joiner keeps the view's read lock until it has
    /// joined the hub, so the follower cannot deliver a batch between `ready`'s sequence and the
    /// join. The test holds the hub, so the joiner waits inside its join; meanwhile the view must
    /// stay locked against a writer (the follower).
    #[test]
    fn a_joiner_holds_the_view_until_it_has_joined() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let (served, hub, _follower) = feed_on_journal(&fixture, USER_LIMITS);
        let held = hub.lock();
        let joiner = {
            let (served, hub) = (Arc::clone(&served), Arc::clone(&hub));
            thread::spawn(move || join_user(&served, &hub, "alice").map(|(seq, _)| seq))
        };
        thread::sleep(Duration::from_millis(200));
        assert!(
            matches!(served.try_write(), Err(std::sync::TryLockError::WouldBlock)),
            "the view was released before the join"
        );
        drop(held);
        assert_eq!(joiner.join().unwrap(), Some(0));
    }

    // The socket.

    const SECRET: &str = "user-feed-test-secret";

    fn token(user: &str, lifetime: Duration, secret: &str) -> String {
        let exp = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + lifetime).as_secs();
        jsonwebtoken::encode(
            &Header::default(),
            &Claims {
                sub: user.into(),
                exp: exp as usize,
            },
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn auth(token: &str) -> String {
        format!(r#"{{"type":"auth","token":"{token}"}}"#)
    }

    async fn feed_server(served: Served, hub: Arc<UserHub>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let key = Arc::new(DecodingKey::from_secret(SECRET.as_bytes()));
        tokio::spawn(async move {
            axum::serve(listener, router(served, hub, key))
                .await
                .unwrap()
        });
        format!("ws://{address}/userfeed/ws")
    }

    /// A client authenticated as `user`, and the sequence its `ready` message names.
    async fn signed_in(url: &str, user: &str) -> (Client, u64) {
        let mut client = connect(url).await;
        request(
            &mut client,
            &auth(&token(user, Duration::from_secs(3600), SECRET)),
        )
        .await;
        let ready = next_json(&mut client).await;
        assert_eq!(ready["type"], "ready", "{ready}");
        (client, ready["seq"].as_u64().unwrap())
    }

    /// End to end over the socket: `ready` names the last applied sequence, each user receives
    /// their own side of a trade and nothing of anyone else's, and a request after `auth` is
    /// answered with an error without closing the connection.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_user_receives_their_own_events_and_nothing_else() {
        let fixture = Fixture::new();
        let (mut store, mut writer) = fixture.start(4096);
        let mut next = 1;
        let mut publish_batch =
            |batch| publish(&mut store, &mut writer, &numbered(batch, &mut next));
        publish_batch(accepted(order("sell-1", "alice", "SELL", 101, 5), vec![]));
        let (served, hub, mut follower) = feed_on_journal(&fixture, Limits::DEFAULT);
        assert!(follower.step().unwrap());
        let url = feed_server(served, hub).await;
        let (mut alice, alice_ready) = signed_in(&url, "alice").await;
        let (mut bob, bob_ready) = signed_in(&url, "bob").await;
        let (mut carol, _) = signed_in(&url, "carol").await;
        assert_eq!((alice_ready, bob_ready), (2, 2));

        request(&mut carol, r#"{"type":"subscribe"}"#).await;
        assert_eq!(
            next_json(&mut carol).await["message"],
            "this feed takes no requests after auth"
        );
        publish_batch(accepted(
            order("buy-1", "bob", "BUY", 101, 2),
            pair("t1", "buy-1", "sell-1", 101, 2),
        ));
        publish_batch(rejected(order("sell-1", "carol", "SELL", 101, 1)));
        publish_batch(accepted(order("sell-2", "alice", "SELL", 105, 1), vec![]));
        while follower.step().unwrap() {}

        let fill = next_json(&mut alice).await;
        assert_eq!(fill["type"], "fill");
        assert_eq!(fill["order_id"], "sell-1");
        assert_eq!(fill["seq"], 6);
        assert_eq!(fill["remaining"], 3);
        assert_eq!(next_json(&mut alice).await["status"], "partially_filled");
        // Carol's refusal names alice's order id, but it is carol's alone.
        assert_eq!(next_json(&mut alice).await["order_id"], "sell-2");
        assert_eq!(next_json(&mut bob).await["type"], "fill");
        assert_eq!(next_json(&mut bob).await["status"], "filled");
        let refusal = next_json(&mut carol).await;
        assert_eq!(refusal["status"], "rejected");
        assert_eq!(refusal["seq"], 8);

        // Bob heard nothing of carol's refusal or alice's later order: his next message is his own.
        publish_batch(accepted(order("buy-2", "bob", "BUY", 90, 1), vec![]));
        while follower.step().unwrap() {}
        assert_eq!(next_json(&mut bob).await["order_id"], "buy-2");
    }

    /// A first message over `MAX_CLIENT_MESSAGE` is never read: the connection ends without an
    /// answer. Without the limit it would be parsed, refused as a token, and answered with `error`
    /// and 1008.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_message_over_the_limit_ends_the_connection_unread() {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message;
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let (served, hub, _follower) = feed_on_journal(&fixture, USER_LIMITS);
        let url = feed_server(served, hub).await;
        let mut client = connect(&url).await;
        request(&mut client, &auth(&"x".repeat(2 * MAX_CLIENT_MESSAGE))).await;
        let end = loop {
            match timeout(Duration::from_secs(10), client.next())
                .await
                .expect("the connection was not ended within 10 s")
            {
                Some(Ok(Message::Text(text))) => panic!("the oversized message was read: {text}"),
                Some(Ok(Message::Close(frame))) => break frame.map(|frame| u16::from(frame.code)),
                Some(Ok(_)) => {}
                None | Some(Err(_)) => break None,
            }
        };
        assert_ne!(end, Some(1008));
    }

    /// Every refused first message gets an error and closes with 1008: a bad signature, a token
    /// expired within `verify_token`'s leeway, a token that is not a JWT, a message that is not
    /// `auth`, a binary frame, and silence past the auth deadline.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation() {
        use futures_util::SinkExt;
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let (served, hub, _follower) = feed_on_journal(&fixture, Limits::DEFAULT);
        let url = feed_server(served, hub).await;

        let forged = token("alice", Duration::from_secs(3600), "another-secret");
        // Expired 10 s ago: within `verify_token`'s 60 s leeway, refused all the same.
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 10;
        let just_expired = jsonwebtoken::encode(
            &Header::default(),
            &Claims {
                sub: "alice".into(),
                exp: exp as usize,
            },
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        for (first, message) in [
            (auth(&forged), "invalid or expired token"),
            (auth(&just_expired), "invalid or expired token"),
            (auth("not-a-jwt"), "invalid or expired token"),
            (
                r#"{"type":"subscribe","channel":"book","symbol":"AAPL"}"#.to_string(),
                "expected {\"type\":\"auth\",\"token\":...} first",
            ),
        ] {
            let mut client = connect(&url).await;
            request(&mut client, &first).await;
            assert_eq!(next_json(&mut client).await["message"], message);
            assert_eq!(close_code(&mut client).await, Some(1008));
        }

        let mut binary = connect(&url).await;
        binary
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                vec![1].into(),
            ))
            .await
            .unwrap();
        assert_eq!(next_json(&mut binary).await["type"], "error");
        assert_eq!(close_code(&mut binary).await, Some(1008));

        let started = std::time::Instant::now();
        let mut silent = connect(&url).await;
        assert_eq!(close_code(&mut silent).await, Some(1008));
        assert!(
            started.elapsed() >= AUTH_DEADLINE,
            "{:?}",
            started.elapsed()
        );
    }

    /// A connection lasts no longer than its token: it closes with 1008 when the token expires.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_closes_when_its_token_expires() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let limits = Limits {
            ping: Duration::from_secs(60),
            ..Limits::DEFAULT
        };
        let (served, hub, _follower) = feed_on_journal(&fixture, limits);
        let url = feed_server(served, hub).await;
        let mut client = connect(&url).await;
        request(
            &mut client,
            &auth(&token("alice", Duration::from_secs(2), SECRET)),
        )
        .await;
        assert_eq!(next_json(&mut client).await["type"], "ready");
        let started = std::time::Instant::now();
        assert_eq!(close_code(&mut client).await, Some(1008));
        // `exp` counts whole seconds, so a two-second token lasts between one and two seconds.
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "{:?}",
            started.elapsed()
        );
    }

    /// A client that answers nothing, not even pings, is closed as gone (1001).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_that_never_answers_pings_is_closed_as_gone() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let limits = Limits {
            ping: Duration::from_millis(200),
            ..Limits::DEFAULT
        };
        let (served, hub, _follower) = feed_on_journal(&fixture, limits);
        let url = feed_server(served, hub).await;
        let (mut client, _) = signed_in(&url, "alice").await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(close_code(&mut client).await, Some(1001));
    }

    /// A halted follower closes every connection (1011), and a connection made afterwards is
    /// closed once it authenticates. Pings are 60 s apart, so a connection the halt missed cannot
    /// be kept alive by them.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_halted_follower_closes_every_connection() {
        let fixture = Fixture::new();
        let (_store, _writer) = fixture.start(4096);
        let limits = Limits {
            ping: Duration::from_secs(60),
            ..Limits::DEFAULT
        };
        let (served, hub, follower) = feed_on_journal(&fixture, limits);
        let url = feed_server(Arc::clone(&served), Arc::clone(&hub)).await;
        let (mut alice, _) = signed_in(&url, "alice").await;
        let (mut bob, _) = signed_in(&url, "bob").await;

        drop(WithdrawOnExit(Arc::clone(&served), Arc::clone(&hub)));
        drop(follower);
        assert_eq!(close_code(&mut alice).await, Some(1011));
        assert_eq!(close_code(&mut bob).await, Some(1011));
        let mut late = connect(&url).await;
        request(
            &mut late,
            &auth(&token("alice", Duration::from_secs(3600), SECRET)),
        )
        .await;
        assert_eq!(close_code(&mut late).await, Some(1011));
    }
}
