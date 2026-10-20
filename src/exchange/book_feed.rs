//! The public L2 book feed over WebSocket (milestone 26 part 1).
//!
//! A client connects to the market-data process (`GET /marketdata/ws`) and subscribes to symbols.
//! For each, it gets a snapshot of the whole book, then a delta for every committed batch that
//! changes that book. Every message carries sequences, so the client always knows whether its copy
//! is complete (`docs/milestones/26-pushed-updates/01-theory.md`, §3 and §4):
//! - a snapshot carries `seq`, the stream point it reflects, and `last`, the sequence of that
//!   symbol's latest delta (0 if none);
//! - a delta carries `seq`, its batch's last envelope sequence, and `prev`, the sequence of the
//!   symbol's previous delta.
//!
//! A client first drops any delta whose `seq` is at or before its snapshot's `seq`. It then
//! applies a delta only if `prev` is the last channel sequence it knows: the snapshot's `last`,
//! then each applied delta's `seq`. Anything else is a gap, and it resubscribes.
//!
//! The follower publishes deltas while it holds the view's write lock, and a subscriber takes its
//! snapshot and joins the channel while it holds the read lock, so no delta can fall between the
//! snapshot and the join. Publishing never waits for a client: a delta is serialized once and put
//! into the symbol's `broadcast` ring. Each connection then has its own bounded queue and one task
//! that writes to the socket. A client that falls behind (its queue full, its ring position
//! overrun, or one socket write past its deadline) is disconnected with code 1013, and a halted
//! follower closes every connection with 1011.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{self, error::TrySendError},
        watch,
    },
    task::JoinHandle,
    time::timeout,
};

use crate::types::types::L2Level;

/// The one channel kind this feed serves.
const BOOK: &str = "book";

/// The longest symbol an order can carry (`exchange_controller::MAX_IDENTIFIER_LEN`).
const MAX_SYMBOL_LEN: usize = 64;

/// "Try again later" (IANA registry): the client fell behind, and may resubscribe.
const SLOW_CONSUMER: u16 = 1013;
/// "Internal error" (RFC 6455): the market-data follower halted.
const UNAVAILABLE: u16 = 1011;
/// "Going away" (RFC 6455): the client stopped answering pings, so it is presumed gone.
const GOING_AWAY: u16 = 1001;
/// "Policy violation" (RFC 6455): the private feed's client did not authenticate, or its token
/// expired.
const POLICY_VIOLATION: u16 = 1008;

/// How a connection is paced. Tests shrink these to see a slow consumer quickly.
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    /// Deltas one symbol's ring holds for its slowest subscriber.
    pub(super) ring: usize,
    /// Messages one connection may have waiting for its socket.
    pub(super) queue: usize,
    /// Longest one write to the socket may take.
    pub(super) write_deadline: Duration,
    /// How often the server pings every connection. A connection from which nothing at all (not
    /// even the pong a ping asks for) has arrived for more than `SILENT_PINGS` intervals is closed
    /// at the next ping: in practice after three unanswered pings, about 4 intervals of silence
    /// (60 s by default).
    pub(super) ping: Duration,
}

/// Ping intervals without a single frame from the client after which its connection is closed,
/// at the next ping.
pub(super) const SILENT_PINGS: u32 = 3;

impl Limits {
    /// The queue is per connection, shared by all its subscriptions, so the stall it absorbs is
    /// 1,024 divided by the sum of the subscribed symbols' message rates. At 5,000 orders/s over
    /// 100 symbols a symbol changes about 50 times a second: about 20 s for a client of one
    /// symbol, about 0.2 s for a client of all 100 (01-theory.md §5, illustrative rates).
    pub(super) const DEFAULT: Limits = Limits {
        ring: 4_096,
        queue: 1_024,
        write_deadline: Duration::from_secs(5),
        ping: Duration::from_secs(15),
    };
}

/// One symbol's changed levels after a batch, each with its new aggregate quantity (0: removed).
/// Bids best first (highest price), asks best first (lowest price).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct BookChange {
    pub(super) symbol: String,
    pub(super) bids: Vec<L2Level>,
    pub(super) asks: Vec<L2Level>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { channel: String, symbol: String },
    Unsubscribe { channel: String, symbol: String },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage<'a> {
    Snapshot {
        channel: &'static str,
        symbol: &'a str,
        seq: u64,
        last: u64,
        bids: &'a [L2Level],
        asks: &'a [L2Level],
    },
    Delta {
        channel: &'static str,
        symbol: &'a str,
        seq: u64,
        prev: u64,
        bids: &'a [L2Level],
        asks: &'a [L2Level],
    },
    Unsubscribed {
        channel: &'static str,
        symbol: &'a str,
    },
    Error {
        message: &'a str,
    },
}

fn json(message: &ServerMessage) -> Utf8Bytes {
    serde_json::to_string(message)
        .expect("a feed message always serializes")
        .into()
}

/// A snapshot message, built by the market-data process from its view.
pub(super) fn snapshot(
    symbol: &str,
    seq: u64,
    last: u64,
    bids: &[L2Level],
    asks: &[L2Level],
) -> Utf8Bytes {
    json(&ServerMessage::Snapshot {
        channel: BOOK,
        symbol,
        seq,
        last,
        bids,
        asks,
    })
}

/// A symbol identifier a client may subscribe to: what the gateway lets an order carry, already
/// trimmed. There is no registry of symbols, and a book disappears when it empties, so any such
/// identifier is accepted; a book that has no orders gives an empty snapshot.
fn valid_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= MAX_SYMBOL_LEN
        && symbol.trim() == symbol
        && !symbol.chars().any(char::is_control)
}

#[derive(Default)]
struct SymbolFeed {
    /// The sequence of this symbol's latest delta. Kept here, not in the book, because the
    /// market-data projection removes a book when it empties (at the latest, at every close).
    last: u64,
    /// Created by the first subscriber.
    sender: Option<broadcast::Sender<Utf8Bytes>>,
}

#[derive(Default)]
struct FeedState {
    symbols: HashMap<String, SymbolFeed>,
    halted: bool,
}

/// The fan-out hub shared by the follower thread and every connection.
pub(super) struct BookFeed {
    state: Mutex<FeedState>,
    halted: watch::Sender<bool>,
    limits: Limits,
}

impl BookFeed {
    pub(super) fn new(limits: Limits) -> Self {
        Self {
            state: Mutex::new(FeedState::default()),
            halted: watch::Sender::new(false),
            limits,
        }
    }

    fn lock(&self) -> MutexGuard<'_, FeedState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Publishes one committed batch's changes. The follower calls this with the view's write
    /// lock held, right after the batch is applied. It never waits for a client.
    pub(super) fn publish(&self, seq: u64, changes: Vec<BookChange>) {
        let mut state = self.lock();
        for change in changes {
            let feed = state.symbols.entry(change.symbol.clone()).or_default();
            let prev = std::mem::replace(&mut feed.last, seq);
            if let Some(sender) = feed.sender.as_ref().filter(|s| s.receiver_count() > 0) {
                // Serialized once, however many subscribers; a send only stores it in the ring.
                let _ = sender.send(json(&ServerMessage::Delta {
                    channel: BOOK,
                    symbol: &change.symbol,
                    seq,
                    prev,
                    bids: &change.bids,
                    asks: &change.asks,
                }));
            }
        }
    }

    /// Joins a symbol's channel: the sequence of its latest delta, and a receiver of every later
    /// one. The caller holds the view's read lock, so no batch is applied in between. `None` once
    /// the follower has halted.
    pub(super) fn join(&self, symbol: &str) -> Option<(u64, broadcast::Receiver<Utf8Bytes>)> {
        let mut state = self.lock();
        if state.halted {
            return None;
        }
        let ring = self.limits.ring;
        let feed = state.symbols.entry(symbol.to_string()).or_default();
        let sender = feed
            .sender
            .get_or_insert_with(|| broadcast::channel(ring).0);
        Some((feed.last, sender.subscribe()))
    }

    /// A subscriber left: once a symbol's ring has no receiver, it is freed (its `last` stays).
    /// A ring of 4,096 slots is allocated whole, so rings must not outlive their subscribers.
    pub(super) fn release(&self, symbol: &str) {
        let mut state = self.lock();
        if let Some(feed) = state.symbols.get_mut(symbol)
            && feed
                .sender
                .as_ref()
                .is_some_and(|sender| sender.receiver_count() == 0)
        {
            feed.sender = None;
        }
    }

    /// The follower stopped: every connection closes, and nobody can join any more.
    pub(super) fn halt(&self) {
        let mut state = self.lock();
        state.halted = true;
        for feed in state.symbols.values_mut() {
            feed.sender = None;
        }
        drop(state);
        self.halted.send_replace(true);
    }
}

/// Why the server ends a connection. Shared with the private feed (`user_feed`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Close {
    SlowConsumer,
    Unavailable,
    Silent,
    /// The private feed's authentication failed, timed out or expired.
    Policy(&'static str),
}

impl Close {
    fn frame(self) -> CloseFrame {
        let (code, reason) = match self {
            Close::SlowConsumer => (SLOW_CONSUMER, "slow consumer: resubscribe"),
            Close::Unavailable => (UNAVAILABLE, "feed unavailable"),
            Close::Silent => (GOING_AWAY, "no answer to pings"),
            Close::Policy(reason) => (POLICY_VIOLATION, reason),
        };
        CloseFrame {
            code,
            reason: reason.into(),
        }
    }
}

/// Ends a connection the server chose to close: the close frame, then the closing handshake.
pub(super) async fn finish(socket: &mut WebSocket, close: Close, limits: Limits) {
    // Best effort: a client too slow to read may never take the close frame either.
    let _ = timeout(
        limits.write_deadline,
        socket.send(Message::Close(Some(close.frame()))),
    )
    .await;
    // The closing handshake: wait, within the deadline, for the client's own close frame,
    // reading what it sent meanwhile. Dropping the socket with unread data from the client makes
    // the kernel reset the connection, and the client can then lose the close frame.
    let _ = timeout(limits.write_deadline, async {
        while let Some(Ok(message)) = socket.recv().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    })
    .await;
}

/// Writes one message within the deadline. `Err(Some(..))`: the client is too slow;
/// `Err(None)`: the connection is gone.
pub(super) async fn send(
    socket: &mut WebSocket,
    message: Message,
    deadline: Duration,
) -> Result<(), Option<Close>> {
    match timeout(deadline, socket.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(None),
        Err(_) => Err(Some(Close::SlowConsumer)),
    }
}

/// Moves one subscription's deltas from its ring into the connection's queue, and reports a
/// client that fell behind or a feed that halted.
pub(super) async fn forward(
    mut receiver: broadcast::Receiver<Utf8Bytes>,
    queue: mpsc::Sender<Utf8Bytes>,
    closing: mpsc::UnboundedSender<Close>,
) {
    loop {
        match receiver.recv().await {
            Ok(delta) => match queue.try_send(delta) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let _ = closing.send(Close::SlowConsumer);
                    return;
                }
                Err(TrySendError::Closed(_)) => return,
            },
            Err(RecvError::Lagged(_)) => {
                let _ = closing.send(Close::SlowConsumer);
                return;
            }
            Err(RecvError::Closed) => {
                let _ = closing.send(Close::Unavailable);
                return;
            }
        }
    }
}

/// Serves one WebSocket connection until either side ends it. `subscribe` returns a symbol's
/// snapshot message and a receiver joined at that snapshot, or `None` when market data is
/// unavailable.
pub(super) async fn serve<F>(mut socket: WebSocket, feed: Arc<BookFeed>, subscribe: F)
where
    F: Fn(&str) -> Option<(Utf8Bytes, broadcast::Receiver<Utf8Bytes>)>,
{
    let limits = feed.limits;
    let (queue_tx, mut queue) = mpsc::channel::<Utf8Bytes>(limits.queue);
    let (closing_tx, mut closing) = mpsc::unbounded_channel::<Close>();
    let mut halted = feed.halted.subscribe();
    let mut forwarders: HashMap<String, JoinHandle<()>> = HashMap::new();
    let mut ping = tokio::time::interval(limits.ping);
    ping.tick().await; // the first tick is immediate
    let already_halted = *halted.borrow_and_update();
    // Any frame from the client, a pong included, shows it is still there.
    let mut last_heard = tokio::time::Instant::now();

    let close = loop {
        if already_halted {
            break Some(Close::Unavailable);
        }
        tokio::select! {
            incoming = socket.recv() => {
                last_heard = tokio::time::Instant::now();
                let text = match incoming {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break None,
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Binary(_))) => {
                        let reply = json(&ServerMessage::Error { message: "send text messages" });
                        match send(&mut socket, Message::Text(reply), limits.write_deadline).await {
                            Ok(()) => continue,
                            Err(close) => break close,
                        }
                    }
                    // Pings are answered by the WebSocket layer; pongs need nothing.
                    Some(Ok(_)) => continue,
                };
                let reply = match serde_json::from_str::<ClientMessage>(text.as_str()) {
                    Err(_) => json(&ServerMessage::Error {
                        message: "expected {\"type\":\"subscribe\"|\"unsubscribe\",\"channel\":\"book\",\"symbol\":...}",
                    }),
                    Ok(ClientMessage::Subscribe { channel, .. })
                    | Ok(ClientMessage::Unsubscribe { channel, .. })
                        if channel != BOOK =>
                    {
                        json(&ServerMessage::Error { message: "unknown channel" })
                    }
                    Ok(ClientMessage::Subscribe { symbol, .. }) => {
                        if !valid_symbol(&symbol) {
                            json(&ServerMessage::Error { message: "invalid symbol" })
                        } else if forwarders.contains_key(&symbol) {
                            json(&ServerMessage::Error { message: "already subscribed" })
                        } else {
                            let Some((snapshot, receiver)) = subscribe(&symbol) else {
                                break Some(Close::Unavailable);
                            };
                            // The snapshot goes out before any of its deltas can be queued.
                            if let Err(close) =
                                send(&mut socket, Message::Text(snapshot), limits.write_deadline).await
                            {
                                // Never handed to a forwarder: release it here, or its ring stays.
                                drop(receiver);
                                feed.release(&symbol);
                                break close;
                            }
                            let task = tokio::spawn(forward(
                                receiver,
                                queue_tx.clone(),
                                closing_tx.clone(),
                            ));
                            forwarders.insert(symbol, task);
                            continue;
                        }
                    }
                    Ok(ClientMessage::Unsubscribe { symbol, .. }) => match forwarders.remove(&symbol) {
                        // Waited for, so a forwarder mid-poll on another thread cannot queue more
                        // after this. Deltas queued before may still arrive; a client ignores
                        // them until its next snapshot of the symbol.
                        Some(task) => {
                            task.abort();
                            let _ = task.await;
                            feed.release(&symbol);
                            json(&ServerMessage::Unsubscribed { channel: BOOK, symbol: &symbol })
                        }
                        None => json(&ServerMessage::Error { message: "not subscribed" }),
                    },
                };
                if let Err(close) = send(&mut socket, Message::Text(reply), limits.write_deadline).await {
                    break close;
                }
            }
            Some(delta) = queue.recv() => {
                if let Err(close) = send(&mut socket, Message::Text(delta), limits.write_deadline).await {
                    break close;
                }
            }
            Some(reason) = closing.recv() => break Some(reason),
            // `changed`, not `wait_for`: the latter's borrowed value is not `Send`.
            Ok(()) = halted.changed() => {
                if *halted.borrow_and_update() {
                    break Some(Close::Unavailable);
                }
            }
            _ = ping.tick() => {
                // A peer gone without a FIN or RST never fails a write: the pings just sit in
                // the kernel's buffer. Silence is the only sign of it.
                if last_heard.elapsed() > limits.ping * SILENT_PINGS {
                    break Some(Close::Silent);
                }
                if let Err(close) = send(&mut socket, Message::Ping(Default::default()), limits.write_deadline).await {
                    break close;
                }
            }
        }
    };

    for (symbol, task) in forwarders {
        task.abort();
        let _ = task.await;
        feed.release(&symbol);
    }
    if let Some(close) = close {
        finish(&mut socket, close, limits).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: u64, quantity: u64) -> L2Level {
        L2Level { price, quantity }
    }

    fn change(symbol: &str, bids: Vec<L2Level>, asks: Vec<L2Level>) -> BookChange {
        BookChange {
            symbol: symbol.into(),
            bids,
            asks,
        }
    }

    fn parsed(text: &Utf8Bytes) -> serde_json::Value {
        serde_json::from_str(text.as_str()).unwrap()
    }

    #[test]
    fn deltas_chain_from_the_snapshots_last_and_last_outlives_a_quiet_symbol() {
        let feed = BookFeed::new(Limits::DEFAULT);
        // A change before anyone listens still moves the symbol's `last`.
        feed.publish(5, vec![change("AAPL", vec![level(100, 3)], vec![])]);
        let (last, mut receiver) = feed.join("AAPL").unwrap();
        assert_eq!(last, 5);

        feed.publish(9, vec![change("MSFT", vec![], vec![level(50, 1)])]);
        feed.publish(12, vec![change("AAPL", vec![level(100, 0)], vec![])]);
        let delta = parsed(&receiver.try_recv().unwrap());
        assert_eq!(delta["type"], "delta");
        assert_eq!(
            (delta["seq"].as_u64(), delta["prev"].as_u64()),
            (Some(12), Some(5))
        );
        assert_eq!(delta["bids"][0]["quantity"], 0);
        // MSFT's change went to nobody here.
        assert!(receiver.try_recv().is_err());

        // A later subscriber of an emptied book still gets the latest sequence.
        let (last, _late) = feed.join("AAPL").unwrap();
        assert_eq!(last, 12);
    }

    #[test]
    fn a_ring_is_freed_when_its_last_subscriber_leaves_and_last_stays() {
        let feed = BookFeed::new(Limits::DEFAULT);
        feed.publish(7, vec![change("AAPL", vec![level(100, 1)], vec![])]);
        let (_, first) = feed.join("AAPL").unwrap();
        let (_, second) = feed.join("AAPL").unwrap();
        drop(first);
        feed.release("AAPL");
        assert!(
            feed.lock().symbols["AAPL"].sender.is_some(),
            "freed while in use"
        );
        drop(second);
        feed.release("AAPL");
        let state = feed.lock();
        assert!(state.symbols["AAPL"].sender.is_none());
        assert_eq!(state.symbols["AAPL"].last, 7);
    }

    #[test]
    fn a_halted_feed_closes_its_rings_and_refuses_new_subscribers() {
        let feed = BookFeed::new(Limits::DEFAULT);
        let (_, mut receiver) = feed.join("AAPL").unwrap();
        feed.halt();
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        assert!(feed.join("AAPL").is_none());
        assert!(*feed.halted.subscribe().borrow());
    }

    #[test]
    fn symbols_follow_the_gateways_identifier_rule() {
        assert!(valid_symbol("AAPL"));
        assert!(valid_symbol(&"X".repeat(64)));
        assert!(!valid_symbol(""));
        assert!(!valid_symbol(&"X".repeat(65)));
        assert!(!valid_symbol(" AAPL"));
        assert!(!valid_symbol("AA\u{0}PL"));
    }
}
