//! `--feed-bench` (milestone 26 part 3): many WebSocket clients of the book feed (the MDP) and the
//! private feed, measuring push latency while `--http-bench` drives orders.
//!
//! **Push latency** is the time from an order's *intended* send time to a client receiving a
//! message caused by it (`01-theory.md` §8). Both benches are given the same `--start-at` (a Unix
//! time in milliseconds) and `--rate`, so order `o{i}` is due at `start_at + i / rate` in both
//! processes: `--http-bench` anchors its schedule at that wall-clock time, not at the moment its
//! sleep ends. The two processes compare wall-clock times (`SystemTime`), which is valid on one
//! host only, while the clock is not stepped.
//!
//! **Which order caused a message.** Every pushed message (a book delta, a private `order`, `fill`
//! or `cancel_rejected`) carries its batch's last sequence. After the run the journal is read once,
//! and each new-order batch's last sequence gives its order id. So every message is timed from the
//! order whose batch produced it: a resting order's fill from the incoming order that filled it.
//! Every batch in the measured window is one order: `--http-bench` sends nothing else.
//!
//! **Clients.** Book client `k` subscribes to symbol `S{k mod symbols}`; user client `k` signs in as
//! `u{k mod users}` with a token signed by `JWT_SECRET`, as `--http-bench` signs its traders'. A
//! stalled client connects at the start, subscribes to every symbol and never reads, so the server
//! must disconnect it (decision 3); after the run it reads what was buffered, to learn whether it
//! was closed. Connecting at the start keeps the run shorter than the book feed's silence limit
//! (more than three 15 s ping intervals), so a close it finds is not a silence close.
//!
//! The run ends when no message has arrived for `--idle` seconds after `--start-at`, or at
//! `--deadline` seconds after it.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use super::{
    committed_batch::{self, CommittedCommand},
    event_stream::StreamReader,
    http_bench::{cpu_micros, percentiles, unix_micros},
};
use crate::models::user::Claims;

const USAGE: &str = "usage: stock --feed-bench --start-at UNIX_MS --rate ORDERS_PER_SEC|0 --orders N --journal PATH --stream PATH [--book URL] [--user URL] [--book-clients N] [--user-clients N] [--stalled N] [--symbols N] [--users N] [--idle SECONDS] [--deadline SECONDS]";

/// How long a client may take to connect and subscribe or sign in.
const HANDSHAKE: Duration = Duration::from_secs(30);

struct Config {
    book: String,
    user: String,
    book_clients: u64,
    user_clients: u64,
    stalled: u64,
    symbols: u64,
    users: u64,
    start_at: u64,
    rate: u64,
    orders: u64,
    journal: PathBuf,
    stream: PathBuf,
    idle: u64,
    deadline: u64,
}

fn parse(args: &[String]) -> Result<Config, String> {
    let mut config = Config {
        book: "ws://127.0.0.1:4001/marketdata/ws".into(),
        user: "ws://127.0.0.1:4006/userfeed/ws".into(),
        book_clients: 0,
        user_clients: 0,
        stalled: 0,
        symbols: 100,
        users: 100,
        start_at: 0,
        rate: 1_000,
        orders: 0,
        journal: PathBuf::new(),
        stream: PathBuf::new(),
        idle: 3,
        deadline: 600,
    };
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let number = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag} needs a whole number\n{USAGE}"))
        };
        match flag.as_str() {
            "--book" => config.book = value.clone(),
            "--user" => config.user = value.clone(),
            "--book-clients" => config.book_clients = number()?,
            "--user-clients" => config.user_clients = number()?,
            "--stalled" => config.stalled = number()?,
            "--symbols" => config.symbols = number()?.max(1),
            "--users" => config.users = number()?.max(1),
            "--start-at" => config.start_at = number()?,
            "--rate" => config.rate = number()?,
            "--orders" => config.orders = number()?,
            "--journal" => config.journal = PathBuf::from(value),
            "--stream" => config.stream = PathBuf::from(value),
            "--idle" => config.idle = number()?.max(1),
            "--deadline" => config.deadline = number()?.max(1),
            _ => return Err(USAGE.to_string()),
        }
    }
    if config.start_at == 0
        || config.orders == 0
        || config.journal.as_os_str().is_empty()
        || config.stream.as_os_str().is_empty()
    {
        return Err(format!(
            "--start-at, --orders, --journal and --stream are required\n{USAGE}"
        ));
    }
    Ok(config)
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// What one message tells the bench; every other field is skipped.
#[derive(Deserialize)]
struct Pushed<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    seq: Option<u64>,
}

/// The order index of a `--http-bench` order id, `o{i}`.
fn order_index(order_id: &str) -> Option<u64> {
    order_id.strip_prefix('o')?.parse().ok()
}

fn now_micros() -> u64 {
    unix_micros(SystemTime::now()) as u64
}

/// What one client saw.
#[derive(Default)]
struct Seen {
    /// (batch sequence, arrival in Unix microseconds) of every delta or private event.
    pushed: Vec<(u64, u64)>,
    messages: u64,
    /// When the last message arrived (Unix microseconds).
    last: u64,
    /// The server ended the connection before the run did: its close code, 0 for no close frame.
    closed_early: Option<u16>,
}

/// Shared by every client: how many are subscribed or signed in, when the last message arrived,
/// and the signal to stop.
struct Run {
    ready: AtomicU64,
    last_message: AtomicU64,
    stop: watch::Receiver<bool>,
}

fn close_code(frame: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>) -> u16 {
    frame.map_or(0, |frame| u16::from(frame.code))
}

/// Reads messages until the run stops or the server ends the connection, recording every message
/// that carries a batch sequence.
async fn follow(mut socket: Socket, run: Arc<Run>, kinds: &[&str]) -> Seen {
    let mut seen = Seen::default();
    let mut stop = run.stop.clone();
    loop {
        tokio::select! {
            message = socket.next() => {
                let arrival = now_micros();
                match message {
                    Some(Ok(Message::Text(text))) => {
                        run.last_message.fetch_max(arrival, Ordering::Relaxed);
                        seen.messages += 1;
                        seen.last = arrival;
                        if let Ok(Pushed { kind, seq: Some(seq) }) = serde_json::from_str(text.as_str())
                            && kinds.contains(&kind)
                        {
                            seen.pushed.push((seq, arrival));
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        seen.closed_early = Some(close_code(frame));
                        break;
                    }
                    Some(Ok(_)) => {}
                    None | Some(Err(_)) => {
                        seen.closed_early = Some(0);
                        break;
                    }
                }
            }
            _ = stop.changed() => break,
        }
    }
    let _ = socket.close(None).await;
    seen
}

async fn connect(url: &str) -> Result<Socket, String> {
    connect_async(url)
        .await
        .map(|(socket, _)| socket)
        .map_err(|error| format!("{url}: {error}"))
}

fn subscription(symbol: &str) -> Message {
    Message::Text(format!(r#"{{"type":"subscribe","channel":"book","symbol":"{symbol}"}}"#).into())
}

/// The next text message, skipping pings and pongs.
async fn next_text(socket: &mut Socket) -> Result<String, String> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            other => return Err(format!("expected a message, got {other:?}")),
        }
    }
}

/// A client's setup, within `HANDSHAKE`: a server that accepts the connection but never answers
/// must not keep the bench waiting forever.
async fn handshake<T>(setup: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::time::timeout(HANDSHAKE, setup)
        .await
        .map_err(|_| format!("no answer within {} s", HANDSHAKE.as_secs()))?
}

async fn book_client(url: String, symbol: String, run: Arc<Run>) -> Result<Seen, String> {
    let socket = handshake(async {
        let mut socket = connect(&url).await?;
        socket
            .send(subscription(&symbol))
            .await
            .map_err(|error| error.to_string())?;
        let snapshot = next_text(&mut socket).await?;
        if !snapshot.contains(r#""type":"snapshot""#) {
            return Err(format!("{symbol}: expected a snapshot, got {snapshot}"));
        }
        Ok(socket)
    })
    .await?;
    run.ready.fetch_add(1, Ordering::Relaxed);
    Ok(follow(socket, run, &["delta"]).await)
}

async fn user_client(url: String, token: String, run: Arc<Run>) -> Result<Seen, String> {
    let socket = handshake(async {
        let mut socket = connect(&url).await?;
        socket
            .send(Message::Text(
                format!(r#"{{"type":"auth","token":"{token}"}}"#).into(),
            ))
            .await
            .map_err(|error| error.to_string())?;
        let ready = next_text(&mut socket).await?;
        if !ready.contains(r#""type":"ready""#) {
            return Err(format!("expected ready, got {ready}"));
        }
        Ok(socket)
    })
    .await?;
    run.ready.fetch_add(1, Ordering::Relaxed);
    Ok(follow(socket, run, &["order", "fill", "cancel_rejected"]).await)
}

/// How a stalled client ended, and how long it had been connected when the run stopped.
struct Stalled {
    /// `Some(code)`: the server ended it (0: with no close frame); `None`: still open.
    ended: Option<u16>,
    connected_for: Duration,
}

/// Connects at the start, subscribes to every symbol and never reads, until the run ends; then
/// reads what was buffered, to learn whether the server ended the connection.
async fn stalled_client(
    url: String,
    symbols: Vec<String>,
    start: u64,
    run: Arc<Run>,
) -> Result<Stalled, String> {
    if let Some(wait) = start.checked_sub(now_micros()) {
        tokio::time::sleep(Duration::from_micros(wait)).await;
    }
    let mut socket = handshake(async {
        let mut socket = connect(&url).await?;
        for symbol in &symbols {
            socket
                .send(subscription(symbol))
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(socket)
    })
    .await?;
    let connected = std::time::Instant::now();
    let mut stop = run.stop.clone();
    let _ = stop.changed().await;
    let connected_for = connected.elapsed();
    let ended = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(frame))) => return Some(close_code(frame)),
                Some(Ok(_)) => {}
                None | Some(Err(_)) => return Some(0),
            }
        }
    })
    .await
    .unwrap_or(None);
    Ok(Stalled {
        ended,
        connected_for,
    })
}

/// Each new-order batch's last sequence, mapped to its order's index.
fn order_batches(config: &Config) -> Result<HashMap<u64, u64>, String> {
    let mut reader = StreamReader::open(&config.journal, &config.stream, None)
        .map_err(|error| format!("journal: {error}"))?;
    let mut batches = HashMap::new();
    while let Some(batch) = reader
        .next_batch()
        .map_err(|error| format!("journal: {error}"))?
    {
        if let Ok(CommittedCommand::NewOrder { order, .. }) = committed_batch::decode(&batch)
            && let (Some(index), Some(last)) = (order_index(&order.order_id), batch.last())
        {
            batches.insert(last.seq_num, index);
        }
    }
    Ok(batches)
}

/// Latencies in microseconds: over all messages, and over the first and last fifth of the orders,
/// so a feed falling behind during the run shows as a later fifth slower than the first.
struct Latencies {
    all: Histogram<u64>,
    first: Histogram<u64>,
    last: Histogram<u64>,
    /// Arrived before their order was due: impossible on one clock, so a sign of a clock step.
    early: u64,
}

impl Latencies {
    fn new() -> Result<Self, String> {
        let histogram = || Histogram::<u64>::new(3).map_err(|error| error.to_string());
        Ok(Self {
            all: histogram()?,
            first: histogram()?,
            last: histogram()?,
            early: 0,
        })
    }

    fn record(&mut self, config: &Config, index: u64, arrival: u64) -> Result<(), String> {
        let due = config.start_at * 1_000 + index * 1_000_000 / config.rate.max(1);
        let latency = arrival.checked_sub(due).unwrap_or_else(|| {
            self.early += 1;
            0
        });
        let fifth = config.orders / 5;
        let record = |histogram: &mut Histogram<u64>| {
            histogram.record(latency).map_err(|error| error.to_string())
        };
        record(&mut self.all)?;
        if index < fifth {
            record(&mut self.first)?;
        } else if index >= config.orders - fifth {
            record(&mut self.last)?;
        }
        Ok(())
    }

    fn report(&self) -> String {
        if self.all.is_empty() {
            return "none timed".into();
        }
        let short = |histogram: &Histogram<u64>| {
            if histogram.is_empty() {
                "-".to_string()
            } else {
                format!(
                    "p50 {} p99 {}",
                    histogram.value_at_quantile(0.5),
                    histogram.value_at_quantile(0.99)
                )
            }
        };
        format!(
            "{}; first fifth {}, last fifth {}{}",
            percentiles(&self.all),
            short(&self.first),
            short(&self.last),
            if self.early > 0 {
                format!("; {} early", self.early)
            } else {
                String::new()
            }
        )
    }
}

/// One feed's report line: messages, how many were matched to an order, how many orders they
/// cover, and their latency.
fn feed_report(
    config: &Config,
    seen: &[Seen],
    batches: &HashMap<u64, u64>,
) -> Result<String, String> {
    let mut latencies = Latencies::new()?;
    let (mut messages, mut pushed, mut unmatched) = (0u64, 0u64, 0u64);
    let mut orders = HashSet::new();
    for seen in seen {
        messages += seen.messages;
        pushed += seen.pushed.len() as u64;
        for (seq, arrival) in &seen.pushed {
            match batches.get(seq) {
                Some(index) => {
                    orders.insert(*index);
                    if config.rate > 0 {
                        latencies.record(config, *index, *arrival)?;
                    }
                }
                None => unmatched += 1,
            }
        }
    }
    Ok(format!(
        "{messages} messages, {pushed} with a sequence ({unmatched} not from an order), covering {} of {} orders; latency us {}",
        orders.len(),
        config.orders,
        if config.rate > 0 {
            latencies.report()
        } else {
            "not timed at max rate".into()
        }
    ))
}

fn codes(closed: &[u16]) -> String {
    let mut counts = BTreeMap::new();
    for code in closed {
        *counts.entry(*code).or_insert(0u64) += 1;
    }
    counts
        .iter()
        .map(|(code, count)| format!("{code} x {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Waits for every client task; a client that failed is counted, not fatal, so one bad connection
/// does not discard a run.
async fn gather<T>(
    tasks: Vec<tokio::task::JoinHandle<Result<T, String>>>,
    failures: &mut Vec<String>,
) -> Vec<T> {
    let mut results = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(error.to_string()),
        }
    }
    results
}

pub async fn run(args: &[String]) -> Result<(), String> {
    let config = parse(args)?;
    let secret = crate::middleware::auth_middleware::jwt_secret()
        .ok_or("JWT_SECRET must be set to the exchange's secret: user tokens are signed here")?;
    let key = jsonwebtoken::EncodingKey::from_secret(secret.as_bytes());
    let expires = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs()
        + 86_400) as usize;
    let symbols: Vec<String> = (0..config.symbols).map(|s| format!("S{s:03}")).collect();
    let start = config.start_at * 1_000;

    let (stop_tx, stop_rx) = watch::channel(false);
    let run = Arc::new(Run {
        ready: AtomicU64::new(0),
        last_message: AtomicU64::new(0),
        stop: stop_rx,
    });
    let mut book = Vec::new();
    for k in 0..config.book_clients {
        let symbol = symbols[(k % config.symbols) as usize].clone();
        book.push(tokio::spawn(book_client(
            config.book.clone(),
            symbol,
            Arc::clone(&run),
        )));
    }
    let mut user = Vec::new();
    for k in 0..config.user_clients {
        let claims = Claims {
            sub: format!("u{}", k % config.users),
            exp: expires,
        };
        let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &key)
            .map_err(|error| error.to_string())?;
        user.push(tokio::spawn(user_client(
            config.user.clone(),
            token,
            Arc::clone(&run),
        )));
    }
    let mut stalled = Vec::new();
    for _ in 0..config.stalled {
        stalled.push(tokio::spawn(stalled_client(
            config.book.clone(),
            symbols.clone(),
            start,
            Arc::clone(&run),
        )));
    }
    println!(
        "feed-bench: {} book clients (one symbol each, of {}), {} user clients (one user each, of {}), {} stalled (every symbol, from the start), rate {}, {} orders from unix ms {}",
        config.book_clients,
        config.symbols,
        config.user_clients,
        config.users,
        config.stalled,
        match config.rate {
            0 => "max".to_string(),
            rate => format!("{rate}/s"),
        },
        config.orders,
        config.start_at
    );

    // The book and user clients connect before the start; the stalled ones at it.
    let clients = config.book_clients + config.user_clients;
    while run.ready.load(Ordering::Relaxed) < clients && now_micros() < start {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ready = run.ready.load(Ordering::Relaxed);
    if ready < clients {
        println!("  warning    : only {ready} of {clients} clients were ready at the start");
    }
    if let Some(wait) = start.checked_sub(now_micros()) {
        tokio::time::sleep(Duration::from_micros(wait)).await;
    }

    // Until the feeds go quiet or the deadline passes. The client CPU is taken at the last check
    // during which a message arrived, so the quiet tail does not dilute it.
    let cpu_before = cpu_micros();
    let since = std::time::Instant::now();
    let (mut cpu_used, mut cpu_seconds, mut seen_last) = (0, 0.0, 0);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = now_micros();
        let last_message = run.last_message.load(Ordering::Relaxed);
        if last_message != seen_last {
            seen_last = last_message;
            cpu_used = cpu_micros().saturating_sub(cpu_before);
            cpu_seconds = since.elapsed().as_secs_f64();
        }
        // A client can stamp an arrival just after `now` was read: saturate, never wrap.
        let quiet_since = last_message.max(start);
        if now > start + config.deadline * 1_000_000
            || now.saturating_sub(quiet_since) > config.idle * 1_000_000
        {
            break;
        }
    }
    let _ = stop_tx.send(true);

    let mut failures = Vec::new();
    let book_seen = gather(book, &mut failures).await;
    let user_seen = gather(user, &mut failures).await;
    let stalled_ends = gather(stalled, &mut failures).await;
    let last_message = run.last_message.load(Ordering::Relaxed);

    let batches = order_batches(&config)?;
    if config.book_clients > 0 {
        println!(
            "  book       : {}",
            feed_report(&config, &book_seen, &batches)?
        );
    }
    if config.user_clients > 0 {
        println!(
            "  user       : {}",
            feed_report(&config, &user_seen, &batches)?
        );
    }
    let early = |seen: &[Seen]| {
        seen.iter()
            .filter_map(|seen| seen.closed_early)
            .collect::<Vec<_>>()
    };
    let (book_closed, user_closed) = (early(&book_seen), early(&user_seen));
    let with_codes = |closed: &[u16]| {
        if closed.is_empty() {
            String::new()
        } else {
            format!(" ({})", codes(closed))
        }
    };
    println!(
        "  closed     : book {} of {}{}, user {} of {}{}",
        book_closed.len(),
        config.book_clients,
        with_codes(&book_closed),
        user_closed.len(),
        config.user_clients,
        with_codes(&user_closed),
    );
    if config.stalled > 0 {
        let closed: Vec<u16> = stalled_ends.iter().filter_map(|end| end.ended).collect();
        let longest = stalled_ends
            .iter()
            .map(|end| end.connected_for)
            .max()
            .unwrap_or_default();
        println!(
            "  stalled    : {} of {} ended by the server{}; connected at most {:.1} s",
            closed.len(),
            config.stalled,
            with_codes(&closed),
            longest.as_secs_f64()
        );
    }
    if !failures.is_empty() {
        println!(
            "  failed     : {} clients (first: {})",
            failures.len(),
            failures[0]
        );
    }
    // The feeds' last messages, to compare with the generator's last reply: how far behind the
    // exchange each feed finished.
    let last = |seen: &[Seen]| seen.iter().map(|seen| seen.last).max().unwrap_or(0);
    println!(
        "  last msg   : book unix us {}, user unix us {}, any {last_message}",
        last(&book_seen),
        last(&user_seen)
    );
    println!(
        "  client cpu : {:.0}% of one hardware thread over {cpu_seconds:.1} s (start to last message)",
        if cpu_seconds > 0.0 {
            cpu_used as f64 / 1e6 / cpu_seconds * 100.0
        } else {
            0.0
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_ids_and_pushed_messages_parse() {
        assert_eq!(order_index("o42"), Some(42));
        assert_eq!(order_index("x42"), None);
        assert_eq!(order_index("o"), None);
        let delta: Pushed = serde_json::from_str(
            r#"{"type":"delta","channel":"book","symbol":"S000","seq":9,"prev":3,"bids":[],"asks":[]}"#,
        )
        .unwrap();
        assert_eq!((delta.kind, delta.seq), ("delta", Some(9)));
        let fill: Pushed = serde_json::from_str(
            r#"{"type":"fill","seq":6,"execution_id":"exec_0","order_id":"o7","symbol":"S000","side":"buy","price":1,"quantity":1,"remaining":0}"#,
        )
        .unwrap();
        assert_eq!((fill.kind, fill.seq), ("fill", Some(6)));
        let ready: Pushed = serde_json::from_str(r#"{"type":"ready","seq":5}"#).unwrap();
        assert_eq!(ready.kind, "ready");
    }

    fn config(rate: u64, orders: u64) -> Config {
        parse(&[
            "--start-at".into(),
            "1000".into(),
            "--orders".into(),
            orders.to_string(),
            "--journal".into(),
            "j".into(),
            "--stream".into(),
            "s".into(),
            "--rate".into(),
            rate.to_string(),
        ])
        .unwrap()
    }

    #[test]
    fn the_stream_is_required() {
        let missing = parse(&[
            "--start-at".into(),
            "1000".into(),
            "--orders".into(),
            "1".into(),
            "--journal".into(),
            "j".into(),
        ]);
        assert!(missing.err().unwrap().contains("required"));
    }

    /// Order i is due at start + i / rate: at 1,000/s, order 10 is due 10 ms after a start of
    /// 1,000 ms, so an arrival at 1,012,500 us is 2,500 us late. The fifths split by order index.
    #[test]
    fn latency_counts_from_each_orders_intended_time() {
        let config = config(1_000, 100);
        let mut latencies = Latencies::new().unwrap();
        latencies.record(&config, 10, 1_012_500).unwrap();
        latencies.record(&config, 90, 1_090_100).unwrap();
        latencies.record(&config, 50, 1_049_000).unwrap();
        assert_eq!(latencies.all.len(), 3);
        assert_eq!(
            latencies.first.max(),
            latencies.first.highest_equivalent(2_500)
        );
        assert_eq!(latencies.last.max(), latencies.last.highest_equivalent(100));
        assert_eq!(latencies.early, 1);
    }

    /// Every message is timed from the order whose batch produced it, through the batch map; a
    /// sequence that is no order's batch is counted, not timed. Orders are counted once each.
    #[test]
    fn messages_are_timed_from_the_order_of_their_batch() {
        let config = config(1_000, 100);
        let batches = HashMap::from([(6, 10), (9, 20)]);
        let seen = Seen {
            // The incoming order's fill and the resting side's fill share batch 6: both are timed
            // from order 10, due at 1,010,000 us.
            pushed: vec![
                (6, 1_013_000),
                (6, 1_014_000),
                (9, 1_021_000),
                (7, 1_030_000),
            ],
            messages: 5,
            last: 1_030_000,
            closed_early: None,
        };
        let report = feed_report(&config, &[seen], &batches).unwrap();
        assert!(
            report.starts_with(
                "5 messages, 4 with a sequence (1 not from an order), covering 2 of 100 orders"
            ),
            "{report}"
        );
        // The slowest: 1,014,000 us for order 10, 4,000 us late (to the histogram's precision).
        assert!(report.contains("max 400"), "{report}");
    }
}
