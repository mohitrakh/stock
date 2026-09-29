//! `--bench`: an in-process load generator for the real exchange worker.
//!
//! It drives the production path — the same bounded command queue, worker thread, durable
//! journal, mmap stream, and snapshot schedule that `main` builds — but skips HTTP and login so
//! the numbers describe the exchange rather than the web framework.
//!
//! Latency is measured from each order's *intended* send time, not from when it was actually
//! sent. When the exchange stalls, the generator does not politely wait before "starting the
//! clock" on the next order; the stall shows up in every order scheduled during it. This is the
//! standard answer to coordinated omission. Latencies are recorded in an HdrHistogram.
//!
//! See `docs/performance/00-benchmark-harness.md`.

use std::{
    path::PathBuf,
    sync::atomic::Ordering,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hdrhistogram::Histogram;
use tokio::sync::{mpsc, oneshot};

use crate::{
    exchange::{
        event_store::JOURNAL_SYNCS,
        runtime::{DEFAULT_SNAPSHOT_INTERVAL, recover_runtime_with_stream_and_snapshot},
    },
    types::types::{ExchangeCommand, Order},
};

const USAGE: &str = "usage: stock --bench EMPTY_DIR [--orders N] [--rate ORDERS_PER_SEC|0] \
                     [--symbols N] [--users N] [--depth RESTING_ORDERS_PER_SYMBOL]                      [--snapshot-every COMMANDS|0]";

/// Every measured order is priced inside this band, so roughly half of them cross.
const BAND_LOW: u64 = 1_000;
const BAND_TICKS: u64 = 20;
/// `--depth` orders rest far above the band: they never trade, they only make the book deep.
const DEPTH_PRICE: u64 = 1_000_000;

struct Config {
    dir: PathBuf,
    orders: u64,
    /// Orders per second, or 0 for "as fast as the exchange accepts them".
    rate: u64,
    symbols: u64,
    users: u64,
    depth: u64,
    /// Core snapshot interval, as in production; 0 turns snapshots off to isolate other costs.
    snapshot_every: u64,
}

fn parse(args: &[String]) -> Result<Config, String> {
    let mut args = args.iter();
    let dir = args.next().ok_or(USAGE)?.into();
    let mut config = Config {
        dir,
        orders: 100_000,
        rate: 0,
        symbols: 100,
        users: 100,
        depth: 0,
        snapshot_every: DEFAULT_SNAPSHOT_INTERVAL,
    };
    while let Some(flag) = args.next() {
        let value: u64 = args
            .next()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| format!("{flag} needs a whole number\n{USAGE}"))?;
        match flag.as_str() {
            "--orders" => config.orders = value,
            "--rate" => config.rate = value,
            "--symbols" => config.symbols = value.max(1),
            "--users" => config.users = value.max(2),
            "--depth" => config.depth = value,
            "--snapshot-every" => config.snapshot_every = value,
            _ => return Err(USAGE.to_string()),
        }
    }
    Ok(config)
}

/// xorshift64: deterministic, so every run sends exactly the same orders.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

type Reply<T> = oneshot::Receiver<Result<T, String>>;

/// Sends one command and returns its reply channel without waiting for the answer.
async fn send<T>(
    tx: &mpsc::Sender<ExchangeCommand>,
    make: impl FnOnce(oneshot::Sender<Result<T, String>>) -> ExchangeCommand,
) -> Result<Reply<T>, String> {
    let (respond_to, reply) = oneshot::channel();
    tx.send(make(respond_to))
        .await
        .map_err(|_| "exchange worker stopped".to_string())?;
    Ok(reply)
}

/// Waits for every reply and fails on the first rejection: setup must succeed completely.
async fn expect_ok<T>(replies: Vec<Reply<T>>) -> Result<(), String> {
    for reply in replies {
        reply
            .await
            .map_err(|_| "exchange worker stopped".to_string())?
            .map_err(|reason| format!("setup command rejected: {reason}"))?;
    }
    Ok(())
}

fn order(
    id: String,
    user: &str,
    symbol: &str,
    side: &str,
    price: u64,
    qty: u32,
    now: f64,
) -> Order {
    Order::new(
        id,
        user.into(),
        symbol.into(),
        side,
        price,
        qty,
        None,
        now,
        0,
    )
    .expect("benchmark orders are valid")
}

fn memory() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("VmRSS") || line.starts_with("VmHWM"))
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join(", ")
}

pub async fn run(args: &[String]) -> Result<(), String> {
    let config = parse(args)?;
    // A run must start from an empty exchange: no journal, snapshot or stream left over from an
    // earlier run. Refuse rather than delete; the directory might hold something that matters.
    if std::fs::read_dir(&config.dir).is_ok_and(|mut entries| entries.next().is_some()) {
        return Err(format!(
            "{} is not empty; point --bench at a new or empty directory",
            config.dir.display()
        ));
    }
    std::fs::create_dir_all(&config.dir).map_err(|error| error.to_string())?;
    let journal = config.dir.join("bench-events.log");
    let (tx, rx) = mpsc::channel(crate::EXCHANGE_COMMAND_QUEUE_SIZE);
    let runtime = recover_runtime_with_stream_and_snapshot(
        rx,
        &journal,
        config.dir.join("bench-events.log.mmap"),
        config.dir.join("bench-events.log.snapshot"),
        match config.snapshot_every {
            0 => u64::MAX,
            every => every,
        },
    )
    .map_err(|error| error.to_string())?;
    let worker = thread::spawn(move || runtime.run());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs() as f64;
    let users: Vec<String> = (0..config.users).map(|u| format!("u{u}")).collect();
    let symbols: Vec<String> = (0..config.symbols).map(|s| format!("S{s:03}")).collect();

    // Setup, untimed: cash for every user and shares in every symbol.
    let mut funded = Vec::new();
    let mut shares = Vec::new();
    for user in &users {
        funded.push(
            send(&tx, |respond_to| ExchangeCommand::Deposit {
                user_id: user.clone(),
                amount: 1_000_000_000_000_000,
                respond_to,
            })
            .await?,
        );
        for symbol in &symbols {
            shares.push(
                send(&tx, |respond_to| ExchangeCommand::DepositShares {
                    user_id: user.clone(),
                    symbol: symbol.clone(),
                    quantity: 1_000_000_000,
                    respond_to,
                })
                .await?,
            );
        }
    }
    expect_ok(funded).await?;
    expect_ok(shares).await?;

    // Optional resting depth, also untimed.
    let mut resting = Vec::new();
    for d in 0..config.depth {
        for symbol in &symbols {
            let user = &users[(d % config.users) as usize];
            let price = DEPTH_PRICE + d % 1_000;
            let order = order(
                format!("depth-{symbol}-{d}"),
                user,
                symbol,
                "SELL",
                price,
                1,
                now,
            );
            resting.push(
                send(&tx, |respond_to| ExchangeCommand::PlaceOrder {
                    order,
                    respond_to,
                })
                .await?,
            );
        }
    }
    expect_ok(resting).await?;

    // Measured phase.
    let syncs_before = JOURNAL_SYNCS.load(Ordering::Relaxed);
    let journal_before = std::fs::metadata(&journal)
        .map_err(|e| e.to_string())?
        .len();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut pending = Vec::with_capacity(config.orders as usize);
    let start = Instant::now();
    for i in 0..config.orders {
        let intended = if config.rate == 0 {
            Instant::now()
        } else {
            start + Duration::from_nanos(i * 1_000_000_000 / config.rate)
        };
        if config.rate > 0 {
            tokio::time::sleep_until(intended.into()).await;
        }
        let user = &users[rng.below(config.users) as usize];
        let symbol = &symbols[rng.below(config.symbols) as usize];
        let side = if rng.below(2) == 0 { "BUY" } else { "SELL" };
        let price = BAND_LOW + rng.below(BAND_TICKS);
        let qty = 1 + rng.below(10) as u32;
        let order = order(format!("o{i}"), user, symbol, side, price, qty, now);
        let reply = send(&tx, |respond_to| ExchangeCommand::PlaceOrder {
            order,
            respond_to,
        })
        .await?;
        pending.push(tokio::spawn(async move {
            let accepted = matches!(reply.await, Ok(Ok(_)));
            (accepted, intended, Instant::now())
        }));
    }

    let mut latency = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut last_reply = start;
    let mut rejected = 0u64;
    for task in pending {
        let (accepted, intended, replied) = task.await.map_err(|e| e.to_string())?;
        rejected += u64::from(!accepted);
        latency
            .record(replied.duration_since(intended).as_micros() as u64)
            .map_err(|e| e.to_string())?;
        last_reply = last_reply.max(replied);
    }
    let elapsed = last_reply.duration_since(start).as_secs_f64();
    let syncs = JOURNAL_SYNCS.load(Ordering::Relaxed) - syncs_before;
    let journal_bytes = std::fs::metadata(&journal)
        .map_err(|e| e.to_string())?
        .len()
        - journal_before;

    let rate = match config.rate {
        0 => "max".to_string(),
        rate => format!("{rate}/s"),
    };
    let snapshots = match config.snapshot_every {
        0 => "off".to_string(),
        every => format!("every {every}"),
    };
    println!(
        "bench: {} orders, rate {rate}, {} symbols, {} users, depth {} per symbol, snapshots {snapshots}",
        config.orders, config.symbols, config.users, config.depth
    );
    println!(
        "  throughput : {:.0} orders/s over {elapsed:.3} s",
        config.orders as f64 / elapsed
    );
    // At "max" the generator keeps the queue full, so latency there is mostly time spent
    // waiting in a 10,000-deep queue. Read latency from fixed-rate runs.
    println!(
        "  latency us : p50 {}  p90 {}  p99 {}  p99.9 {}  max {}{}",
        latency.value_at_quantile(0.50),
        latency.value_at_quantile(0.90),
        latency.value_at_quantile(0.99),
        latency.value_at_quantile(0.999),
        latency.max(),
        if config.rate == 0 {
            "  (queue-bound at max rate)"
        } else {
            ""
        }
    );
    println!(
        "  syncs      : {syncs} ({:.1} orders per sync)",
        config.orders as f64 / syncs.max(1) as f64
    );
    println!(
        "  journal    : {:.0} bytes per order",
        journal_bytes as f64 / config.orders as f64
    );
    println!("  rejected   : {rejected}");
    println!("  memory     : {}", memory());

    drop(tx);
    worker
        .join()
        .map_err(|_| "exchange worker panicked".to_string())?;
    Ok(())
}
