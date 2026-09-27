//! Independent PostgreSQL reporting projection. It consumes only committed batches and never
//! participates in order entry, matching, journal durability, or mmap publication.

use std::{
    error::Error,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};

use super::{
    committed_batch::{self, CancelOutcome, CommittedCommand, ExecutionPair, NewOrderOutcome},
    event_stream::{ReaderCheckpoint, StreamReader},
};
use crate::types::types::{Order, Side};

const DEFAULT_ADDR: &str = "127.0.0.1:4002";
const IDLE_POLL: Duration = Duration::from_millis(10);
const CHECKPOINT_ID: bool = true;

type ReporterResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn number(value: u64) -> String {
    value.to_string()
}

fn quantity(value: u32) -> ReporterResult<i64> {
    Ok(i64::from(value))
}

fn side(side: &Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn parse_number(name: &str, value: String) -> ReporterResult<u64> {
    value
        .parse()
        .map_err(|_| invalid(format!("invalid {name} in reporter checkpoint")).into())
}

async fn connect() -> ReporterResult<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| invalid("DATABASE_URL must be set for reporter"))?;
    Ok(PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?)
}

async fn load_checkpoint(pool: &PgPool) -> ReporterResult<Option<ReaderCheckpoint>> {
    let row: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT journal_device::text, journal_inode::text, next_sequence::text, byte_offset::text \
         FROM reporter_checkpoint WHERE singleton = $1",
    )
    .bind(CHECKPOINT_ID)
    .fetch_optional(pool)
    .await?;
    row.map(|(device, inode, next, offset)| {
        Ok(StreamReader::checkpoint_from_parts(
            parse_number("journal device", device)?,
            parse_number("journal inode", inode)?,
            parse_number("next sequence", next)?,
            parse_number("byte offset", offset)?,
        ))
    })
    .transpose()
}

async fn ensure_consistent_saved_state(
    pool: &PgPool,
    checkpoint: Option<&ReaderCheckpoint>,
) -> ReporterResult<()> {
    let (orders, trades): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM reported_orders), (SELECT count(*) FROM reported_trades)",
    )
    .fetch_one(pool)
    .await?;
    if checkpoint.is_none() && (orders != 0 || trades != 0) {
        return Err(invalid("reporter rows exist without a reporter checkpoint").into());
    }
    Ok(())
}

async fn save_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    checkpoint: &ReaderCheckpoint,
) -> ReporterResult<()> {
    // The checkpoint is private to event_stream; serde is not an external database interface.
    let encoded = serde_json::to_value(checkpoint)?;
    let field = |name: &str| -> ReporterResult<u64> {
        encoded
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| invalid(format!("missing {name} in reader checkpoint")).into())
    };
    let device = field("device")?;
    let inode = field("inode")?;
    let next_sequence = field("next_sequence")?;
    let byte_offset = field("byte_offset")?;
    sqlx::query(
        "INSERT INTO reporter_checkpoint (singleton, journal_device, journal_inode, next_sequence, byte_offset) \
         VALUES ($1, $2::numeric, $3::numeric, $4::numeric, $5::numeric) \
         ON CONFLICT (singleton) DO UPDATE SET journal_device = EXCLUDED.journal_device, \
         journal_inode = EXCLUDED.journal_inode, next_sequence = EXCLUDED.next_sequence, byte_offset = EXCLUDED.byte_offset",
    ).bind(CHECKPOINT_ID).bind(number(device)).bind(number(inode))
        .bind(number(next_sequence)).bind(number(byte_offset)).execute(&mut **tx).await?;
    Ok(())
}

async fn insert_order(
    tx: &mut Transaction<'_, Postgres>,
    order: &Order,
    status: &str,
    acceptance_sequence: Option<u64>,
    rejection_reason: Option<&str>,
) -> ReporterResult<()> {
    let original = quantity(order.quantity)?;
    sqlx::query(
        "INSERT INTO reported_orders (order_id, user_id, symbol, side, limit_price, original_quantity, filled_quantity, remaining_quantity, status, creation_time, acceptance_sequence, rejection_reason) \
         VALUES ($1, $2, $3, $4, $5::numeric, $6, 0, $6, $7, $8, $9::numeric, $10)",
    ).bind(&order.order_id).bind(&order.user_id).bind(&order.symbol).bind(side(&order.side))
        .bind(number(order.price.minor_units())).bind(original).bind(status).bind(order.timestamp)
        .bind(acceptance_sequence.map(number)).bind(rejection_reason).execute(&mut **tx).await?;
    Ok(())
}

async fn apply_trade(
    tx: &mut Transaction<'_, Postgres>,
    pair: &ExecutionPair,
) -> ReporterResult<()> {
    let execution = &pair.first;
    let quantity = quantity(execution.quantity)?;
    for order_id in [&execution.buy_order_id, &execution.sell_order_id] {
        let updated = sqlx::query(
            "UPDATE reported_orders SET filled_quantity = filled_quantity + $1, remaining_quantity = remaining_quantity - $1, \
             status = CASE WHEN remaining_quantity - $1 = 0 THEN 'filled' ELSE 'partially_filled' END \
             WHERE order_id = $2 AND status IN ('new', 'partially_filled') AND remaining_quantity >= $1",
        ).bind(quantity).bind(order_id).execute(&mut **tx).await?;
        if updated.rows_affected() != 1 {
            return Err(invalid(format!(
                "trade references a missing or non-resting order {order_id}"
            ))
            .into());
        }
    }
    sqlx::query(
        "INSERT INTO reported_trades (trade_sequence, symbol, price, quantity, buy_order_id, sell_order_id, first_execution_id, second_execution_id, trade_time) \
         VALUES ($1::numeric, $2, $3::numeric, $4, $5, $6, $7, $8, $9)",
    ).bind(number(pair.first_sequence)).bind(&execution.symbol).bind(number(execution.price.minor_units()))
        .bind(quantity).bind(&execution.buy_order_id).bind(&execution.sell_order_id)
        .bind(&pair.first.execution_id).bind(&pair.second.execution_id).bind(execution.timestamp)
        .execute(&mut **tx).await?;
    Ok(())
}

async fn apply_command(
    tx: &mut Transaction<'_, Postgres>,
    command: CommittedCommand,
) -> ReporterResult<()> {
    match command {
        CommittedCommand::Other => Ok(()),
        CommittedCommand::NewOrder {
            order,
            outcome: NewOrderOutcome::Rejected { reason },
        } => insert_order(tx, &order, "rejected", None, Some(&reason)).await,
        CommittedCommand::NewOrder {
            order,
            outcome:
                NewOrderOutcome::Accepted {
                    matching_sequence,
                    executions,
                },
        } => {
            insert_order(tx, &order, "new", Some(matching_sequence), None).await?;
            for pair in &executions {
                apply_trade(tx, pair).await?;
            }
            Ok(())
        }
        CommittedCommand::Cancellation {
            order_id,
            outcome: CancelOutcome::Canceled { matching_sequence },
        } => {
            let updated = sqlx::query(
                "UPDATE reported_orders SET status = 'canceled', cancellation_sequence = $1::numeric, cancellation_outcome = 'canceled', cancellation_reason = NULL \
                 WHERE order_id = $2 AND status IN ('new', 'partially_filled')",
            ).bind(number(matching_sequence)).bind(order_id).execute(&mut **tx).await?;
            if updated.rows_affected() != 1 {
                return Err(
                    invalid("cancellation references a missing or non-resting order").into(),
                );
            }
            Ok(())
        }
        CommittedCommand::Cancellation {
            order_id,
            outcome: CancelOutcome::Rejected { reason },
        } => {
            sqlx::query(
                "UPDATE reported_orders SET cancellation_outcome = 'rejected', cancellation_reason = $1 WHERE order_id = $2",
            ).bind(reason).bind(order_id).execute(&mut **tx).await?;
            Ok(())
        }
    }
}

async fn apply_batch(
    pool: &PgPool,
    batch: &[crate::types::exchange_event::EventEnvelope],
    checkpoint: &ReaderCheckpoint,
) -> ReporterResult<()> {
    let command = committed_batch::decode(batch).map_err(invalid)?;
    let mut tx = pool.begin().await?;
    apply_command(&mut tx, command).await?;
    save_checkpoint(&mut tx, checkpoint).await?;
    tx.commit().await?;
    Ok(())
}

async fn persist_initial_checkpoint(
    pool: &PgPool,
    checkpoint: &ReaderCheckpoint,
) -> ReporterResult<()> {
    let mut tx = pool.begin().await?;
    save_checkpoint(&mut tx, checkpoint).await?;
    tx.commit().await?;
    Ok(())
}

async fn follow(mut reader: StreamReader, pool: PgPool) -> ReporterResult<()> {
    loop {
        match reader.next_batch()? {
            Some(batch) => apply_batch(&pool, &batch, &reader.checkpoint()).await?,
            None => tokio::time::sleep(IDLE_POLL).await,
        }
    }
}

#[derive(Clone)]
struct ReporterState {
    available: Arc<AtomicBool>,
}

async fn health(State(state): State<ReporterState>) -> impl IntoResponse {
    if state.available.load(Ordering::Acquire) {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "reporter unavailable")
    }
}

pub async fn run(args: &[String]) -> ReporterResult<()> {
    if !(2..=3).contains(&args.len()) {
        return Err(invalid("usage: stock --reporter JOURNAL STREAM [LISTEN_ADDR]").into());
    }
    let address: SocketAddr = args
        .get(2)
        .map(String::as_str)
        .unwrap_or(DEFAULT_ADDR)
        .parse()?;
    let pool = connect().await?;
    let checkpoint = load_checkpoint(&pool).await?;
    ensure_consistent_saved_state(&pool, checkpoint.as_ref()).await?;
    let mut reader = StreamReader::open(&args[0], &args[1], checkpoint)?;
    let mut advanced = false;
    while let Some(batch) = reader.next_batch()? {
        apply_batch(&pool, &batch, &reader.checkpoint()).await?;
        advanced = true;
    }
    if !advanced && load_checkpoint(&pool).await?.is_none() {
        persist_initial_checkpoint(&pool, &reader.checkpoint()).await?;
    }

    let available = Arc::new(AtomicBool::new(true));
    let follower_available = Arc::clone(&available);
    let follower_pool = pool.clone();
    thread::Builder::new()
        .name("reporter-follower".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime.and_then(|runtime| {
                runtime
                    .block_on(follow(reader, follower_pool))
                    .map_err(io::Error::other)
            }) {
                Ok(()) => {}
                Err(error) => eprintln!("reporter follower halted: {error}"),
            }
            follower_available.store(false, Ordering::Release);
        })?;

    let app = Router::new()
        .route("/health", get(health))
        .with_state(ReporterState { available });
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Reporter is listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
