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
/// Most committed batches applied in one PostgreSQL transaction. A group also ends whenever the
/// reporter has caught up, so on a quiet exchange each batch still commits at once.
const MAX_GROUP: u64 = 1_000;

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
    // Naming every table also fails startup, before anything is written, when the milestone 21
    // migration is missing.
    let rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM reported_orders) + (SELECT count(*) FROM reported_trades) \
         + (SELECT count(*) FROM rejected_orders) + (SELECT count(*) FROM rejected_cancellations)",
    )
    .fetch_one(pool)
    .await?;
    if checkpoint.is_none() && rows != 0 {
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

/// Accepted orders only: the engine never accepts an order id twice, so it can be the key.
async fn insert_accepted_order(
    tx: &mut Transaction<'_, Postgres>,
    order: &Order,
    acceptance_sequence: u64,
) -> ReporterResult<()> {
    let original = quantity(order.quantity)?;
    sqlx::query(
        "INSERT INTO reported_orders (order_id, user_id, symbol, side, limit_price, original_quantity, filled_quantity, remaining_quantity, status, creation_time, acceptance_sequence) \
         VALUES ($1, $2, $3, $4, $5::numeric, $6, 0, $6, 'new', $7, $8::numeric)",
    ).bind(&order.order_id).bind(&order.user_id).bind(&order.symbol).bind(side(&order.side))
        .bind(number(order.price.minor_units())).bind(original).bind(order.timestamp)
        .bind(number(acceptance_sequence)).execute(&mut **tx).await?;
    Ok(())
}

/// A rejected submission is keyed by the journal sequence of its input, because its order id need
/// not be unique: a client retry of an existing id, a reused rejected id, or another user's id.
async fn insert_rejected_order(
    tx: &mut Transaction<'_, Postgres>,
    input_sequence: u64,
    order: &Order,
    reason: &str,
) -> ReporterResult<()> {
    sqlx::query(
        "INSERT INTO rejected_orders (input_sequence, order_id, user_id, symbol, side, limit_price, quantity, creation_time, reason) \
         VALUES ($1::numeric, $2, $3, $4, $5, $6::numeric, $7, $8, $9)",
    ).bind(number(input_sequence)).bind(&order.order_id).bind(&order.user_id).bind(&order.symbol)
        .bind(side(&order.side)).bind(number(order.price.minor_units()))
        .bind(quantity(order.quantity)?).bind(order.timestamp).bind(reason)
        .execute(&mut **tx).await?;
    Ok(())
}

/// Records one trade in ONE round trip: a data-modifying CTE fills both orders, and the trade row
/// is inserted only if both fills applied (each order was resting with enough quantity left).
/// Three statements per trade became one; round trips, not commits, now limit the reporter.
async fn apply_trade(
    tx: &mut Transaction<'_, Postgres>,
    pair: &ExecutionPair,
) -> ReporterResult<()> {
    let execution = &pair.first;
    let quantity = quantity(execution.quantity)?;
    let inserted = sqlx::query(concat!(
        "WITH filled AS (",
        " UPDATE reported_orders SET filled_quantity = filled_quantity + $4,",
        " remaining_quantity = remaining_quantity - $4,",
        " status = CASE WHEN remaining_quantity - $4 = 0 THEN 'filled' ELSE 'partially_filled' END",
        " WHERE order_id IN ($5, $6) AND status IN ('new', 'partially_filled')",
        " AND remaining_quantity >= $4",
        " RETURNING order_id)",
        " INSERT INTO reported_trades (trade_sequence, symbol, price, quantity, buy_order_id,",
        " sell_order_id, first_execution_id, second_execution_id, trade_time)",
        " SELECT $1::numeric, $2, $3::numeric, $4, $5, $6, $7, $8, $9",
        " WHERE (SELECT count(*) FROM filled) = 2",
    ))
    .bind(number(pair.first_sequence))
    .bind(&execution.symbol)
    .bind(number(execution.price.minor_units()))
    .bind(quantity)
    .bind(&execution.buy_order_id)
    .bind(&execution.sell_order_id)
    .bind(&pair.first.execution_id)
    .bind(&pair.second.execution_id)
    .bind(execution.timestamp)
    .execute(&mut **tx)
    .await?;
    if inserted.rows_affected() != 1 {
        return Err(invalid(format!(
            "trade references a missing or non-resting order ({} / {})",
            execution.buy_order_id, execution.sell_order_id
        ))
        .into());
    }
    Ok(())
}

/// `input_sequence` is the journal sequence of the command's input: the identity of anything the
/// report records per command rather than per order.
async fn apply_command(
    tx: &mut Transaction<'_, Postgres>,
    input_sequence: u64,
    command: CommittedCommand,
) -> ReporterResult<()> {
    match command {
        CommittedCommand::Other => Ok(()),
        CommittedCommand::NewOrder {
            order,
            outcome: NewOrderOutcome::Rejected { reason },
        } => insert_rejected_order(tx, input_sequence, &order, &reason).await,
        CommittedCommand::NewOrder {
            order,
            outcome:
                NewOrderOutcome::Accepted {
                    matching_sequence,
                    executions,
                },
        } => {
            insert_accepted_order(tx, &order, matching_sequence).await?;
            for pair in &executions {
                apply_trade(tx, pair).await?;
            }
            Ok(())
        }
        CommittedCommand::Cancellation {
            order_id,
            user_id,
            outcome: CancelOutcome::Canceled { matching_sequence },
        } => {
            // The engine lets only the owner cancel, so checking the owner costs nothing and stops
            // the reporter if the journal and this projection ever disagree.
            let updated = sqlx::query(
                "UPDATE reported_orders SET status = 'canceled', cancellation_sequence = $1::numeric, cancellation_outcome = 'canceled', cancellation_reason = NULL \
                 WHERE order_id = $2 AND user_id = $3 AND status IN ('new', 'partially_filled')",
            ).bind(number(matching_sequence)).bind(order_id).bind(user_id).execute(&mut **tx).await?;
            if updated.rows_affected() != 1 {
                return Err(
                    invalid("cancellation references a missing or non-resting order").into(),
                );
            }
            Ok(())
        }
        CommittedCommand::Cancellation {
            order_id,
            user_id,
            outcome: CancelOutcome::Rejected { reason },
        } => {
            // A refused attempt is a fact about the attempt, not the order: it gets its own row
            // with who asked, and the order's row stays exactly as it was.
            sqlx::query(
                "INSERT INTO rejected_cancellations (input_sequence, order_id, requested_by, reason) \
                 VALUES ($1::numeric, $2, $3, $4)",
            ).bind(number(input_sequence)).bind(order_id).bind(user_id).bind(reason)
                .execute(&mut **tx).await?;
            Ok(())
        }
    }
}

/// Applies every batch the reader has, up to `MAX_GROUP` per transaction. Each group commits
/// together with the checkpoint just after its last applied batch, so the rows and the position
/// they represent always move together; returns how many batches were applied. Any error drops
/// the open transaction, rolling back the whole group, and is terminal: the reader's cursor may
/// be ahead of the database then, which is harmless only because the reporter stops.
/// See `docs/performance/06-reporter-batched-transactions.md`.
async fn apply_available(reader: &mut StreamReader, pool: &PgPool) -> ReporterResult<u64> {
    let mut applied = 0;
    while let Some(first) = reader.next_batch()? {
        let mut tx = pool.begin().await?;
        let mut batch = first;
        let mut in_group = 0;
        loop {
            let command = committed_batch::decode(&batch).map_err(invalid)?;
            apply_command(&mut tx, batch[0].seq_num, command).await?;
            in_group += 1;
            // Decide after applying, and take the position before reading any further batch.
            let after = reader.checkpoint();
            let next = if in_group < MAX_GROUP {
                reader.next_batch()?
            } else {
                None
            };
            match next {
                Some(following) => batch = following,
                None => {
                    save_checkpoint(&mut tx, &after).await?;
                    tx.commit().await?;
                    break;
                }
            }
        }
        applied += in_group;
    }
    Ok(applied)
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
        if apply_available(&mut reader, &pool).await? == 0 {
            tokio::time::sleep(IDLE_POLL).await;
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
    // Catch-up uses the same grouping, so its last group commits before the listener binds.
    let advanced = apply_available(&mut reader, &pool).await? > 0;
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
