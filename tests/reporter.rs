//! Isolated PostgreSQL acceptance for Reporter recovery. Run with:
//! REPORTER_TEST_DATABASE_URL=postgresql://... cargo test --test reporter -- --ignored --nocapture
//!
//! The supplied database is deliberately reset by these tests, one test at a time. It must not be
//! shared with a real application or another test run.

use std::{
    fs,
    io::Write,
    net::{TcpListener, TcpStream},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const CAPACITY: usize = 4096;
/// The reporter's migrations, applied in this order.
const MIGRATIONS: [&str; 4] = [
    "20260926000000_create_reporter_tables.sql",
    "20260930000000_reporter_rejections.sql",
    "20261005000000_reporter_expiry.sql",
    "20261005100000_reporter_trading_days.sql",
];
/// Every test resets the one supplied database, so they must never overlap.
static DATABASE: Mutex<()> = Mutex::new(());

fn crc(bytes: &[u8]) -> u32 {
    let mut value = !0u32;
    for byte in bytes {
        value ^= *byte as u32;
        for _ in 0..8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ 0xedb88320
            } else {
                value >> 1
            };
        }
    }
    !value
}

fn record(events: serde_json::Value) -> Vec<u8> {
    let payload = serde_json::to_vec(&events).unwrap();
    let mut record = Vec::with_capacity(8 + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc(&payload).to_le_bytes());
    record.extend(payload);
    record
}

fn order(
    first_sequence: u64,
    id: &str,
    user: &str,
    symbol: &str,
    side: &str,
    quantity: u32,
) -> serde_json::Value {
    serde_json::json!({
        "seq_num": first_sequence,
        "event": {"direction":"input","event":{"kind":"new_order_requested","data":{"order":{
            "order_id":id,"user_id":user,"symbol":symbol,"side":side,
            "price":100,"quantity":quantity,"leaves_qty":quantity,"timestamp":1.0,"seq_num":0
        }}}}
    })
}

fn execution(id: &str, buy: &str, sell: &str, symbol: &str, quantity: u32) -> serde_json::Value {
    serde_json::json!({
        "execution_id":id,"buy_order_id":buy,"sell_order_id":sell,"symbol":symbol,
        "price":100,"quantity":quantity,"timestamp":3.0
    })
}

fn order_id(input: &serde_json::Value) -> serde_json::Value {
    input["event"]["event"]["data"]["order"]["order_id"].clone()
}

/// The input, its acceptance, then each execution record in the order given.
fn accepted_order(
    input: serde_json::Value,
    matching_sequence: u64,
    executions: &[serde_json::Value],
) -> Vec<u8> {
    let first_sequence = input["seq_num"].as_u64().unwrap();
    let id = order_id(&input);
    let mut events = vec![
        input,
        serde_json::json!({
            "seq_num": first_sequence + 1,
            "event":{"direction":"output","event":{"kind":"order_accepted","data":{"order_id":id,"seq_num":matching_sequence}}}
        }),
    ];
    for (index, execution) in executions.iter().enumerate() {
        events.push(serde_json::json!({
            "seq_num": first_sequence + 2 + index as u64,
            "event":{"direction":"output","event":{"kind":"execution_created","data":{"execution":execution}}}
        }));
    }
    record(serde_json::Value::Array(events))
}

fn rejected_order(input: serde_json::Value, reason: &str) -> Vec<u8> {
    let first_sequence = input["seq_num"].as_u64().unwrap();
    let id = order_id(&input);
    record(serde_json::json!([
        input,
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"order_rejected","data":{"order_id":id,"reason":reason}}}}
    ]))
}

fn canceled(first_sequence: u64, order_id: &str, user_id: &str, matching_sequence: u64) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"cancel_order_requested","data":{"order_id":order_id,"user_id":user_id}}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"order_canceled","data":{"order_id":order_id,"seq_num":matching_sequence}}}}
    ]))
}

fn cancel_rejected(first_sequence: u64, order_id: &str, user_id: &str, reason: &str) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"cancel_order_requested","data":{"order_id":order_id,"user_id":user_id}}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"cancel_rejected","data":{"order_id":order_id,"reason":reason}}}}
    ]))
}

fn opened(first_sequence: u64, trading_day: &str) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"market_open_requested","data":{"trading_day":trading_day}}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"market_opened","data":{"trading_day":trading_day}}}}
    ]))
}

/// The close, then one expiry per resting order with its matching sequence.
fn closed(first_sequence: u64, trading_day: &str, expired: &[(&str, u64)]) -> Vec<u8> {
    let mut events = vec![
        serde_json::json!({"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"market_close_requested"}}}),
        serde_json::json!({"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"market_closed","data":{"trading_day":trading_day}}}}),
    ];
    for (index, (order_id, matching_sequence)) in expired.iter().enumerate() {
        events.push(serde_json::json!({
            "seq_num": first_sequence + 2 + index as u64,
            "event":{"direction":"output","event":{"kind":"order_expired","data":{"order_id":order_id,"seq_num":matching_sequence}}}
        }));
    }
    record(serde_json::Value::Array(events))
}

fn close_refused(first_sequence: u64, reason: &str) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"market_close_requested"}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"session_rejected","data":{"reason":reason}}}}
    ]))
}

/// Every lifecycle the report distinguishes, including the three cases that used to halt the
/// reporter or damage a row: a rejected retry of an accepted id, an accepted order reusing a
/// rejected id, refused cancellations (by an intruder, and of an order already canceled), and a
/// second symbol whose book numbers its executions from `exec_0` again. The day then closes,
/// expiring one untouched and one partly filled order, and a second close is refused. On the next
/// day an id from the first day is accepted again and trades, with execution ids that continue.
fn lifecycle_records() -> Vec<Vec<u8>> {
    vec![
        opened(1, "2026-10-05"),
        accepted_order(order(3, "sell-a", "seller-a", "AAPL", "sell", 5), 1, &[]),
        accepted_order(order(5, "sell-b", "seller-b", "AAPL", "sell", 5), 2, &[]),
        accepted_order(
            order(7, "buy-1", "buyer", "AAPL", "buy", 8),
            3,
            &[
                execution("exec_0", "buy-1", "sell-a", "AAPL", 5),
                execution("exec_1", "buy-1", "sell-a", "AAPL", 5),
                execution("exec_2", "buy-1", "sell-b", "AAPL", 3),
                execution("exec_3", "buy-1", "sell-b", "AAPL", 3),
            ],
        ),
        canceled(13, "sell-b", "seller-b", 4),
        cancel_rejected(15, "missing", "buyer", "order not found"),
        rejected_order(order(17, "reject-1", "buyer", "AAPL", "buy", 1), "no funds"),
        rejected_order(
            order(19, "buy-1", "buyer", "AAPL", "buy", 8),
            "order already exists",
        ),
        accepted_order(order(21, "reject-1", "buyer", "AAPL", "buy", 1), 5, &[]),
        cancel_rejected(23, "reject-1", "intruder", "unauthorized"),
        cancel_rejected(25, "sell-b", "seller-b", "order is not open"),
        accepted_order(
            order(27, "msft-sell", "seller-a", "MSFT", "sell", 2),
            6,
            &[],
        ),
        accepted_order(
            order(29, "msft-buy", "buyer", "MSFT", "buy", 2),
            7,
            &[
                execution("exec_0", "msft-buy", "msft-sell", "MSFT", 2),
                execution("exec_1", "msft-buy", "msft-sell", "MSFT", 2),
            ],
        ),
        accepted_order(
            order(33, "rest-partial", "seller-a", "AAPL", "sell", 4),
            8,
            &[],
        ),
        accepted_order(
            order(35, "taker", "buyer", "AAPL", "buy", 1),
            9,
            &[
                execution("exec_4", "taker", "rest-partial", "AAPL", 1),
                execution("exec_5", "taker", "rest-partial", "AAPL", 1),
            ],
        ),
        closed(39, "2026-10-05", &[("reject-1", 10), ("rest-partial", 11)]),
        close_refused(43, "AlreadyClosed"),
        opened(45, "2026-10-06"),
        accepted_order(order(47, "buy-1", "buyer", "AAPL", "buy", 2), 12, &[]),
        accepted_order(
            order(49, "sell-d2", "seller-a", "AAPL", "sell", 2),
            13,
            &[
                execution("exec_6", "buy-1", "sell-d2", "AAPL", 2),
                execution("exec_7", "buy-1", "sell-d2", "AAPL", 2),
            ],
        ),
        cancel_rejected(53, "reject-1", "buyer", "order not found"),
    ]
}

/// Writes `records` as a journal, and a stream whose cache holds only the last record: a real
/// Reporter process then catches up from the journal and hands off to the cache.
fn write_journal(dir: &Path, records: &[Vec<u8>]) -> (PathBuf, PathBuf) {
    let journal = dir.join("events.log");
    let stream = dir.join("events.mmap");
    let mut journal_bytes = b"EXCHLOG1".to_vec();
    for record in records {
        journal_bytes.extend_from_slice(record);
    }
    fs::write(&journal, &journal_bytes).unwrap();

    let cache = records.last().unwrap();
    let last_batch: serde_json::Value = serde_json::from_slice(&cache[8..]).unwrap();
    let last_sequence = last_batch.as_array().unwrap().last().unwrap()["seq_num"]
        .as_u64()
        .unwrap();
    let metadata = fs::metadata(&journal).unwrap();
    let mut header = b"EXCHBUS1".to_vec();
    for value in [
        metadata.dev(),
        metadata.ino(),
        CAPACITY as u64,
        journal_bytes.len() as u64,
        last_sequence,
        journal_bytes.len() as u64 - cache.len() as u64,
        cache.len() as u64,
    ] {
        header.extend_from_slice(&value.to_le_bytes());
    }
    header.extend_from_slice(&crc(&header).to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    header.extend_from_slice(&1u64.to_ne_bytes());
    header.extend_from_slice(cache);
    header.resize(80 + CAPACITY, 0);
    fs::write(&stream, header).unwrap();
    (journal, stream)
}

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stock-reporter-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    dir
}

fn unused_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

fn health(address: &str) -> Option<u16> {
    let mut stream = TcpStream::connect(address).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    write!(
        stream,
        "GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut body = String::new();
    std::io::Read::read_to_string(&mut stream, &mut body).ok()?;
    body.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

fn psql(database_url: &str, arguments: &[&str]) -> std::process::Output {
    Command::new("psql")
        .arg(database_url)
        .args(["-v", "ON_ERROR_STOP=1"])
        .args(arguments)
        .output()
        .unwrap()
}

fn run_sql(database_url: &str, sql: &str) {
    let output = psql(database_url, &["-c", sql]);
    assert!(
        output.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn query(database_url: &str, sql: &str) -> String {
    let output = psql(database_url, &["-At", "-c", sql]);
    assert!(
        output.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// `orders:trades:rejected orders:rejected cancellations:checkpoint next sequence`.
fn summary(database_url: &str) -> String {
    query(
        database_url,
        "SELECT (SELECT count(*) FROM reported_orders) || ':' || (SELECT count(*) FROM reported_trades) || ':' || (SELECT count(*) FROM rejected_orders) || ':' || (SELECT count(*) FROM rejected_cancellations) || ':' || COALESCE((SELECT next_sequence::text FROM reporter_checkpoint), 'none')",
    )
}

fn reset_database(database_url: &str) {
    run_sql(
        database_url,
        "DROP TABLE IF EXISTS reported_trades, reported_orders, reporter_checkpoint, rejected_orders, rejected_cancellations CASCADE",
    );
    for migration in MIGRATIONS {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("migrations")
            .join(migration);
        let output = psql(database_url, &["-f", path.to_str().unwrap()]);
        assert!(
            output.status.success(),
            "migration {migration} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn spawn_reporter(database_url: &str, journal: &Path, stream: &Path, address: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_stock"))
        .env("DATABASE_URL", database_url)
        .args([
            "--reporter",
            journal.to_str().unwrap(),
            stream.to_str().unwrap(),
            address,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn wait_until_ready(child: &mut Child, address: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while health(address) != Some(200) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "reporter exited before ready"
        );
        assert!(Instant::now() < deadline, "reporter never became ready");
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_fails_before_ready(mut child: Child) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                !status.success(),
                "reporter unexpectedly survived injected failure"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "reporter did not fail as expected"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            if child.try_wait().unwrap().is_none() {
                child.kill().unwrap();
            }
            child.wait().unwrap();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
#[ignore = "requires a resettable isolated REPORTER_TEST_DATABASE_URL"]
fn reporter_rolls_back_then_restarts_without_duplicate_history() {
    let _serial = DATABASE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let database_url =
        std::env::var("REPORTER_TEST_DATABASE_URL").expect("set REPORTER_TEST_DATABASE_URL");
    reset_database(&database_url);
    // Reporters from before the milestone 21 and milestone 22 migrations cannot save a checkpoint.
    for old_reporter_checkpoint in [
        "INSERT INTO reporter_checkpoint (singleton, journal_device, journal_inode, next_sequence, byte_offset) VALUES (true, 1, 1, 1, 8)",
        "INSERT INTO reporter_checkpoint (singleton, report_version, journal_device, journal_inode, next_sequence, byte_offset) VALUES (true, 2, 1, 1, 1, 8)",
        "INSERT INTO reporter_checkpoint (singleton, report_version, journal_device, journal_inode, next_sequence, byte_offset) VALUES (true, 3, 1, 1, 1, 8)",
    ] {
        assert!(
            !psql(&database_url, &["-c", old_reporter_checkpoint])
                .status
                .success()
        );
    }
    let dir = temp_dir();
    let (journal, stream) = write_journal(&dir, &lifecycle_records());

    // Checkpoint persistence fails after Reporter has started order/trade writes. The database
    // transaction must roll back every one of those writes with the checkpoint.
    run_sql(
        &database_url,
        "CREATE OR REPLACE FUNCTION reporter_fail_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END; $$; CREATE TRIGGER reporter_fail_checkpoint BEFORE INSERT OR UPDATE ON reporter_checkpoint FOR EACH ROW EXECUTE FUNCTION reporter_fail_checkpoint()",
    );
    assert_fails_before_ready(spawn_reporter(
        &database_url,
        &journal,
        &stream,
        &unused_address(),
    ));
    assert_eq!(summary(&database_url), "0:0:0:0:none");
    run_sql(
        &database_url,
        "DROP TRIGGER reporter_fail_checkpoint ON reporter_checkpoint; DROP FUNCTION reporter_fail_checkpoint()",
    );

    let address = unused_address();
    let mut first = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(first.child(), &address);
    // Accepted orders only. buy-1 is untouched by its rejected retry, reject-1 was accepted after
    // its rejection, and neither refused cancellation changed reject-1 or sell-b. The close
    // expired the two orders still resting, with the matching sequences it gave them. On the next
    // day buy-1 is a new order with its own row.
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(trading_day::text || ':' || order_id || ':' || status || ':' || filled_quantity::text || ':' || remaining_quantity::text || ':' || COALESCE(cancellation_sequence::text, 'none') || ':' || COALESCE(expiry_sequence::text, 'none'), ',' ORDER BY trading_day, order_id COLLATE \"C\") FROM reported_orders",
        ),
        "2026-10-05:buy-1:filled:8:0:none:none,2026-10-05:msft-buy:filled:2:0:none:none,2026-10-05:msft-sell:filled:2:0:none:none,2026-10-05:reject-1:expired:0:1:none:10,2026-10-05:rest-partial:expired:1:3:none:11,2026-10-05:sell-a:filled:5:0:none:none,2026-10-05:sell-b:canceled:3:2:4:none,2026-10-05:taker:filled:1:0:none:none,2026-10-06:buy-1:filled:2:0:none:none,2026-10-06:sell-d2:filled:2:0:none:none"
    );
    // MSFT's first trade reuses AAPL's execution ids; the ids are unique per symbol, and they
    // continue across days. The second day's trade fills that day's buy-1.
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(trade_sequence::text || ':' || trading_day::text || ':' || symbol || ':' || first_execution_id || ':' || buy_order_id || ':' || sell_order_id || ':' || quantity::text, ',' ORDER BY trade_sequence) FROM reported_trades",
        ),
        "9:2026-10-05:AAPL:exec_0:buy-1:sell-a:5,11:2026-10-05:AAPL:exec_2:buy-1:sell-b:3,31:2026-10-05:MSFT:exec_0:msft-buy:msft-sell:2,37:2026-10-05:AAPL:exec_4:taker:rest-partial:1,51:2026-10-06:AAPL:exec_6:buy-1:sell-d2:2"
    );
    // Refusals carry the day the journal was in, so the second day's refused cancellation of
    // reject-1 cannot be confused with the first day's order.
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(input_sequence::text || ':' || trading_day::text || ':' || order_id || ':' || user_id || ':' || reason, ',' ORDER BY input_sequence) FROM rejected_orders",
        ),
        "17:2026-10-05:reject-1:buyer:no funds,19:2026-10-05:buy-1:buyer:order already exists"
    );
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(input_sequence::text || ':' || trading_day::text || ':' || order_id || ':' || requested_by || ':' || reason, ',' ORDER BY input_sequence) FROM rejected_cancellations",
        ),
        "15:2026-10-05:missing:buyer:order not found,23:2026-10-05:reject-1:intruder:unauthorized,25:2026-10-05:sell-b:seller-b:order is not open,53:2026-10-06:reject-1:buyer:order not found"
    );
    let checkpoint_day = "SELECT trading_day::text FROM reporter_checkpoint";
    assert_eq!(query(&database_url, checkpoint_day), "2026-10-06");
    assert_eq!(summary(&database_url), "10:5:2:4:55");

    // This process has committed its projection. On restart, Reporter must load that checkpoint,
    // with its trading day, and avoid duplicating any lifecycle, trade, or rejection row.
    first.stop();
    let mut second = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(second.child(), &address);
    assert_eq!(summary(&database_url), "10:5:2:4:55");
    assert_eq!(query(&database_url, checkpoint_day), "2026-10-06");
    second.stop();
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "requires a resettable isolated REPORTER_TEST_DATABASE_URL"]
fn a_close_that_leaves_an_order_resting_stops_the_reporter() {
    let _serial = DATABASE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let database_url =
        std::env::var("REPORTER_TEST_DATABASE_URL").expect("set REPORTER_TEST_DATABASE_URL");
    reset_database(&database_url);
    let dir = temp_dir();
    let mut records = vec![
        rejected_order(order(1, "early", "buyer", "AAPL", "buy", 1), "MarketClosed"),
        opened(3, "2026-10-05"),
        accepted_order(order(5, "left-behind", "seller", "AAPL", "sell", 1), 1, &[]),
    ];
    let (journal, stream) = write_journal(&dir, &records);
    let address = unused_address();
    let mut reporter = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(reporter.child(), &address);
    // A refusal before the first open belongs to no trading day.
    assert_eq!(
        query(
            &database_url,
            "SELECT order_id || ':' || COALESCE(trading_day::text, 'none') FROM rejected_orders",
        ),
        "early:none"
    );
    reporter.stop();

    // This close expires nothing, yet the report holds an order resting on that day: the journal
    // and the report disagree, so the reporter stops without recording the close, every time.
    records.push(closed(7, "2026-10-05", &[]));
    write_journal(&dir, &records);
    assert_fails_before_ready(spawn_reporter(
        &database_url,
        &journal,
        &stream,
        &unused_address(),
    ));
    assert_eq!(summary(&database_url), "1:0:1:0:7");
    assert_eq!(
        query(&database_url, "SELECT status FROM reported_orders"),
        "new"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "requires a resettable isolated REPORTER_TEST_DATABASE_URL"]
fn a_failed_group_rolls_back_only_itself() {
    let _serial = DATABASE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let database_url =
        std::env::var("REPORTER_TEST_DATABASE_URL").expect("set REPORTER_TEST_DATABASE_URL");
    reset_database(&database_url);
    let dir = temp_dir();

    // The open and 999 resting sells fill exactly one group. The next group holds a trade against
    // the first of them, then an order whose insert a trigger refuses.
    let mut records = vec![opened(1, "2026-10-05")];
    records.extend((1..=999u64).map(|n| {
        let id = format!("rest-{n}");
        accepted_order(order(2 * n + 1, &id, "seller", "AAPL", "sell", 1), n, &[])
    }));
    records.push(accepted_order(
        order(2_001, "taker", "buyer", "AAPL", "buy", 1),
        1_000,
        &[
            execution("exec_0", "taker", "rest-1", "AAPL", 1),
            execution("exec_1", "taker", "rest-1", "AAPL", 1),
        ],
    ));
    records.push(accepted_order(
        order(2_005, "poison", "buyer", "AAPL", "buy", 1),
        1_001,
        &[],
    ));
    let (journal, stream) = write_journal(&dir, &records);

    run_sql(
        &database_url,
        "CREATE OR REPLACE FUNCTION reporter_refuse_order() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected order failure'; END; $$; CREATE TRIGGER reporter_refuse_order BEFORE INSERT ON reported_orders FOR EACH ROW WHEN (NEW.order_id = 'poison') EXECUTE FUNCTION reporter_refuse_order()",
    );
    assert_fails_before_ready(spawn_reporter(
        &database_url,
        &journal,
        &stream,
        &unused_address(),
    ));
    // The first group committed with the checkpoint just after its 1,000th batch (sequence 2000).
    // The failed group rolled back whole: no trade, and rest-1 is still resting.
    assert_eq!(summary(&database_url), "999:0:0:0:2001");
    let rest_1 = "SELECT status FROM reported_orders WHERE order_id = 'rest-1'";
    assert_eq!(query(&database_url, rest_1), "new");
    run_sql(
        &database_url,
        "DROP TRIGGER reporter_refuse_order ON reported_orders; DROP FUNCTION reporter_refuse_order()",
    );

    // A restart resumes at that checkpoint and applies the rest exactly once.
    let address = unused_address();
    let mut reporter = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(reporter.child(), &address);
    assert_eq!(summary(&database_url), "1001:1:0:0:2007");
    assert_eq!(query(&database_url, rest_1), "filled");
    reporter.stop();
    fs::remove_dir_all(dir).unwrap();
}
