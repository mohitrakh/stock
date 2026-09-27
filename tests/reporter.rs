//! Isolated PostgreSQL acceptance for Reporter recovery. Run with:
//! REPORTER_TEST_DATABASE_URL=postgresql://... cargo test --test reporter -- --ignored --nocapture
//!
//! The supplied database is deliberately reset by this test. It must not be shared with a real
//! application or another test run.

use std::{
    fs,
    io::Write,
    net::{TcpListener, TcpStream},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const CAPACITY: usize = 4096;

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
    side: &str,
    quantity: u32,
) -> serde_json::Value {
    serde_json::json!({
        "seq_num": first_sequence,
        "event": {"direction":"input","event":{"kind":"new_order_requested","data":{"order":{
            "order_id":id,"user_id":user,"symbol":"AAPL","side":side,
            "price":100,"quantity":quantity,"leaves_qty":quantity,"timestamp":1.0,"seq_num":0
        }}}}
    })
}

fn execution(id: &str, buy: &str, sell: &str, quantity: u32, timestamp: f64) -> serde_json::Value {
    serde_json::json!({
        "execution_id":id,"buy_order_id":buy,"sell_order_id":sell,"symbol":"AAPL",
        "price":100,"quantity":quantity,"timestamp":timestamp
    })
}

fn accepted_order(
    first_sequence: u64,
    id: &str,
    user: &str,
    side: &str,
    quantity: u32,
    matching_sequence: u64,
    executions: &[serde_json::Value],
) -> Vec<u8> {
    let mut events = vec![
        order(first_sequence, id, user, side, quantity),
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

fn rejected_order(first_sequence: u64, id: &str, user: &str) -> Vec<u8> {
    record(serde_json::json!([
        order(first_sequence, id, user, "buy", 1),
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"order_rejected","data":{"order_id":id,"reason":"no funds"}}}}
    ]))
}

fn canceled(first_sequence: u64, order_id: &str, user_id: &str, matching_sequence: u64) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"cancel_order_requested","data":{"order_id":order_id,"user_id":user_id}}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"order_canceled","data":{"order_id":order_id,"seq_num":matching_sequence}}}}
    ]))
}

fn cancel_rejected(first_sequence: u64, order_id: &str, user_id: &str) -> Vec<u8> {
    record(serde_json::json!([
        {"seq_num":first_sequence,"event":{"direction":"input","event":{"kind":"cancel_order_requested","data":{"order_id":order_id,"user_id":user_id}}}},
        {"seq_num":first_sequence + 1,"event":{"direction":"output","event":{"kind":"cancel_rejected","data":{"order_id":order_id,"reason":"order not found"}}}}
    ]))
}

fn fixture(dir: &Path) -> (PathBuf, PathBuf) {
    let records = vec![
        accepted_order(1, "sell-a", "seller-a", "sell", 5, 1, &[]),
        accepted_order(3, "sell-b", "seller-b", "sell", 5, 2, &[]),
        accepted_order(
            5,
            "buy-1",
            "buyer",
            "buy",
            8,
            3,
            &[
                execution("buy-exec-a", "buy-1", "sell-a", 5, 3.0),
                execution("sell-exec-a", "buy-1", "sell-a", 5, 3.0),
                execution("buy-exec-b", "buy-1", "sell-b", 3, 3.0),
                execution("sell-exec-b", "buy-1", "sell-b", 3, 3.0),
            ],
        ),
        canceled(11, "sell-b", "seller-b", 4),
        cancel_rejected(13, "missing", "buyer"),
        rejected_order(15, "reject-1", "buyer"),
    ];
    let journal = dir.join("events.log");
    let stream = dir.join("events.mmap");
    let mut journal_bytes = b"EXCHLOG1".to_vec();
    for record in &records {
        journal_bytes.extend_from_slice(record);
    }
    fs::write(&journal, &journal_bytes).unwrap();

    // Older records are only in the journal; the final record is in mmap. This exercises
    // journal catch-up and the handoff back to the cache in one real Reporter process.
    let cache = records.last().unwrap();
    let metadata = fs::metadata(&journal).unwrap();
    let mut header = b"EXCHBUS1".to_vec();
    for value in [
        metadata.dev(),
        metadata.ino(),
        CAPACITY as u64,
        journal_bytes.len() as u64,
        16,
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

fn reset_database(database_url: &str) {
    run_sql(
        database_url,
        "DROP TABLE IF EXISTS reported_trades, reported_orders, reporter_checkpoint CASCADE",
    );
    let migration = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260926000000_create_reporter_tables.sql");
    let output = psql(database_url, &["-f", migration.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
    let database_url =
        std::env::var("REPORTER_TEST_DATABASE_URL").expect("set REPORTER_TEST_DATABASE_URL");
    reset_database(&database_url);
    let dir = std::env::temp_dir().join(format!("stock-reporter-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    let (journal, stream) = fixture(&dir);

    // Checkpoint persistence fails after Reporter has started order/trade writes. The database
    // transaction must roll back every one of those writes with the checkpoint.
    run_sql(
        &database_url,
        "CREATE FUNCTION reporter_fail_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END; $$; CREATE TRIGGER reporter_fail_checkpoint BEFORE INSERT OR UPDATE ON reporter_checkpoint FOR EACH ROW EXECUTE FUNCTION reporter_fail_checkpoint()",
    );
    assert_fails_before_ready(spawn_reporter(
        &database_url,
        &journal,
        &stream,
        &unused_address(),
    ));
    assert_eq!(
        query(
            &database_url,
            "SELECT (SELECT count(*) FROM reported_orders) || ':' || (SELECT count(*) FROM reported_trades) || ':' || (SELECT count(*) FROM reporter_checkpoint)",
        ),
        "0:0:0"
    );
    run_sql(
        &database_url,
        "DROP TRIGGER reporter_fail_checkpoint ON reporter_checkpoint; DROP FUNCTION reporter_fail_checkpoint()",
    );

    let address = unused_address();
    let mut first = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(first.child(), &address);
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(order_id || ':' || status || ':' || filled_quantity::text || ':' || remaining_quantity::text || ':' || COALESCE(cancellation_outcome, 'none'), ',' ORDER BY order_id) FROM reported_orders",
        ),
        "buy-1:filled:8:0:none,reject-1:rejected:0:1:none,sell-a:filled:5:0:none,sell-b:canceled:3:2:canceled"
    );
    assert_eq!(
        query(
            &database_url,
            "SELECT string_agg(trade_sequence::text || ':' || buy_order_id || ':' || sell_order_id || ':' || quantity::text, ',' ORDER BY trade_sequence) FROM reported_trades",
        ),
        "7:buy-1:sell-a:5,9:buy-1:sell-b:3"
    );
    assert_eq!(
        query(
            &database_url,
            "SELECT next_sequence::text FROM reporter_checkpoint"
        ),
        "17"
    );

    // This process has committed its projection. On restart, Reporter must load that checkpoint
    // and avoid duplicating either historical lifecycle rows or trades.
    first.stop();
    let mut second = ChildGuard::new(spawn_reporter(&database_url, &journal, &stream, &address));
    wait_until_ready(second.child(), &address);
    assert_eq!(
        query(
            &database_url,
            "SELECT (SELECT count(*) FROM reported_orders) || ':' || (SELECT count(*) FROM reported_trades) || ':' || (SELECT next_sequence::text FROM reporter_checkpoint)",
        ),
        "4:2:17"
    );
    second.stop();
    fs::remove_dir_all(dir).unwrap();
}
