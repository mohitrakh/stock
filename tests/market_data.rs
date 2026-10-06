//! Process-level acceptance for the shipped MDP executable. The journal and mmap fixtures are
//! encoded independently so this test checks the public wire boundary without PostgreSQL.

use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::{fs::FileExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const CAPACITY: usize = 4096;

struct Fixture {
    dir: PathBuf,
    journal: PathBuf,
    stream: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("stock-mdp-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        Self {
            journal: dir.join("events.log"),
            stream: dir.join("events.mmap"),
            state: dir.join("market-data.json"),
            dir,
        }
    }

    fn initialize(&self, records: &[Vec<u8>], last_sequence: u64) {
        // The journal header is the magic and a 16-byte journal id; the stream names the same id.
        let mut journal = b"EXCHLOG2".to_vec();
        journal.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        for record in records {
            journal.extend_from_slice(record);
        }
        fs::write(&self.journal, &journal).unwrap();
        let cache = records.last().unwrap();
        let start = journal.len() as u64 - cache.len() as u64;
        create_stream(
            &self.journal,
            &self.stream,
            journal.len() as u64,
            last_sequence,
            start,
            cache,
        );
    }

    fn append_and_publish(&self, record: &[u8], last_sequence: u64) {
        let old_end = fs::metadata(&self.journal).unwrap().len();
        let mut journal = OpenOptions::new().append(true).open(&self.journal).unwrap();
        journal.write_all(record).unwrap();
        journal.sync_all().unwrap();
        publish_header_and_cache(
            &self.journal,
            &self.stream,
            old_end + record.len() as u64,
            last_sequence,
            old_end,
            record,
        );
    }

    fn reset_stream_after_writer_restart(&self, last_sequence: u64) {
        let end = fs::metadata(&self.journal).unwrap().len();
        publish_header_and_cache(&self.journal, &self.stream, end, last_sequence, end, &[]);
    }

    fn interrupt_stream_publication(&self) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.stream)
            .unwrap();
        file.lock().unwrap();
        file.write_all_at(&0u64.to_ne_bytes(), 72).unwrap();
        file.unlock().unwrap();
    }

    fn spawn(&self, address: &str) -> ChildGuard {
        let child = Command::new(env!("CARGO_BIN_EXE_stock"))
            .current_dir(&self.dir)
            .env_remove("DATABASE_URL")
            .args([
                "--market-data",
                self.journal.to_str().unwrap(),
                self.stream.to_str().unwrap(),
                self.state.to_str().unwrap(),
                address,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        ChildGuard(Some(child))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let status = child.wait().unwrap();
            assert!(status.signal().is_some() || !status.success());
        }
    }

    fn assert_running(&mut self) {
        if let Some(status) = self.0.as_mut().unwrap().try_wait().unwrap() {
            let mut stderr = String::new();
            self.0
                .as_mut()
                .unwrap()
                .stderr
                .as_mut()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("MDP exited with {status}: {stderr}");
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

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

fn accepted_order(
    first_sequence: u64,
    order_id: &str,
    user_id: &str,
    side: &str,
    price: u64,
    quantity: u32,
    matching_sequence: u64,
) -> Vec<u8> {
    record(serde_json::json!([
        {
            "seq_num": first_sequence,
            "event": {"direction":"input","event":{"kind":"new_order_requested","data":{"order":{
                "order_id":order_id,"user_id":user_id,"symbol":"AAPL","side":side,
                "price":price,"quantity":quantity,"leaves_qty":quantity,"timestamp":1.0,"seq_num":0
            }}}}
        },
        {
            "seq_num": first_sequence + 1,
            "event": {"direction":"output","event":{"kind":"order_accepted","data":{
                "order_id":order_id,"seq_num":matching_sequence
            }}}
        }
    ]))
}

fn accepted_buy_with_trade(
    first_sequence: u64,
    order_id: &str,
    quantity: u32,
    matching_sequence: u64,
    sell_order_id: &str,
    price: u64,
    timestamp: f64,
) -> Vec<u8> {
    record(serde_json::json!([
        {
            "seq_num": first_sequence,
            "event": {"direction":"input","event":{"kind":"new_order_requested","data":{"order":{
                "order_id":order_id,"user_id":"private-buyer","symbol":"AAPL","side":"buy",
                "price":price,"quantity":quantity,"leaves_qty":quantity,"timestamp":timestamp,"seq_num":0
            }}}}
        },
        {
            "seq_num": first_sequence + 1,
            "event": {"direction":"output","event":{"kind":"order_accepted","data":{
                "order_id":order_id,"seq_num":matching_sequence
            }}}
        },
        {
            "seq_num": first_sequence + 2,
            "event": {"direction":"output","event":{"kind":"execution_created","data":{"execution":{
                "execution_id":format!("{order_id}-buy"),"buy_order_id":order_id,"sell_order_id":sell_order_id,
                "symbol":"AAPL","price":price,"quantity":quantity,"timestamp":timestamp
            }}}}
        },
        {
            "seq_num": first_sequence + 3,
            "event": {"direction":"output","event":{"kind":"execution_created","data":{"execution":{
                "execution_id":format!("{order_id}-sell"),"buy_order_id":order_id,"sell_order_id":sell_order_id,
                "symbol":"AAPL","price":price,"quantity":quantity,"timestamp":timestamp
            }}}}
        }
    ]))
}

/// The 16-byte journal id in a journal's header.
fn journal_id(journal: &Path) -> Vec<u8> {
    fs::read(journal).unwrap()[8..24].to_vec()
}

fn create_stream(
    journal: &Path,
    stream: &Path,
    end: u64,
    last_sequence: u64,
    cache_start: u64,
    cache: &[u8],
) {
    let mut bytes = header(
        &journal_id(journal),
        end,
        last_sequence,
        cache_start,
        cache.len() as u64,
    );
    bytes.extend_from_slice(cache);
    bytes.resize(80 + CAPACITY, 0);
    fs::write(stream, bytes).unwrap();
}

fn header(
    journal_id: &[u8],
    end: u64,
    last_sequence: u64,
    cache_start: u64,
    cache_len: u64,
) -> Vec<u8> {
    let mut header = b"EXCHBUS2".to_vec();
    header.extend_from_slice(journal_id);
    for value in [CAPACITY as u64, end, last_sequence, cache_start, cache_len] {
        header.extend_from_slice(&value.to_le_bytes());
    }
    header.extend_from_slice(&crc(&header).to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    header.extend_from_slice(&1u64.to_ne_bytes());
    header
}

fn publish_header_and_cache(
    journal: &Path,
    stream: &Path,
    end: u64,
    last_sequence: u64,
    cache_start: u64,
    cache: &[u8],
) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(stream)
        .unwrap();
    file.lock().unwrap();
    file.write_all_at(&0u64.to_ne_bytes(), 72).unwrap();
    if !cache.is_empty() {
        file.write_all_at(cache, 80).unwrap();
    }
    let bytes = header(
        &journal_id(journal),
        end,
        last_sequence,
        cache_start,
        cache.len() as u64,
    );
    file.write_all_at(&bytes[..68], 0).unwrap();
    file.write_all_at(&1u64.to_ne_bytes(), 72).unwrap();
    file.unlock().unwrap();
}

fn unused_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn request(address: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(address).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (headers, body) = response.split_once("\r\n\r\n")?;
    let status = headers
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body.to_string()))
}

fn wait_for(
    child: &mut ChildGuard,
    address: &str,
    path: &str,
    expected: impl Fn(u16, &str) -> bool,
) -> (u16, String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        child.assert_running();
        if let Some(response) = request(address, path)
            && expected(response.0, &response.1)
        {
            return response;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {path}");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn mdp_catches_up_serves_l2_resumes_state_and_follows_restarted_stream() {
    let fixture = Fixture::new();
    let records = vec![
        accepted_order(1, "sell-101", "private-seller", "sell", 101, 5, 1),
        accepted_order(3, "buy-99", "private-buyer", "buy", 99, 2, 2),
        accepted_order(5, "buy-98", "private-buyer", "buy", 98, 3, 3),
    ];
    // Only the final record is cached. Starting at sequence 1 must read the earlier records from the
    // journal, then return to mmap for the last record.
    fixture.initialize(&records, 6);
    let address = unused_address();
    let mut child = fixture.spawn(&address);

    wait_for(&mut child, &address, "/health", |status, _| status == 200);
    let (_, body) = wait_for(
        &mut child,
        &address,
        "/marketdata/orderbook/AAPL",
        |status, body| status == 200 && body.contains("\"bids\""),
    );
    let view: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        view["bids"][0],
        serde_json::json!({"price":99,"quantity":2})
    );
    assert_eq!(
        view["bids"][1],
        serde_json::json!({"price":98,"quantity":3})
    );
    assert_eq!(
        view["asks"][0],
        serde_json::json!({"price":101,"quantity":5})
    );
    let (_, one_level_body) = request(&address, "/marketdata/orderbook/AAPL?depth=0").unwrap();
    let one_level: serde_json::Value = serde_json::from_str(&one_level_body).unwrap();
    assert_eq!(one_level["bids"].as_array().unwrap().len(), 1);
    assert_eq!(one_level["asks"].as_array().unwrap().len(), 1);
    assert_eq!(
        request(&address, "/marketdata/orderbook/UNKNOWN")
            .unwrap()
            .0,
        404
    );
    assert_eq!(
        request(&address, "/exchange/orderbook/AAPL").unwrap().0,
        404
    );
    let state = fs::read_to_string(&fixture.state).unwrap();
    assert!(!state.contains("private-buyer"));
    assert!(!state.contains("private-seller"));
    assert!(!state.contains("user_id"));

    child.stop();
    // Simulate the exchange writer restarting: the journal identity and committed watermark stay,
    // while the disposable cache starts empty. The saved MDP checkpoint must still resume.
    fixture.reset_stream_after_writer_restart(6);
    let mut restarted = fixture.spawn(&address);
    wait_for(&mut restarted, &address, "/health", |status, _| {
        status == 200
    });
    let (_, restarted_body) = request(&address, "/marketdata/orderbook/AAPL").unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&restarted_body).unwrap(),
        view
    );

    let live = accepted_order(7, "sell-102", "another-private-user", "sell", 102, 4, 4);
    fixture.append_and_publish(&live, 8);
    let (_, live_body) = wait_for(
        &mut restarted,
        &address,
        "/marketdata/orderbook/AAPL",
        |status, body| status == 200 && body.contains("102"),
    );
    let live_view: serde_json::Value = serde_json::from_str(&live_body).unwrap();
    assert_eq!(
        live_view["asks"][1],
        serde_json::json!({"price":102,"quantity":4})
    );

    fixture.interrupt_stream_publication();
    wait_for(&mut restarted, &address, "/health", |status, _| {
        status == 503
    });
    assert_eq!(
        request(&address, "/marketdata/orderbook/AAPL").unwrap().0,
        503
    );
    restarted.stop();
}

#[test]
fn mdp_catches_up_persists_and_follows_one_minute_candles() {
    let fixture = Fixture::new();
    let records = vec![
        accepted_order(1, "sell-100", "private-seller", "sell", 100, 2, 1),
        accepted_buy_with_trade(3, "buy-100", 2, 2, "sell-100", 100, 61.9),
    ];
    fixture.initialize(&records, 6);
    let address = unused_address();
    let mut child = fixture.spawn(&address);

    let (_, body) = wait_for(
        &mut child,
        &address,
        "/marketdata/candles?symbol=AAPL&start_time=0&end_time=119",
        |status, body| status == 200 && body.contains("trade_count"),
    );
    let view: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        view,
        serde_json::json!({"symbol":"AAPL","candles":[{
            "symbol":"AAPL","start_time":60,"open":100,"high":100,"low":100,
            "close":100,"volume":2,"trade_count":1
        }]})
    );
    assert_eq!(
        request(
            &address,
            "/marketdata/candles?symbol=AAPL&start_time=120&end_time=119"
        )
        .unwrap()
        .0,
        400
    );
    assert_eq!(
        request(&address, "/marketdata/candles?start_time=0&end_time=119")
            .unwrap()
            .0,
        400
    );
    child.stop();

    fixture.reset_stream_after_writer_restart(6);
    let mut restarted = fixture.spawn(&address);
    let (_, restarted_body) = wait_for(
        &mut restarted,
        &address,
        "/marketdata/candles?symbol=AAPL&start_time=0&end_time=119",
        |status, _| status == 200,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&restarted_body).unwrap(),
        view
    );

    let resting = accepted_order(7, "sell-101", "private-seller", "sell", 101, 3, 3);
    fixture.append_and_publish(&resting, 8);
    let trade = accepted_buy_with_trade(9, "buy-101", 3, 4, "sell-101", 101, 120.0);
    fixture.append_and_publish(&trade, 12);
    let (_, live_body) = wait_for(
        &mut restarted,
        &address,
        "/marketdata/candles?symbol=AAPL&start_time=0&end_time=179",
        |status, body| status == 200 && body.matches("start_time").count() == 2,
    );
    let live: serde_json::Value = serde_json::from_str(&live_body).unwrap();
    assert_eq!(live["candles"][1]["start_time"], 120);
    assert_eq!(live["candles"][1]["volume"], 3);
    restarted.stop();
}

#[test]
fn mdp_refuses_corrupt_saved_state_without_database_configuration() {
    let fixture = Fixture::new();
    let record = accepted_order(1, "buy", "private-user", "buy", 100, 1, 1);
    fixture.initialize(&[record], 2);
    fs::write(&fixture.state, b"not valid state").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_stock"))
        .current_dir(&fixture.dir)
        .env_remove("DATABASE_URL")
        .args([
            "--market-data",
            fixture.journal.to_str().unwrap(),
            fixture.stream.to_str().unwrap(),
            fixture.state.to_str().unwrap(),
            &unused_address(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("market data:"));
}
