//! Opt-in process acceptance for Reporter v1. Run after applying migrations with:
//! REPORTER_TEST_DATABASE_URL=postgresql://... cargo test --test reporter -- --ignored --nocapture

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

fn accepted_record() -> Vec<u8> {
    let payload = serde_json::to_vec(&serde_json::json!([
        {"seq_num":1,"event":{"direction":"input","event":{"kind":"new_order_requested","data":{"order":{"order_id":"report-order","user_id":"report-user","symbol":"AAPL","side":"buy","price":100,"quantity":2,"leaves_qty":2,"timestamp":1.0,"seq_num":0}}}}},
        {"seq_num":2,"event":{"direction":"output","event":{"kind":"order_accepted","data":{"order_id":"report-order","seq_num":1}}}}
    ])).unwrap();
    let mut record = Vec::new();
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc(&payload).to_le_bytes());
    record.extend(payload);
    record
}

fn fixture(dir: &Path) -> (PathBuf, PathBuf) {
    let journal = dir.join("events.log");
    let stream = dir.join("events.mmap");
    let record = accepted_record();
    let mut journal_bytes = b"EXCHLOG1".to_vec();
    journal_bytes.extend_from_slice(&record);
    fs::write(&journal, &journal_bytes).unwrap();
    let metadata = fs::metadata(&journal).unwrap();
    let mut header = b"EXCHBUS1".to_vec();
    for value in [
        metadata.dev(),
        metadata.ino(),
        CAPACITY as u64,
        journal_bytes.len() as u64,
        2,
        8,
        record.len() as u64,
    ] {
        header.extend_from_slice(&value.to_le_bytes());
    }
    header.extend_from_slice(&crc(&header).to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    header.extend_from_slice(&1u64.to_ne_bytes());
    header.extend_from_slice(&record);
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

#[test]
#[ignore = "requires a migrated isolated REPORTER_TEST_DATABASE_URL"]
fn reporter_process_catches_up_and_persists_its_checkpoint() {
    let database_url =
        std::env::var("REPORTER_TEST_DATABASE_URL").expect("set REPORTER_TEST_DATABASE_URL");
    let truncate = Command::new("psql")
        .args([
            &database_url,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "TRUNCATE reported_trades, reported_orders, reporter_checkpoint",
        ])
        .status()
        .unwrap();
    assert!(truncate.success(), "apply migrations before this test");
    let dir = std::env::temp_dir().join(format!("stock-reporter-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    let (journal, stream) = fixture(&dir);
    let address = unused_address();
    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_stock"))
        .current_dir(&dir)
        .env("DATABASE_URL", &database_url)
        .args([
            "--reporter",
            journal.to_str().unwrap(),
            stream.to_str().unwrap(),
            &address,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while health(&address) != Some(200) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "reporter exited before ready"
        );
        assert!(Instant::now() < deadline, "reporter never became ready");
        thread::sleep(Duration::from_millis(20));
    }
    let output = Command::new("psql").args([&database_url, "-At", "-c", "SELECT order_id || ':' || status || ':' || next_sequence::text FROM reported_orders CROSS JOIN reporter_checkpoint"]).output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "report-order:new:3"
    );
    child.kill().unwrap();
    child.wait().unwrap();
    fs::remove_dir_all(dir).unwrap();
}
