//! Process-level acceptance for the local warm-replica executable. The fixture is encoded by hand
//! so this test exercises the public journal/mmap boundary rather than sharing runtime helpers.

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
    snapshot: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("stock-warm-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        Self {
            journal: dir.join("events.log"),
            stream: dir.join("events.mmap"),
            snapshot: dir.join("events.snapshot"),
            dir,
        }
    }

    fn initialize(&self) {
        let record = record(serde_json::json!([
            {
                "seq_num": 1,
                "event": {"direction":"input","event":{
                    "kind":"funds_deposit_requested",
                    "data":{"user_id":"buyer","amount":10}
                }}
            },
            {
                "seq_num": 2,
                "event": {"direction":"output","event":{
                    "kind":"funds_deposited",
                    "data":{"user_id":"buyer","amount":10}
                }}
            }
        ]));
        // The journal header is the magic and a 16-byte journal id; the stream names the same id.
        let mut journal = b"EXCHLOG2".to_vec();
        journal.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        journal.extend_from_slice(&record);
        fs::write(&self.journal, &journal).unwrap();
        create_stream(
            &self.journal,
            &self.stream,
            journal.len() as u64,
            2,
            24,
            &record,
        );
    }

    fn spawn(&self, address: &str) -> ChildGuard {
        let child = Command::new(env!("CARGO_BIN_EXE_stock"))
            .current_dir(&self.dir)
            .env_remove("DATABASE_URL")
            .env("JWT_SECRET", "warm-replica-test")
            .args([
                "--warm-replica",
                self.journal.to_str().unwrap(),
                self.stream.to_str().unwrap(),
                self.snapshot.to_str().unwrap(),
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
            panic!("warm replica exited with {status}: {stderr}");
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let status = child.wait().unwrap();
            assert!(status.signal().is_some() || !status.success());
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
    record.extend_from_slice(&payload);
    record
}

fn create_stream(
    journal: &Path,
    stream: &Path,
    end: u64,
    last_sequence: u64,
    cache_start: u64,
    cache: &[u8],
) {
    let mut header = b"EXCHBUS2".to_vec();
    header.extend_from_slice(&fs::read(journal).unwrap()[8..24]);
    for value in [
        CAPACITY as u64,
        end,
        last_sequence,
        cache_start,
        cache.len() as u64,
    ] {
        header.extend_from_slice(&value.to_le_bytes());
    }
    header.extend_from_slice(&crc(&header).to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    header.extend_from_slice(&1u64.to_ne_bytes());
    header.extend_from_slice(cache);
    header.resize(80 + CAPACITY, 0);
    fs::write(stream, header).unwrap();
}

fn unused_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn request(address: &str, method: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(address).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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
        if let Some(response) = request(address, "GET", path)
            && expected(response.0, &response.1)
        {
            return response;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {path}");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn warm_replica_process_refuses_promotion_while_another_process_owns_the_journal() {
    let fixture = Fixture::new();
    fixture.initialize();
    // This lock belongs to the test process, while the warm executable is a distinct process.
    // It proves the actual `--warm-replica` control endpoint observes the OS writer fence.
    let primary_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fixture.journal)
        .unwrap();
    primary_lock.lock().unwrap();
    let journal_before = fs::read(&fixture.journal).unwrap();
    let stream_before = fs::read(&fixture.stream).unwrap();
    let address = unused_address();
    let mut child = fixture.spawn(&address);

    let (_, status) = wait_for(&mut child, &address, "/status", |status, body| {
        status == 200 && body.contains("\"role\":\"warm-replica\"")
    });
    assert!(status.contains("\"next_event_sequence\":3"));
    let (status, body) = request(&address, "POST", "/promote").unwrap();
    assert_eq!(status, 409);
    assert!(body.contains("current primary still owns"));
    wait_for(&mut child, &address, "/health", |status, _| status == 200);
    assert_eq!(fs::read(&fixture.journal).unwrap(), journal_before);
    assert_eq!(fs::read(&fixture.stream).unwrap(), stream_before);
    primary_lock.unlock().unwrap();
}

/// A writer killed while publishing leaves the ready marker cleared. The warm replica process
/// keeps running, says so on `/status`, and can still be promoted once the writer is gone.
#[test]
fn warm_replica_process_waits_out_an_interrupted_publication_and_stays_promotable() {
    let fixture = Fixture::new();
    fixture.initialize();
    let address = unused_address();
    let mut child = fixture.spawn(&address);
    wait_for(&mut child, &address, "/status", |status, body| {
        status == 200 && body.contains("\"stream_interrupted\":false")
    });

    OpenOptions::new()
        .write(true)
        .open(&fixture.stream)
        .unwrap()
        .write_all_at(&0u64.to_ne_bytes(), 72)
        .unwrap();
    let (_, status) = wait_for(&mut child, &address, "/status", |status, body| {
        status == 200 && body.contains("\"stream_interrupted\":true")
    });
    assert!(status.contains("\"next_event_sequence\":3"));
    wait_for(&mut child, &address, "/health", |status, _| status == 200);

    let (status, body) = request(&address, "POST", "/promote").unwrap();
    assert_eq!(status, 202, "{body}");
}
