//! Exercise the shipped executable without PostgreSQL. The fixture is encoded independently
//! using the documented journal/stream wire format, rather than the implementation's helpers.
use std::{fs, os::unix::fs::MetadataExt, path::PathBuf, process::Command};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("stock-probe-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn run(&self, args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_stock"))
            .current_dir(&self.0)
            .env_remove("DATABASE_URL")
            .args(args)
            .output()
            .unwrap()
    }
    fn history(&self) {
        let events = serde_json::json!([
            {"seq_num":1,"event":{"direction":"input","event":{"kind":"funds_deposit_requested","data":{"user_id":"buyer","amount":10}}}},
            {"seq_num":2,"event":{"direction":"output","event":{"kind":"funds_deposited","data":{"user_id":"buyer","amount":10}}}}
        ]);
        let payload = serde_json::to_vec(&events).unwrap();
        let mut log = b"EXCHLOG1".to_vec();
        log.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        log.extend_from_slice(&crc(&payload).to_le_bytes());
        log.extend(payload);
        fs::write(self.0.join("events.log"), &log).unwrap();
        let meta = fs::metadata(self.0.join("events.log")).unwrap();
        let mut bus = b"EXCHBUS1".to_vec();
        for value in [
            meta.dev(),
            meta.ino(),
            4096,
            log.len() as u64,
            2,
            8,
            log.len() as u64 - 8,
        ] {
            bus.extend_from_slice(&value.to_le_bytes());
        }
        bus.extend_from_slice(&crc(&bus).to_le_bytes());
        bus.extend_from_slice(&[0; 4]);
        bus.extend_from_slice(&1u64.to_ne_bytes());
        bus.extend_from_slice(&log[8..]);
        bus.resize(80 + 4096, 0);
        fs::write(self.0.join("events.mmap"), bus).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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

#[test]
fn probe_outputs_whole_batch_and_resumes_checkpoint_without_database() {
    let fixture = Fixture::new();
    fixture.history();
    let original = fs::read(fixture.0.join("events.log")).unwrap();
    let args = [
        "--event-probe",
        "events.log",
        "events.mmap",
        "cursor.json",
        "--once",
    ];
    let first = fixture.run(&args);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let batch: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(batch.as_array().unwrap().len(), 2);
    assert_eq!(batch[0]["seq_num"], 1);
    assert_eq!(batch[1]["seq_num"], 2);
    let second = fixture.run(&args);
    assert!(second.status.success());
    assert!(second.stdout.is_empty());
    assert_eq!(fs::read(fixture.0.join("events.log")).unwrap(), original);
}

#[test]
fn probe_refuses_bad_arguments_and_checkpoint_over_journal() {
    let fixture = Fixture::new();
    fixture.history();
    assert!(!fixture.run(&["--event-probe"]).status.success());
    assert!(
        !fixture
            .run(&["--event-probe", "events.log", "--once"])
            .status
            .success()
    );
    let before = fs::read(fixture.0.join("events.log")).unwrap();
    let output = fixture.run(&[
        "--event-probe",
        "events.log",
        "events.mmap",
        "events.log",
        "--once",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must not overwrite"));
    assert_eq!(fs::read(fixture.0.join("events.log")).unwrap(), before);
}
