//! Configuration the exchange refuses to start without.

use std::process::{Command, Output};

/// The primary checks login tokens, and so does a warm replica once promoted. Without
/// `JWT_SECRET`, or with an empty one, both refuse to start: the warm replica before it follows
/// anything, the primary before it opens its journal.
#[test]
fn the_exchange_and_the_warm_replica_refuse_to_start_without_a_jwt_secret() {
    let dir = std::env::temp_dir().join(format!("stock-startup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let journal = dir.join("events.log");
    let run = |secret: Option<&str>, args: &[&str]| -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_stock"));
        command
            .current_dir(&dir)
            .env_remove("DATABASE_URL")
            .env("EVENT_LOG_PATH", &journal)
            .args(args);
        match secret {
            Some(secret) => command.env("JWT_SECRET", secret),
            None => command.env_remove("JWT_SECRET"),
        };
        command.output().unwrap()
    };
    let primary: &[&str] = &[];
    let warm: &[&str] = &[
        "--warm-replica",
        journal.to_str().unwrap(),
        "events.mmap",
        "events.snapshot",
        "127.0.0.1:0",
    ];

    for args in [primary, warm] {
        for secret in [None, Some("")] {
            let output = run(secret, args);
            assert_eq!(output.status.code(), Some(1), "{args:?} with {secret:?}");
            assert!(String::from_utf8_lossy(&output.stderr).contains("JWT_SECRET"));
        }
    }
    assert!(!journal.exists());
    std::fs::remove_dir_all(dir).unwrap();
}
