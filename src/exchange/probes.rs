//! Two small measurements that the HTTP baseline of milestone 24 needs, each a mode of the `stock`
//! binary so that it runs with the same build, in the same container, as the exchange:
//!
//! - `--sync-probe DIR`: how long the disk under DIR takes to make one appended record durable,
//!   with the same `write_all` + `sync_all` pair the journal uses. This is the latency floor of
//!   every acknowledged command.
//! - `--bcrypt-probe`: how long one bcrypt hash and one verify take at the cost the gateway uses,
//!   on one thread and on several at once.
//!
//! See `docs/milestones/24-gateway/performance/` for how they are run and what they found.

use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

const SYNC_USAGE: &str = "usage: stock --sync-probe EMPTY_OR_NEW_DIR [--count N] [--size BYTES]";
const BCRYPT_USAGE: &str =
    "usage: stock --bcrypt-probe [--count N_PER_THREAD] [--cost C] [--threads T]";

/// Parses `--flag N` pairs into whole numbers; anything else is a usage error.
fn numbers(
    args: &[String],
    usage: &str,
    mut set: impl FnMut(&str, u64) -> bool,
) -> Result<(), String> {
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| format!("{flag} needs a whole number\n{usage}"))?;
        if !set(flag, value) {
            return Err(usage.to_string());
        }
    }
    Ok(())
}

fn summary(name: &str, histogram: &Histogram<u64>, unit: &str) -> String {
    format!(
        "  {name:<10}: n {}  mean {:.1}  p50 {}  p90 {}  p99 {}  p99.9 {}  max {}  ({unit})",
        histogram.len(),
        histogram.mean(),
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.90),
        histogram.value_at_quantile(0.99),
        histogram.value_at_quantile(0.999),
        histogram.max(),
    )
}

pub fn sync_probe(args: &[String]) -> Result<(), String> {
    let dir: PathBuf = args.first().ok_or(SYNC_USAGE)?.into();
    let (mut count, mut size) = (10_000u64, 600u64);
    numbers(&args[1..], SYNC_USAGE, |flag, value| match flag {
        "--count" => {
            count = value.max(1);
            true
        }
        "--size" => {
            size = value.max(1);
            true
        }
        _ => false,
    })?;
    std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    let path = dir.join("sync-probe.dat");
    // `create_new`: never overwrite a file that might matter, and always start from an empty one,
    // so every run measures appends to a growing file, as the journal does.
    let mut file = OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let record = vec![b'x'; size as usize];
    let mut write = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut sync = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut total = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let started = Instant::now();
    for _ in 0..count {
        let before = Instant::now();
        file.write_all(&record).map_err(|error| error.to_string())?;
        let written = Instant::now();
        file.sync_all().map_err(|error| error.to_string())?;
        let synced = Instant::now();
        write
            .record(written.duration_since(before).as_micros() as u64)
            .map_err(|e| e.to_string())?;
        sync.record(synced.duration_since(written).as_micros() as u64)
            .map_err(|e| e.to_string())?;
        total
            .record(synced.duration_since(before).as_micros() as u64)
            .map_err(|e| e.to_string())?;
    }
    let seconds = started.elapsed().as_secs_f64();
    println!(
        "sync-probe: {count} appends of {size} bytes to {}, write_all + sync_all each",
        path.display()
    );
    println!("{}", summary("write_all", &write, "us"));
    println!("{}", summary("sync_all", &sync, "us"));
    println!("{}", summary("total", &total, "us"));
    println!(
        "  rate      : {:.0} durable appends/s over {seconds:.3} s",
        count as f64 / seconds
    );
    Ok(())
}

pub fn bcrypt_probe(args: &[String]) -> Result<(), String> {
    let (mut count, mut cost, mut threads) = (20u64, u64::from(bcrypt::DEFAULT_COST), 1u64);
    numbers(args, BCRYPT_USAGE, |flag, value| match flag {
        "--count" => {
            count = value.max(1);
            true
        }
        "--cost" => {
            cost = value;
            true
        }
        "--threads" => {
            threads = value.max(1);
            true
        }
        _ => false,
    })?;
    let cost = u32::try_from(cost).map_err(|_| "--cost is too large".to_string())?;
    let parallelism = thread::available_parallelism().map_or(0, |n| n.get());
    println!(
        "bcrypt-probe: cost {cost} (2^{cost} = {} key-schedule rounds), {threads} thread(s) x {count} hashes and {count} verifies; available_parallelism {parallelism}",
        1u64 << cost
    );
    let password = "bench-password-1";
    let stored = bcrypt::hash(password, cost).map_err(|error| error.to_string())?;

    // Each thread hashes, then verifies, `count` times. The barrier starts them together, so with
    // several threads every measured call competes for the cores, as simultaneous logins would.
    let barrier = Arc::new(Barrier::new(threads as usize));
    let started = Instant::now();
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let stored = stored.clone();
            thread::spawn(move || -> Result<(Vec<Duration>, Vec<Duration>), String> {
                barrier.wait();
                let mut hashes = Vec::new();
                let mut verifies = Vec::new();
                for _ in 0..count {
                    let before = Instant::now();
                    bcrypt::hash(password, cost).map_err(|error| error.to_string())?;
                    hashes.push(before.elapsed());
                }
                for _ in 0..count {
                    let before = Instant::now();
                    let ok =
                        bcrypt::verify(password, &stored).map_err(|error| error.to_string())?;
                    verifies.push(before.elapsed());
                    if !ok {
                        return Err("bcrypt verify rejected the right password".to_string());
                    }
                }
                Ok((hashes, verifies))
            })
        })
        .collect();
    let mut hash = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut verify = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    for worker in workers {
        let (hashes, verifies) = worker
            .join()
            .map_err(|_| "a bcrypt thread panicked".to_string())??;
        for sample in hashes {
            hash.record(sample.as_micros() as u64)
                .map_err(|e| e.to_string())?;
        }
        for sample in verifies {
            verify
                .record(sample.as_micros() as u64)
                .map_err(|e| e.to_string())?;
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    println!("{}", summary("hash", &hash, "us"));
    println!("{}", summary("verify", &verify, "us"));
    println!(
        "  rate      : {:.2} bcrypt calls/s in total over {seconds:.3} s",
        (2 * count * threads) as f64 / seconds
    );
    Ok(())
}
