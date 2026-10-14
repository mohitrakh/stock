//! Stage-by-stage timing of orders, compiled only with `--features stage-timing`.
//!
//! Every order's round trip is cut into stages, from the HTTP handler to the reply. Each stage
//! has its own histogram, so a report shows where the time goes, and the sum of the stage medians
//! can be set against the measured end-to-end median. See
//! `docs/milestones/25-latency/part-1-stage-timing.md`.
//!
//! The exchange worker records into its own `Stages` (no lock per order) and merges them into the
//! global ones once per group. The gateway records straight into the global ones, under a mutex.
//! Without the feature none of this exists, so the normal build pays nothing.

use std::{
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

/// The stages, in the order an order passes through them. Values are microseconds, except
/// `GroupSize`, which counts commands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// HTTP request reached the order handler's middleware → the command was built (JSON body,
    /// JWT, validation, `Order::new`).
    HttpBeforeQueue,
    /// The command was built → its reply was back in the handler: everything on the exchange
    /// side, as the handler sees it.
    HttpAsk,
    /// The whole order request inside the server, middleware in to middleware out. It excludes
    /// reading the request off the socket before routing and writing the response after.
    HttpTotal,
    /// The command was built → the worker took it off the queue: waiting in the queue, including
    /// any wait for queue space and for the group in progress to finish.
    Queue,
    /// The worker took it → its own staging began: waiting behind the commands before it in
    /// the same group.
    WaitInGroup,
    /// `prepare_input_event`: risk, collateral, the match plan, settlement checks.
    Prepare,
    /// Building the event envelopes and encoding the record (JSON).
    Encode,
    /// Applying the prepared change to the in-memory core.
    Commit,
    /// Its own staging ended → its group's flush began: waiting for the rest of the group.
    WaitForGroup,
    /// The group's `write_all` (per group).
    Write,
    /// The group's `sync_all` (per group).
    Sync,
    /// Waiting for the replica's confirmation after the sync (per group; replicated only).
    Confirm,
    /// Publishing the group's batches to the mmap stream and running callbacks (per group).
    Publish,
    /// Releasing the group's replies (per group).
    Release,
    /// The worker took it → its reply was sent: everything the worker did for it.
    Worker,
    /// Commands in the group (a count, not a time).
    GroupSize,
}

const ALL: [Stage; 16] = [
    Stage::HttpBeforeQueue,
    Stage::HttpAsk,
    Stage::HttpTotal,
    Stage::Queue,
    Stage::WaitInGroup,
    Stage::Prepare,
    Stage::Encode,
    Stage::Commit,
    Stage::WaitForGroup,
    Stage::Write,
    Stage::Sync,
    Stage::Confirm,
    Stage::Publish,
    Stage::Release,
    Stage::Worker,
    Stage::GroupSize,
];

/// One histogram per stage.
pub struct Stages(Vec<Histogram<u64>>);

impl Stages {
    pub fn new() -> Self {
        Stages(
            ALL.iter()
                .map(|_| Histogram::new(3).expect("3 significant digits is valid"))
                .collect(),
        )
    }

    pub fn record(&mut self, stage: Stage, elapsed: Duration) {
        self.count(stage, elapsed.as_micros() as u64);
    }

    pub fn count(&mut self, stage: Stage, value: u64) {
        // `record` only fails beyond the histogram's range, which auto-resizes.
        let _ = self.0[stage as usize].record(value);
    }

    /// Adds these samples into the global stages and empties these.
    pub fn merge_into_global(&mut self) {
        self.merge_into(&mut GLOBAL.lock().expect("stage timing lock"));
    }

    fn merge_into(&mut self, into: &mut Stages) {
        for (into, from) in into.0.iter_mut().zip(self.0.iter_mut()) {
            let _ = into.add(&*from);
            from.reset();
        }
    }

    fn report(&self) -> String {
        let mut out = String::from(
            "stage            n          mean      p50      p90      p99    p99.9      max  (us; group_size in commands)\n",
        );
        for (stage, histogram) in ALL.iter().zip(&self.0) {
            if histogram.is_empty() {
                continue;
            }
            out.push_str(&format!(
                "{:<14} {:>8} {:>11.1} {:>8} {:>8} {:>8} {:>8} {:>8}\n",
                format!("{stage:?}"),
                histogram.len(),
                histogram.mean(),
                histogram.value_at_quantile(0.50),
                histogram.value_at_quantile(0.90),
                histogram.value_at_quantile(0.99),
                histogram.value_at_quantile(0.999),
                histogram.max(),
            ));
        }
        out
    }
}

static GLOBAL: LazyLock<Mutex<Stages>> = LazyLock::new(|| Mutex::new(Stages::new()));

/// Records one sample into the global stages (the gateway's path).
pub fn record(stage: Stage, since: Instant) {
    GLOBAL
        .lock()
        .expect("stage timing lock")
        .record(stage, since.elapsed());
}

/// Everything recorded since the last reset.
pub fn report() -> String {
    GLOBAL.lock().expect("stage timing lock").report()
}

pub fn reset() {
    *GLOBAL.lock().expect("stage timing lock") = Stages::new();
}

/// When the order middleware first saw the request; the handler reads it from the extensions.
#[derive(Clone, Copy)]
pub struct RequestStart(pub Instant);

/// Axum middleware for the order route: stamps the request on the way in, records `HttpTotal`
/// on the way out.
pub async fn http_layer(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let start = Instant::now();
    request.extensions_mut().insert(RequestStart(start));
    let response = next.run(request).await;
    record(Stage::HttpTotal, start);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    // The global stages are shared with every runtime test of a feature build, so this works on
    // its own instances.
    #[test]
    fn merging_moves_samples_into_the_report_and_empties_the_source() {
        let (mut local, mut total) = (Stages::new(), Stages::new());
        local.record(Stage::Sync, Duration::from_micros(800));
        local.record(Stage::Sync, Duration::from_micros(1_200));
        local.count(Stage::GroupSize, 12);
        local.merge_into(&mut total);
        assert!(local.0.iter().all(Histogram::is_empty));
        let report = total.report();
        let sync = report
            .lines()
            .find(|line| line.starts_with("Sync "))
            .expect("a Sync line");
        assert!(
            sync.split_whitespace().nth(1) == Some("2"),
            "two samples: {sync}"
        );
        assert!(report.lines().any(|line| line.starts_with("GroupSize")));
        assert!(!report.contains("Prepare"), "empty stages are left out");
    }

    /// A stage faster than a microsecond records as 0. It must still be counted.
    #[test]
    fn a_sample_under_a_microsecond_is_still_counted() {
        let mut stages = Stages::new();
        stages.record(Stage::Publish, Duration::from_nanos(300));
        let report = stages.report();
        let publish = report
            .lines()
            .find(|line| line.starts_with("Publish"))
            .unwrap_or_else(|| panic!("Publish missing:\n{report}"));
        assert_eq!(publish.split_whitespace().nth(1), Some("1"), "{publish}");
    }
}
