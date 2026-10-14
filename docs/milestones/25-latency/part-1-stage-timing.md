# Part 1 — Where the time goes

**Status: complete on 2026-10-14.**

## What this part is

Before optimizing anything, measure where an order's time goes, stage by stage, in every
configuration: in-process and HTTP, alone and replicated, at 1,000 and 5,000 orders/s, and
in-process at the maximum rate. The stages and the reasoning for measuring them are in `01-theory.md`, sections 1–5.
The result decides whether Part 2 (a pipelined sync) and Part 3 (a binary format) are worth doing.

## What changed in the code

All the timing is compiled in only with `--features stage-timing` (decision 1). The normal build is
unchanged in behaviour, apart from the two small restructurings marked *(both builds)* below.

### `Cargo.toml`

A new feature, `stage-timing = []`, off by default.

### New: `src/exchange/stage_timing.rs` (feature only)

- **`enum Stage`.** The 16 stages, each with a comment saying where it starts and ends:
  - HTTP: `HttpBeforeQueue`, `HttpAsk`, `HttpTotal`;
  - per command: `Queue`, `WaitInGroup`, `Prepare`, `Encode`, `Commit`, `WaitForGroup`, `Worker`;
  - per group: `Write`, `Sync`, `Confirm`, `Publish`, `Release`;
  - `GroupSize`, a count rather than a time.
- **`struct Stages`.** One HdrHistogram per stage, with 3 significant digits:
  - `record(stage, Duration)` stores microseconds; `count(stage, n)` stores a plain number;
  - `merge_into_global()` adds this instance's samples into the process-wide report and empties it;
  - `report()` prints one line per non-empty stage: n, mean, p50, p90, p99, p99.9 and max.
- **A global `Stages`** behind a `Mutex` in a `LazyLock`, with `record(stage, since)` (used by the
  gateway), `report()` and `reset()`.
- **`RequestStart` and `http_layer`.** An Axum middleware for the order route. It stamps the request
  on the way in (the stamp goes into the request's extensions for the handler) and records
  `HttpTotal` on the way out.

### `src/types/types.rs`

- `ExchangeCommand::PlaceOrder` gains `queued_at: Instant`, present only in the feature build.
- A new constructor, `ExchangeCommand::place_order(order, respond_to)`, fills it when the feature is
  on.
- All 19 places that built a `PlaceOrder` now call the constructor *(both builds)*: the controller,
  both benchmarks, and the tests in `market_data.rs` and `runtime.rs`. The worker's match arm uses
  `..`, so it compiles with or without the field. The alternatives, and why they were rejected, are
  in `decisions.md`.

### `src/exchange/runtime.rs` (the worker)

- **A thread-local `Stages`** (module `timing`). The worker records without any lock, and merges once
  per group, at the end of `handle_group`.
- **`stage_input_event`** reads the clock at the start, after `prepare_input_event`, after
  `encode_record`, and after `commit`, and records `Prepare`, `Encode` and `Commit`.
- **`handle_group`:**
  - reads `dequeued` once, at the start; the group was taken off the queue just before, within
    microseconds;
  - for each command, before staging: `Queue` = `dequeued` − `queued_at` (orders only; saturating, so
    it can never be negative) and `WaitInGroup` = now − `dequeued`;
  - after staging, it keeps the end time; at the flush, `WaitForGroup` = flush start − that end, for
    every command, and `GroupSize`;
  - after each reply is sent, `Worker` = now − `dequeued`;
  - after all replies, `Release`, then the merge.
- **`flush`** times `Write`, `Sync`, `Confirm` (replicated only) and `Publish` (mmap publication plus
  callbacks), once per group.
- **(both builds)** The un-replicated flush used to call `store.append_record(..)`. It now calls
  `store.write_records(..)` and then `store.sync()`, which is exactly what `append_record` did, so
  that the two can be timed apart. The replicated flush already called them separately. The order of
  the replicated steps (write, then tell the replication link, then sync, then wait for the
  confirmation) is unchanged.

### `src/exchange/event_store.rs`

`append_record` is now `#[cfg(test)]`: only tests call it, and leaving it would add a dead-code
warning to the normal build. Its comment says why the runtime no longer uses it.

### `src/controllers/exchange_controller.rs` and `src/routes/exchange_routes.rs`

- In the feature build, `place_order` takes the middleware's `RequestStart` as an extractor.
- It records `HttpBeforeQueue` just before sending the command, and `HttpAsk` when the reply is back.
- The route for `POST /exchange/orders` gets the `http_layer` middleware. No other route is timed.

### `src/exchange/operator.rs`

In the feature build only: `GET /stages` (the report) and `POST /stages/reset`.

### `src/exchange/bench.rs` and `src/exchange/http_bench.rs`

- `--bench`, in the feature build, resets the report after setup and prints it at the end.
- `--http-bench` (any build) posts `/stages/reset` to the operator port just before its measured
  orders, and prints `GET /stages` at the end when it answers 200. Against a normal build both get
  404, and nothing happens.
- *(both builds)* Both now print the **mean** end-to-end latency after the max. Means add up across
  stages and medians do not (`01-theory.md` §2), so the stage means can be checked against it. It
  was added after the first stage run, which therefore has no end-to-end mean; see "How the
  measurements were run".

## Tests

| Test | Build | What it proves | Result |
|---|---|---|---|
| `stage_timing::tests::merging_moves_samples_into_the_report_and_empties_the_source` | feature | Merging adds every sample to the target and empties the source. The report shows the right count, includes `GroupSize`, and leaves empty stages out. It uses its own instances, because the global report is shared with every runtime test. | pass |
| `stage_timing::tests::a_sample_under_a_microsecond_is_still_counted` | feature | A stage faster than 1 µs records as 0 and is still counted. It was added while chasing a test failure that turned out to be a stale build (see the mutation check below). | pass |
| `runtime::tests::a_group_records_every_worker_stage` | feature | One durable group (an open, two deposits and two orders) through `handle_group` records all 12 worker stages: `Queue`, `WaitInGroup`, `Prepare`, `Encode`, `Commit`, `WaitForGroup`, `Write`, `Sync`, `Publish`, `Release`, `Worker`, `GroupSize`. It checks presence, not counts, because the global report is shared. **It proves this only when run alone**: in the full suite, other runtime tests also merge into the shared report and could fill a stage this group missed. That was found by the independent review, and the mutation check below runs it alone. | pass (alone and in the suite) |
| **Mutation check**, `performance/scripts/m25p1-mutate.sh`, output `performance/results/m25p1-mutate.txt` | feature | The test above, run alone, on the real tree (control) and on three copies, each missing one piece: `Publish` never recorded; `Queue` never recorded; the worker never merging into the report. | control: pass; each mutation: FAILED, as it must |
| The whole suite, normal build: `cargo test --locked` | normal | Nothing changed for the normal build, including the 19 rewritten construction sites and the split `write_records` + `sync`. | 213 passed, 1 ignored |
| The whole suite, feature build: `cargo test --locked --features stage-timing` | feature | The same, with every instrumented path compiled in. | 216 passed, 1 ignored |
| `cargo fmt -- --check` | — | — | clean |
| Release build warnings | both | No new warning. A first build had 13: `append_record` had become dead code. It is now test-only. | 12, as before |

All were run on the office machine on 2026-10-14, inside `stock-rust` (cargo 1.98.1). The commands
are `~/stock-scripts/check.sh`, plus `cargo test --locked --features stage-timing`.

**A trap in the mutation check, and how it was avoided.** A first mutation check was run by hand:
mutate a copy, run the test, then run the real tree. The real tree's run *failed*, the same way
the mutation did. The cause was the build, not the code:
- the mutated copy and the real tree were both mounted at `/src`, sharing one cargo target
  directory;
- `cp -r` gave the copy newer file times than the real tree, whose files `tar` had extracted with
  their original, older times;
- so cargo judged the mutated build up to date for the real tree, and ran it.

Touching the real tree's file made it rebuild, and the test passed. `m25p1-mutate.sh` now builds
mutations in their own target volume (`stock-cargo-target-mut`). Milestone 24's `m24p2-mutate.sh`
had the same hazard; its script is fixed the same way. Its 2026-10-12 results were not affected:
each mutated copy was newer and so was rebuilt, and every later real-tree test run passed, which a
reused mutated build could not have done.

## How the measurements were run

Binaries, both release builds of the same tree:
- `~/stock-scripts/stock-m25p1`, the normal build;
- `~/stock-scripts/stock-m25p1-timed`, with `--features stage-timing`.

Scripts (in `performance/scripts/`, copied to `~/stock-scripts/`; they reuse milestone 24's
`m24-lib.sh`):
- `m25p1-overhead.sh`: the normal and timed builds, interleaved, in-process, alone, at 5,000/s and at
  the maximum rate, three runs each. Output: `results/m25p1-overhead.txt`.
- `m25p1-stages.sh`: the timed build, three runs of each configuration:
  - in-process alone and replicated, at 1,000/s, 5,000/s and the maximum rate;
  - HTTP alone and replicated, at 1,000/s and 5,000/s.
  - Output: `results/m25p1-stages.txt`.
  - A first complete run, before the end-to-end mean was added to the output, is kept as
    `results/m25p1-stages-first.txt`. Its stage reports are complete and valid. Only the
    end-to-end mean is missing.

- `m25p1-overhead-reversed.sh`: the same pairs with the timed build first, added when the first
  overhead script showed the second run of every pair faster. Output:
  `results/m25p1-overhead-reversed.txt`.
- `m25p1-summarize.awk` and `m25p1-decompose.awk`: average each configuration's three runs, and
  check that the stage means add up. Outputs: `results/m25p1-stages-summary.txt` and
  `results/m25p1-stages-decomposition.txt`.

## Results

The details and the arithmetic are in the two performance files.

- **The instrumentation's cost** (`performance/01-instrumentation-overhead.md`).
  - Nothing measurable at 5,000 orders/s: p50 averaged over six runs is 6,986 µs timed against
    7,163 µs normal, within a run-to-run spread of about 2 ms.
  - About 3% of throughput at the maximum rate: 46,335 against 47,767 orders/s, averaged over both
    positions in the pair.
  - On the way, a **run-order effect of about 9.5%** at the maximum rate: the second run of a pair
    is faster, whichever build it is. Every later comparison must alternate which build runs first.
- **The stage means add up** (`performance/02-where-the-time-goes.md`). `Queue` + `Worker` accounts
  for the end-to-end mean. What is left is the generator's own lateness and the reply's wake-up:
  0.9–1.2 ms in-process, and 1.5–2.8 ms outside the server on HTTP. So 10–24% of what a benchmark
  reports is the apparatus and the client, not the exchange. Inside `Worker`, below the maximum
  rate, no stage covers 10–11% in-process alone, 12–21% on HTTP alone, and 25–30% replicated. The likely reason, an inference,
  is that group stages are averaged per group while `Worker` is averaged per order.
- **At 1,000 and 5,000 orders/s the disk dominates.**
  - `Queue` + `Sync` + `Confirm` are 42–69% of the latency.
  - The order's own CPU (prepare, encode, commit) is 33–91 µs, 0.2–1.3%.
  - `Encode` alone is 0.05–0.33%.
  - One run (HTTP replicated, 5,000/s, run 1) hit a disk stall, with syncs up to 73.5 ms, 1,084
    refused connections and 11 unanswered orders. It is kept in the averages and flagged. Without
    it, that configuration's p99 is 57.4 ms rather than 129.7 ms.
- **At the maximum rate one group's cycle is 22.17 ms** alone and 28.97 ms replicated:
  - staging, 13.0 ms;
  - sync, 6.0 ms (8.35 ms replicated, plus a 4.44 ms confirm);
  - publish, 2.4 ms;
  - write and release, 0.8 ms.

  The cycle gives 45,800 and 34,900 orders/s. These stage runs measured 42,458–47,420 and
  32,242–38,119.
- **What that means for the next parts. These are projections, not measurements.**
  - A pipelined sync could raise the maximum rate by about 37–66% alone and about 79% replicated.
  - It would gain at most about 8% of latency at moderate load, because each order still waits for
    its own sync.
  - A binary format could gain at most about 15% of maximum throughput, and nothing measurable in
    latency.

## Independent review

A separate review read the code and documents, re-ran both awk scripts (their outputs matched the
committed derived files byte for byte), and recomputed the figures.

**No code defect.** It confirmed:
- the normal build behaves as before: the replicated flush still does write, end, `written`, sync,
  confirm, in that order, and the un-replicated `write_records` + `sync` equals the old
  `append_record`;
- every stage is measured between the right points, and `dequeued` is taken at the right time;
- the feature build's `cfg` uses are all valid.

**Documentation findings, all fixed:**
1. **(high) An undisclosed disturbed run.** "HTTP replicated 5,000/s" run 1 had 1,084 refused
   connections, 11 unanswered orders and a p99 of 274 ms. That one run drove the reported 130 ms tail
   and the "fifteen times" ratio. Investigating it showed a disk stall: single syncs of up to 73.5 ms.
   It is now disclosed in the method, with the tail given with and without it (57.4 ms, 7.7 times the
   sync p99, without).
2. "No measurable cost at 1,000 and 5,000 orders/s": only 5,000/s was measured. Corrected, with the
   1,000/s case stated as an inference.
3. Wrong ranges, corrected:
   - the apparatus's share was 10–24%, not 15–25%;
   - the gap inside `Worker` on HTTP alone reaches 21%;
   - the `Encode` share's minimum was 0.05%;
   - the timed build's measured maximum was 48,140, not 52,879.
4. "`Sync` is the largest stage everywhere" did not hold for HTTP at 5,000/s, where `Queue` is larger.
   Qualified.
5. The 5,000/s pipelining projection used the maximum rate's 12.8 µs per command for staging. At
   5,000/s it is about 58 µs, so the projected gain is at most about 8%, not "a few percent". Redone.
6. Smaller corrections:
   - 19 construction sites, not 20;
   - 9 clock reads per group, 10 replicated;
   - 45,800 and 34,900 orders/s are derived, not measured;
   - the README's status;
   - the theory's medians example did not show medians disagreeing; it now does
     (median(A + B) = 11 ms against median(A) + median(B) = 2 ms);
   - a timing range quoted for both builds was the normal build's alone.
7. Plausible, both handled:
   - the replicated `Sync` span also covers `store.end()` and `replication.written`, now noted;
   - the feature test proves its stages only when run alone, now noted, and proven by the mutation
     check run alone.
