# Testing Notes

Local notes, never committed (`.gitignore` lists this file). They record how each part was tested
from milestone 21 Part 3 to milestone 23 Part 1, the flow that was followed every time, and what
each check showed.

Benchmarks are left out on purpose. They have their own documents with raw output:
`docs/performance/05`, `06` and `07`, and `docs/performance/results/`. Milestone 22 Part 4 was a
pure benchmark, so it appears here only as a pointer.

---

## 1. Where the tests run

The crate uses Unix-only APIs (advisory file locks, device/inode identity, mmap), so it does not
build on Windows.

**Why the office Ubuntu box.** Until 2026-09-30 tests ran in Docker Desktop on the Windows PC. The
aimess containers loaded that VM so heavily (RabbitMQ about 235% CPU, MongoDB 137%) that a test
container died mid-build with `error waiting for container: unexpected EOF`. Since then everything
runs on the office Ubuntu machine.

| Item | Value |
|---|---|
| Machine | `ubuntu@10.0.127.253`: i3-7100 (2 cores, 4 threads), SATA SSD, 16 GB, Ubuntu 22.04, SSH key login |
| Build image | `stock-rust`: `rust:latest` plus `psql` and `curl`, built from `~/stock-scripts/Dockerfile` |
| Cargo caches | volumes `stock-cargo-registry` and `stock-cargo-target`, with `CARGO_TARGET_DIR=/target` |
| Benchmark data | volume `stock-bench-data`, mounted at `/data` |
| PostgreSQL | container `stock-test-pg` (`postgres:16`, password `bench`) on network `stock-test-net` |
| Source copy | `~/stock`, a tar copy of the repo, refreshed before every run |
| Scripts | `~/stock-scripts` (list below) |

**Copy the repo to the box.** It never sends `.env`, which holds secrets:

```bash
tar czf - --exclude=./target --exclude=./.git --exclude=./.env --exclude='*.log' --exclude='*.mmap' --exclude='*.snapshot' . | ssh ubuntu@10.0.127.253 'rm -rf ~/stock.new && mkdir ~/stock.new && tar xzf - -C ~/stock.new && rm -rf ~/stock && mv ~/stock.new ~/stock'
```

**The standard check:** formatting, every unit and process test, then the PostgreSQL acceptance
tests.

```bash
ssh ubuntu@10.0.127.253 'docker run --rm --network stock-test-net -v ~/stock:/src -v ~/stock-scripts:/scripts -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust bash -c "bash /scripts/check.sh; bash /scripts/reptest.sh"'
```

**Scripts in `~/stock-scripts`:**

| Script | What it does |
|---|---|
| `check.sh` | `cargo fmt -- --check`, then `cargo test --locked`; prints the result lines and any failure |
| `reptest.sh` | creates `reporter_test` in `stock-test-pg` if needed, then runs the ignored reporter acceptance tests with `REPORTER_TEST_DATABASE_URL` |
| `live-session.sh` | milestone 22 Part 1 live run: opening and closing the market |
| `live-expiry.sh` | milestone 22 Part 2 live run: expiry at the close, with market data and the reporter |
| `live-days.sh` | milestone 22 Part 3 live run: two trading days, with market data, the reporter and the warm replica |
| `live_sweep.py` | milestone 23 Part 1 live run: one order sweeping 55,000 resting orders |
| `live_overflow.py` | milestone 23 Part 1 live run: a fill that overflows a balance |
| `m23-before.sh` | milestone 23 Part 2 "before": market-data, warm-replica and promotion startups on milestone 22's five-day journal (hard-linked, so nothing is copied) |
| `m23-after.sh` | the same startups on a new five-day journal built by the new binary |
| `m23-copy.sh` | copies a journal with its stream, snapshot and market-data state to another directory, as onto another machine, and starts from the copies |
| `m23-rollback.sh [in-place]` | puts an older copy of a two-day journal back in place, then tries to promote a warm replica onto it and to restart the primary on it. By default the copy is renamed into place; `in-place` copies it over the followed file |
| `stock-m21`, `stock-m22` | prebuilt binaries of earlier milestones, for before/after runs (`stock-m21` is commit `a4860c7`; `stock-m22` is the milestone 22 tree) |
| `rep-*.sh`, `m22-*.sh` | benchmark scripts (see the performance docs) |

Every live script uses a disposable database and journal and generates test-only credentials,
such as `JWT_SECRET=live-test-<nanoseconds>` and random passwords.

---

## 2. The flow, every part

1. **Read before coding.** Read every code path the change touches, and the tests that exercise
   it.
2. **Code and unit tests together.** A new rule gets a test that fails without it, ideally using a
   small stand-in for a big constant (a cap of 2 instead of 200,000).
3. **Copy to Ubuntu and run `check.sh`.** Formatting differences are applied by hand on Windows,
   because `rustfmt` rewrites the CRLF files it touches to LF.
4. **Run `reptest.sh`** whenever the decoder, the reporter or the migrations change, and at the end
   of every part.
5. **Heavy release-only tests** when a part claims a size or capacity limit. They are `#[ignore]`d
   by default and run with `cargo test --release --bin stock -- --ignored NAME --nocapture`.
6. **Release warning count.** `cargo build --release` must still report the old dead-code
   warnings (12 since milestone 22) and nothing new.
7. **A live run of the real binaries.** A script drives the real exchange (and market data, the
   reporter and the warm replica when they matter) over HTTP, step by numbered step. Each step
   prints the HTTP status and body. Most runs include a `SIGKILL` and a restart, to prove the
   journal brings everything back.
8. **Before and after.** When a part claims to fix a bug, the same live script runs on the
   previous milestone's binary too, to show the bug was real.
9. **An independent review.** A read-only subagent reviews the diff, with concrete failure
   scenarios. Every finding is fixed, or written down as a known limit; then everything runs again.
   Since milestone 23 the reviewer also re-checks the fixes.
10. **Docs.** The task write-up, `PROJECT_DIRECTION.md`, `SYSTEM_DOCUMENTATION.md` and the TODO
    record what was verified.

---

## 3. Test layers, and what each one catches

| Layer | Where | What it catches |
|---|---|---|
| Unit tests | `#[cfg(test)]` modules in `src/` (172 tests now) | logic: matching, settlement, ledgers, replay, snapshots, the decoder, projections |
| Process tests | `tests/*.rs`: they start the real binary with `CARGO_BIN_EXE_stock` | startup and wiring: the probe, the market-data process, the warm replica's promotion fence, refusals to start |
| PostgreSQL acceptance | `tests/reporter.rs`, ignored unless `REPORTER_TEST_DATABASE_URL` is set | the reporter against a real database: rollbacks, restarts without duplicates, migrations |
| Heavy release tests | ignored tests such as `a_full_book_of_the_longest_ids_closes_in_one_record` | claims about size limits at full scale |
| Live runs | the scripts above | the whole system as a client sees it, across processes and restarts |
| Reviews | a read-only subagent | gaps nobody thought to test |

Both review rounds of milestone 23 Part 1 found real bugs that every test had missed. That is why
the review step stays.

---

## 4. Part by part

### Milestone 21 Part 3: reporter bug fixes (2026-09-30)

**What was tested:** three reporter bugs, each with a new acceptance-test case:
- a client retrying an order id stopped the reporter;
- a refused cancellation overwrote the owner's order row;
- execution ids repeating across symbols stopped the reporter.

**Flow and outcome:**
- **First run, on the pushed commit `014683d`.** Formatting failed (`src/exchange/market_data.rs`),
  and the PostgreSQL test would fail: it applied only the old migration. Both were fixed.
- **`tests/reporter.rs` rewritten** to apply both migrations and cover these cases:
  - a retried id;
  - a reused rejected id;
  - a cancellation by another user;
  - two symbols sharing execution ids;
  - a failed group right after a full 1,000-batch group.
- **The review found a serious new bug.** A cancel naming a multi-kilobyte order id would stop the
  reporter on every restart, because PostgreSQL cannot index a value that large. Fix: the gateway
  accepts only ids and symbols of 1–64 bytes with no control characters, and new tests cover it.
- **Other review fixes:**
  - the migration runs in one transaction, with a `report_version` guard;
  - the reporter's `/health` goes to 503 however its thread ends;
  - two market-data tests were tightened, because they proved less than their names said.
- **Final:** formatting clean, 142 unit tests, both PostgreSQL tests pass.
- **Check on real journals:** the fixed reporter caught up over a 10-symbol, 200,000-order journal.
  The old reporter stopped that journal at event 255. Speed numbers: `docs/performance/06`.

### Milestone 22 Part 1: opening and closing the market (2026-10-02)

**First test run:** 18 tests failed because their orders now needed an open market. Five more
still passed, but only because their orders were now refused, so they no longer tested anything.
All 23 were fixed, and new runtime tests cover refusals while closed and journaled session
refusals that replay.

**Final:** formatting clean, 147 unit tests, both PostgreSQL tests pass. A sanity run of the
benchmark gave 45,960 orders/s with nothing rejected, as before.

**Live run, `live-session.sh`.** The real exchange on its customer port (4000) and operator port
(4004). The first try failed because `stock-test-pg` was down after a reboot.

| Step | Outcome |
|---|---|
| 1 session at start | `200 {"trading_day":null,"open":false}` |
| 2–3 cash and share deposits while closed | 200 |
| 4 order while closed | `409 MarketClosed` |
| 5 open 2026-10-02 | 200, open |
| 6 open again | `409 AlreadyOpen` |
| 7–8 sell 5, then buy 5 at 100 | 201; the buy filled |
| 9 close | 200, closed |
| 10 order after the close | `409 MarketClosed` |
| 11 reopen the same day | `409 NotAfterLastTradingDay(2026-10-02)` |
| 12 operator path on the public port | 404 |
| 13–14 after `SIGKILL` and restart | session still closed; the buyer's fill survived |
| 15–16 open 2026-10-05, order | 200, then 201 |

The journal held every step in order, the refusals included.

### Milestone 22 Part 2: expiry at the close (2026-10-05)

**First run:** 152 unit tests and both PostgreSQL tests passed the first time. Only formatting
needed fixing.

**Heavy test, before the cap existed:** a close of 400,000 resting orders was refused in 0.78 s
and changed nothing. (`cargo test --lib` failed with "no library targets"; this crate needs
`--bin stock`.)

**Live run, `live-expiry.sh`.** The exchange, market data and the reporter:

| Step | Outcome |
|---|---|
| 1–5 | open; a sell of 10, a buy of 4 that fills, a resting buy of 5 at 99, a resting sell of 3 at 101 |
| 6 market-data book before the close | bids 99×5; asks 100×6 and 101×3 |
| 7 buyer's balance | balance 99,600, locked 495 |
| 8 report before | `sell-1:partially_filled`, `buy-2:new`, `ask-2:new`, `buy-1:filled` |
| 9 close | 200 |
| 10–11 the resting orders | `expired`, with their filled and remaining quantities kept |
| 12–14 balances and positions | every lock released (locked 0) |
| 15 risk usage | 4, the quantity that traded |
| 16 cancel an expired order | `400 InvalidTransition("order buy-2 is already Expired")` |
| 17 market-data book | 404: the book is empty |
| 18 report after | all three `expired`, with expiry sequences 5, 6 and 7, oldest first |
| 19–20 after `SIGKILL` | still closed, still expired |
| 21–24 next day | risk usage 0; a new order rests; market data shows it |

The close was one journal record: `MarketClosed`, then three `OrderExpired`.

**Review:** no correctness bug in expiry. One medium finding needed your decision:
- **The problem:** a close refused for size could never be fixed. Only owners can cancel, so the
  market would stay open for good.
- **The cause:** one user could flood about 253,000 small orders with worst-case ids.
- **Your choice:** cap the books at 200,000 resting orders (`BookFull`, 409).

Lower findings, all fixed:
- an expiry tie-breaker;
- a size test that could never fail, replaced by one through the real close;
- a stale comment;
- the reporter's "nothing still rests" check, moved to Part 3.

**After the cap:**
- 153 unit tests and both PostgreSQL tests pass.
- Heavy test `full_book`, 200,000 worst-case orders: the next resting order is refused, and the
  close fits one record of 50,089,106 bytes (53,000,239 at the widest sequences, against
  67,108,864), prepared in 0.47 s.
- Warnings still 12, and the live run passed again.

### Milestone 22 Part 3: the next open clears the day (2026-10-05)

**First run:** 157 unit tests and both PostgreSQL tests passed. Only formatting needed fixing.

**Live run, `live-days.sh`.** The exchange, market data, the reporter and the warm replica (with
`EVENT_SNAPSHOT_INTERVAL=1000`):

| Step | Outcome |
|---|---|
| 2–3 day 1: `s-1` sells 5, `b-1` buys 3 | 201, a trade |
| 4 `b-1` reused the same day | `409 AlreadyExists("b-1")` |
| 5–7 close; `b-1` and its fill before the next open | still readable |
| 8–10 open day 2; `b-1`; fills | `404`; `[]` |
| 11 balance | carried over (99,700) |
| 12–13 `b-1` and `s-1` reused on day 2 | accepted, and they trade |
| 14 day-2 execution id | `exec_2`: execution ids continue across days |
| 16 market-data book on day 2 | only the new resting order |
| 17–19 report | one row per day per order; trades with their day; the checkpoint on day 2 |
| 20 warm replica | wrote a snapshot right after each open; the last 653 bytes |
| 22–24 after `SIGKILL` | day 2 still open; orders as they were |

**Review:** no high-severity bug. Findings, all fixed:
- **Medium:** rejected orders and cancellations did not record their trading day, so once ids
  repeat they could not be matched to their order.
- **Low:**
  - the open's "keep what rests" step protected nothing, so it was dropped;
  - the client-order-id comment and the API docs now say ids are per day;
  - a test now takes its snapshot before an open;
  - a new acceptance test shows a close that leaves an order resting stops the reporter;
  - stale docs were updated;
  - two operational notes were added: remove an old snapshot along with an old journal, and every
    symbol keeps an empty book.

**Final:**
- 157 unit tests and all 3 PostgreSQL tests pass.
- `full_book` prepared in 0.39 s; warnings still 12.
- The live run passed again.

### Milestone 22 Part 4: benchmark

Benchmark only. See `docs/performance/07-trading-days-bound-the-state.md` and
`docs/performance/results/results-m22.txt`. Before measuring, a two-day run of 2,000 orders
checked that `--days` opened, traded and closed each day.

### Milestone 23 Part 1: no command can stop the exchange (2026-10-05)

**First run:** 161 unit tests passed; only formatting failed.

**The worst-case size test** printed one trade at 1,376 bytes, so 10,000 trades take 13,760,982
bytes against 67,108,864. That number went into the code comment.

**Live run 1, `live_sweep.py`.** It places 55,000 resting sells whose ids and symbol are the
gateway's longest (64 `"` or `\` characters, each escaped to two bytes in JSON). Then one buy sweeps
them all. Same script, both binaries:

| | Milestone 22 binary | New binary |
|---|---|---|
| The sweep | `503 ... record exceeds size limit` after 0.91 s; worker halted | `409 TooManyFills` in 0.02 s; a 768-byte rejection journaled |
| Health | 503 | 200 |
| After it | restart in 2.0 s, recovered through sequence 110,006 with the book intact; the same sweep halted it again | an order taking exactly 10,000 filled in 0.48 s (a 12.4 MB record); a small order after it filled |

The first version of the script checked `/health` straight after the halt and still saw 200. The
health flag flips only when the worker thread exits, so the script now waits up to 2 seconds for
it to change.

**Review, first round.** It confirmed the fill cap and the `JWT_SECRET` check, and found:
- **High:**
  - **The problem:** a deposit of `u64::MAX` followed by a one-share trade overflowed the seller's
    balance and halted the worker.
  - **The fix:** a deposit is refused if total cash, or a symbol's total shares, would pass
    `u64::MAX`.
- **Medium:** the new write-up was git-ignored by `*.md`, so it needed a `!` exception.
- **Low:**
  - a size test's final assert could never fail, because `encode_record` refuses first; it now
    measures the payload directly;
  - a test name said "journaled" though no journal was written; it was renamed;
  - a refused sweep built 20,000 executions before refusing; planning now finds the trades by
    reference first;
  - three wording errors in the write-up.

**Live run 2, `live_overflow.py`.** A deposits `u64::MAX` and sells one share at 1; B deposits 1
and buys it:

| | Milestone 22 binary | New binary |
|---|---|---|
| B deposits 1 | accepted | `400 WalletRejected("Overflow")` |
| B buys the share | `503 exchange internal fault: wallet balance invalid`; worker halted | `400 WalletRejected("InsufficientFunds")`; health 200 |
| A share deposit on top of another user's `u64::MAX` | not run | `400 PositionRejected("Overflow")` |

**Review, second round (re-checking the fixes).** It confirmed three things:
- with the totals capped, no settlement sum can overflow;
- the two-step planner gives the same results as before for every order within the cap;
- no test passes vacuously.

It found two problems that the deposit cap itself had introduced:
- **Each share deposit scanned every holding of every symbol.** Clients can create unlimited
  symbols, so share deposits kept getting slower. The totals are now running sums: deposits add to
  them, fills leave them alone, and snapshots rebuild them. A test checks that the totals survive
  a trade and a snapshot round trip.
- **One client can use up the total** and block every later deposit, because nothing withdraws.
  This is a known limit until deposits become operator-only.

**Tests that had to change:**
- Three settlement-overflow tests, and the runtime test of a fault in the middle of a group, used
  to build their overflowing ledgers through deposits, which are now refused. They force the
  ledger state directly instead (`commit_settlement`, `force_balance_for_test`). Each still reaches
  the internal fault.
- In the group-fault test, the forced balance saturates total cash, so its other commands became
  share deposits.

**Final:**
- formatting clean; 164 unit tests and the process tests pass, including the new
  `tests/startup.rs` (the primary and the warm replica refuse to start without `JWT_SECRET`, or
  with an empty one);
- all 3 PostgreSQL tests pass; warnings still 12;
- the overflow live run passed again on the final code.

### Milestone 23 Part 2: a journal that names itself (2026-10-06)

**Before writing any code:** I measured the current cost on milestone 22's five-day journal
(788 MB, 1,000,000 orders) with `m23-before.sh`. The journal was hard-linked, not copied, so the
old identity check (device and inode) still passed. The originals were untouched.

**First test run:** 167 of 168 passed. The failure was my own new test: a reader starting from the
beginning now checks the first record when it opens, so a damaged first record is refused at open
rather than at the first read. Both are refusals, so the test's expectation was wrong, not the
code. Then three formatting changes.

**The old 5-day journal can't be reused:** its `EXCHLOG1` header is refused, which is the point. So
`m23-after.sh` regenerates the same deterministic workload with the new binary (16 bytes longer:
the header grew from 8 to 24 bytes), opens day 6 on a real primary, and lets a real warm replica
write its snapshot right after that open, as the "before" conditions had it.

| Startup | Before | After |
|---|---|---|
| Market data, restart from its saved checkpoint | 9,654 ms | **9 ms** |
| Warm replica, start from the 667 KB snapshot | 9,620 ms | **68 ms** |
| Market data, built from sequence 1 | 17,393 ms | 16,723 ms (must read everything) |
| Promotion | 18,350 ms | 18,885 ms (Part 3's job) |

Both runs reached the same history: `next_event_sequence` 3,662,308.

**Live run, `m23-copy.sh`:** the files copied to another directory, as onto another machine.

| | Milestone 22 binary | New binary |
|---|---|---|
| Warm replica on the copies | refused: "snapshot belongs to a different journal", then "stream and journal identities differ"; did not start | ready after 67 ms |
| Market data from its copied state | not run | ready after 9 ms |

**After the first round of tests:**
- formatting clean; 168 unit tests and the process tests pass;
- all 3 PostgreSQL tests pass with the fifth migration;
- the acceptance test checks that a milestone 22 reporter, and a checkpoint inside the 24-byte
  header, cannot save into the migrated table.

**Review, first round.** No high-severity bug. Two medium findings:
- **An older copy of the journal would have started trading.** The id says which journal a file
  is, not how far it goes, so a shorter backup of the same journal put back in place would have
  been accepted. The primary would then have published a smaller watermark, lost the acknowledged
  commands after the backup, and reused offsets readers had already consumed. Two checks now
  refuse it:
  - the primary refuses to start when its stream file already published more than the journal
    holds (a record is published only after its sync, so this can only mean lost history);
  - promotion refuses a journal shorter than what the warm replica already applied
    (`ShorterThanApplied`), before reading or repairing anything.

  Still not caught: an old journal restored together with its own old stream file. Recorded as a
  known limitation; to go back, start a new journal.
- **A reporter acceptance test passed vacuously.** The "close with an order still resting in the
  report" test rewrote its journal, and every rewrite got a new random id. The restarted reporter
  stopped at the id check, never reaching the close, and the test only checked that it stopped.
  Now the rewrite keeps the id, and all three tests that expect the reporter to stop check its
  error output for the reason ("orders of the closed day are still resting in the report",
  "injected checkpoint failure", "injected order failure").

Smaller findings, all fixed: an old snapshot is now refused for its version instead of for a field
that no longer parses; new tests cover a mid-journal checkpoint with the wrong sequence, a refused
open that leaves a torn file untouched, and a real version 2 market-data state file; the docs no
longer claim the primary's normal restart re-reads the old journal.

**Live run, `m23-rollback.sh`:** a real primary opened and closed two days, with a copy of the
journal taken after day 1. A warm replica caught up, the primary stopped, and the day-1 copy (477
bytes instead of 930) went back in place.

The first version copied the older file over the followed one. The promotion step printed
nothing: the warm replica had already stopped itself, so there was nothing to promote. That
showed the replica's own check, but not the promotion check. So the script now restores the copy
the way backup tools do, as a new file renamed into place, and keeps the first way as `in-place`.

| | Renamed into place (default) | Copied over the followed file (`in-place`) |
|---|---|---|
| Warm replica | kept following the file it had open; promotion refused: "event log is 477 bytes, shorter than the 930 bytes already applied from it: it may be an older copy of the journal" | stopped following: "committed stream read failed: journal identity changed or committed history regressed" |
| Primary, restarted | refused: "the journal ends before what its stream already published: acknowledged commands are missing (an older copy of the journal, or a disk that lost synced writes)" | the same refusal |

**Review, second round (re-checking the fixes).** It confirmed two things:
- the new stream check refuses no legitimate restart: a crash before or during publication, a torn
  stream header and a promoted warm replica all still start;
- no test still passes vacuously.

Its smaller findings, all fixed:
- **Promotion compared the raw file length.** An older copy whose torn last record reached past
  the applied length passed, and recovery then cut the record off. It now compares the length of
  the complete records, before cutting anything. The test gained that case: the last record's
  length field claims 100 more bytes than it has, so the file is exactly as long as what was
  applied.
- **The refusal named only one cause.** A disk or VM that acknowledges fsync without keeping the
  data can lose published records in a power loss. The message now names both causes, and the
  operator notes say that starting anyway means deleting the stream file and resetting every
  reader.
- **The known limitation missed the likely case:** a restored journal with no stream file at all.
- **Two tests could miss a regression:**
  - nothing showed that promotion passes the applied length (passing 0 would go unnoticed).
    `promotion_refuses_an_older_copy_of_the_journal_it_followed` now does;
  - the stream test tripped both of its conditions at once. It now checks length only, sequence
    only, and both, then that the whole journal still starts.

**Final:**
- formatting clean; 172 unit tests and the process tests pass;
- all 3 PostgreSQL tests pass, each refusal now checked by its reason;
- the rollback live run passes both ways on the final code;
- warnings still 12.

---

## 5. Gotchas met while testing

- **The Windows Docker VM** is shared with the aimess stack and can starve or kill a build. Use the
  Ubuntu box.
- **`stock-test-pg` has no restart policy.** After the box reboots, live scripts fail with
  "could not translate host name". Run `docker start stock-test-pg` and wait for `pg_isready`.
- **`rustfmt` and CRLF.** Most working-tree files are CRLF, and running `cargo fmt` on them makes
  them LF. Apply its diffs by hand instead.
- **`cargo test --lib` does not work**, because there is no library target. Use `--bin stock` to
  pick unit tests.
- **`grep` through the `rtk` proxy** sometimes mangles its output. Read files directly when the
  output looks odd.
- **Piping the benchmark into `head`** ends with a "Broken pipe" panic. That is harmless.
- **Health right after a halt** can still read 200 for a moment. Wait for it to settle.
- **New task write-ups need a `.gitignore` exception** (`!docs/tasks/NN-....md`), or they are never
  committed.
- **A test can pass for the wrong reason.** After a rule change, check that tests still reach the
  path they claim, not an earlier refusal. Milestone 22 Part 1 found five such tests.

---

## 6. Rerun everything

```bash
# 1. copy the repo (never .env), then the standard check
tar czf - --exclude=./target --exclude=./.git --exclude=./.env --exclude='*.log' --exclude='*.mmap' --exclude='*.snapshot' . | ssh ubuntu@10.0.127.253 'rm -rf ~/stock.new && mkdir ~/stock.new && tar xzf - -C ~/stock.new && rm -rf ~/stock && mv ~/stock.new ~/stock'
ssh ubuntu@10.0.127.253 'docker start stock-test-pg >/dev/null; docker run --rm --network stock-test-net -v ~/stock:/src -v ~/stock-scripts:/scripts -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust bash -c "bash /scripts/check.sh; bash /scripts/reptest.sh"'

# 2. the heavy release tests
ssh ubuntu@10.0.127.253 'docker run --rm -v ~/stock:/src -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust bash -c "cargo test --locked --release --bin stock -- --ignored --nocapture"'

# 3. a live run (replace the script name; Python scripts take BINARY LABEL)
ssh ubuntu@10.0.127.253 'docker run --rm --network stock-test-net -v ~/stock:/src -v ~/stock-scripts:/scripts -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust bash -c "cargo build --release --locked -q; python3 /scripts/live_overflow.py /target/release/stock new; python3 /scripts/live_overflow.py /scripts/stock-m22 m22"'
```
