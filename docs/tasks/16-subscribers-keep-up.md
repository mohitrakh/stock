# Task 16 - Subscribers Keep Up (Milestone 21)

## Goal

Make the exchange's two subscribers, the market-data publisher (MDP) and the PostgreSQL reporter,
follow the exchange at its own speed. Also fix the reporter's recorded correctness bugs, plus the
ones this milestone's benchmark and its independent review found.

Parts 1 and 2 are optimizations. Their full explanations and measurements are in
`docs/performance/05-market-data-keeps-up.md` and `docs/performance/06-reporter-batched-transactions.md`.
The raw output is in `docs/performance/results/results-m21.txt`. This file records why the milestone
was chosen, the decisions behind all three parts, and the bug fixes in full.

## Why this was next

After milestones 19 and 20 the exchange did about 22,000–26,000 orders/s on disk. Its subscribers
did not come close:

- **The MDP applied about 39 commands/s.** For every command it copied both projections, serialized
  its whole state to JSON and synced two files. Under any real load the public order book, which the
  design calls "real-time", fell further behind every second. Catching up on a 200,000-order journal
  would have taken about 86 minutes.
- **The reporter applied about 365 commands/s.** It used one PostgreSQL transaction, and so one WAL
  sync, per command.
- **Two reporter defects were on record** in `DEFERRED_ITEMS.md`. An ordinary client retry halted
  it permanently. A refused cancellation was written onto the order owner's row.

The recorded plan had a trading-day boundary first. This milestone went ahead of it because a public
feed that cannot keep up is a visible failure on every run, while unbounded state only hurts long
runs. The trading-day boundary is next, then replication to a second machine.

## Part 1 - Market data applies in place and saves once a second

See `docs/performance/05-market-data-keeps-up.md`.

The served view is now one `Arc<RwLock<Option<View>>>`. `View` holds the order book, the candles,
and `applied`, the reader checkpoint just after the last batch applied to them. `None` means
unavailable. A batch is read outside the lock and applied in place under the write lock. If applying
fails, the follower replaces the view with `None` before releasing the lock, so a half-applied view
can never be served. The view and `applied` are saved together at most once a second, counted from
the end of the previous save, and once more at the end of catch-up before the listener binds. A crash
replays at most about a second of journal onto the last saved pair, and replay is deterministic, so
nothing is counted twice. A drop guard withdraws the view if the follower thread ends for any reason.

Decisions:

- **Fail closed at the level of the whole view, not per projection.** The old code applied each
  batch to copies so that a failure left the served state unchanged. Withdrawing the whole view on
  failure gives the same guarantee (nothing half-applied is ever served) without a copy per command.
  The candle projection therefore lost its internal copy too.
- **Save `applied`, never `reader.checkpoint()`.** The reader has already moved past a batch that
  may have failed.
- **Count the interval from the end of the previous save.** Counted from the start, a save slower
  than the interval would trigger another save after every batch.

Rejected: keeping the copies and only saving less often (the copies alone are O(state) per command),
validate-then-apply (more code for a guarantee the withdrawal already gives), a separate saver
thread (a 3 MB save takes about 50 ms once a second), and a configurable interval (one constant until
a measurement asks for more).

Result: catch-up went from about 39 to about 56,000 commands/s. Live, the MDP stayed within a second
of the exchange at 5,000 orders/s and at the exchange's maximum, about 34,500 orders/s.

## Part 2 - The reporter commits many batches per transaction

See `docs/performance/06-reporter-batched-transactions.md`.

`apply_available`, used for both catch-up and live following, puts up to 1,000 committed batches in
one PostgreSQL transaction and commits them with the checkpoint just after the last applied batch. A
group also ends when the reporter is caught up, so a quiet exchange still commits each batch at once.
Any error rolls back the whole group and stops the reporter. Each trade became one statement: a
data-modifying CTE fills both orders and inserts the trade only if both fills applied.

Decisions:

- **One function for catch-up and following.** Catch-up's last group therefore commits before
  `/health` binds, by construction.
- **Take the checkpoint after applying and before reading further.** Otherwise the checkpoint could
  claim a batch that was never written.
- **Stop at 1,000 batches.** The commit is then paid once per 1,000 commands, and a failure rolls
  back a bounded amount of work.

Result: 365 → 1,050 commands/s with group commit, → 1,578 with one round trip per trade (4.3× in
all). Live, it keeps up at 1,000 orders/s; at 5,000 orders/s it falls behind and drains a 10-second
burst in about 21 s. PostgreSQL's per-row work now dominates, so set-based writes or `COPY` are the
next lever.

## Part 3 - Reporter bug fixes

A, B and C were planned: two recorded bugs and one found by this milestone's benchmark. D and E were
found by the independent review of this milestone.

### A. A reused order id halted the reporter

**The bug.** Every new-order batch, accepted or rejected, became an `INSERT` into `reported_orders`,
whose primary key is `order_id`. A client that retries an order with the same `client_order_id` is
refused with `409`, and the journal records `OrderRejected` carrying that same order id. The
reporter inserted a second row with an existing key, the transaction failed, and the reporter
stopped. Every restart replayed the same batch and failed the same way. An id rejected first (for
example, not enough cash) and accepted later hit the same key the other way round.

**The fix.** `reported_orders` holds accepted orders only. That is safe as a key because the engine
never accepts an id twice. A rejected submission goes to a new table, `rejected_orders`, keyed by
`input_sequence`: the journal sequence of the command's input (`batch[0].seq_num`). The order id,
user, symbol, side, price, quantity, time and reason are stored as data, with indexes on order id and
user.

**Why the journal sequence is the key.** It is unique by construction, it is the same on every
replay, and it points at the exact journal record for an audit. An order id is not unique for a
rejected submission: a retry of an accepted id, a reuse of a rejected id, or another user's id.

Rejected alternatives:

- `ON CONFLICT DO NOTHING` on `reported_orders`. It would silently drop the refused attempt, and it
  would also hide a genuine duplicate caused by a bug, which the key exists to catch.
- One table for both, keyed by `(order_id, input_sequence)`. Accepted orders would lose their
  one-row-per-id check, and a lifecycle row would sit beside rows that have no lifecycle.
- Not recording rejections at all. A refused submission is part of what happened, and support and
  compliance questions start from it.

### B. A refused cancellation overwrote the owner's order row

**The bug.** A refused cancellation ran `UPDATE reported_orders SET cancellation_outcome =
'rejected' ... WHERE order_id = $2`, with no check of owner, status or rows changed. The shared
decoder also dropped the requester's user id, so the reporter could not tell whose attempt it was.
A user who tried to cancel someone else's order was refused by the exchange as `Unauthorized`, yet
the reporter wrote that refusal onto the owner's row. A late attempt to cancel an already-canceled
order left `status = 'canceled'` beside `cancellation_outcome = 'rejected'`.

**The fix.**

- The decoder's `CommittedCommand::Cancellation` now carries the requester's `user_id`.
- A refused cancellation becomes a row in a new table, `rejected_cancellations` (`input_sequence`,
  `order_id`, `requested_by`, `reason`), and never touches `reported_orders`.
- A successful cancellation's `UPDATE` also requires `user_id` to match. The engine only lets the
  owner cancel, so this check costs nothing and stops the reporter if the journal and the report
  ever disagree.
- The columns `cancellation_outcome` and `cancellation_reason` are dropped, because nothing gave them
  meaning any more. A new constraint makes `status = 'canceled'` equivalent to a recorded
  `cancellation_sequence`.

**Why a separate table.** A refused attempt is a fact about the attempt: who asked, when, and why it
was refused. It is not part of the order's lifecycle, and one order can be the target of many refused
attempts by different users.

### C. Execution ids repeat across symbols

**How it was found.** The Part 1 benchmark journal has 10 symbols. The old reporter stopped on it at
event sequence 255 with `duplicate key value violates unique constraint
reported_trades_first_execution_id_key`. Part 2 had to be measured on a one-symbol journal for that
reason.

**The cause.** Each symbol's order book has its own execution counter, so `exec_0` exists once per
symbol. The reporter's schema required execution ids to be unique across all symbols.

**The fix.** The unique constraints are now `(symbol, first_execution_id)` and
`(symbol, second_execution_id)`. A trade's primary key stays its `trade_sequence`, the journal
sequence of its first execution record, which is unique across the whole exchange.

**Rejected: make the engine's execution ids globally unique.** That changes the outputs the engine
writes to the journal, so every existing journal would fail deterministic replay with an
`OutputMismatch`. A reporting constraint should follow the engine's real identity rule, not the
other way round.

### The migration

`migrations/20260930000000_reporter_rejections.sql`:

It runs as one transaction, so a failure part-way leaves the old schema and its rows untouched.

1. Empties the report (`TRUNCATE reported_trades, reported_orders, reporter_checkpoint`). The next
   reporter start rebuilds everything from journal sequence 1.
2. Adds a required `report_version` column (always 2) to `reporter_checkpoint`.
3. Creates `rejected_orders` and `rejected_cancellations`.
4. Drops `rejection_reason`, `cancellation_outcome` and `cancellation_reason`, makes
   `acceptance_sequence` required, limits `status` to the four accepted-order states, and ties
   `canceled` to `cancellation_sequence`.
5. Replaces the global execution-id constraints with per-symbol ones.

**Why rebuild instead of converting the old rows.** The rows are derived, and the old schema had
already lost information: bug B overwrote refusals onto owners' rows, and bug A meant some
rejections were never stored. Only the journal has the complete history, so the report is rebuilt
from it.

**Operating it.** Stop the reporter, apply the migration, and start the new binary. Mismatched
versions fail safely:

- **An old reporter left running** can no longer write anything. Its order inserts name dropped
  columns, and its checkpoint save omits `report_version`, which has no default. Without that column
  it could still have saved its old position after a deposit (a batch that writes no order rows).
  The new reporter would then have started from that position and silently skipped the history
  before it. The independent review found this gap.
- **The new reporter on the old schema** refuses to start before writing anything, because its
  startup check counts rows in all four tables and the two new ones are missing.

### D. Found by the review: client identifiers are checked at the gateway

**The problem.** The new `rejected_cancellations` table indexes `order_id`. A PostgreSQL btree
index entry holds at most about 2.7 KB, and the cancel endpoint accepted any `order_id` a logged-in
user sent. A cancellation with an id of a few kilobytes is refused by the exchange and journaled like
any refusal, and then the reporter's insert fails. That stops the reporter at that record on every
restart. The old code's `UPDATE ... WHERE order_id = $2` did not index the value, so this was new in
this milestone. The same class of problem already existed for NUL bytes, which PostgreSQL text
cannot store, in any client string, and for very long symbols, through the `(symbol, status)` index.

**The fix is at the trust boundary, not in the reporter.** One helper in the HTTP controller,
`identifier`, trims a client-supplied order id or symbol and requires 1 to 64 bytes with no control
characters. It is used for the client order id and symbol of a new order, the symbol of a share
deposit and a risk limit, and the order id of a cancellation. No order can have an id that fails the
check, so such a cancellation answers 404 without reaching the journal. Symbols used to be trimmed
for deposits and limits but not for orders, and are now trimmed everywhere.

### E. The reporter's `/health` survives a panic

The follower thread marked the reporter unavailable only when `follow` returned. A panic skipped
that, and `/health` kept answering 200 while the report stopped moving. A drop guard
(`UnavailableOnExit`) now marks it unavailable however the thread ends, as the MDP's
`WithdrawOnExit` does since Part 1. The review found this; it predates the milestone.

### Tests

- `a_cancellation_carries_who_asked` (unit, `committed_batch.rs`): a refused cancellation decodes
  with the requester's id.
- `reporter_rolls_back_then_restarts_without_duplicate_history` (PostgreSQL acceptance) now covers,
  in one journal:
  - a rejected retry of the accepted `buy-1`;
  - `reject-1` rejected and later accepted;
  - an intruder's refused cancellation of `reject-1`;
  - a refused cancellation of the already-canceled `sell-b`;
  - an MSFT trade reusing AAPL's `exec_0` and `exec_1`.

  It asserts the exact rows in all four tables, that neither refused cancellation changed the
  order's row, and that a restart adds nothing. The injected checkpoint failure must leave all four
  tables and the checkpoint empty. It also checks that a checkpoint written the old way, without
  `report_version`, is refused by the migrated schema.
- `a_failed_group_rolls_back_only_itself` (PostgreSQL acceptance, see `06`): the 1,000-batch group
  boundary under a failure in the next group.
- The two acceptance tests reset the same database, so a mutex makes them run one at a time.
- `identifiers_are_trimmed_bounded_and_printable` (unit, controller): trimming, the 64-byte limit,
  and refusal of blank values, NUL and newline.
- The review also showed that two Part 1 tests proved less than their names said; both were fixed,
  as described in `05`.

### Measured

Part 3 was measured on the office Ubuntu machine: an Intel i3-7100 (2 cores, 4 threads) with a SATA
SSD, running PostgreSQL 16 in Docker. Parts 1 and 2 were measured on the Docker Desktop VM, so their
absolute numbers are not comparable with these; only rows from the same machine are compared here.
On this machine the benchmark journals themselves were written at about 44,500 orders/s (one symbol)
and 42,500 orders/s (10 symbols).

| Reporter catch-up from empty tables | Part 2 code | Part 3 code |
|---|---|---|
| One symbol, 200,020 commands | 1,404–1,538 commands/s (4 runs) | 2,087–2,141 commands/s (5 runs) |
| One symbol, freshly started PostgreSQL | 1,874 commands/s | 2,104 commands/s |
| 10 symbols, 200,110 commands | stops at the first repeated execution id | 2,102 commands/s, 144,411 trades across all 10 symbols |

Both versions produced the same 200,000 orders, 144,670 trades and final checkpoint on the
one-symbol journal. The benchmark workload has no rejections or cancellations, so the rejection
tables stayed empty there. The acceptance tests cover them.

**The fixes cost nothing, and Part 3 measured faster.** The first comparison showed a 1.4× gap,
which bug fixes should not produce, so it was taken apart:

1. **Not run order.** Alternating the two, with a PostgreSQL `CHECKPOINT` before each run, kept the
   gap.
2. **Not the build.** The Part 2 binary had been compiled on the other machine. Rebuilding Part 2's
   code on this machine, with the same toolchain, gave 1,404 commands/s again.
3. **Mostly database state.** On a freshly started PostgreSQL the Part 2 code did 1,874 commands/s,
   against 1,404–1,538 on the one that had already served many runs. Part 3 did about 2,100 on
   both. Why the older database hurt only the old schema was not isolated.
4. **The rest is inside PostgreSQL.** With `pg_stat_statements` on the fresh database, total
   PostgreSQL execution time was 64.1 s for Part 2 and 54.1 s for Part 3. Most of the difference is
   the trade statement: 0.290 ms against 0.228 ms per trade. The order insert (0.099 against 0.093
   ms) and the foreign-key checks were nearly equal. Part 3's schema is what differs on that
   statement's path: the order row it rewrites has different constraints, and the trade row's
   execution-id indexes include the symbol. Which of those saves the time was not measured.
5. **Outside PostgreSQL**, decoding, round trips and the client took about 41–43 s in both runs.

The practical lesson: compare reporter numbers only on the same machine and the same database
state.

## Results in short

| | Before | After |
|---|---|---|
| MDP catch-up, 200,110 commands (VM) | ~39 commands/s (about 86 minutes) | **~56,000 commands/s (3.6 s)** |
| MDP live | fell behind above ~39 commands/s | within a second at 5,000 orders/s and at the maximum (~34,500) |
| Reporter catch-up, 200,020 commands (VM) | ~365 commands/s | **1,578 commands/s** (4.3×) |
| Reporter catch-up, same journal (Ubuntu machine) | — | about 2,100 commands/s |
| Reporter on a 10-symbol journal | stopped at event 255 | completes |
| A client retry, a reused rejected id, a refused cancellation | stopped the reporter, or damaged the owner's row | recorded in their own tables; owners' rows untouched |

## What is left

- **Reporter throughput.** About 1,600–2,100 commands/s, depending on the machine, is enough for
  1,000 orders/s, not for the exchange's maximum. About 40% of each run is spent outside
  PostgreSQL, on round trips and the client, so set-based writes or `COPY` are the next step (see
  `06`).
- **Identity keys and the trading day.** `reported_orders` assumes an order id is accepted once
  ever, and trades assume a symbol's execution numbering never restarts. A trading-day boundary must
  revisit both first; this is recorded in `DEFERRED_ITEMS.md`.
- **MDP state still grows** with candle history, and a restart re-scans the journal to validate its
  checkpoint.
- **No reporting API.** The rejection tables, like the rest of the report, are read directly from
  PostgreSQL.

## Verification

- Built and tested on the office Ubuntu machine, in the `rust` Docker image with the repository
  copied in, because the Docker Desktop VM on Windows was saturated by other containers.
- `cargo fmt -- --check` is clean. `cargo test --locked` passes 142 unit tests (138 before this
  milestone) and the executable integration tests.
- Both PostgreSQL acceptance tests pass against a fresh PostgreSQL 16:
  `REPORTER_TEST_DATABASE_URL=... cargo test --locked --test reporter -- --ignored`.
- Before this milestone's fixes the pushed tree failed `cargo fmt -- --check`, and its reporter
  acceptance test failed because it still built the old schema. Both are fixed.
- An independent read-only review of the whole milestone found:
  - the oversized-identifier problem (D);
  - the old-reporter checkpoint gap in the migration, and the missing transaction around it;
  - the reporter's `/health` after a panic (E);
  - two Part 1 tests that proved less than they claimed;
  - two stale sentences in the docs.

  All were fixed. It confirmed the invariants the milestone depends on:
  - the reporter's checkpoint is never ahead of its committed rows;
  - groups end at exactly 1,000 batches;
  - every error rolls back the open group;
  - the trade statement's update and insert commit or roll back together;
  - a half-applied market-data view is never served or saved;
  - the migration's constraint names match the first migration's.
