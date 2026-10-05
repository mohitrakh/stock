# Task 17 - Trading Day (Milestone 22)

## Goal

Give the exchange a trading day (open, trade, close) and bound its memory to one day. The design
asks for normal trading hours only, but until now the exchange accepted orders at any time and kept
every order and fill forever. Its memory, snapshots, restart time and the warm replica's snapshot
cost therefore grew without limit. A snapshot was already 82 MB at 200,000 orders.

The specification, with the owner's decisions, is the "22. Trading Day" section of
`PROJECT_DIRECTION.md`. The decisions were:
- a loopback operator port opens and closes the market;
- every order still resting at the close expires (day orders only);
- the previous day leaves memory at the next open;
- earlier days are read from the reporter's database only.

The milestone has four parts. This file records each part as it completes.

## Why this was next

Milestone 21 made the subscribers keep up. What remained in the recorded plan was a trading-day
boundary, then copying the journal to a second machine. The day comes first for three reasons:
- **It is a design requirement.** The design asks for normal trading hours.
- **It removes the cause of unbounded growth.** Today the state never shrinks.
- **Replication should copy a settled shape.** This milestone changes events and state, so it is
  better to settle them first and replicate them once.

## Part 1 - Opening and closing the market

### What changed

- **Two new journaled inputs.**
  - `MarketOpenRequested { trading_day }`, where `trading_day` is a calendar date such as
    `2026-10-01`.
  - `MarketCloseRequested`.
- **Three new outputs.**
  - `MarketOpened { trading_day }`.
  - `MarketClosed { trading_day }`.
  - `SessionRejected { reason }`.
- **Session state in the core.** `ExchangeCore` holds a `Session`: the last trading day (`None`
  until the first open) and whether the market is open. A new exchange has never opened, so it
  starts closed.
  - An open is accepted only while the market is closed, and only for a day later than the last
    one.
  - A close is accepted only while the market is open.
  - A refusal (`AlreadyOpen`, `NotAfterLastTradingDay(day)`, `AlreadyClosed`) is journaled as
    `SessionRejected` and changes nothing, like any business rejection.
- **Orders are refused while the market is closed.** The check sits in `prepare_input_event`, the
  one function that live trading, journal replay and the warm replica all use. All three therefore
  agree on which orders the session refused. A refused order is journaled as `OrderRejected` with
  the reason `MarketClosed`, and the customer API answers `409`. Deposits, share deposits,
  risk-limit changes and cancellations are accepted at any time.
- **The risk day is the trading day.**
  - **Before:** `RiskManager` derived "today" from each order's timestamp, in UTC 86,400-second
    buckets, and rolled its counters when a timestamp crossed into a new bucket.
  - **Now:** the counters restart when the market opens (`RiskManager::start_day`), and timestamps
    play no part. Quantity still resting counts toward the new day, as before.
  - **Simpler:** the timestamp maths, the "only roll forward" guard against clock jitter, and the
    risk snapshot's day field are gone.
- **The operator port.** A loopback-only listener in the exchange process (`EXCHANGE_OPERATOR_ADDR`,
  default `127.0.0.1:4004`):
  - `POST /session/open` with `{"trading_day":"2026-10-01"}`;
  - `POST /session/close`;
  - `GET /session`.

  It answers 200 with the session, 409 with the reason when the session refuses, and 503 when the
  worker has stopped. It uses the same trust model as the warm replica's management port:
  unauthenticated, and refusing to bind anything but a loopback address. A promoted warm replica
  serves it too, because both start the same way.
- **Snapshots** carry the session; the format version is now 2. A version-1 snapshot is refused,
  and startup falls back to replaying the journal.
- **The benchmark** opens a fixed day (`2026-01-02`) before its setup, so every run journals the
  same history.
- **The shared decoder** treats session batches as "no market data" for now (`Other`). Part 2 gives
  the close its expiries.

### Decisions

**Journaled commands, never a clock.** This is the rule that made risk limits events. A decision
derived from the clock replays differently on another day or another machine. With the open and
close in the journal, any replay sees exactly the same days at exactly the same points.

**The operator names the day.** The exchange could derive the day from the wall clock at the open.
It does not, for the same reason: the date is input, so it goes in the journal. The only rule is that
days move forward. That stops a mistyped date from reopening a closed day, whose risk usage has
already been reset.

**The check lives in `prepare_input_event`, not in `ExchangeCore::prepare_add_order`.**
- *Why it works:* every journaled input passes through `prepare_input_event`, live and in replay.
- *What it keeps:* the core's own order methods stay pure matching and ledger logic.
- *What it saves:* the 36 core unit tests call the core directly to test matching and ledgers. None
  of them needed a session, except the one about day boundaries.

Runtime tests that place orders open the market first. Tests that replay their history journal the
open. The others open it on the core directly, the same way they already deposit cash directly.

**A new exchange starts closed.** Starting open would mean a fresh journal accepts orders before
any trading day exists, and the risk day would be undefined.

**Cancellations are allowed while closed.** In Part 1 an order can still rest across a close, and
its owner should be able to withdraw it. From Part 2, nothing rests after a close, so a late
cancellation simply finds nothing to cancel.

**409 for a closed market.** The request is valid; it is the exchange's state that refuses it. That
is the same choice as a duplicate client order id.

### Rejected

- **Reading market hours from the clock or a schedule.** That is not deterministic, and the owner
  chose manual operation. A scheduler can later call the operator port.
- **Open and close routes on the customer API.** The customer API serves traders with ordinary
  authentication; there is no operator role. A separate loopback port is the pattern the warm
  replica already uses.
- **Keeping the timestamp-based risk day beside the sessions.** Two definitions of "today" would
  disagree whenever an order's timestamp and the open crossed midnight differently.
- **Starting old journals "open".** Every earlier milestone that changed what replay produces asked
  for a new journal instead of guessing. Old journals now fail replay, because their orders were
  accepted with no open session.

### Tests

- Core:
  - `the_session_opens_only_forward_and_closes_only_when_open`.
  - `a_new_trading_day_expires_the_previous_days_traded_usage`.
  - `overnight_fills_and_cancellation_cannot_refund_todays_traded_usage`, rewritten to use real days
    instead of order timestamps.
- Risk manager: `a_new_day_starts_only_when_the_market_opens` (a later timestamp alone changes
  nothing) and the overnight-exposure test now use `start_day`.
- Runtime:
  - `orders_are_refused_while_the_market_is_closed_and_replay_agrees`: refused before the open and
    after the close; accepted in between; replay regenerates the same refusals and state.
  - `refused_session_changes_are_journaled_and_change_nothing`.
  - `overnight_risk_usage_replays_and_continues_after_restart`, rewritten with a journaled open,
    close and open.
- Events: `session_events_round_trip_with_an_iso_trading_day`.
- Operator port: `the_operator_opens_and_closes_a_journaled_trading_day`, run against the real
  worker over HTTP:
  - 200 and 409 answers;
  - 422 for a value that is not a date;
  - after a restart, the same session recovered from the journal.
- Restart: snapshot recovery, the warm replica's snapshot and restart, and a durable restart all now
  carry the session across.
- 18 existing tests failed once orders needed an open market, as expected. Five more still passed,
  but only because their orders were now refused, so they no longer tested what their names say:
  - `append_failure_does_not_publish_execution_callbacks`, whose fill would never have happened;
  - `a_users_order_is_not_readable_by_anyone_else`;
  - `committed_stream_matches_runtime_history_and_survives_restart`;
  - the warm replica's catch-up test;
  - the warm replica's snapshot test.

  All were updated to trade on an open market. The warm replica's snapshot test now also closes the
  market before its last command, so its restart checks a closed session from a snapshot.

### Verification

- `cargo fmt -- --check` is clean. `cargo test --locked` passes 147 unit tests (142 before) and
  the integration tests. The release build has 12 warnings, all pre-existing dead code in older
  APIs (13 before).
- **A live run** of the real exchange binary, with PostgreSQL for logins, on the office Ubuntu
  machine:
  - the session started closed;
  - deposits were accepted while closed, and an order was refused with `409 MarketClosed`;
  - the operator opened 2026-10-02; a second open got `409 AlreadyOpen`;
  - a sell and a buy traded;
  - the close was accepted; a later order got `409`; reopening the same day got
    `409 NotAfterLastTradingDay(2026-10-02)`;
  - `/session` on the customer port is 404: the operator API is not public;
  - after `SIGKILL` and a restart, `/session` still showed 2026-10-02 closed, and the fill was
    intact;
  - opening 2026-10-05 then accepted a new order.

  The journal showed every one of those steps, refusals included, in order.
- The benchmark opens its day and still runs at about 46,000 orders/s on that machine (200,000
  orders, one symbol), with no rejections.

## Part 2 - Expiry at the close

### What changed

- **Every order is a day order.** When the market closes, every order still resting expires:
  - its unfilled collateral is released: cash at its limit price for a buy, shares for a sell;
  - its unfilled risk allowance is released;
  - its state becomes `Expired` (status `"expired"` in the API), and it leaves the book;
  - it consumes a matching sequence, as a cancellation does.

  The ledgers end up exactly where cancelling each resting order would have left them. Only the
  final state differs.
- **One journal record per close.** The close's record is the input, then `MarketClosed
  { trading_day }`, then one new output, `OrderExpired { order_id, seq_num }`, per resting order.
- **Oldest first.** Orders expire in the order they were accepted (by their matching sequence).
  The books live in a hash map, whose iteration order differs from one process to the next, so
  walking the books directly would give replay different sequence numbers.
- **At most 200,000 resting orders.** A journal record may hold at most 64 MiB, and the close must
  fit in one. So the books hold at most 200,000 resting orders across all symbols. An order that
  would rest beyond that is refused as `OrderRejected { reason: "BookFull" }`, which the customer
  API answers with 409. An order that trades without resting is never refused, nor is one that
  takes as many resting orders out of the book as it adds. With the longest ids the gateway allows
  (64 bytes, each byte escaped to two in JSON), a close of 200,000 orders takes about 50 MB.
- **A safety net for the close.** If a close's record would still be too large, which only an
  order that skipped the gateway's id check could cause, the close is journaled as `SessionRejected
  { reason: "TooManyRestingOrders(n)" }` and nothing changes.
- **A simpler risk manager.** Nothing rests overnight any more, so the separate open-exposure
  counter that carried resting orders into the next day had no job left. It is gone, with the
  fill-time bookkeeping that fed it. Usage is now one number per user and symbol: it grows when an
  order is accepted, stays the same on a fill, shrinks by the unfilled quantity on a cancellation
  or an expiry, and restarts from zero at each open.
- **Subscribers learn the expiries.**
  - The shared decoder turns a close into `CommittedCommand::MarketClosed { trading_day,
    expired }`. It checks that the expiries take consecutive matching sequences and name each
    order once. A refused close decodes as "no change".
  - The market-data publisher removes every expired order from its book. If any order is still
    in its book after a close, its view has drifted from the exchange, so it stops serving.
  - The reporter marks the expired orders `expired`, with the matching sequence each expiry used,
    in one SQL statement per close. A new migration, `20261005000000_reporter_expiry.sql`, adds
    the status and an `expiry_sequence` column, and again empties the report for a rebuild.
- **Snapshots** are format version 3. Loading one now also checks that no order rests while the
  market is closed.

### Decisions

**The close plans everything before it changes anything.** As with every command, preparation
works out the whole expiry: which orders, their sequences, and what each ledger releases. It checks
the totals per user against what is actually reserved, so the commit cannot fail half way. A user
with ten resting buys has ten small releases; checking each one alone could not prove that their
sum fits.

**Expiry reuses the cancellation's release.** The order manager releases an expired order with the
same code that releases a cancelled one, so "exactly like a cancellation" holds by construction.
A test also compares the two outcomes field by field.

**Cap the book, so the close always fits.** The first version only refused a close that was too
large. The independent review showed that this could leave the exchange stuck:
- only an order's owner can cancel it, and the operator port can only open and close;
- so after a refused close, nothing could bring the book down, and the market stayed open and kept
  taking orders;
- every later close was refused, and the next day could never open.

One user could cause this on purpose with about 253,000 small orders whose ids are made of
characters JSON escapes. With the cap, the same flood only fills the book until that day's close,
and the close always succeeds. The owner chose the cap over a close spread across several records
(see "Rejected").

**Count orders, sized for the worst id.** The cap counts resting orders. That is one comparison on
the order path, using a count the matching engine already keeps. It is sized for the worst case the
gateway allows: 64-byte ids, every byte escaped. A test fills the book with exactly such ids and
closes it.

**The size check counts every sequence number at its widest.** Whether the record fits must not
depend on where in the journal the close lands, or replay could disagree with the live decision.
So the check serializes the close with every envelope sequence at its largest possible value (20
digits). The real record is always smaller, so a close that passed the check can always be written.
A test pins both halves, through the real close path with a small limit.

**Refuse rather than halt, as a last resort.** Before this part, a record over 64 MiB made the
worker halt when it tried to write it. With the cap, that can no longer happen to a close of
gateway orders. The refusal remains for any other entry point: refusing changes nothing, while
halting would stop trading.

**The reporter updates a whole close in one statement.** A close can expire hundreds of thousands
of orders; one round trip each would take minutes. One `UPDATE ... FROM unnest(...)` marks them
all, and the reporter checks that it changed exactly as many rows as there were expiries.

### Rejected

- **Refusing an oversized close as the only protection.** That was the first version. It could
  leave the market stuck open for good, as described above.
- **A close spread across several records.** A "closing" state would stop new orders, and the close
  would expire orders over several records. There would be no cap, but it needs a new session
  state, a new event, and handling in every subscriber. It is the recorded follow-up if 200,000
  resting orders is ever too few. Closing symbol by symbol has the same problem: one symbol can hold
  too many orders.
- **A byte budget instead of a count.** Tracking each resting order's exact share of the close's
  record would admit more orders with short ids. It needs bookkeeping on every path that adds or
  removes a resting order.
- **Keeping open-exposure tracking "just in case".** It only existed for overnight orders. Keeping
  dead state costs every fill and every snapshot.

### Tests

- Core:
  - `the_close_expires_every_resting_order_exactly_like_a_cancellation`: two identical days, one
    closed with orders resting and one with those orders cancelled first; balances, positions and
    risk usage match, and the book is empty.
  - `expiries_take_sequences_oldest_first_and_the_next_day_continues_both_counters`: expiry order,
    sequences, a refused cancel of an expired order, a snapshot round trip, and execution ids that
    continue on the next day.
  - `a_snapshot_cannot_hold_resting_orders_while_the_market_is_closed`.
  - `an_order_that_would_rest_beyond_the_cap_is_refused_but_trading_goes_on`, with a cap of two:
    - a third resting order is refused;
    - an order that trades without resting is accepted;
    - an order that fills one resting order and rests keeps the count level.
- Runtime:
  - `the_close_expires_resting_orders_and_a_restart_continues_the_next_day`: the journaled close
    record, released collateral, a restart, the next day's full allowance and next sequence.
  - `a_close_too_large_for_one_record_is_refused_and_changes_nothing`, through the real close path
    with a small limit:
    - a close exactly at the limit is accepted;
    - one byte smaller is refused, and the refusal changes nothing;
    - the real record is smaller than the widest one.
  - `a_full_book_of_the_longest_ids_closes_in_one_record` (ignored by default; run in release):
    - fills the book with 200,000 resting orders whose 64-byte ids are made only of escaped
      characters;
    - the 200,001st resting order is refused as `BookFull`;
    - the close fits and empties the book.
  - `orders_are_refused_while_the_market_is_closed_and_replay_agrees` now also sees the expiry.
- Decoder: `a_close_lists_its_expiries_and_a_refused_close_changes_nothing`, including a gap in
  the sequences, a repeated order and an expiry without its close.
- Market data: `the_close_removes_every_expired_order_and_refuses_to_leave_one_behind`, and
  `the_exchange_close_empties_the_projection`, driven by the real worker.
- Risk manager: `releasing_returns_only_the_unfilled_allowance`; the overnight test is gone.
- Reporter (PostgreSQL): the lifecycle journal now closes the day, expiring one untouched and one
  partly filled order, then has a refused second close. A version-2 reporter cannot save a
  checkpoint.

### Verification

- `cargo fmt -- --check` is clean. `cargo test --locked` passes 153 unit tests (147 before) and
  the integration tests. Both PostgreSQL acceptance tests pass. The release build's 12 warnings are
  the same old dead code as before.
- **The full-book test, in release.** Preparing the close of 200,000 worst-case orders took 0.47 s.
  The record was 50.1 MB, or 53.0 MB with every sequence counted at its widest, against a limit of
  67.1 MB (64 MiB).
- **An independent review found no correctness bug in the expiry path.** Its findings:
  - the stuck close, fixed by the cap;
  - a size test that could not fail, replaced by one through the real close path;
  - an expiry order that relied on unique sequence numbers, now broken by the order id;
  - a stale comment;
  - the reporter does not check that nothing still rests after a close. That check means scanning
    every report row at each close until rows carry their trading day, so it moves to Part 3.
- **A live run** of the real exchange, market-data and reporter binaries on the office Ubuntu
  machine:
  - one fill and three resting orders (one partly filled), then the close;
  - the three orders expired oldest first, with matching sequences 5, 6 and 7;
  - the buyer's locked cash and both sellers' locked shares went to zero; risk usage kept only the
    4 shares traded;
  - cancelling an expired order answered 400 `already Expired`;
  - the market-data book went from three levels to 404, and the report showed the three orders as
    `expired` with their sequences;
  - after `SIGKILL` and a restart, the orders were still expired and the market still closed;
  - the next day opened with zero risk usage, and a new order appeared in the market-data book.

## Part 3 - Clearing the previous day at the next open

### What changed

- **Memory holds one trading day.** When the market opens, the previous day's finished orders
  (filled, canceled, expired) and the per-user fills index leave memory. Nothing is resting at
  an open, because the close expired it all. What stays:
  - balances and positions;
  - risk limits (daily usage restarts from zero, as before);
  - each symbol's book, empty, with its execution counter, so an execution id never repeats.

  Between a close and the next open, the day just closed can still be read.
- **Client order ids per trading day.** An id must be unique within its trading day, even after
  its order has finished, and can be used again on a later day (the FIX rule for tag 11). This
  came for free: the duplicate check looks at the orders in memory, which are now only today's.
  `GET /exchange/orders/{id}` and `GET /exchange/executions` answer for the current or just-closed
  day; earlier days are in the reporter's tables.
- **Snapshots** are format version 4, and the warm replica also writes one right after each open.
  - **Why the new version:** a version-3 snapshot could hold yesterday's orders, which replay
    under the new rule would have cleared.
  - **Why snapshot at the open:** the state is smallest then, and a restart replays only the
    current day.
- **The reporter follows the day.**
  - The decoder now turns an open into `CommittedCommand::MarketOpened { trading_day }`.
  - The reporter remembers the day the journal is in and saves it in its checkpoint row. Order
    and trade batches do not name their day, so a restart in the middle of a day needs it.
  - Orders are keyed by `(trading_day, order_id)`, and each trade carries the day of the two
    orders it fills.
  - Rejected orders and rejected cancellations record the day the journal was in (NULL before
    the first open). An id now repeats across days, so a refusal needs its day to be matched to
    the right order.
  - After a close the reporter checks that nothing from that day still rests. That was the review
    finding deferred from Part 2; now it reads one day's rows instead of the whole history.
  - The migration `20261005100000_reporter_trading_days.sql` makes these changes, empties the
    report again for a rebuild, and moves `report_version` to 4.

### Decisions

**The open drops every order.** By the time an open happens, every order has finished: the close
expired whatever rested, and a snapshot that holds a resting order while the market is closed is
refused. The first version kept "any order still resting" as a supposed safety net. The
independent review pointed out that this protected nothing. The same open resets risk usage, so
such an order would fail its release later and halt the worker somewhere unrelated. Replacing the
maps outright is simpler, and it also returns their memory.

**Clear at the open, not at the close.** Clearing at the close would hide the day's results from
the API at the moment people want to see them. Between a close and the next open, a trader can
still read their orders and fills.

**A fourth migration, not a third.** The plan was one migration for the milestone. Part 2 had
already added one for expiries, which may be committed on its own, and a committed migration
should not change afterwards. A new migration costs nothing here, because each one empties the
report and the reporter rebuilds it from the journal.

**The reporter learns the day from the journal.** It could have used the order timestamp's date,
but that is the gateway's clock, not the exchange's day. The open in the journal is the only
authority. The day goes into the checkpoint row, in the same transaction as the rows it keys.

**Execution ids stay unique per symbol without the day.** Each book keeps its counter across days,
so `(symbol, execution id)` remains a valid unique key. No change was needed there.

### Rejected

- **Keeping finished orders in memory for longer** (for example, a week) so they can still be
  read. That would bring back the unbounded growth this milestone removes. History belongs to the
  reporter.
- **Archiving the old day inside the exchange**, or writing one journal file per day. Both are
  out of scope for this milestone; the journal is still one file.
- **Leaving the day off rejected orders and rejected cancellations.** The first version did, on
  the grounds that their journal-sequence key is unique forever. The review pointed out that
  uniqueness was never the problem: a refusal names an order id, and without its day it cannot be
  matched to the right order once ids repeat.

### Tests

- Core: `the_next_open_clears_the_previous_day_and_its_ids_return`:
  - the closed day can be read until the open;
  - after the open, no orders and no fills remain, and cash and shares carry over;
  - the same ids are accepted again, and the next execution id is `exec_2`;
  - a snapshot round trip.
- Runtime: `a_client_order_id_returns_on_the_next_day_and_every_recovery_agrees`:
  - a retry of a finished order's id is refused within its day and accepted the next day;
  - the snapshot is taken after the close, so it still holds the first day. The restart replays
    only the suffix after it (the open, the reused id and a cancellation), and the resulting state
    is identical to replaying the whole journal. This is the case snapshot version 4 protects.
- Warm replica: `warm_replica_writes_a_snapshot_right_after_each_open`:
  - with an interval of 1,000 commands, the second open still leaves a snapshot at its exact
    position;
  - a restart from it replays nothing and has the same state.

  The existing warm snapshot test now counts the snapshot after the open.
- Decoder: `an_open_names_its_day_and_a_refused_open_changes_nothing`, including an open whose
  output names another day.
- Market data: the real-worker close test continues into a second day, where an id from the first
  day rests again.
- Reporter (PostgreSQL):
  - The lifecycle journal starts with an open and adds a second day. On it, `buy-1` is accepted
    again and trades as `exec_6`, and a cancellation of the first day's `reject-1` is refused.
  - The report has one `buy-1` row per day. The day-two trade points at day two's `buy-1`, the
    refusals carry their days, and the checkpoint row holds the new day across a restart.
  - A version-3 reporter cannot save a checkpoint.
  - The group-rollback test starts with an open too.
  - New: `a_close_that_leaves_an_order_resting_stops_the_reporter`. A refusal before the first
    open has no day. A close that expires nothing while the report holds a resting order of that
    day stops the reporter at every start, and the close is never recorded.

### Verification

- `cargo fmt -- --check` is clean. `cargo test --locked` passes 157 unit tests and the integration
  tests. Both PostgreSQL acceptance tests and the release-only full-book test pass.
- **A live run** over two trading days of the real exchange, market data, reporter and warm
  replica on the office Ubuntu machine:
  - reusing `b-1` on the same day answered 409 `AlreadyExists`;
  - after the close, `b-1` and the buyer's fill could still be read;
  - after the next open, `b-1` answered 404 and the fills list was empty, while the balance
    carried over;
  - `b-1` and `s-1` were accepted again and traded, with execution id `exec_2`;
  - the market-data book showed the new day's resting order;
  - the report had one `b-1` row per day, trades marked with their days, and its checkpoint on the
    new day;
  - with an interval of 1,000 commands, the warm replica wrote exactly two snapshots, one right
    after each open. The last, at the second open, was 653 bytes;
  - after `SIGKILL`, the exchange restarted from that snapshot with day two intact.
- **An independent review** found no high-severity bug. It confirmed:
  - replay, snapshot recovery, warm-replica following and promotion agree across
    open, close and open;
  - nothing is still keyed by order id alone across days;
  - the reporter handles the day correctly in every case it checked.

  Its findings, all fixed:
  - the rejected tables lacked their day (medium);
  - the "keep what rests" safety claim;
  - a stale comment about client order ids, and the matching API advice;
  - a missing test for a snapshot taken before an open;
  - several stale doc statements;
  - two operational notes, below.

### Upgrading

A journal from before milestone 22 does not replay, so:
- start a new journal;
- remove the old snapshot file (`EVENT_SNAPSHOT_PATH`) and the market-data state file with the old
  journal. A snapshot bound to another journal, or of an older format, is kept for diagnosis and
  turns snapshot writing off. A market-data checkpoint for another journal refuses startup;
- apply the reporter migrations in order. Each empties the report, and the reporter rebuilds it.

The review also noted a growth that predates this milestone: every symbol ever traded keeps an
empty book with its execution counter, in memory and in every snapshot. There is no instrument
registry to limit symbols, so this is recorded as a known limitation.

## What is left

- **Part 4:** a multi-day benchmark, before and after. Measure memory, snapshot size and write
  time, warm-replica lag and restart time per day, against the milestone 21 binary on the same
  order volume.
