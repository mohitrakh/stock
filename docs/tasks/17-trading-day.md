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

## What is left

- **Part 2:** at the close, every resting order expires (`OrderExpired`), releasing its cash or
  shares and its risk exposure. The decoder, the market-data publisher and the reporter learn the
  expiries.
- **Part 3:** the next open clears the previous day's finished orders and fills from memory.
  - Client order ids become unique per trading day.
  - The reporter keys orders by `(trading_day, order_id)`, which needs a migration.
  - The warm replica snapshots right after each open.
- **Part 4:** a multi-day benchmark, before and after.
