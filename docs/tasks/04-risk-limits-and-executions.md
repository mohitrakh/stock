# Task 04 — Risk Limits, Executions, and Ledger Cleanup

**Date:** 2026-09-16
**Status:** Complete. `cargo fmt -- --check` clean, `cargo test` 70 passed, verified live including a
restart.
**Milestone before this:** Sell-Side Positions (`docs/tasks/03-sell-side-positions.md`).
**Milestone after this:** Not selected — see section 7.

This task closes out the specification's remaining unmet requirements, plus the small correctness
gaps that had accumulated in the limitations list. Three pieces:

- **A.** Risk limits actually enforced, with a trading day that survives replay.
- **B.** `GET /execution` — the last read the design document specifies.
- **C.** Three ledger and settlement gaps that had been flagged and deferred.

---

## Part A — Risk limits

### What was wrong

`RiskManager` existed, was tested, had a `RiskRejected` error variant, and was called from
`prepare_order`. It rejected nothing. `set_limit` was never called from the live path, and with no
limit configured, `check` returned `Ok` unconditionally. `record` did not even track volume unless a
limit already existed.

This is the same shape as the problem milestone 7 was created to fix: a component that exists,
compiles, passes tests, and cannot influence anything. It is worse than a missing feature, because
the architecture reads as though the requirement is handled.

The requirement is not optional in the source design:

> "the exchange is a regulated facility, so we need to make sure it runs risk checks"
> "a user can only trade a maximum of 1 million shares of Apple stock in one day"

### The interesting problem: what is "a day"?

A daily cap needs a day boundary, and the obvious implementation destroys the event log.

If `RiskManager` clears its counters based on `SystemTime::now()`, then replaying a log tomorrow
produces different state than it did today. Orders that were accepted start being rejected, replay
fails with an `OutputMismatch`, and the exchange refuses to start on its own history. The durable log
built in milestone 8 would have been quietly undermined by the next feature added on top of it.

The fix is to take the day from the data rather than the clock. `Order.timestamp` already exists, is
already stamped by the gateway, and is already serialized inside `NewOrderRequested` — so it is
already in the log. `roll_day` converts it to a day number and clears the counters when that number
advances:

```rust
fn day_of(timestamp: f64) -> i64 {
    (timestamp / SECONDS_PER_DAY).floor() as i64
}
```

Nothing in the risk path consults the system clock, so the same log rebuilds the same state on any
future date, on any machine.

It only rolls **forward**. A timestamp that jumps backwards across a boundary — clock jitter at the
gateway — is ignored rather than handing the allowance back a second time.

### The same trap, one level up: where limits come from

The identical problem applies to the limits themselves. A limit read from an environment variable or
a config file would make replay depend on the environment: the same log would rebuild different
state on a differently configured box, and a limit changed between runs would silently rewrite
history.

So limits are **events**. `RiskLimitSetRequested` / `RiskLimitSet` join the input and output event
types, with `ExchangeCommand::SetRiskLimit` and `POST /exchange/risk/limits`. This also makes
`set_limit` reachable for the first time.

The fallback when nobody has set anything is `DEFAULT_MAX_DAILY_QUANTITY = 1_000_000` — the design
document's own figure — as a compiled-in constant. A constant is deterministic in a way that
configuration is not, and it means the documented cap applies out of the box rather than only after
someone remembers to configure it.

### Two ambiguities in the specification, and how they were resolved

**Submitted or traded volume?** The document contradicts itself: line 30 says "trade a maximum of 1
million shares", line 182 says "trade volume is below $1M a day" — shares in one place, notional in
the other.

Resolved as **shares**, because line 30 is the requirements interview (the authoritative statement)
while line 182 is a parenthetical in a later deep-dive. And counted at **submission**, not at fill,
because the design document places the risk check before matching:

> "The order manager performs risk checks... After passing risk checks, the order manager verifies
> there are sufficient funds"

A pre-trade check cannot count fills that have not happened yet. Counting submissions also closes
the obvious hole: if only fills counted, a user could submit unlimited orders and blow past the cap
as they filled.

**Does cancelling give the allowance back?** Yes. Without it, placing and cancelling would burn the
day's allowance on shares that never traded, which contradicts the requirement's own wording
("trade a maximum of"). With it, the counter means *traded today, plus currently at risk of
trading*: filled quantity is never returned, open quantity is. `complete_cancel` now releases risk
allowance alongside the cash or shares it was already releasing — a third ledger following a shape
already established twice.

### Other fixes in `RiskManager`

`current_volume + incoming_qty` was an unchecked add on `u64`. Now `checked_add`, treated as a limit
breach rather than a wrap. The unused `check_and_record` convenience method was deleted.

---

## Part B — `GET /exchange/executions`

The last read in the design document's API section. Filters are all optional: `symbol`, `order_id`,
`start_time`, `end_time`, with inclusive time bounds on the same epoch-seconds scale as an order's
`creation_time`.

### Where the index lives, and why not the event log

`ExecutionCreated` events are already in the durable log, so an execution query could have been
served by scanning it. That would be the design document's reporter pattern — a projection over the
event store — and it is genuinely where this belongs eventually.

It was not done that way here, for two reasons. Scanning the whole log per request is O(history) on
every call. More importantly, the log lives in `ExchangeRuntime` and `ExchangeCore` cannot see it, so
a projection would mean introducing a subscriber component — a real architectural move that belongs
to the market-data and reporter milestone rather than being smuggled in under an API endpoint.

Instead `OrderManager` builds the index as settlement runs, which is the natural place: it is the
only layer that knows which party was on which side of each match.

That choice has a pleasant consequence. Because the index is built by `apply_execution`, and
`apply_execution` runs again during replay, execution history is rebuilt on restart for free with no
extra persistence. Verified rather than assumed.

### One match, two records

A single match produces two `ExecutionView` rows — one per party — differing in `order_id` and
`side`, because "which side was I on" is a property of the viewer, not of the trade. The design
document's response schema has exactly these fields, which is what gives the shape away.

Only your own fills are visible.

---

## Part C — Ledger and settlement cleanup

Three items that had been sitting in the limitations list:

**`Wallet::deposit` could overflow silently.** It was a bare `*entry += amount`, which panics in a
debug build and wraps a balance to near zero in a release build. Now `checked_add` returning
`Result`, matching `Positions::credit`, which has done this since it was written. This rippled: a
deposit can now fail, so `FundsDepositRejected` joins the output events, mirroring
`SharesDepositRejected`.

**`apply_executions` silently dropped an odd trailing execution.** It read
`for chunk in executions.chunks(2) { if chunk.len() == 2 { ... } }` — so if the matching engine ever
emitted an odd number of records, the last fill would never settle, with no error anywhere. The
invariant (two records per match, one per side) is real, but silently tolerating its violation meant
a fill could vanish. It is now an error.

**42 `unused_must_use` warnings in tests.** Changing `deposit` and `handle_command` to return
`Result` left dozens of statement-position calls ignoring them. All now `unwrap()` — in a test, a
failed deposit should fail the test loudly, not pass silently. Warning count went from 46 to 4, and
the four that remain are genuinely dead pre-existing code.

---

## 4. Files changed

```
src/types/risk_manager.rs      rewritten: day roll from event timestamps, default limit,
                               release on cancel, checked arithmetic, own tests
src/types/wallet.rs            deposit returns Result on overflow
src/types/order_manager.rs     execution index, risk release on cancel, strict pair check,
                               deposit_funds / set_risk_limit / execution_views / risk_limit_view
src/types/exchange_event.rs    + RiskLimitSetRequested / RiskLimitSet / FundsDepositRejected
src/types/types.rs             + ExecutionView, RiskLimitView, 3 commands
src/exchange/core.rs           passthroughs; deposit now fallible
src/exchange/runtime.rs        risk-limit input event, 2 query commands, 1 mutating command
src/controllers/exchange_controller.rs   3 handlers
src/routes/exchange_routes.rs  + /executions, + /risk/limits (GET and POST)
```

New endpoints:

| Method | Path | Notes |
|---|---|---|
| `GET` | `/exchange/executions?symbol=&order_id=&start_time=&end_time=` | all filters optional |
| `POST` | `/exchange/risk/limits` | `{symbol, max_daily_quantity}` |
| `GET` | `/exchange/risk/limits?symbol=` | limit and usage today |

---

## 5. Verification

### Tests: 57 → 70

Six on `RiskManager` directly, including the two that matter most:

| Test | What it pins |
|---|---|
| `the_day_rolls_from_the_order_timestamp_not_the_clock` | the determinism trap — nothing reads the clock |
| `a_backwards_timestamp_does_not_reset_the_day_again` | jitter cannot refund an allowance |
| `the_default_limit_is_the_documented_one` | the documented 1M applies unconfigured |
| `cancelling_returns_the_unfilled_allowance` | the counter means what the requirement says |
| `volume_accumulates_until_the_limit_is_reached` | the boundary either side of the cap |
| `limits_are_per_user_and_per_symbol` | no cross-contamination |

Plus, at the core and runtime level: limit rejection is pre-trade (no reservation taken, no sequence
consumed), the default applies unconfigured, cancelling refunds, executions are recorded for both
sides with their own side and order id, execution filters work, a rejected order is recorded so
replay reproduces the rejection, and — the payoff —
`risk_limits_and_executions_survive_a_restart`.

### Live, over HTTP

Risk limits:

```
GET  /risk/limits?symbol=AAPL   -> {"max_daily_quantity":1000000,"used_today":0}   (default)
POST /risk/limits {AAPL, 10}    -> 200
buy 6   -> 201,  used_today 6
buy 5   -> 400   RiskRejected("LimitExceeded { current_volume: 6, limit: 10 }")
buy 4   -> 201   (exactly fits)
cancel  -> used_today back to 6
GET  /risk/limits?symbol=MSFT   -> {"max_daily_quantity":1000000,"used_today":0}   (untouched)
```

Executions, the same match seen from both sides:

```
alice: [{"execution_id":"exec_0","order_id":"a1","side":"buy", "quantity":6,...}]
bob:   [{"execution_id":"exec_0","order_id":"b1","side":"sell","quantity":6,...}]
?symbol=MSFT -> []      ?order_id=a1 -> 1 row      ?start_time=9000000000 -> []
```

Then a hard kill and restart — 18 events recovered:

```
alice risk limit: {"symbol":"AAPL","max_daily_quantity":10,"used_today":6}
alice fills:      [{"execution_id":"exec_0","order_id":"a1","side":"buy",...}]
bob   fills:      [{"execution_id":"exec_0","order_id":"b1","side":"sell",...}]
```

The limit of 10 came back because it was set by an event. A limit read from configuration could not
have survived that, and would have replayed differently on a differently configured machine. The
fills came back without any persistence of their own, because settlement runs again during replay.

---

## 6. What this task did *not* do

- **Admin authorisation.** `POST /exchange/risk/limits` sets the *caller's own* cap, so a trader can
  raise their own limit. A real exchange makes this a compliance action. It is a placeholder in the
  same spirit as letting anyone deposit themselves unlimited cash or shares.
- **Notional limits.** The cap counts shares, per the requirements interview. The deep-dive's "$1M a
  day" phrasing would need a separate notional counter.
- **Execution queries as a projection over the event store.** Deliberately deferred to the
  market-data and reporter milestone — see Part B.
- **Cross-symbol or portfolio-level risk.** Limits are per `(user, symbol)`.
- **A real trading calendar.** "Day" is a UTC 86,400-second bucket. No market hours, no weekends, no
  holidays — the design document explicitly scoped after-hours trading out.
- **Rollback on a failed settlement leg.** Still the open problem noted in milestone 8.

Dead-code warnings: 7 → 4, and all four are pre-existing (`get_order_leaves`, `subscribe`,
`PriceLevel.price`, `peek_front`/`pop_front`).

---

## 7. What comes next

Every functional requirement in the design document's API section is now implemented. What remains
is the architecture beyond the critical path, and it is all downstream of the event store that
milestone 8 built:

1. **Market data publisher.** Candlestick charts and L2 publishing, as a subscriber to the event
   store. The design document's version uses ring buffers and multicast, both currently on the
   do-not-start list — so the first step would be the subscriber boundary itself, not the
   optimisations.
2. **Reporter.** Trading history and compliance records written to PostgreSQL off the critical path.
   The database connection already exists and is used only for authentication.
3. **Hot-warm matching engine.** The design document's high-availability answer: a warm instance
   consuming the same events and taking over on failure.

All three are the same architectural move — a component that subscribes to the event store and keeps
its own state — which is why Part B's execution index was deliberately *not* built that way. Doing it
once, properly, as its own milestone is worth more than three ad-hoc versions.

Discuss before starting, per the repository's own rule.
