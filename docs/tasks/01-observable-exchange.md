# Task 01 — Observable Exchange

**Date:** 2026-09-16
**Status:** Complete. `cargo fmt -- --check` clean, `cargo test` 38 passed, verified live over HTTP.
**Milestone before this:** Deterministic in-memory replay (+ event serialization checkpoint).
**Milestone after this:** Durable event log and startup recovery.

---

## 1. Why this task existed

This task was not on the roadmap. The selected next milestone was *Durable Event Log and Startup
Recovery*. It got displaced after an audit of the running system, and the reason it got displaced
is the most important thing in this document.

The audit started from the compiler's own dead-code warnings:

```
warning: function `replay_event_log` is never used
warning: associated items `from_event_log` and `event_log` are never used
warning: methods `best_bid_ask`, `is_resting`, and `get_order_leaves` are never used
warning: methods `best_bid` and `best_ask` are never used
warning: methods `get_state` and `subscribe` are never used
warning: methods `set_limit` and `check_and_record` are never used
warning: methods `balance`, `locked`, `available`, and `commit_fill` are never used
warning: struct `BalancedResponse` is never constructed
```

Every one of those is real work from a completed milestone, and none of it was reachable from the
running binary. The deployed exchange was three POST endpoints over in-memory state:

```
POST /exchange/deposit
POST /exchange/orders          -> 201, body = a bare uuid string
POST /exchange/orders/cancel
```

A client could place an order and then had **no way to ever learn what happened to it**. No balance
query, no order status, no order book. The response body did not even say whether the order filled.
The target design (`stock-exchange-system-design.md`) specifies a response carrying
`filledQuantity`, `remainingQuantity` and `status`; that had never been built.

This is the pattern the task exists to break: the project had been adding architectural depth to a
system with no observable surface. Building durable storage next would have added a seventh layer of
unreachable machinery, and — worse — the durable-log milestone's own acceptance test says to
"verify that balances, locked funds, order state, order-book state ... continue correctly" after a
restart. None of those four things could be read from a running exchange. The milestone could not
have been honestly verified even once it was written.

**So: you cannot prove persistence works until you can see the thing being persisted. Observability
comes first.**

### The second reason: a verified matching bug

While auditing, self-trade prevention in `order_book.rs` turned out to be broken. The code was:

```rust
if sell_resting.user_id == order.user_id {
    break;  // exits the ENTIRE matching loop
}
```

`SYSTEM_DOCUMENTATION.md` described this as "skips matches if user IDs are the same." It did not
skip — it stopped matching altogether. A temporary probe test confirmed the consequence:

```
executions=0 bid=100 ask=100 crossed=true
```

Setup: Alice rests a sell at 100. Bob rests a sell at 100 behind her in the same price level's FIFO
queue. Alice sends a buy at 100. It should match **Bob** — a completely legitimate counterparty.
Instead it matched nobody and rested as a bid at 100, leaving the book **crossed**: a bid of 100
sitting next to an ask of 100, permanently, until someone cancels.

A crossed book is the one state a matching engine must never be in. This lived inside Milestone 1,
"core order lifecycle correctness," which was marked complete. It shipped in the same task as the
read APIs because the read APIs are what make it *visible* — `GET /orderbook` is how you catch a
crossed book, and there was no such endpoint.

---

## 2. What was built

### New endpoints

| Method | Path | Auth | Returns |
|---|---|---|---|
| `GET` | `/exchange/balance` | required | `BalanceView` — balance, locked, available |
| `GET` | `/exchange/orders/{order_id}` | required, owner only | `OrderView` |
| `GET` | `/exchange/orderbook/{symbol}?depth=N` | **public** | `OrderBookView` — L2 depth |

### Changed endpoint

`POST /exchange/orders` now returns the full `OrderView` as JSON instead of a bare uuid string:

```json
{"order_id":"33383b4f-...","symbol":"AAPL","side":"buy","price":100,
 "quantity":5,"filled_quantity":5,"remaining_quantity":0,
 "status":"filled","creation_time":1789541150.0}
```

`status` is one of `new`, `partially_filled`, `filled`, `canceled`. The design doc lists only
`new`/`canceled`/`filled`; `partially_filled` is exposed too because the engine genuinely has that
state and collapsing it would lie to the client.

### Fixed

- Self-trade prevention now skips the aggressor's own resting orders and keeps matching.
- `POST /exchange/orders/cancel` returns **404** for an unknown order. It previously compared the
  error against the literal string `"Order not found"`, but the runtime formats errors with
  `format!("{:?}", err)`, which produces `OrderNotFound("...")`. The comparison could never match,
  so every missing-order cancel returned 400.

---

## 3. How it was built, and why each decision went the way it did

### 3.1 Reads travel the command channel, but never enter the event log

Exchange state is owned by one worker thread. A read cannot touch it directly, so query variants
were added to `ExchangeCommand`:

```rust
GetBalance   { user_id, respond_to: oneshot::Sender<BalanceView> },
GetOrder     { order_id, user_id, respond_to: oneshot::Sender<Option<OrderView>> },
GetOrderBook { symbol, depth, respond_to: oneshot::Sender<Option<OrderBookView>> },
```

**The decision that matters:** these are `ExchangeCommand` variants and deliberately have **no**
corresponding `ExchangeInputEvent`. In `handle_command` they are answered straight from the core and
never call `record_and_process_input_event`.

The reasoning is the project's existing boundary, applied consistently. `ExchangeCommand` is live
gateway plumbing that carries a response channel; `ExchangeEvent` is replayable business fact. A
read changes no state, so recording it would mean every replay after that point re-processes
thousands of lookups that cannot change a single outcome — a permanently slower recovery in exchange
for zero information. Determinism does not require logging reads; it requires logging everything
that *mutates*.

This is enforced by a test rather than left as a convention:
`queries_do_not_append_to_the_event_log` places an order, records the log length, fires all three
queries, asserts they answered, and asserts the log length is unchanged.

### 3.2 The order response is read back from `OrderManager`, not summed from executions

`AddOrderOutcome` already carried `executions`, so filled quantity could have been computed by
summing them. It is not, for a specific reason: **executions arrive in duplicated pairs.** One match
pushes two `Execution` records that are identical except for `execution_id` (the design doc's "one
fill for the buy side, one for the sell side"). Summing all of them double-counts every fill.
`OrderManager` already tracks `remaining_quantity` as the single authority, so `add_order` now reads
the view back from it after settlement:

```rust
let view = self.order_manager
    .order_view(&order_id, &user_id)
    .expect("order was registered above, so its view must exist");
```

`expect` is deliberate. `register_order` inserted this exact id moments earlier with this exact
user, so `None` is unreachable; an `expect` documents the invariant instead of inventing an error
path that can never be taken.

The view rides the **live reply only**. `ExchangeOutputEvent::OrderAccepted` is unchanged, so a
log written before this task still replays identically. That was a hard constraint: this task must
not invalidate the replay work that preceded it.

### 3.3 Non-owners get 404, not 403

`order_view(order_id, user_id)` returns `None` when the order exists but belongs to someone else —
the same answer as a missing order. A 403 would confirm that an order id exists, letting a caller
enumerate other users' order ids. 404 leaks nothing.

Verified live: Bob requesting Alice's order id gets `HTTP 404`.

### 3.4 Market data is the one public endpoint

`GET /exchange/orderbook/{symbol}` requires no auth, while every other exchange route does. This
follows the target design, which treats market data as a public resource (its DDoS section is
entirely about protecting public market-data endpoints). The L2 view is aggregate — price and total
size per level — and carries no order ids and no user identity, so there is nothing in the response
to protect.

`depth` defaults to 10 and is clamped to 50. That cap is a response-size bound on an unauthenticated
endpoint, not a correctness rule; a client asking for 9,999 levels gets 50.

### 3.5 The self-trade fix, and why the loop had to be restructured

The obvious fix — change `break` to `continue` — deadlocks. The loop re-reads "the best price level"
on every pass:

```rust
while order.leaves_qty > 0 {
    let best_ask_price = self.sell_levels.first_key_value() ... ;
    let resting = level.peek_front_mut() ... ;
```

It only terminates because a filled resting order gets removed and an emptied level gets dropped,
so `first_key_value()` eventually advances. If matching skips an order instead of consuming it, that
order stays, the level stays, `first_key_value()` returns the same level forever, and the loop spins.

Two changes fix it:

**`PriceLevel::first_matchable_mut(exclude_user)`** walks the level's linked list from the head and
returns the first order not owned by the aggressor, instead of blindly returning the head. This is
what makes "skip" possible at all — the old `peek_front_mut` could only ever see position zero, which
is exactly why a single self-order at the front could hide everyone behind it. (`peek_front_mut` had
no other callers and was deleted.)

**`match_order` now snapshots the crossing prices before walking them:**

```rust
let crossing_prices: Vec<Price> = match order.side {
    Side::Buy  => self.sell_levels.range(..=order.price).map(|(p, _)| *p).collect(),
    Side::Sell => self.buy_levels.range(..=Reverse(order.price)).map(|(r, _)| r.0).collect(),
};
```

`range(..=order.price)` on the ask side gives every ask at or below the limit, ascending — best ask
first. On the bid side the keys are `Reverse(Price)`, so `..=Reverse(order.price)` gives every bid at
or above the limit, and ascending-in-`Reverse` is descending-in-price — highest bid first. Both are
correct price priority, and because the list is fixed up front, a level that cannot be consumed is
simply moved past instead of retried. Matching within one level moved into `match_at_level`.

The price snapshot allocates one `Vec` per aggressive order. That is marked in the source with a
`ponytail:` comment naming the ceiling and the upgrade path (an in-place `BTreeMap` cursor) so the
shortcut is tracked rather than forgotten.

**Known limitation, deliberately not fixed here.** Skip-based STP cannot prevent a user from crossing
*against themselves*. If the only order at the best ask is Alice's and Alice bids at that price, her
bid rests and the book is crossed — against her own order, by both orders being hers. Removing that
requires cancelling one of the two sides, which means the engine generating cancellations, which
means new output events and a change to replay. That belongs in its own task, not smuggled into this
one. The bug that was actually costing correctness — a self-order hiding a *valid counterparty* — is
gone.

### 3.6 A shared `ask` helper in the controller

Six handlers each need the same three failure lines: build a `oneshot`, send on the bounded queue,
await the reply. One generic helper replaced all of it:

```rust
async fn ask<T>(state: &AppState,
                make_command: impl FnOnce(oneshot::Sender<T>) -> ExchangeCommand)
    -> Result<T, AppError>
```

This was worth abstracting only because it *removed* more lines than it added — the controller's
three original handlers got shorter even as three new ones appeared.

---

## 4. Files changed

```
src/types/price_level.rs        + first_matchable_mut, - peek_front_mut
src/types/order_book.rs         match_order restructured, + match_at_level, + l2_snapshot
src/types/matching_engine.rs    + l2_snapshot (symbol -> OrderBookView)
src/types/order_manager.rs      + order_view, + balance_view, + OrderState::as_str
src/types/types.rs              + BalanceView, OrderView, L2Level, OrderBookView
                                + 3 query variants on ExchangeCommand
                                PlaceOrder reply: Result<String,_> -> Result<OrderView,_>
src/exchange/core.rs            + balance_view / order_view / l2_snapshot passthroughs
                                AddOrderOutcome gains `view`
src/exchange/runtime.rs         query commands routed to core, bypassing the event log
src/controllers/exchange_controller.rs
                                + ask helper, + 3 GET handlers, JSON order response
                                cancel 404 mapping fixed, - dead BalancedResponse
src/routes/exchange_routes.rs   + 3 GET routes (axum 0.8 `{param}` syntax)
```

644 insertions, 200 deletions across 9 files.

---

## 5. How it was verified

### Tests: 30 → 38

New tests, and what each one would catch if it broke:

| Test | Catches |
|---|---|
| `self_trade_prevention_skips_own_order_and_fills_the_next_user` | the original bug returning |
| `aggressor_never_rests_across_a_matchable_counterparty` | a crossed book forming |
| `a_level_of_only_own_orders_does_not_block_a_worse_level` | the infinite-loop / skip-past-level case |
| `views_serialize_to_the_documented_json_shape` | any silent change to the wire contract |
| `queries_do_not_append_to_the_event_log` | reads polluting replay history |
| `place_order_reply_reports_fill_state` | the order response losing fill information |
| `a_users_order_is_not_readable_by_anyone_else` | the ownership check regressing |
| `routes_build` | axum path syntax errors (these panic at route-add time, not at request time) |

The JSON test asserts the exact serialized string rather than field-by-field, so a renamed or
reordered field fails loudly instead of silently changing the client contract.

### Live HTTP verification

The exchange was run against a disposable `postgres:16` container (`exchange` database, users
migration applied), two users registered and logged in, and every endpoint exercised. The container
was removed afterwards and no existing database was touched.

The decisive sequence:

```
alice SELL 5@100   -> {"status":"new","remaining_quantity":5}        (rests, front of queue)
bob   SELL 5@100   -> {"status":"new","remaining_quantity":5}        (rests behind alice)
alice BUY  5@100   -> {"status":"filled","filled_quantity":5}        <- matched BOB
GET /exchange/orderbook/AAPL
                   -> {"bids":[],"asks":[{"price":100,"quantity":5}]}
```

Before the fix that buy returned zero executions and left `bid=100 ask=100 crossed=true`. After it,
the buy fills against Bob and only Alice's own ask remains — no bid at all, nothing crossed.

Also confirmed live: balance arithmetic through a fill (100000 → 99500 after buying 5 @ 100), lock
release on cancel (locked 1000 → 400), `status: "canceled"` after cancelling, 404 for another user's
order id, 404 for an unknown symbol, 404 for cancelling a non-existent order, and `depth=9999`
clamping without error.

---

## 6. What this task did *not* do

Deliberately out of scope, listed so they are not mistaken for oversights:

- **Sell-side inventory.** There is still no share ledger. A user with no position can sell shares
  they do not own and be credited cash. This is the largest remaining correctness hole in the
  system and deserves its own task.
- **Risk limits.** `set_limit` is still never called from the live path, so the design doc's
  "1M shares per day" rule remains unenforceable, and there is no day-boundary concept.
- **Self-crossing under STP** (section 3.5).
- **`GET /execution`.** The design doc specifies an executions query; orders and book came first
  because they are what the durable-log milestone needs to verify.
- **Two sources of truth for quantity.** `Order.leaves_qty` (the matching engine's copy) and
  `ManagedOrder.remaining_quantity` (the order manager's) are still updated independently.
- **`apply_executions` pair assumption.** It still does `chunks(2)` and silently ignores an odd
  trailing execution rather than erroring. Safe today only because the book always pushes pairs.

Dead-code warnings went from 12 to 10. The ones that remain are mostly the replay machinery
(`replay_event_log`, `from_event_log`, `event_log`) — that is precisely what the next milestone
wires up.

---

## 7. What comes next

**Durable event log and startup recovery**, as originally planned — now verifiable. Its acceptance
example ("deposit, partial fill, stop the application, start it again, cancel the resting quantity,
verify balances, locked funds, order state, order-book state and both sequence counters") can now be
executed as a sequence of curl calls against a restarted server rather than as another unit test.
That is the difference this task was meant to make.

After that, in rough priority order: sell-side positions, then real risk limits, then `GET /execution`.
