# Part 2 — The private feed

**Status: complete on 2026-10-20.** Code, tests, mutation check (three runs), an independent
code review and four verifier rounds (the first of them reviewing alongside the code review), the
PostgreSQL acceptance tests.

## What this part is

A new process, `stock --user-feed JOURNAL STREAM STATE_FILE [LISTEN_ADDR]` (default
`127.0.0.1:4006`), follows the committed stream exactly as the market-data process (MDP) does. It
pushes to each authenticated user, over a WebSocket at `GET /userfeed/ws`, that user's own fills and
order changes: accepted, partially filled, filled, canceled, expired at the close, rejected, and
refused cancellations.

The design and its reasons are in `01-theory.md`, sections 2, 5, 6 and 7, and the owner's
decisions 2, 3, 4 and 5 (`decisions.md`): its own process, disconnecting slow consumers, the token
in the first message, no replay.

**Why its own process (decision 2).** The MDP's state holds no user id, by rule since milestone 14,
and a test checks its state file for one. The private feed needs user ids. Keeping it out of the
MDP keeps that rule simple, and keeping it out of the primary keeps a slow client or a memory
problem off the trading process.

## Who is told what

A fill names a buy order id and a sell order id, never a user (`ExecutionCreated`). The batch that
fills a resting order names only the incoming order's user (`NewOrderRequested`). So the process
keeps its own **owner map**: every open order, by order id, with its user, symbol, side, price,
quantity and remaining quantity. It is built from accepted orders only.

For each committed batch (`committed_batch::decode`):

| Batch | Who is told | Messages, in order | The map |
|---|---|---|---|
| New order, accepted, no fill | the user in the input | `order` `new` | the order is added |
| New order, accepted, with fills | per fill: the incoming user, then the resting order's owner from the map | per fill: `fill` to the incoming user; `fill` to the resting owner; `order` to the resting owner (`partially_filled` or `filled`). Then one `order` to the incoming user (`partially_filled` or `filled`) | each resting order loses the fill; a filled one is removed; the incoming remainder, if any, is added |
| New order, rejected | the user in the input | `order` `rejected` with the reason | **untouched** |
| Cancellation, done | the owner from the map | `order` `canceled` | removed |
| Cancellation, refused | the requester in the input | `cancel_rejected` with the reason | untouched |
| Market close | each expired order's owner from the map | `order` `expired`, one per order, oldest first | emptied |
| Anything else (open, deposits, risk limits, a term) | nobody | — | untouched |

**Why a rejection never consults the map.** Clients choose order ids, and the exchange keeps them
unique within a trading day across all users (`OrderManagerError::AlreadyExists`). Mallory can
therefore submit an order with the id of Alice's live order. The exchange rejects it. If the feed
looked that id up in the map, it would tell Alice about Mallory's rejection, and tell Mallory
nothing. The rejection is Mallory's, and the input names her (`01-theory.md` §7).

**Why a refused cancellation goes to the requester.** The same argument: Mallory may ask to cancel
Alice's order. The exchange refuses (`cancel_order_for_user` checks the owner). Mallory is told;
Alice is not, since nothing happened to her order.

**Drift is an error, never a guess.** Each of these means the map no longer matches the exchange,
and the batch fails:
- an accepted order whose id is already open;
- an accepted order whose initial remaining quantity is not its quantity;
- a fill whose incoming side, symbol or quantity does not match the incoming order;
- a fill naming a resting order the map does not hold, on the wrong symbol or side, or at another
  price than the resting order's (the engine trades at the resting price);
- a fill larger than either order's remaining quantity;
- a done cancellation of an order the map does not hold, or whose owner is not the requester;
- an expiry of an order the map does not hold;
- any order left in the map after a close.

A failed batch withdraws the whole view (`*served = None` under the write lock): `/health` answers
503, every connection closes with 1011, and the error is terminal. Nothing half-applied is ever
delivered or saved: events are delivered only after the whole batch applied.

**Both sides of a fill are different users.** The engine skips the aggressor's own resting orders
(self-trade prevention, `01-theory.md` §7), so the two `fill` messages of a trade go to two users.
The feed does not depend on it: if both sides were one user, that user would get both messages.

## The protocol

All messages are JSON text frames. The client's first message, within 5 s (`AUTH_DEADLINE`):

```json
{"type":"auth","token":"<the JWT from POST /auth/login>"}
```

The server answers:

```json
{"type":"ready","seq":1000}
```

Then, for example, a buy of 4 against Alice's resting sell of 5 at 101 (each user sees only their
own lines):

```json
{"type":"fill","seq":1006,"execution_id":"exec_16","order_id":"sell-1","symbol":"AAPL","side":"sell","price":101,"quantity":4,"remaining":1}
{"type":"order","seq":1006,"order_id":"sell-1","symbol":"AAPL","side":"sell","price":101,"quantity":5,"filled":4,"remaining":1,"status":"partially_filled"}
```

Other messages:

```json
{"type":"order","seq":1010,"order_id":"o9","symbol":"AAPL","side":"buy","price":99,"quantity":3,"filled":0,"remaining":0,"status":"rejected","reason":"..."}
{"type":"cancel_rejected","seq":1012,"order_id":"sell-1","reason":"..."}
{"type":"error","message":"this feed takes no requests after auth"}
```

- `seq` is the last envelope sequence of the batch the message comes from. Every message after
  `ready` has a `seq` greater than `ready`'s. Several messages can share one `seq`: they come from
  one batch.
- `ready`'s `seq` is the last sequence the feed had applied when the connection joined
  (`View.applied.next_sequence − 1`). The join happens under the view's read lock, and the follower
  delivers each batch under the write lock, so no batch falls between the two: everything after
  `ready` arrives, and nothing before it (`01-theory.md` §4, the same pairing as the book feed).
- `fill`: `execution_id` is the first of the trade's two execution events, the one
  `GET /exchange/executions` returns. Execution ids count per symbol, so `(symbol, execution_id)`
  identifies a trade. `side`, `order_id` and `remaining` are the receiving user's own. `price` is
  the trade's price.
- `order`: the order's state after the batch, with the statuses of the REST `GET /exchange/orders/{id}`
  (`new`, `partially_filled`, `filled`, `canceled`, `expired`) plus `rejected`. For a rejection,
  `filled` and `remaining` are 0: the order never existed.
- After `auth`, any text or binary frame is answered with `error`, and the connection stays open.

**Nothing is replayed (decision 5).** A client that reconnects gets a new `ready` and only what
follows. Anything it missed it reads once through REST (`GET /exchange/executions`,
`GET /exchange/orders/{id}`), deduplicating fills on `(symbol, execution_id)`. The limits of that
catch-up are listed with decision 5: REST holds one trading day, its replies carry no stream
sequence, and an order is looked up by id.

**Closing codes:**
- 1008 (RFC 6455 "policy violation"): the first message was not a valid `auth` (an `error` message
  says why first), no first message came within 5 s, or the token expired;
- 1011 (RFC 6455 "internal error"): the follower halted;
- 1013 (IANA "try again later"): the client fell behind (its queue of 1,024 messages filled, its
  ring of 1,024 overran, or one socket write missed its 5 s deadline). The ring is a quarter of the
  book feed's 4,096, because tokio allocates a ring's slots whole and every connected user has
  one: about 60 KB per user instead of about 240 KB (an estimate, at about 60 bytes a slot). One
  user's events are few, except for one order that trades with many resting orders: a batch giving
  one user more events than the ring holds before the forwarder moves them on (an inference: an
  order with more than about 1,000 fills, each one event for its user; the engine allows up to
  10,000) closes that user with 1013, and the client reads REST;
- 1001 (RFC 6455 "going away"): nothing came from the client for more than three ping intervals
  (15 s each).

The last three, and the closing handshake, are Part 1's, shared through `book_feed.rs`.

## Authentication

**The token is checked like the gateway's.** `verify_token(token, key)` is the function the HTTP
extractor `AuthUser` now calls too: `jsonwebtoken::decode` with `Validation::default()`, which
checks the HS256 signature and requires `exp`. The process refuses to start without `JWT_SECRET`
(`user feed: JWT_SECRET must be set`), as the exchange and the warm replica do. So a token means
the same user to both.

**One difference: no leeway at the start.** `Validation::default()` accepts a token up to 60 s past
its `exp` (jsonwebtoken's default leeway), and the gateway keeps that. A feed connection lasts only
until `exp`, so a token already past it would be served for no time at all: `ready`, then 1008 at
once, and a client retrying with the same token would loop. The feed refuses such a token at
`auth` instead, with `invalid or expired token`.

**The size of a client's messages.** The only request is `auth`, a few hundred bytes. Axum's
default limit is 64 MiB per message, which would let an unauthenticated client make the server
buffer that much per connection before any check. The route sets 16 KiB for a message and a frame
(`MAX_CLIENT_MESSAGE`); a larger one ends the connection unread.

**Why the first message (decision 4).** A token in the URL ends up in access logs and histories.
A browser cannot set an `Authorization` header on a WebSocket handshake. The first message travels
only inside the encrypted (with TLS) or loopback connection.

**The deadline.** A connection that sends nothing, or only pings and pongs, for 5 s is closed with
1008 "authenticate first". It holds no ring and no queue meanwhile: the user is not known yet.

**Expiry.** The gateway's tokens last 24 h (`POST /auth/login`). An HTTP request is checked once; a
connection can outlive its token. The connection keeps a timer to the token's `exp` and closes with
1008 "token expired" when it fires. `exp` counts whole seconds.

## What changed in the code

### New: `src/exchange/user_feed.rs`

- **`OpenOrder`**: one entry of the owner map.
- **`Event`**: the three pushed messages (`order`, `fill`, `cancel_rejected`), serialized with
  `"type"` as the tag.
- **`Owners`**: the owner map.
  - `apply_batch(batch)` returns the list of `(user, event)` pairs, in order, or an error for drift.
    It implements the table above.
  - `from_orders` (loading a saved state) refuses an empty order id or user id, a remaining
    quantity of 0 or above the quantity, and a duplicate id.
  - `persisted()` lists the map sorted by order id, for a stable file.
- **The state file**: `{"version":1,"checkpoint":...,"orders":[...]}`, written by Part 1's
  `save_state` (now generic over what it saves): one buffer, a private temporary file created with
  mode 0600, file and directory sync, atomic rename. It holds user ids, so mode 0600 matters here.
  It is saved at most once a second, and once at the end of catch-up before the listener binds. A
  crash replays up to about a second of journal onto the last saved pair. Catch-up runs before the
  listener binds, so the replayed batches' events reach nobody; clients reconnect and get a fresh
  `ready` after them.
- **`UserHub`**: one `tokio::sync::broadcast` ring per connected user.
  - `deliver(events)`: each event to its user's ring, serialized once, only if that user has a
    receiver. An event for a user with no connection is dropped: nothing is kept for later
    (decision 5).
  - `join(user)`: a receiver for that user, creating the ring on first use. `None` once halted.
  - `release(user)`: frees the ring once no receiver is left; called at the end of every
    connection.
  - `halt()`: refuses later joins and drops every ring. Each connection's forwarder then sees its
    ring closed and ends the connection with 1011. Unlike the book feed, the hub needs no `watch`
    channel: a book-feed connection can be idle, with no subscription, but a private connection
    always holds a receiver once it is served.
- **`Follower`**, as in the MDP: reads a batch outside the lock, applies it under the view's write
  lock, and delivers its events **before releasing that lock**. On drift it withdraws the view
  under the same lock. `WithdrawOnExit` withdraws the view and halts the hub when the follower
  thread ends for any reason.
- **`join_user(served, hub, user)`**: under the view's read lock, the last applied sequence and a
  receiver. The lock order is the view, then the hub, everywhere, as in the MDP.
- **`serve(socket, hub, key, join)`**, one connection's task:
  1. waits up to 5 s for the first text frame, skipping pings and pongs;
  2. authenticates it, or sends `error` and closes with 1008;
  3. joins, or closes with 1011 if the feed is unavailable;
  4. starts Part 1's `forward` task, from the user's ring into the connection's bounded queue;
  5. sends `ready`;
  6. then waits, with `tokio::select!`, for whichever comes first: a frame from the client
     (answered with `error`; any frame counts as a sign of life), an event from the queue, a close
     request from the forwarder, the token's expiry, or the ping timer (closing a client silent for
     more than three intervals);
  7. at the end, aborts the forwarder **and waits for it**, releases the user's ring, then closes
     with the closing handshake (`book_feed::finish`).
- **`run(args)`**: checks `JWT_SECRET`, refuses a state path that aliases the journal or the stream
  (Part 1's `ensure_safe_state_path`), loads the state, catches up completely, saves, then starts
  the follower thread and binds. Routes: `GET /health` (200 while the view is served, 503 after a
  halt) and `GET /userfeed/ws`, which caps a client's message and frame at 16 KiB
  (`MAX_CLIENT_MESSAGE`).
- **`USER_LIMITS`**: the book feed's `Limits` with a ring of 1,024 instead of 4,096 (the queue,
  5 s write deadline and 15 s ping are the same).

### `src/exchange/book_feed.rs`

Shared with the private feed, with no change in behavior for the book feed:
- `Close` gains `Policy(reason)`, closing with 1008. `Close::Unavailable`'s reason text is now
  "feed unavailable" instead of "market data unavailable" (the code, 1011, is unchanged).
- `finish(socket, close, limits)`: the close frame and the closing handshake, moved out of
  `serve` unchanged.
- `send`, `forward` and `SILENT_PINGS` are visible to the private feed.

### `src/exchange/market_data.rs`

- `save_state` is generic over the file it writes, and `save_state`, `ensure_safe_state_path` and
  `MdResult` are visible to the private feed. Their error messages no longer say "market-data"
  (`invalid state path`, `the state file must not overwrite the journal or stream`, `... must not
  alias ...`), since both processes print them.
- The test module's batch, journal and socket helpers are visible to the private feed's tests
  (`pub(in crate::exchange)`); none changed.

### `src/middleware/auth_middleware.rs`

`verify_token(token, key) -> Option<Claims>`: the decode the extractor did inline, now shared. The
extractor's answers are unchanged (401 `Invalid or expired token`).

### `src/main.rs` and `src/exchange/mod.rs`

The `--user-feed` mode, dispatched before any database or gateway setup, as `--market-data` is,
and in the usage line. `pub mod user_feed`.

## Tests

Run on the office machine on 2026-10-20, inside `stock-rust`, from the tree in `~/stock-m26`. The
tests are in `src/exchange/user_feed.rs` (unit and socket) and `tests/market_data.rs` (the shipped
executable).

| Test | What it proves | Result |
|---|---|---|
| `user_feed::tests::every_event_reaches_only_its_owner_and_matches_the_core` | **The oracle.** 3,000 random steps from four users on two symbols (prices 95–105, quantities 1–9) go through the authoritative `ExchangeCore`: new orders, cancellations by the owner, cancellations by someone else (which the core must refuse), and new orders reusing another user's resting order id (which the core must reject). After **every** batch: (1) the set of messages told is exactly the expected one, built from the core's own executions: per trade a `fill` to each side's owner and an `order` to the resting side's owner, then one `order` to the incoming user; one message for a cancellation or a refusal; (2) every `fill` and `order` names an order its receiver owns in the core (`order_view(id, user)` is `Some`), and every refusal goes to whoever asked; (3) every `order` message's status, filled and remaining equal the core's view after the batch, and the `fill` quantities told so far add up to its filled quantity; (4) every order in the map is in the core with its owner and remaining quantity, and the map's price levels equal the core's `l2_snapshot` on both sides of both books. Then the close: every resting order (more than 10) expires to its owner, each `expired` as the core says, and the map empties. Then the next day another user reuses an expired id, and it is theirs. The flow must produce at least 1,000 fills and 100 each of `partially_filled`, `filled`, `canceled`, refused cancellations and rejected reused ids. | pass |
| `user_feed::tests::refusals_go_to_whoever_asked_and_never_touch_the_owner_map` | Mallory reuses Alice's live order id: the rejection goes to Mallory alone, with its reason, and `filled` and `remaining` 0. Mallory's cancellation of Alice's order is refused to Mallory alone. The map is unchanged, and Alice's next fill still reaches her: the messages are Bob's `fill`, Alice's `fill`, Alice's `partially_filled`, Bob's `filled`, in that order, with the first execution id of the pair. | pass |
| `user_feed::tests::a_batch_the_owner_map_cannot_explain_is_an_error` | Each kind of drift is an error naming it: an accepted id already open, a fill naming an unknown order, a fill at another price than the resting order's, a fill larger than the resting order, a fill against an order on the same side, a done cancellation by someone else, a cancellation or an expiry of an unknown order, and an order left after the close. | pass |
| `user_feed::tests::the_close_tells_each_owner_and_the_next_day_starts_empty` | The close's expiries go to each owner, oldest first, with their remaining quantity; the map empties; the next day's order reusing an id belongs to its new user. | pass |
| `user_feed::tests::state_round_trips_with_owner_only_permissions_and_bad_state_refuses_startup` | The saved map and checkpoint load back equal; the file's mode is 0600; version 99, a duplicate id, a remaining quantity of 0 or above the quantity, and an empty user id are refused; a missing file means a start from sequence 1. | pass |
| `user_feed::tests::a_users_ring_is_freed_when_their_last_connection_leaves` | A ring lives while one of the user's two connections does and is freed with the last; an event for a user with no connection creates no ring; a halt closes the rings and refuses joins. | pass |
| `user_feed::tests::a_restart_from_the_saved_map_still_tells_the_owners_of_older_orders` | **A real exchange runtime.** Alice's sell rests; the feed saves and stops. Then Bob buys 3 of it, Carol tries to cancel it (refused), and the market closes. A new feed starting from the saved file tells Alice `fill` (remaining 2, at 101, the resting price), `partially_filled` and `expired`; Bob `fill` and `filled`; Carol `cancel_rejected`; and its map ends empty. Without the saved map, the fill would name an unknown order. | pass |
| `user_feed::tests::nothing_reaches_a_user_before_its_sync` | **Durable before visible.** A real runtime with its syncs held: while the sync of Alice's order is held, the follower finds nothing and Alice's ring is empty; once it syncs, her `new` arrives. | pass |
| `user_feed::tests::a_join_racing_the_follower_gets_exactly_the_events_after_its_sequence` | **The join and `ready`'s sequence are one step.** Up to 100 joins race a follower thread applying 2,000 batches, each one message for Alice. Every join's sequence is 0 or a batch's last sequence, never inside one, and its receiver gets exactly the batches after it, in order. At least 3 joins must land mid-run, or the test proves nothing. | pass, three runs in a row (2.3 s each) |
| `user_feed::tests::a_joiner_holds_the_view_until_it_has_joined` | **The same property, deterministic.** The test holds the hub's mutex, so a joiner waits inside its join; after 200 ms the view must still refuse a writer (`try_write` is `WouldBlock`): the joiner holds the read lock across the join, so the follower cannot deliver a batch between `ready`'s sequence and the join. Released, the joiner gets sequence 0. | pass |
| `user_feed::tests::each_user_receives_their_own_events_and_nothing_else` | **Over real WebSockets**, Alice, Bob and Carol connected: `ready` names the last applied sequence (2); Bob's buy against Alice's sell tells Alice `fill` (`seq` 6, remaining 3) and `partially_filled`, and Bob `fill` and `filled`; Carol's rejected order reusing Alice's id reaches Carol only (`seq` 8). Alice's next message is her own later order, and Bob's next is his own, so neither heard anything else. A request after `auth` gets `error` and the connection stays open. | pass |
| `user_feed::tests::a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation` | Each refused first message gets `error` with its reason, then 1008: a token signed with another secret, a token expired 10 s ago (inside the 60 s leeway), a string that is not a JWT, a request that is not `auth`, and a binary frame. A client that sends nothing is closed with 1008 after the 5 s deadline, not before. | pass |
| `user_feed::tests::a_client_message_over_the_limit_ends_the_connection_unread` | A 32 KiB first message is never parsed: no `error` comes back, and the connection does not end with 1008. | pass |
| `user_feed::tests::a_connection_closes_when_its_token_expires` | A token that expires 2 s after it was issued (`exp` counts whole seconds, so between 1 and 2 s after the connection) closes the connection with 1008 after at least 0.9 s. Pings are 60 s apart, so they cannot end it first. | pass |
| `user_feed::tests::a_client_that_never_answers_pings_is_closed_as_gone` | With pings every 200 ms, an authenticated client that reads nothing is closed with 1001. | pass |
| `user_feed::tests::a_halted_follower_closes_every_connection` | A halt closes two authenticated connections with 1011, and a connection that authenticates afterwards is closed with 1011. | pass |
| `tests/market_data.rs::user_feed_pushes_each_user_their_own_fills_across_a_restart` | **The shipped executable**, `stock --user-feed`, on a journal and stream written independently of the code under test: the state file's mode is 0600 once it serves; the seller and the buyer each get their side of a trade (`execution_id` `buy-101-buy`, `seq` 6); after the trade is saved the process is killed and restarted on the same files; the seller's `ready` is 6, and a second buy against the same order tells the seller `fill` and `filled`. An interrupted stream then closes the connection with 1011. | pass |
| `tests/market_data.rs::user_feed_refuses_to_start_without_a_token_secret` | Without `JWT_SECRET` the process exits with failure and `user feed: JWT_SECRET must be set`. | pass |
| The whole suite, normal build: `~/stock-scripts/check.sh` (`cargo fmt -- --check`, then `cargo test --locked`) | First run, before the review: 244 passed (12 new tests; the ring test came just after, so the first mutation run's build had 245 runnable tests plus 1 ignored); after the review's fixes, 247. Final run: | fmt exit 0; 248 passed, 1 ignored (232 before, plus 16 new); integration tests: `market_data` 6 passed (4 before) |
| The whole suite, feature build: `cargo test --locked --features stage-timing` | First run: 247; then 250. Final run: | 251 passed, 1 ignored |
| PostgreSQL acceptance tests: `REPORTER_TEST_DATABASE_URL=postgres://postgres:bench@stock-test-pg:5432/reporter_m26p2 cargo test --locked --test reporter -- --ignored`, on a fresh database | The reporter is unchanged. | 3 passed |
| `cargo fmt -- --check` | `cargo fmt` asked for reflows twice (about 30 blocks, then 8); applied. | clean |
| Release build warnings: `cargo build --locked --release` | | 12, as before |

**Mutation check:** `performance/scripts/m26p2-mutate.sh`, run as
`TREE=~/stock-m26 bash ~/stock-scripts/m26p2-mutate.sh`. Each mutation is applied to a copy of the
tree, and only its covering test runs, in its own cargo target volume. Final run 2026-10-20 10:51,
on the final code: `performance/results/m26p2-mutate.txt`.

| Mutation | Covering test | Must | Result |
|---|---|---|---|
| Control: the real tree, each of the 13 tests alone | — | pass | 13 × ok |
| A resting order's `fill` goes to the incoming order's user | the oracle | fail | FAILED |
| A rejection goes to the map's owner of that id | `refusals_go_to_whoever_asked...` | fail | FAILED |
| A refused cancellation goes to the order's owner | the oracle | fail | FAILED |
| A done cancellation by someone else is accepted | `a_batch_the_owner_map_cannot_explain...` | fail | FAILED |
| No auth deadline (5 s × 100) | `a_connection_that_does_not_authenticate...` | fail | FAILED (20.2 s: the test's 20 s wait runs out) |
| A token's signature is not checked (`insecure_disable_signature_validation`, in `verify_token`) | `a_connection_that_does_not_authenticate...` | fail | FAILED |
| A token's expiry is ignored once connected | `a_connection_closes_when_its_token_expires` | fail | FAILED (20.0 s) |
| A halt leaves the rings open | `a_halted_follower_closes_every_connection` | fail | FAILED (20.0 s) |
| A ring is never freed | `a_users_ring_is_freed...` | fail | FAILED |
| Every event goes to every connected user | `each_user_receives_their_own_events...` | fail | FAILED |
| `ready` names the next sequence, not the last applied | `each_user_receives_their_own_events...` | fail | FAILED |
| The saved state holds no orders | `a_restart_from_the_saved_map...` | fail | FAILED |
| Silence is never noticed | `a_client_that_never_answers_pings...` | fail | FAILED (21.5 s) |
| A resting order's own `order` message is dropped | the oracle | fail | FAILED |
| `ready`'s sequence and the join under separate locks (read the sequence, release the lock, yield, join) | `a_joiner_holds_the_view_until_it_has_joined` | fail | FAILED (0.21 s) |
| The same | `a_join_racing_the_follower...` | — | FAILED this run; passed in the second run |
| A token within the leeway past `exp` is accepted | `a_connection_that_does_not_authenticate...` | fail | FAILED |
| No limit on a client's message size | `a_client_message_over_the_limit...` | fail | FAILED |

All 17 mutations fail their covering test. The racing test's two results for the same mutation,
one pass and one fail, are the reason for the deterministic test: it catches the mutation by the
lock it holds, not by a race it may lose.

Two earlier runs are kept:
- `results/m26p2-mutate-first.txt`, 10:21, on the first version: 10 controls and the first 13
  mutations, all failing.
- `results/m26p2-mutate-second.txt`, 10:41, after the review's fixes: 12 controls and 17
  mutations. The separate-locks mutation **survived** the racing test, its only covering test
  then; the other 16 failed. Fixed: the deterministic test above.

## Independent review

Two independent reviews ran on the first version: one on the code and the tests, one (the
verifier) on the change against the plan, the documents against the code, and the mutation script.
Neither found a way for one user's event, or a user id, to reach another user, a gap or a
duplicate between `ready` and the first event, or a connection or ring left behind. They confirmed:
- the routing: fills and order states through the map, a rejection to the input's user, a refused
  cancellation to the requester;
- that order ids are unique across the whole core within a day, and self-trade prevention makes the
  two sides of a fill different users;
- that `pair.first.execution_id` is the id REST returns for both sides;
- the lock order (the view, then the hub) everywhere, and delivery under the write lock;
- that every path after a join aborts and awaits the forwarder, then releases the ring;
- that the 13 mutations' strings each occur once and give the intended change.

Findings, and what was done:
1. **(medium) A ring per user sized for a whole book.** tokio allocates a ring's 4,096 slots whole,
   about 240 KB per connected user (an estimate), about 240 MB for 1,000 users. Fixed:
   `USER_LIMITS`, a ring of 1,024. The cost, a large order closing its own user with 1013, is
   documented above.
2. **(low) A token inside jsonwebtoken's 60 s leeway** passed `auth`, got `ready`, and was closed
   at once with 1008; a client retrying with it would loop. Fixed: refused at `auth`. Tested (a
   token expired 10 s ago) and mutated. The conversion of `exp` now uses `checked_add`, so a huge
   `exp` cannot panic.
3. **(low) Axum's default 64 MiB message limit before authentication.** Fixed: 16 KiB. Tested and
   mutated.
4. **(low) A vacuous assertion** in the ring test ("an event kept for a user who left": a new
   receiver never sees old messages anyway). Removed, and the doc comment says what the test
   proves.
5. **(low) No test for the join and `ready`'s sequence being one step.** Added
   `a_join_racing_the_follower_gets_exactly_the_events_after_its_sequence`, and a mutation that
   takes the sequence, releases the lock, and joins after. **The racing test did not catch that
   mutation** (second run, below): the gap between releasing the lock and joining is a few
   microseconds, and no batch fell into it. So the property got a deterministic test as well,
   `a_joiner_holds_the_view_until_it_has_joined`, which holds the hub and checks that the joiner
   still holds the view. The racing test stays: it checks the outcome end to end.
6. **(low) The oracle checked that every message told was right, not that every expected message
   was told**, and ran no close. Fixed: after each batch the set of messages must equal the one
   built from the core's executions; the oracle now ends with the close and a next-day reuse,
   both against the core. A mutation drops the resting side's `order` message.
7. **(low) Comments and documents.** The module comment said every later message has a sequence
   (an `error` has none); the shared state-path errors said "market-data"; the README and the
   TODO marked the part complete before its results were written; the example execution id was
   odd, but a trade's first id is always even (`exec_16`, not `exec_17`); the ring size and the
   leeway were not in this document. All fixed.
8. **(low) The first mutation run predated the fixes.** Rerun on the final tree with four more
   mutations (below).

The verifier also judged the racing test's "at least 3 mid-run joins" timing-dependent on a
loaded machine. It passed three runs in a row at 2.3 s each, and every full-suite run since. The
deterministic test does not depend on it.

**Second verifier round**, on the fixes: no code bug. Findings: `PROJECT_DIRECTION.md` called the
part complete before the final mutation run (corrected until the run reported); the separate-locks
mutation might survive the racing test (it did, see above); and a code comment bounded the 1013
case by the ring plus the queue, where the ring alone is the bound (corrected).

**Third verifier round**, on the deterministic test, the script's loop and the final documents: no
code finding. It confirmed the deterministic test passes on the real code and fails under the
mutation (with the lock released, `try_write` succeeds whether or not the joiner has run), that it
cannot deadlock (a panicking test drops the hub guard, and `UserHub::lock` recovers a poisoned
mutex), and that only a machine too loaded to start a thread within 200 ms could fail it falsely.
Its findings were stale test counts in `PROJECT_DIRECTION.md`, a missing list entry there, and a
reference to results files this document did not yet list; all fixed.

**Fourth verifier round**, on the recorded results: every row of the mutation table matches the
raw file and the script, no placeholder is left, and the status agrees in all five documents. It
asked why the first suite run (244) and the first mutation run's build (245 runnable) differ: the
ring test was added between them, as noted in the Tests table.

## Compatibility

- The exchange, the journal, the stream, the MDP's REST routes and state file are unchanged.
- The book feed's 1011 close now gives the reason "feed unavailable", and the MDP's state-path
  errors no longer name the market data.
- The private feed needs `JWT_SECRET`, the gateway's secret, and no database.
