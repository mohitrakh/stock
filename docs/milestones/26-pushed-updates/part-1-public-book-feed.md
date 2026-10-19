# Part 1 — The public L2 book feed

**Status: complete on 2026-10-19.** Code, tests, mutation check, two rounds of independent review,
the PostgreSQL acceptance tests.

## What this part is

The market-data process (MDP) already follows the committed stream and keeps every L2 book. This
part adds a WebSocket endpoint to it, `GET /marketdata/ws`, on the MDP's port (default
`127.0.0.1:4001`). A client subscribes to symbols. For each, it receives:
1. the whole book, as a snapshot;
2. then a delta for every committed batch that changes that book.

The design and its reasons are in `01-theory.md`, sections 2 to 6, and the owner's decisions 1, 3
and 6 (`decisions.md`): WebSocket, disconnecting slow consumers, full depth.

## The protocol

All messages are JSON text frames. The client sends:

```json
{"type":"subscribe","channel":"book","symbol":"AAPL"}
{"type":"unsubscribe","channel":"book","symbol":"AAPL"}
```

The server sends:

```json
{"type":"snapshot","channel":"book","symbol":"AAPL","seq":1000,"last":900,
 "bids":[{"price":99,"quantity":5}],"asks":[{"price":101,"quantity":30},{"price":102,"quantity":50}]}
{"type":"delta","channel":"book","symbol":"AAPL","seq":1006,"prev":900,
 "bids":[],"asks":[{"price":101,"quantity":0},{"price":102,"quantity":40}]}
{"type":"unsubscribed","channel":"book","symbol":"AAPL"}
{"type":"error","message":"invalid symbol"}
```

- `seq` of a snapshot is the stream point it reflects: the last journal sequence applied
  (`View.applied.next_sequence − 1`).
- `last` of a snapshot is the sequence of that symbol's latest delta, 0 if it never changed.
- `seq` of a delta is the last envelope sequence of the batch it comes from. `prev` is the symbol's
  previous delta's `seq`.
- A level in a delta carries its **new** aggregate quantity; 0 removes it. Bids come best (highest)
  first, asks best (lowest) first.

**The client's rules, in this order:**
1. Drop a delta whose `seq` is at or before the snapshot's `seq`: it is already in the snapshot.
2. Apply a delta only if its `prev` equals the last channel sequence known: the snapshot's `last`
   at first, then each applied delta's `seq`. Otherwise a delta was missed: resubscribe.

The test `ClientBook` in `src/exchange/market_data.rs` implements exactly these rules. The oracle,
the drop-and-gap test, the close test and the resubscription test apply every message through it;
the other feed tests check individual messages.

**Errors.** An unknown channel, an invalid symbol (empty, longer than 64 bytes, with leading or
trailing whitespace, or with a control character: the gateway's rule for order symbols), a second subscription
to the same symbol, an unsubscription that was never subscribed, JSON that is not one of the two
requests, and a binary frame each get an `error` message. The connection stays open.

**Symbols.** The MDP has no registry of symbols and removes a book when it empties, so every valid
identifier is accepted, and a book with no orders gives an empty snapshot.

**Closing codes:**
- 1013 (IANA "try again later"): the client fell behind (decision 3); it may resubscribe;
- 1011 (RFC 6455 "internal error"): the MDP's follower halted; nothing more will be published;
- 1001 (RFC 6455 "going away"): nothing at all, not even a pong, came from the client for more
  than three ping intervals; it is closed at the next ping, in practice after three unanswered
  pings, about 60 s of silence by default. It is presumed gone.

After its close frame the server waits, within the write deadline, for the client's own close
frame, reading whatever the client sent meanwhile. That is the WebSocket closing handshake.
Dropping the socket with unread data from the client makes the kernel reset the connection, and
the client can then lose the close frame; a test found exactly that (see "Independent review").

## What changed in the code

### `Cargo.toml` and `Cargo.lock`

- `axum` gains the `ws` feature. It adds `tokio-tungstenite` 0.29, `tungstenite` 0.29 and six small
  crates (`data-encoding`, `getrandom` 0.3, `r-efi`, `rand` 0.9, `rand_chacha` 0.9, `rand_core`
  0.9), all now in `Cargo.lock`.
- Dev-dependencies for the tests' WebSocket clients: `tokio-tungstenite` 0.29 (the same version)
  and `futures-util` 0.3, already in the tree; `futures-macro` joins `Cargo.lock` for them.

### New: `src/exchange/book_feed.rs`

- **`Limits`**: the per-symbol ring size (4,096), the per-connection queue (1,024 messages), the
  write deadline (5 s) and the ping interval (15 s). Tests shrink them.
- **`BookChange`**: one symbol's changed levels after a batch.
- **`BookFeed`**, the hub shared by the follower and every connection:
  - `publish(seq, changes)`: for each changed symbol, swaps its `last` for `seq` (the old one is the
    delta's `prev`), serializes the delta **once**, and sends it into the symbol's
    `tokio::sync::broadcast` ring if anyone subscribed. It never waits for a client.
  - `join(symbol)`: the symbol's `last` and a receiver of every later delta; it creates the ring on
    first use. `None` once halted.
  - `release(symbol)`: frees a symbol's ring once no receiver is left (its `last` stays). A ring
    of 4,096 slots is allocated whole, so it must not outlive its subscribers. A connection
    releases on unsubscribing and when it ends.
  - `halt()`: drops every ring, refuses later joins, and wakes every connection through a
    `watch` channel.
  - Each symbol's `last` lives in the hub, not in the book. The MDP removes a book when it empties,
    and `last` must survive that, or the next delta would carry `prev` 0 and every client would see
    a false gap.
- **`serve(socket, feed, subscribe)`**, one connection's task. It waits, with `tokio::select!`, for
  whichever comes first:
  - a client request (subscribe, unsubscribe, or anything else, answered with `error`);
  - a delta from the connection's queue;
  - a close request from a forwarder;
  - the hub's halt;
  - the ping timer.

  For a subscription it sends the snapshot itself, then starts a **forwarder** task that moves the
  subscription's deltas from the symbol's ring into the connection's bounded queue. On
  unsubscribing it aborts the forwarder **and waits for it to stop**: a forwarder in the middle of
  a poll on another thread could otherwise still queue deltas after the reply. Every frame from the
  client counts as a sign of life; at each ping, a client silent for more than three ping intervals
  is closed with 1001. A peer gone without a FIN or RST never fails a write, because the pings just sit in
  the kernel's buffer, so silence is the only sign of it.
- **`forward(...)`**: reports a full queue or a `Lagged` ring as a slow consumer, and a closed ring
  as unavailable.
- **`send(...)`**: every socket write has a deadline. A client that stops reading fills its TCP
  buffers, and its task would otherwise wait in the write forever and never notice its queue
  overflowing (`01-theory.md` §5).

### `src/exchange/market_data.rs`

- `MarketDataProjection` records every level `add_level` and `subtract_level` touch (`touched`).
  `take_changes()` turns them into one `BookChange` per symbol, reading each level's new quantity
  from the book (0 if the level is gone). `from_orders` (loading a saved state) clears them: a load
  is not a change anyone was told about. `levels(symbol)` returns the whole book, both sides empty
  when there is none.
- `Follower` holds the hub. After a batch applies, `step` publishes its changes **before releasing
  the view's write lock**, so a subscriber's snapshot and its join happen between two batches,
  never inside one (`01-theory.md` §4).
- `subscribe_book(served, feed, symbol)` takes the read lock, then joins and builds the snapshot
  from the same view. The lock order is the same everywhere (the view, then the hub), so the
  follower and a subscriber cannot deadlock.
- `book_socket`, the route's handler: upgrades the request and runs `serve` with `subscribe_book`.
- `WithdrawOnExit` also halts the hub, so when the follower thread ends for any reason, every feed
  connection closes with 1011.
- `router(served, feed)`: the routes, now a function the tests can serve.

### `src/exchange/runtime.rs`

`hold_syncs_for_test()` (test-only): milestone 25's sync gate, now callable from other modules'
tests. The runtime's own tests use it too.

### `src/types/types.rs`

`L2Level` also derives `Deserialize`, for the tests' clients.

## Tests

Run on the office machine on 2026-10-19, inside `stock-rust`, from the tree in `~/stock-m26`.

| Test | What it proves | Result |
|---|---|---|
| `market_data::tests::feed_snapshots_and_deltas_rebuild_every_book_at_every_sequence` | **The oracle.** 2,000 random orders and cancellations (three users, two symbols, prices 95–105) go through the authoritative `ExchangeCore`, and their batches through the projection and the hub. After **every** batch, a client that subscribed at the start, and one that subscribed after step 700, hold exactly the projection's book and the core's `l2_snapshot`, at full depth. The flow must cross a book (one user's buy above their own sell, which self-trade prevention lets rest) and produce over 1,000 deltas. | pass |
| `market_data::tests::a_delta_in_the_snapshot_is_dropped_and_a_missing_one_is_a_gap` | A delta already in a snapshot is dropped, not applied twice; a withheld delta is reported as a gap. | pass |
| `market_data::tests::nothing_is_published_before_its_sync` | **Durable before visible.** A real runtime with its syncs held: while an order's sync is held, the follower finds nothing to apply and no delta exists; once it syncs, the delta arrives. | pass |
| `market_data::tests::the_close_empties_books_on_the_feed_and_a_later_subscriber_follows_the_next_day` | A real runtime's close empties the book through ordinary deltas; a client subscribing after the close gets an empty snapshot whose `last` is the close's sequence, and the next day's first delta chains to it. | pass |
| `market_data::tests::the_socket_serves_snapshots_deltas_and_refusals` | Over a real WebSocket: the snapshot's `seq` and `last`, a live delta chained to it, an empty book, unsubscribing, and every refusal (already subscribed, invalid symbol, unknown channel, malformed JSON, not subscribed, binary frame). No user id in the snapshot or the delta. | pass |
| `market_data::tests::a_client_that_stops_reading_is_disconnected_at_the_write_deadline` | A client that stops reading while about 25 MB of deltas arrive is disconnected. The queue and ring are large, so only the write deadline can end the connection. | pass |
| `market_data::tests::a_client_whose_queue_overflows_is_closed_as_a_slow_consumer` | With a queue of 1, a client whose socket is blocked overflows its queue and is closed with 1013 once its socket drains. | pass |
| `market_data::tests::a_halted_follower_closes_every_connection` | A halted follower closes a subscribed connection and an idle one with 1011, and closes a later connection at once. Its pings are 60 s apart, so a connection the halt missed cannot be kept alive by pings. | pass |
| `market_data::tests::resubscribing_starts_from_a_fresh_snapshot_with_no_gap` | Over a real WebSocket: deltas flow, the client unsubscribes, an order arrives meanwhile, the client subscribes again. Every delta that arrives after the `unsubscribed` reply is already in the new snapshot (its `seq` is at or before the snapshot's), the new snapshot holds the order placed in between, and the following deltas chain to it with no gap. The client's book then equals the projection's. | pass |
| `market_data::tests::a_client_that_never_answers_pings_is_closed_as_gone` | With pings every 200 ms, a client that reads nothing (so sends no pongs) is closed with 1001, although every write to it succeeds. | pass |
| `book_feed::tests::a_ring_is_freed_when_its_last_subscriber_leaves_and_last_stays` | A ring is kept while one subscriber remains, freed when the last leaves, and the symbol's `last` survives. | pass |
| `book_feed::tests::deltas_chain_from_the_snapshots_last_and_last_outlives_a_quiet_symbol` | `prev` chains from the last change, including one made before anyone subscribed; one symbol's change does not reach another's subscribers; `last` survives the book emptying. | pass |
| `book_feed::tests::a_halted_feed_closes_its_rings_and_refuses_new_subscribers` | A halt closes every ring and refuses joins. | pass |
| `book_feed::tests::symbols_follow_the_gateways_identifier_rule` | The symbol rule at its edges (64 and 65 bytes, a leading space, a NUL). | pass |
| `tests/market_data.rs::mdp_pushes_the_book_over_websocket_and_closes_when_the_stream_breaks` | **The shipped executable.** The snapshot equals the REST view; a batch appended to the journal arrives as a delta chained to it (`seq` 8, `prev` 4); REST then agrees; neither the snapshot nor the delta carries a user; an interrupted stream closes the connection with 1011. | pass |
| The whole suite, normal build: `~/stock-scripts/check.sh` | First run: 229 passed. Final run, with the review's fixes: | 232 passed, 1 ignored (218 before, plus 14 new unit tests); integration tests: `market_data` 4 passed (3 before) |
| The whole suite, feature build: `cargo test --locked --features stage-timing` | First run: 232. Final run: | 235 passed, 1 ignored |
| PostgreSQL acceptance tests (`REPORTER_TEST_DATABASE_URL=... cargo test --locked --test reporter -- --ignored`, fresh database `reporter_m26p1`) | The reporter is unchanged. | 3 passed |
| `cargo fmt -- --check` | `cargo fmt` applied the reflows it asked for (23 blocks the first time). | clean |
| Release build warnings | | 12, as before |
| The 31 market-data and feed unit tests, three times in a row | No timing-dependent failure. | 3 × 31 passed |

**Mutation check:** `performance/scripts/m26p1-mutate.sh`, run as
`TREE=~/stock-m26 bash ~/stock-scripts/m26p1-mutate.sh` (the script's default tree, `~/stock-m25`,
is the git checkout, which has Part 1 only once it is committed). Final run 2026-10-19 18:16, on
the final code: `performance/results/m26p1-mutate.txt`.

| Mutation | Covering test | Must | Result |
|---|---|---|---|
| Control: the real tree, each of the 5 tests alone | — | pass | 5 × ok |
| A delta's `prev` is not chained (`last` never moves) | the oracle | fail | FAILED |
| A snapshot's `last` is always 0 | the oracle | fail | FAILED (the clients joining at step 700 see a gap) |
| A level's decrease is never reported (`subtract_level` not recorded) | the oracle | fail | FAILED |
| No write deadline | `a_client_that_stops_reading...` | fail | FAILED (23.3 s: the connection never ends, and the test's 20 s wait runs out) |
| A full queue is not reported | `a_client_whose_queue_overflows...` | fail | FAILED (21.2 s) |
| A halt does not reach idle connections | `a_halted_follower_closes_every_connection` | fail | FAILED (20.1 s) |
| Silence is never noticed | `a_client_that_never_answers_pings...` | fail | FAILED (21.5 s) |

Two earlier runs are kept:
- `results/m26p1-mutate-first.txt`, 17:26: six mutations. The halt mutation printed no result: its
  test waited for a close while the server's pings, every 15 s, kept arriving within each 20 s wait,
  so the test hung until the script's 600 s limit killed it. Fixed: the halt test pings every 60 s,
  `close_code` has one overall 20 s deadline, and the script prints "no result (killed after 600 s)"
  instead of an empty result.
- The second run, 17:56, before the second review's fixes, gave the same results as the final one
  (on the office machine as `~/m26-out/m26p1-mutate-second.txt`).

## Independent review

**First round**, on the code (one reviewer) and on the mutation script and this document (another).
Neither found a way for a subscriber to miss or double a delta, or to get a wrong `last` or `prev`.
They confirmed:
- the lock order (the view, then the hub) everywhere, with no await under a std lock;
- the publish-under-write-lock and join-under-read-lock pairing;
- that every level change goes through `add_level` or `subtract_level`;
- that a halt cannot be missed;
- that every write has a deadline;
- the crate list added to `Cargo.lock`.

Findings, and what was done:
1. **(confirmed, medium) No resubscription test**, although the plan requires one. Added
   `resubscribing_starts_from_a_fresh_snapshot_with_no_gap`.
2. **(plausible, low) Unsubscribing aborted the forwarder without waiting.** A forwarder in the
   middle of a poll on another thread could still queue deltas after the `unsubscribed` reply; with a
   quick resubscription the client would see a false gap. Fixed: the forwarder is aborted and
   awaited.
3. **(confirmed, low) Rings were never freed.** `broadcast::channel(4096)` allocates its slots
   whole, and a symbol's ring stayed after its subscribers left. Fixed: `BookFeed::release`, called
   on unsubscribing and at the end of a connection.
4. **(confirmed, low) The queue-sizing comment was per symbol**, but the queue is per connection.
   Corrected: it absorbs 1,024 divided by the sum of the subscribed symbols' rates.
5. **(plausible, low) Pings detected nothing.** The theory said ping/pong detects a dead peer, but the
   server never checked for answers. Fixed: a client silent for more than three ping intervals is
   closed with 1001, with a test and a mutation.
6. **(plausible, low) The timing tests assumed small loopback buffers.** Fixed on the client side:
   the test client asks for a 64 KiB receive buffer. The server's send buffer is still the host's
   (Linux's default maximum is 4 MiB, against about 25 MB sent).
7. **(confirmed) The halt mutation hung** (above), and this document's wording on whitespace in
   symbols and on user ids overstated the tests. Fixed.

Fixing finding 5 exposed one more bug: the server dropped the socket right after its close frame.
Unread pongs from the client were still pending, so the kernel reset the connection, and the
client lost the close frame (`None` instead of 1001). Fixed with the closing handshake: the server
reads until the client's close frame, within the write deadline.

**Second round**, on those fixes (one reviewer). Findings:
1. **(confirmed, low-medium) A ring could still leak** when the snapshot write failed: the receiver
   was never handed to a forwarder, so the connection's final release skipped it. A client could
   subscribe to new symbols and drop each connection at once. Fixed: that path releases too.
2. **(confirmed, low) The silence timing was overstated.** A silent client is closed at the first
   ping after three silent intervals, in practice after about four, 60 s by default, not 45 s.
   Corrected here and in the code's comments.
3. **(plausible, low) The resubscription test could finish without resubscribing**, if the server
   handled the unsubscription late. Rewritten: it waits for the `unsubscribed` reply, then for the
   new snapshot, and checks that every delta between them is already in it.

## Compatibility

- The REST routes, the state file and its version, and the journal are unchanged.
- The MDP still needs no database and no `JWT_SECRET`.
- The follower now does a little more per batch: recording touched levels and, for symbols with
  subscribers, serializing one delta. What that costs at the maximum rate is measured in Part 3.
