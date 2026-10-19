# Milestone 26 — Theory

The background needed to read this milestone's code and measurements cold. Every example is worked
through. Figures that are not measured are marked as illustrative.

## 1. Polling against pushing

A client that wants to know when something changes can ask repeatedly (**polling**), or be told
(**pushing**).

**Polling.** N clients each ask every T seconds:
- the server answers N / T requests per second, whether anything changed or not;
- a change waits, on average, T / 2 before a client sees it, and up to T;
- each answer carries the whole state (a whole book), not the change.

**Pushing.** The server sends a message when something changes, to each client that wants it:
- the server sends (changes per second) × (subscribers per change) messages;
- a change reaches a client after the server's own processing and one network hop, with no
  waiting for the next poll;
- each message carries only the change.

**Worked example, illustrative numbers.** 1,000 clients each watching one of 100 symbols, 10 per
symbol, with the exchange at 5,000 orders/s spread evenly over the symbols:
- polling every 100 ms: 1,000 / 0.1 = 10,000 requests/s, each a whole book, and changes seen
  50 ms late on average;
- pushing: each symbol changes about 5,000 / 100 = 50 times/s, so 50 × 10 = 500 messages/s per
  symbol, 50,000 messages/s in all, each a few levels, seen within milliseconds.

Pushing is not always less traffic. It is traffic in proportion to change multiplied by interest,
and it is fresh. Polling is traffic in proportion to clients divided by the polling interval, and
it is stale by half an interval.

## 2. The WebSocket protocol (RFC 6455)

A WebSocket is a long-lived, two-way connection that starts as an ordinary HTTP request.

**The handshake.** The client sends a `GET` with these headers:
- `Upgrade: websocket`
- `Connection: Upgrade`
- `Sec-WebSocket-Version: 13`
- `Sec-WebSocket-Key`: 16 random bytes, in base64

The server answers `101 Switching Protocols` with `Sec-WebSocket-Accept`. That value is the
base64 of the SHA-1 of the key followed by the fixed string
`258EAFA5-E914-47DA-95CA-C5AB0DC85B11`. It proves that the server understood the request as a
WebSocket. After that, the TCP connection carries frames, not HTTP.

**Frames.** Each frame has:
- a FIN bit, set on the last frame of a message;
- an opcode:
  - `0x1` text, `0x2` binary;
  - `0x8` close;
  - `0x9` ping, `0xA` pong;
- a length, in 7 bits, or 16, or 64.

Frames from client to server must be **masked**: XORed with a 4-byte key sent in the frame. The
mask stops a hostile page from making the browser send bytes that a proxy would read as HTTP.
Frames from server to client are not masked.

**Keeping the connection alive.** Either side sends a ping, and the other must answer with a pong.
That detects a dead peer, and it keeps idle connections open through NATs and proxies.

**Closing.** A close frame carries a 2-byte code and an optional reason. Codes used here:
- defined in RFC 6455:
  - 1000: normal closure;
  - 1001: going away (the server is shutting down);
  - 1008: policy violation (a bad or expired token);
  - 1011: internal error (here, the MDP's follower halted);
- 1013, "try again later", from the IANA WebSocket close-code registry rather than the RFC: used
  here for a slow consumer being dropped.

**What browsers cannot do.** The browser's `WebSocket` API lets a page choose only the URL and the
subprotocols, so it cannot set an `Authorization` header. A token must travel some other way:
- in the URL;
- in the `Sec-WebSocket-Protocol` header;
- in a cookie, which the browser sends on the handshake;
- in the first message after the handshake.

Section 7 compares them.

**Server-Sent Events, the alternative.** These are a plain HTTP response that never ends: the
server writes `data: ...` lines, and the browser's `EventSource` reconnects by itself. On
reconnection it sends `Last-Event-ID`, which a server can use to resume. They are one-way and need
no protocol upgrade. Their limits: one stream per connection, and nothing from the client after the
request. Decision 1.

## 3. Snapshot plus deltas, and sequence numbers

A client cannot apply changes to a book it does not have. So a feed sends:
1. **a snapshot**: the whole book at one point in the stream, tagged with that point;
2. **deltas**: each later change, tagged with its point in the stream.

The client applies the deltas to the snapshot, in order, and holds the server's book at every
point. FIX names this message `MarketDataIncrementalRefresh` (after a `MarketDataSnapshotFullRefresh`),
and a feed of aggregated price levels is called market by price (MBP).

**One delta here** is the effect of one committed batch (one command) on one symbol's book: the
levels whose quantity changed, each with its new aggregate quantity. A level whose new quantity
is 0 is removed. Sending the new quantity, not the difference, makes a delta idempotent: applying
it twice gives the same book.

**Sequence numbers.** Every journal envelope has a global `seq_num`, contiguous across batches.
A batch's last sequence names the stream point after that batch. One symbol's deltas skip the
sequences of other symbols' batches, so a client cannot tell a gap from a missing sequence alone.
So each message carries a second number:
- a snapshot carries `seq`, the stream point it reflects, and `last`, the sequence of that
  symbol's latest delta (0 if the book never changed);
- a delta carries `seq`, its batch's last sequence, and `prev`, the sequence of the symbol's
  previous delta.

**The gap rule, in this order:**
1. A delta with a `seq` at or before the snapshot's `seq` is already in the snapshot, and is
   dropped (section 4). This check comes first: such a delta's `prev` is older than the snapshot's
   `last`, and the gap check would wrongly call it a gap.
2. A delta is in order when its `prev` equals the last channel sequence the client knows: the
   snapshot's `last` at first, then each applied delta's `seq`. Any other `prev` means a delta was
   missed, and the client must resubscribe for a fresh snapshot.

The server keeps each symbol's `last` outside the book itself. The MDP removes a book when it
empties, at the latest at every close. If `last` went with the book, the next delta would carry
`prev` 0, and every subscribed client would see a false gap.

**Worked example.** AAPL's book last changed at sequence 900. Other symbols trade until the
snapshot at 1000. AAPL's asks are 101 × 30 and 102 × 50, one resting order at each level. A buy of
40 at 102 then fills 30 at 101 and 10 at 102. Its batch is 6 envelopes, 1001 to 1006: the input, `OrderAccepted`, and two
`ExecutionCreated` per trade (one match emits two).

```text
snapshot  seq 1000  last 900   asks [101:30, 102:50]
delta     seq 1006  prev 900   asks [101:0, 102:40]   -> asks [102:40]   (prev = last: in order)
delta     seq 1290  prev 1250  ...                    -> gap: 1250 was never seen; resubscribe
```

## 4. The subscribe race

A subscriber needs a snapshot and every delta after it, with nothing lost and nothing doubled.
Doing the two steps one after the other leaves a window between them:
- **snapshot first, then join the channel:** a delta published between the two steps is lost;
- **join first, then snapshot:** a delta published between them arrives but is already in the
  snapshot. The client must drop every delta whose sequence is not after the snapshot's.

Two standard fixes:
1. **Take both under the same lock the publisher holds.** The snapshot and the join then happen
   between two batches, never inside one.
2. **Tag everything with sequences.** The client drops deltas at or before the snapshot's
   sequence. That stays correct even if fix 1 is imperfect.

The MDP's follower already applies each batch under the view's write lock, so fix 1 is available,
on one condition. The follower must send the batch's deltas **before** it releases the lock. If it
released the lock first, a subscriber could take a snapshot that already includes the batch, join,
and then receive the batch's delta as well. Fix 2 would catch that, but fix 1 would no longer
hold. This milestone uses both fixes.

## 5. Fan-out, backpressure and slow consumers

One batch may concern many subscribers. **Fan-out** is the step from one event to many queues.

**The slow consumer.** A client that reads more slowly than its messages arrive builds a backlog.
Something must give:
- **block the producer:** every other client then waits for the slowest. This is head-of-line
  blocking, and it is never acceptable for market data;
- **buffer without limit:** memory grows until the process dies;
- **drop messages:** the client's copy of the book becomes wrong without its knowing;
- **disconnect:** the client knows it is behind and resynchronizes;
- **conflate:** replace the queued deltas with one fresh snapshot. The client stays connected and
  correct. This needs more code, and it has no meaning for private events, where each fill must
  arrive.

Decision 3 recommends disconnect.

**Where the waiting must not happen.** The MDP's follower thread must never wait for a client. It
puts each delta into a bounded queue and goes on. One task per connection takes from that queue
and writes to the socket, and only that task waits on the network.

**`tokio::sync::broadcast`, one candidate.** It is a single ring buffer of C slots, shared by every
receiver, and each receiver has its own position in it:
- a message is stored once, however many receivers there are;
- a receiver that falls C messages behind loses the oldest, and its next receive returns
  `Lagged(n)`.

That fits the disconnect policy: `Lagged` means the client is too slow. Sending is a plain function
call, so a std thread can send. One channel per symbol keeps clients of quiet symbols from paying
for busy ones.

**A client that stops reading entirely.** `Lagged` is noticed only by a task that calls `recv`. A
client that stops reading lets its TCP buffers fill. Its task then waits inside the socket write,
never calls `recv` again, and never learns it lagged. It would hold its connection forever. So each
write to the socket also gets a deadline, of a few seconds. A write that does not finish in time
closes the connection as a slow consumer.

**How big a queue.** By Little's law, the backlog L = λ × W: the message rate times the delay to be
absorbed. At 50 messages/s per symbol (section 1) and a tolerated stall of 2 s, L = 100 messages.
At the maximum rate, about 50,000 orders/s over 100 symbols, that is 500 messages/s per symbol and
1,000 messages for the same 2 s. *These rates are illustrative*; Part 3 measures the real ones.

## 6. Durable before visible, for a feed

The rule since milestone 8: nothing about a command may be seen before the disk holds it, and with
replication before both disks hold it.

A feed that reads the committed mmap stream keeps that rule by construction:
- the primary publishes a batch to the stream only after its sync, and with replication only
  after the replica's confirmation (milestone 25 part 2 kept this);
- the MDP and the private feed read only what the stream published.

So no pushed message can describe a command that might be lost. A test checks it as milestone 25
did: hold a sync, and see no message.

**What is not ordered.** A client that places an order may get the pushed fill before its HTTP
reply arrives, or after. Both come after durability, from different processes over different
connections. A client must not assume either order.

## 7. Privacy and authentication on a long-lived connection

**Isolation.** A private event goes only to its owner. The owner comes from the authenticated
token, never from anything the client asks for. Each connection receives from its user's channel
only. A test with several users trading against one another proves that no event crosses.

**Both sides of a fill.** A fill names a buy order and a sell order. The feed tells the owner of
each:
- the buyer gets its own order id and side Buy;
- the seller gets its own order id and side Sell.

The incoming order's owner is in the same batch; the resting order's owner comes from the owner
map. The two are always different users: the engine skips the aggressor's own resting orders
(`OrderBook`, self-trade prevention). A side effect reaches the public feed. One user's buy and
sell can rest at crossing prices, and the book is then locked or crossed (bid ≥ ask). The feed
publishes such a book as it is.

**Order ids are not private keys.** Clients choose them, and one id can be taken by another user's
live order on the same day. So a rejected order is reported to the user who submitted it, named in
that input, and never to the owner the map holds for the same id. A refused cancellation goes to
the user who asked, who may not be the owner. Only accepted orders, and what happens to them,
are routed through the map.

**Where the token travels** (decision 4):
- *in the URL query* (`?token=...`): simple, but URLs are written to logs, histories and proxies;
- *in `Sec-WebSocket-Protocol`*: it works in browsers, but the field is meant to name protocols;
- *in a cookie*: browsers send it on the handshake, and the server must check the `Origin` header,
  because any page can open a WebSocket to any host. This gateway issues bearer tokens, not
  cookies;
- *in the first message*: it never appears in a URL or a header log. The server must give it a
  deadline: a connection that does not authenticate within a few seconds is closed. This is the
  recommendation.

**Expiry.** An HTTP request is checked once; a connection lives for hours. A token valid at
connection time can expire while the connection is open (the gateway's tokens last 24 h). So the
server closes the connection at the token's `exp`, with code 1008, and the client reconnects with
a fresh token.

## 8. Measuring push latency

**What to measure.** The time from the moment an order was meant to be sent to the moment a client
receives a message caused by it. Measuring from the intended send time, not the actual one,
avoids coordinated omission (milestone 24's `glossary.md`).

**Clocks across processes.** The order generator and the feed clients are different processes.
Rust's `Instant` cannot be compared between processes, so both use the wall clock
(`SystemTime`). On one host there is one wall clock, so the difference is valid, provided the
clock is not stepped during a run. Across machines, clocks differ by more than the latencies
measured here, so this method holds on one host only.

**Linking a message to its order.** A fill message carries the order id. The generator records
each order id's intended send time, and the client looks it up. For the public feed, the delta's
sequence locates the batch, and the order it came from can be found through the journal.

**What the clients themselves cost.** A thousand clients in one process, on a machine with 4
hardware threads, compete for CPU with the exchange and the feeds. Part 3 records each container's
CPU (milestone 25's cgroup sampler) and checks the exchange with feeds against without.

**Fairness,** from the design, for later. Delivering to subscribers in connection order lets early
connectors see data first. Shuffling the order of delivery for each message, or delivering in a
random order fixed per connection, removes that advantage. This milestone delivers through
independent per-connection tasks, which the runtime schedules without a fixed order, and does not
measure fairness.
