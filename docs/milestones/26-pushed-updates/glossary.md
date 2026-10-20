# Milestone 26 — Glossary

Terms used in this milestone, alphabetically. Milestone 24's `glossary.md` defines the measurement
terms it builds on: coordinated omission, HdrHistogram and Little's law.

**Backpressure.** A slow reader making a fast writer slow down. A market data feed must never let
one slow client do this to the others. Theory §5.

**cgroup.** A Linux control group: the kernel's accounting of the processes in one container.
`cpu.stat` in a container's cgroup gives its CPU time (`usage_usec`). Milestone 25 part 4 reads
it once a second to measure each container's CPU (`25-latency/decisions.md`, Part 4 choices).

**Channel.** One stream of messages a client can subscribe to: one symbol's book, or one user's
private events.

**Close code.** The 2-byte reason in a WebSocket close frame. Codes 1000, 1001, 1008 and 1011 come
from RFC 6455; 1013 comes from the IANA registry. Theory §2.

**Conflation.** Replacing queued changes with their combined result, for example a fresh snapshot
in place of many deltas. Theory §5.

**Crossed / locked book.** A book whose best bid is above (crossed) or equal to (locked) its best
ask. Here it happens when one user's buy and sell rest at such prices, because the engine never
trades a user against themselves. The public feed publishes the book as it is. Theory §7.

**Delta.** One change to a book: the levels one committed batch changed, each with its new
quantity. Theory §3.

**Fan-out.** Delivering one event to many subscribers. Theory §5.

**Frame.** The unit a WebSocket carries after the handshake: a FIN bit, an opcode, a length, an
optional mask, and the payload. Theory §2.

**Gap.** A missing message on a channel. It is detected because a delta's `prev` is not the last
channel sequence the client knows: the snapshot's `last` at first, then each applied delta's
`seq`. Theory §3.

**Handshake.** The HTTP request and the `101 Switching Protocols` answer that turn a connection into
a WebSocket. Theory §2.

**Head-of-line blocking.** One slow item holding up everything queued behind it. Theory §5.

**Idempotent.** Giving the same result when applied twice. A delta that carries each level's new
quantity, not the difference, is idempotent. Theory §3.

**Incremental refresh / market by price (MBP).** A feed of a book's aggregated price levels, sent
as a snapshot followed by changes: L2 data delivered this way. FIX's message for the changes is
`MarketDataIncrementalRefresh`. Theory §3.

**Leeway (token).** How long past its `exp` a JWT is still accepted, to absorb clock differences
between machines. `jsonwebtoken`'s `Validation::default()` allows 60 s. The gateway keeps it; the
private feed refuses a token already past `exp`, because a connection lasts only until then
(`part-2-private-feed.md`).

**`Lagged`.** What `tokio::sync::broadcast` returns to a receiver that fell behind its buffer, with
the number of messages it lost. Theory §5.

**Masking.** XORing every client-to-server frame with a 4-byte key sent in the frame, so a hostile
page cannot make a browser send bytes a proxy would read as HTTP. Theory §2.

**Opcode.** A frame's type: text, binary, close, ping or pong. Theory §2.

**Oracle test.** A test that checks a result against an independent source known to be right: here,
the feed's book against the projection's view and the core's `l2_snapshot`.

**Owner map.** The private feed's table from each open order to its user, symbol, side, price,
quantity and remaining quantity. Fills name order ids, not users, so the feed needs it. It is
consulted only for orders it accepted. Theory §7.

**Polling.** A client asking repeatedly whether something changed. Theory §1.

**Push.** A server sending a client a change when it happens. Theory §1.

**`ready`.** The private feed's first message after a successful `auth`: the last stream sequence
the feed had applied when the connection joined. Every later event has a greater sequence; nothing
before it is sent (decision 5).

**Resubscribe / resynchronize.** A client that detected a gap, or was disconnected, asking again
and starting from a fresh snapshot.

**Run-order effect.** A run measuring differently because of its position in a back-to-back pair,
whichever build it is. Milestone 25 found it, and found that three runs in a 2-to-1 order can
mislead (`25-latency/performance/01-instrumentation-overhead.md` and `04-final-measurement.md`).
So this milestone's comparisons use six runs with the order balanced.

**Self-trade prevention.** The engine's rule that an incoming order skips resting orders of its own
user (`OrderBook`). So every fill has two different users. Theory §7.

**Sequence (stream sequence).** The journal's global envelope number, `seq_num`. A message
carries the sequence of the batch it reflects. Theory §3.

**Server-Sent Events (SSE).** A one-way push over a never-ending HTTP response, with automatic
reconnection and `Last-Event-ID`. Theory §2.

**Slow consumer.** A client that reads more slowly than its messages arrive. Theory §5.

**Snapshot.** The whole book at one stream sequence (`seq`), with the sequence of that symbol's
latest delta (`last`). Theory §3.

**Subprotocol.** A name the client offers in `Sec-WebSocket-Protocol`, for the application protocol
it wants to speak over the WebSocket. Theory §2 and §7.

**Subscribe race.** The window between taking a snapshot and joining a channel, where a delta can
be lost or doubled. Theory §4.

**WebSocket.** A long-lived, two-way connection upgraded from HTTP (RFC 6455): frames, masking,
ping/pong and close codes. Theory §2.

**Write deadline.** A limit on how long one write to a client's socket may take. It catches a
client that stopped reading, whose task would otherwise wait in the write forever. Theory §5.
