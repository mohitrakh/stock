# Milestone 24 — Theory

Everything you need to know to read this milestone's code and measurements without any earlier
context. Each section starts from first principles, works an example with numbers, and ends with
what it means for this exchange. Terms in **bold** are defined in `glossary.md` as well.

Contents:

1. Trust boundaries, and where each check belongs
2. Authentication and authorization
3. Journaled decisions and gateway decisions: why replay forces the split
4. Reference data: what a "product" is on a real exchange
5. Rate limiting: the algorithms
6. Rate limiting: on real exchanges, and in this one
7. Password hashing and bcrypt
8. Async runtimes: why a slow function in an `async fn` stalls unrelated requests
9. User enumeration and timing side channels
10. Measuring latency: open loops, coordinated omission, HdrHistogram, Little's law
11. `fsync`, drive write caches and power-loss protection
12. Measuring inside containers

---

## 1. Trust boundaries, and where each check belongs

### 1.1 The idea

A **trust boundary** is any line in a system where data crosses from a party you do not trust to
code that must stay correct. Everything that crosses it must be checked, *on the trusted side*,
before it can change anything that matters.

The important word is "before". A check that runs after the state has already changed is not a
check at all, just a report of the damage.

In this exchange there are three kinds of caller:

| Caller | How it reaches the exchange | Trusted? |
|---|---|---|
| Customer (trader) | HTTP on the customer port, `127.0.0.1:4000`, with a JWT | No |
| Operator | HTTP on the operator port, `127.0.0.1:4004` (loopback only, no credentials) | Yes |
| Another exchange process (replica, warm replica, subscribers) | Files (journal, mmap stream) and the replication TCP link | Yes, on a private network |

So there are two trust boundaries that matter for this milestone. One is the customer port. The
other is the edge of the operator port, which today is protected only by being reachable from
loopback alone.

### 1.2 Two places a check can live

A request passes through two very different pieces of code on its way to changing state:

```text
customer ──HTTP──▶ gateway (Axum handlers on Tokio worker threads)
                     │  may read the clock, the database, the environment
                     │  may refuse with an HTTP error; a refusal here is never journaled
                     ▼
                   bounded command queue (10,000 slots)
                     ▼
                   exchange worker thread → prepare_input_event → journal → commit
                     │  a pure function of (current core state, journaled input)
                     │  a refusal here IS journaled, as a rejection output event
```

- **The gateway** can use anything: the clock, the database, the network, randomness. But nothing
  it decides is recorded. Replay cannot repeat a gateway decision, so a gateway decision must never
  change exchange state. It may only stop a request from reaching the core at all.
- **The core** (everything from `prepare_input_event` down) may use only two things: its current
  state and the journaled input. Everything it decides is recorded in the journal as an output
  event, and replay recomputes it and compares (milestone 6). In exchange, it may not read the clock,
  the database, the environment, or anything else replay cannot reproduce.

### 1.3 The rule for placing a check

Ask one question: **does the decision depend only on the exchange's own state and the command
itself?**

- If yes, it belongs in the core, and its rejection is journaled. Replay will reach the same
  decision because it has the same state and the same command.
- If no, because it depends on the clock, the database or the order of arrival, it belongs in the
  gateway. It must only *admit or refuse*, before the command is enqueued.

Applied to this milestone:

| Check | Depends on | Where | Journaled? |
|---|---|---|---|
| JWT signature and expiry | token, key, **clock** (`exp`) | gateway | no |
| Rate limit per user | **arrival times** | gateway | no |
| Operator-only actions | which port the request arrived on | gateway (routing) | no |
| User exists before an operator deposit (decision 2) | **PostgreSQL** | operator gateway | no |
| Identifier syntax (1–64 bytes, no control characters) | the request only | gateway (and it never changes) | no |
| Product is listed | core state (the list is journaled) | core | yes |
| Per-user resting-order cap | core state (the books) | core | yes |
| Risk cap, cash and share collateral | core state | core | yes (since milestones 9–10) |
| Market open | core state (sessions are journaled) | core | yes (since milestone 22) |

### 1.4 Checking in both places

Some checks happen at the edge *and* in the core. The gateway refuses a malformed identifier so
that bad data never reaches the journal (milestone 21 part 3: a multi-kilobyte id would have stopped
the reporter on every restart). If something ever skipped the gateway, the core must still be safe.
Milestone 22's close-size check is an example: it is a "safety net for orders that skipped the
gateway's id check". This is **defence in depth**: each layer protects the next without assuming
the layer before it worked.

### 1.5 Authority, not just validity

Checks of *validity* ask "is this request well formed and affordable?". Checks of *authority* ask
"may this caller ask for this at all?". The exchange was good at validity and had no authority
checks: any authenticated caller could request any command, including ones only an operator should
send (deposits, risk limits). This milestone adds authority by moving those commands to a port that
only the operator can reach.

---

## 2. Authentication and authorization

### 2.1 Two different questions

- **Authentication** answers "who is this?". Here, login checks an email and password, and from
  then on a **JWT** proves the identity on each request.
- **Authorization** answers "may this identity do this?". Here, before this milestone, the answer
  was "any authenticated customer may do anything a customer route offers". After it, some actions
  belong to the operator only.

Mixing them up is a classic mistake. "The user is logged in" (authentication) is not the same as
"the user may set risk limits" (authorization). The exchange before this milestone made exactly
this mistake with deposits and limits.

### 2.2 What a JWT is, and what checking one proves

A **JSON Web Token** is three base64url-encoded parts joined by dots: `header.claims.signature`.

```text
header  = {"alg":"HS256","typ":"JWT"}
claims  = {"sub":"42","exp":1760300000}
signature = HMAC-SHA256(key = JWT_SECRET, message = base64url(header) + "." + base64url(claims))
```

- `sub` (subject) is the user id. The exchange uses it as the `user_id` of every command the
  request sends.
- `exp` is an expiry time in seconds since 1970. Login sets it 24 hours ahead.
- HS256 is an HMAC, a keyed hash. Only someone with `JWT_SECRET` can produce a valid signature, and
  the server checks one by recomputing it.

Checking a token proves two things: the claims were produced by someone holding the key, and they
have not expired. It proves nothing else. In particular:

- **It cannot be revoked.** A stolen token works until `exp`. Revocation needs server-side state,
  such as a deny list or short-lived tokens with refresh.
- **It carries no permissions** unless the claims include them. Ours carry only `sub`.
- **The key is everything.** Anyone with `JWT_SECRET` can mint a token for any user. Milestone 23
  part 1 removed the fallback key `"secret"` for this reason. The milestone 23 load client used
  exactly this property: it signed its own tokens with the run's throwaway secret. This milestone's
  HTTP load generator does the same (see `part-1-baseline.md`).

### 2.3 Authorization by reachability

The operator port binds `127.0.0.1` only, so only processes on the same machine, in the same
network namespace, can connect. That is **authorization by network reachability**: whoever can
reach the port is trusted. It is weak in the ways you would expect. Any process on the host is
trusted, there is no record of *which* operator did something, and nothing protects against a
mistake.

It is the trust model milestones 18 and 22 already chose for the warm replica's and the session's
ports. This milestone keeps it and records the gap ("Not in this milestone: operator
authentication and an audit of which operator did what"). A real exchange would authenticate every
operator action and log who did it, often with a second person approving (the **four-eyes
principle**).

---

## 3. Journaled decisions and gateway decisions: why replay forces the split

### 3.1 Determinism, restated

The whole design since milestone 6 rests on one property: the exchange core is a **deterministic
state machine**:

```text
state(n+1), outputs(n+1) = f(state(n), input(n+1))
```

`f` is the same code on every machine, at every time. The journal stores every input *and* every
output. Replay (milestone 6), recovery (milestone 8), the warm replica (milestone 18) and promotion
(milestone 23) all re-run `f` over the journaled inputs and check that the outputs match, record
by record. One mismatch stops everything (`OutputMismatch`).

### 3.2 What breaks determinism

Anything `f` reads that is not in `state` or `input`:

- **The clock.** Milestone 10 found this. Resetting the daily risk counters from
  `SystemTime::now()` would have made yesterday's journal replay differently today. The fix took
  the day from the order's own journaled timestamp, and milestone 22 replaced that with journaled
  session commands.
- **Configuration.** Milestone 10 made risk limits events, not settings, "which would have made
  replay depend on the environment".
- **A database.** If `f` asked PostgreSQL "does this user exist?", replay months later would get a
  different answer.
- **Arrival order of anything not journaled.**
- **Randomness, and hash-map iteration order.** Milestone 22 part 2 found this one: "the books'
  hash-map order differs between processes", so expiries are ordered by sequence number instead.

### 3.3 Two ways to use non-deterministic information safely

1. **Journal the decision as an input.** A session open is a command from the operator; the
   operator's choice of day is non-deterministic, but once journaled it is just data. The product
   list in this milestone works the same way: `ProductListRequested` is an input, so the list is
   part of the state and replay rebuilds it exactly.
2. **Decide before the core, and only refuse.** A rate limit depends on arrival times, which are
   not journaled. If the gateway refuses a request, the command never exists. Nothing is enqueued,
   nothing is journaled, nothing changes, and replay has nothing to reproduce. A gateway decision
   is safe if and only if it can only *prevent* a command, never *alter* one.

What would be **wrong**: letting the gateway's arrival-time decision become part of a command,
for example by passing "this user is over the limit" into the core and having the core journal a
rejection. Replay would see the journaled rejection and have to reach the same decision without
the arrival times. It could not, unless the decision itself were made an input. That would be
option 1 in disguise, and pointless: it would fill the journal with refused spam.

### 3.4 The asymmetry that makes the per-user book cap different

The per-user resting-order cap (behaviour 3) is *also* about limiting one customer. But it depends
only on the books, which are core state. So it belongs in the core and is journaled, like
`BookFull` from milestone 22. The two limits in this milestone sit on opposite sides of the
boundary for exactly the reason in 3.3:

- the **rate limit** limits *how fast* a customer may send. That depends on time, so it lives in
  the gateway and is not journaled;
- the **book share** limits *how much* of the exchange a customer may occupy. That depends on
  state, so it lives in the core and is journaled.

---

## 4. Reference data: what a "product" is on a real exchange

### 4.1 Reference data

**Reference data** (also called static data or the securities master) is everything the exchange
must know about an instrument before it can trade it. Typical fields:

- an identifier: a ticker symbol, plus usually a global one such as an ISIN (a 12-character
  International Securities Identification Number, e.g. `US0378331005`);
- the instrument type (equity, ETF, future, option) and its currency;
- the **tick size**, the smallest allowed price increment, e.g. $0.01. Prices that are not a
  multiple of it are refused;
- the **lot size**, the smallest (or standard) quantity unit;
- price limits, such as bands or circuit-breaker levels around a reference price;
- the trading status: listed, halted, suspended, delisted.

In the FIX protocol, the standard message format between brokers and exchanges, an instrument's
definition is a `SecurityDefinition` message (MsgType `d`), and changes in its trading status are
`SecurityStatus` messages (MsgType `f`). Clients learn the list of tradable instruments from the
exchange; they cannot invent one.

### 4.2 Why listing is an exchange action

Listing is a regulated, deliberate act. A company applies, the exchange admits the security, and
from a set date it trades. The exchange creates the order book when the instrument is listed, not
when someone first sends an order for it. Before this milestone, our exchange created a book the
first time any customer named a symbol, which let customers invent instruments. That is the
unbounded state in `00-selection.md` A.3.

### 4.3 What this milestone builds

Decision 1 keeps a product to its symbol only. A product exists when a journaled
`ProductListRequested` has produced `ProductListed`. Tick size, lot size, price bands, halts and
delisting are left out, and recorded as such. They are all real reference-data features. Each
changes price or quantity validation for every client, and each deserves its own milestone.

---

## 5. Rate limiting: the algorithms

A **rate limiter** decides, for each request, whether the sender is within an allowed rate. All
the algorithms below keep some state per **key** (here: per user, or per IP address) and answer
"admit" or "refuse" in constant time.

The running example: **100 requests per second, with bursts of up to 200**.

### 5.1 Fixed window counter

Count requests in each calendar second; refuse after 100 in the same second.

- State per key: the current window's start, and a counter.
- Flaw: the **boundary burst**. Send 100 requests at 0.999 s and 100 more at 1.001 s. Each window
  sees 100, both are admitted, and the receiver got 200 requests in 2 ms. The real peak rate is up
  to twice the limit, at every window boundary.
- It also has no notion of "burst": the 101st request in a window is refused even after an idle
  hour.

### 5.2 Sliding window log

Keep the timestamp of every admitted request in the last second, and admit only if there are
fewer than 100.

- Exact, with no boundary burst.
- State per key: up to 100 timestamps, so memory grows with the limit. At 8 bytes each, that is
  800 bytes per user, and every check must also drop old entries.

### 5.3 Sliding window counter

Keep the counts of the current and the previous fixed window. Estimate the count over the last
second as `current + previous × (fraction of the previous window still inside the last second)`.

- Constant memory, much smaller boundary error. It is an approximation: it assumes the previous
  window's requests were spread evenly.

### 5.4 Token bucket

A bucket holds up to **B** tokens (the **burst**, here 200). Tokens drip in at **r** per second
(here 100), and the bucket never holds more than B. Each request takes one token, and a request
that finds the bucket empty is refused.

You do not need a timer to add tokens. Store the token count and the time it was last updated, and
add the tokens owed whenever a request arrives:

```text
on request at time t:
    tokens = min(B, tokens + (t − last) × r)
    last   = t
    if tokens ≥ 1:  tokens −= 1;  admit
    else:           refuse; the next token arrives in (1 − tokens) / r seconds
```

Worked example, B = 200, r = 100 per second:

- A user idle for 2 s or more has a full bucket: 200 tokens.
- They send 300 requests at once. The first 200 are admitted, which empties the bucket, and the
  next 100 are refused.
- If instead they keep sending as fast as they can after the burst, they are admitted at exactly
  r: one request per 1 / 100 s = 10 ms.
- Over any interval of length T, the admitted count is at most B + r × T. Over 1 s that is
  200 + 100 = 300; over 10 s, 200 + 1,000 = 1,200. The long-run rate is r, and the burst allowance
  is B.

State per key: the token count and the last update time, about 16 bytes. The **`Retry-After`** a
refused client is given is `(1 − tokens) / r`, rounded up to whole seconds (the header's unit).

### 5.5 Leaky bucket

There are two different things under this name:

1. **As a meter.** Each request pours one unit into a bucket that leaks at r units per second and
   holds at most B units. A request that would overflow it is refused. This is the token bucket
   seen from the other side: the bucket's *fill* is B minus the token count, and the two admit
   exactly the same requests.
2. **As a queue.** Requests wait in a FIFO and leave at exactly r per second. This *smooths*
   traffic instead of refusing it, and adds latency. It is used for network shaping, and is not
   what an exchange gateway wants: a held order is worse than a refused one, because the client
   does not know when it will run.

### 5.6 GCRA (generic cell rate algorithm)

Designed for ATM networks. It is a token bucket stored as one timestamp instead of two numbers:

- **T** = the emission interval = 1 / r = 10 ms.
- **τ** (tau) = the tolerance, how far ahead of schedule a sender may be = (B − 1) × T
  = 199 × 10 ms = 1,990 ms.
- **TAT** = the theoretical arrival time: when the next request "should" arrive if the sender
  kept exactly to rate r.

```text
on request at time t:
    if TAT − t > τ:  refuse; retry after (TAT − τ − t)
    else:            TAT = max(TAT, t) + T;  admit
```

Check that it admits a burst of exactly B = 200 after an idle period. Idle means TAT ≤ t. After k
admitted requests at the same instant t, TAT = t + k × T. The next one is admitted while
TAT − t ≤ τ, that is while k × 10 ms ≤ 1,990 ms, that is while k ≤ 199. So requests number 1 to
200 (k = 0 to 199) are admitted and the 201st is refused. That is the same as the token bucket.

State per key: one timestamp, 8 bytes. `governor`, the crate offered in decision 5, implements
GCRA. Decision 5 chose a hand-written token bucket instead; the two admit the same traffic, and the
token bucket is the easier one to read.

### 5.7 Choices every rate limiter has to make

- **The key.** Per user (from the JWT) is fair between users. Per IP is the only option before
  login. But many users can share one IP behind a NAT, and one attacker can hold many addresses
  (an IPv6 user often controls a whole /64 network, 2^64 addresses).
- **Bounded memory.** A map with one entry per key grows with the number of keys. An attacker who
  can mint keys (new IPs, new accounts) can grow it. Entries whose bucket would be full anyway, i.e.
  idle for at least B / r = 2 s, carry no information and can be dropped.
- **Precision.** Use a monotonic clock (`Instant`), never wall time, which can jump backwards.
- **Where the clock is read.** Only in the gateway. See section 3.

---

## 6. Rate limiting: on real exchanges, and in this one

### 6.1 Real exchanges

Exchanges limit message rates per connection or session, usually as so many messages per second.
A session over its limit has messages rejected, or is disconnected. Many also watch the
**order-to-trade ratio**: messages sent per trade done. A participant who sends thousands of orders
and cancels for every trade loads the system without adding liquidity. In the EU, MiFID II requires
trading venues to have such controls. The exact numbers and penalties vary by venue and are not
reproduced here.

### 6.2 Why this exchange needs one

The command queue is bounded at 10,000 (`EXCHANGE_COMMAND_QUEUE_SIZE`). When it is full, every
handler's `tx.send(...).await` waits for a free slot. That is **back-pressure**: the queue cannot
grow without limit. But back-pressure is shared. One customer sending as fast as possible fills
the queue, and from then on *every* customer's request waits behind theirs. There is no fairness
between customers on the way in. A per-user rate limit makes one customer's excess fail fast
(429) instead of delaying everyone.

### 6.3 HTTP 429 and Retry-After

`429 Too Many Requests` is defined by RFC 6585. It may carry a `Retry-After` header (RFC 9110), in
seconds, telling the client when to try again. A well-behaved client waits that long. A refused
request was never enqueued, so retrying it is always safe.

---

## 7. Password hashing and bcrypt

### 7.1 Why passwords are hashed slowly

A server must be able to check a password without storing it. It stores `H(password)` and compares
`H(attempt)` against it. If the database is stolen, the attacker has the hashes and tries guesses
offline, as fast as their hardware allows:

- With a fast hash such as SHA-256, one GPU computes billions of hashes per second. Every
  common password, and every short one, falls in minutes.
- A **salt** (random bytes stored beside each hash and mixed into it) stops one guess being tested
  against every user at once, and defeats precomputed tables. It does not make each guess
  slower.
- A **slow hash** makes each guess expensive. If one hash takes 0.25 s, one CPU core tries only 4
  guesses per second per account.

The cost lands on the server too: every login pays one slow hash. That is the trade, and why
logins need rate limits (section 9 and Part 5).

### 7.2 How bcrypt works

bcrypt was designed by Niels Provos and David Mazières in 1999. It is built on the Blowfish cipher,
whose key schedule is unusually expensive. Blowfish's state is:

- the P-array: 18 subkeys of 32 bits = 72 bytes;
- four S-boxes of 256 entries × 32 bits = 4 × 1 KiB = 4,096 bytes;
- 4,168 bytes in total.

`ExpandKey(state, salt, key)` mixes the key (and salt) into the P-array, then regenerates every
entry of the P-array and the S-boxes by repeatedly encrypting with the current state. The state is
18 + 1,024 = 1,042 32-bit words, and each Blowfish encryption produces 64 bits = 2 words, so one
`ExpandKey` costs 1,042 / 2 = **521 Blowfish encryptions**.

bcrypt's setup, "EksBlowfish" (expensive key schedule):

```text
state = initial Blowfish state (digits of π)
ExpandKey(state, salt, password)
repeat 2^cost times:
    ExpandKey(state, 0, password)
    ExpandKey(state, 0, salt)
then encrypt the 24-byte text "OrpheanBeholderScryDoubt" 64 times with the final state
```

So the work is dominated by `2^cost × 2 × 521` Blowfish encryptions. At **cost 12**:

```text
2^12 × 2 × 521 = 4,096 × 1,042 = 4,268,032 Blowfish block encryptions
```

Each encryption is 16 Feistel rounds, each round 4 S-box lookups plus additions and XORs. If one
encryption takes about 50 ns on a desktop core (an **estimate** of the right order, not a
measurement), cost 12 takes about 4.27 million × 50 ns ≈ 0.21 s. The real figure for the office
machine is measured in Part 1 (`performance/02-bcrypt-cost.md`). The point of the arithmetic is
the *shape*: the time is proportional to 2^cost, so cost 13 takes twice as long as 12, and cost 11
half as long.

The stored string looks like this:

```text
$2b$12$R9h/cIPz0gi.URNNX3kh2OPST9/PgBkqquzi.Ss7KIUgO2t0jWMUW
 │  │  └─ 22 characters: the 16-byte salt  └─ 31 characters: the 23-byte hash
 │  └─ cost
 └─ version
```

(Base64 here uses bcrypt's own alphabet. 16 bytes need 22 characters and 23 bytes need 31.)

Verifying a password reads the cost and salt from that string and recomputes. So **hashing and
verifying cost the same**: a login and a registration both pay one full cost-12 computation.

Limits worth knowing:
- bcrypt uses at most 72 bytes of the password. Implementations either ignore the rest or refuse
  longer passwords.
- It needs only about 4 KiB of memory. That makes it cheaper to attack on GPUs than **memory-hard**
  functions such as scrypt or Argon2id, which force each guess to use a lot of memory. Current
  guidance (for example OWASP's password-storage cheat sheet) prefers Argon2id and accepts bcrypt
  at a sufficient cost. Changing the algorithm is outside this milestone.

### 7.3 The cost as a denial-of-service lever

A slow hash is **asymmetric**. The client sends about 100 bytes, and the server spends about
0.2 s (estimated) of a core. At that cost, 4 hardware threads finish only about 4 / 0.21 s ≈ 19
calls per second, so about 19 requests per second would keep a 4-thread machine
saturated. (Measured on the office machine: about 15 per second, `performance/02-bcrypt-cost.md`.)
That is why login and registration need a rate limit per IP address, and why decision 7
caps the bcrypt calls in flight.

---

## 8. Async runtimes: why a slow function in an `async fn` stalls unrelated requests

### 8.1 Futures and polling

In Rust, an `async fn` compiles to a **future**: a state machine that does some work each time it
is **polled**, and either finishes (`Ready`) or reports that it is waiting for something
(`Pending`). It only reports `Pending` at an `.await` on something not ready yet: a socket read, a
channel receive, a timer.

Between two `.await`s, the code runs straight through on whatever thread polled it. Nothing can
interrupt it. This is **cooperative multitasking**: a task gives up the thread only by reaching an
`.await` that is not ready.

### 8.2 Tokio's multi-threaded runtime

`#[tokio::main]` builds Tokio's multi-threaded runtime:

- **Worker threads**: by default one per CPU core Tokio detects. (It can be changed with
  `#[tokio::main(worker_threads = N)]` or the `TOKIO_WORKER_THREADS` environment variable.) On the
  office i3-7100, with no container CPU limit, that is 4.
- Each worker polls tasks from its own local queue. An idle worker **steals** tasks from busy
  workers' queues, so work spreads out.
- Every Axum connection and request is a task, so all HTTP work runs on these workers.

### 8.3 What a blocking call does

Suppose a task calls a function that runs for 200 ms without any `.await` (bcrypt), or sleeps the
thread, or does synchronous file I/O. For those 200 ms:

- that worker thread polls nothing else;
- tasks already in its local queue can be stolen by other workers, *if* other workers are free;
- if all workers are blocked the same way, **no task in the whole runtime makes progress**:
  - no request is parsed;
  - no order is enqueued;
  - no reply is written, including replies to orders the exchange worker has already finished.

With 4 workers, 4 simultaneous bcrypt calls block everything for the length of one bcrypt. With 8
simultaneous calls the workers run two rounds back to back, and the stall is twice as long. Every
order request arriving during the stall is charged the whole of it.

This is why a CPU-heavy call inside an `async fn` is a bug, even though it compiles and works when
tested alone.

### 8.4 The blocking pool: `spawn_blocking`

Tokio keeps a second pool of threads for exactly this: `tokio::task::spawn_blocking(f)` runs `f`
on a **blocking-pool thread** and returns a future that becomes ready when `f` finishes. The worker
that awaits it is free in the meantime. Defaults: up to 512 blocking threads, created on demand,
each kept for 10 s after its last job.

What it fixes, and what it does not:
- It fixes the stall of the *runtime*. The workers stay free to parse orders and send replies.
- It does not create CPU. The blocking threads still need cores. 16 simultaneous logins on 16
  blocking threads compete with the 4 workers and the exchange thread for 4 hardware threads. The
  OS scheduler shares the time, so everything slows, but nothing stops.
- Hence decision 7: a **semaphore** (`tokio::sync::Semaphore` with N permits) admits at most N
  bcrypt calls at once. The (N+1)-th login waits asynchronously, without occupying any thread,
  until a permit is free. Choosing N below the number of cores guarantees that some CPU is always
  left for orders.

### 8.5 How this exchange's threads are laid out

```text
Tokio workers (4)      Axum: parse, JWT, enqueue ─────┐            ┌── wake reply task, write response
                                                     ▼            │
exchange worker (1 std::thread)       bounded queue → prepare → journal sync → commit → reply (oneshot)
blocking pool (0..512)                (after Part 2) bcrypt
other processes        replica, warm replica, MDP, reporter, PostgreSQL, ...
```

The exchange worker is a plain `std::thread`, outside Tokio (`serve_primary`), so a blocked Tokio
runtime never blocks *matching*. But an order's round trip crosses Tokio twice, on the way in and
on the way out, so a blocked runtime still delays every order.

---

## 9. User enumeration and timing side channels

**User enumeration** means learning which accounts exist. It matters because a list of valid
emails is the first input to password guessing and phishing.

Two ways a login can leak it:

1. **By content.** Different messages for "no such user" and "wrong password". The fix is one
   message for both.
2. **By timing**, a **side channel**: information leaked by how long something takes rather than
   by what it returns. Our login returns right after the database lookup for an unknown email, but
   runs a cost-12 bcrypt for a known one, so the difference is about one bcrypt (estimated in the
   hundreds of milliseconds). That gap is easy to measure from across the internet, and averaging a
   few attempts removes the network noise. The fix is to always run one bcrypt verify: against the
   stored hash when the user exists, and against a fixed dummy hash when not. Both paths then cost
   one bcrypt.

Registration also enumerates: it answers "user already exists" for a taken email. That is harder
to fix without changing the product. The usual answer is to always say "check your email" and send
the message only if the address is new. That needs email delivery, which this project does not
have. Part 2 records it as a known limitation rather than half-fixing it.

The comparison itself must also be constant time: checking two hashes byte by byte and stopping
at the first difference leaks how many leading bytes match. The `bcrypt` crate's `verify` compares
in constant time.

---

## 10. Measuring latency: open loops, coordinated omission, HdrHistogram, Little's law

### 10.1 Closed and open loops

- A **closed-loop** load generator has N clients. Each sends a request, waits for the reply, then
  sends the next. Its rate is set by the system's speed: if the system slows, the clients slow
  with it.
- An **open-loop** generator sends requests on a schedule, request i at `start + i / R`, whether
  or not earlier ones were answered. That is how real traffic behaves: thousands of independent
  customers do not slow down because the exchange is slow.

To measure latency at a given load, the generator must be open loop.

### 10.2 Coordinated omission

A closed-loop tester measures from when it *actually* sent each request. Suppose the system stalls
for 2 s. The tester sends nothing during the stall, records one slow request, and its histogram
says 99.9% of requests were fast. But at 1,000 requests/s, 2,000 real requests would have arrived
during the stall and waited. The tester "coordinated" with the system and omitted exactly the bad
period. Gil Tene named this **coordinated omission**.

The fix the project has used since milestone 19 (Harness, "Coordinated omission"): time each request
from its **intended** send time. If the generator itself falls behind, the delay is charged to the
system, as a real client would experience it.

The HTTP generator in this milestone follows the same rule.

### 10.3 Percentiles, and why p99 matters

The **pN** latency is the value below which N% of requests fall. p50 is the median.

Why the design cares about p99 more than the average: a customer does not send one order. If each
of their requests independently has a 1% chance of being slower than the p99, then over 100
requests the chance of hitting at least one is:

```text
1 − 0.99^100 = 1 − 0.366 = 0.634, i.e. 63%
```

So most active customers see the p99 several times a day. The tail is what people experience.

### 10.4 HdrHistogram

To compute percentiles you either keep every sample (memory grows with the run) or bucket them. A
**High Dynamic Range histogram** buckets with *bounded relative error*. With 3 significant decimal
digits, every recorded value is accurate to within 0.1% whatever its size, so 1 µs and 10 s are
both stored precisely enough. The memory depends on the range of values (logarithmically), not on
how many are recorded. The project uses the `hdrhistogram` crate with 3 significant digits.

### 10.5 Little's law

For any stable system:

```text
L = λ × W
L = average number of requests in the system
λ = average arrival rate
W = average time each spends in the system
```

At λ = 5,000 orders/s and W = 10 ms, L = 5,000 × 0.010 = 50 orders in flight on average. This
sizes the generator: with HTTP/1.1 each connection carries one request at a time, so the client
needs at least about 50 connections, and more at the tail. With fewer, orders queue *inside the
client* and the client measures itself. The milestone 23 Python client had 16 connections (16
threads), so above about 16 / 0.010 s = 1,600 orders/s at 10 ms it could not have kept an open
loop.

### 10.6 The generator must not be the bottleneck

The generator runs on the same 4 hardware threads as the exchange. Its CPU per request (building
the HTTP request, JSON, parsing the response) is taken from the exchange. So the generator reports
its own CPU time (user and system, from `/proc/self/stat`) per order. A generator that uses a large
share of the machine is a disclosed limitation of the measurement.

---

## 11. `fsync`, drive write caches and power-loss protection

### 11.1 The layers a write passes through

```text
write(fd, bytes)           → the kernel's page cache (RAM); returns at once
fsync(fd) / sync_all()     → the kernel writes the dirty pages and the file's metadata to the
                             device, then asks the device to make them durable (a cache FLUSH,
                             or writes with Force Unit Access); returns when the device says done
fdatasync / sync_data()    → the same, minus metadata that is not needed to read the data back
                             (e.g. modification time; but an append still needs the new size)
the device                 → its own volatile write cache (DRAM or SLC), then NAND flash
```

Only after `fsync` returns is the data guaranteed to survive a power cut, and only if the device
tells the truth about its flush.

### 11.2 Why consumer SSDs sync slowly

A consumer SSD acknowledges ordinary writes from its volatile cache, which is fast. On a flush it
must get the data into NAND flash first, because a power cut would wipe the cache. Programming
NAND takes on the order of hundreds of microseconds to milliseconds, and the drive may first have
to finish internal work such as garbage collection. So a flush on such a drive takes
**milliseconds**, with a long, noisy tail.

### 11.3 Power-loss protection

Server and enterprise SSDs often carry capacitors that store enough energy to write the whole
volatile cache to flash after power is lost. Such a drive can treat its cache as durable and
acknowledge a flush as soon as the data reaches the cache. Flushes then commonly take **tens of
microseconds**.

### 11.4 What this means here

Every acknowledged command in this exchange waits for at least one `sync_all`, or two when
replicated. The floor of the latency is therefore the flush time of the slowest drive involved. The
office machine's drive is an ADATA SU650, a consumer SATA SSD. Part 1 measures its flush latency
directly (`performance/04-raw-sync.md`), so that later work can say how much of the latency is the
disk and how much is software.

### 11.5 "fsyncgate"

In 2018 PostgreSQL developers found that on Linux, after a failed `fsync`, the kernel may drop the
dirty pages and clear the error, so a retried `fsync` "succeeds" while the data is lost. The safe
response is to stop and recover from the log, never to retry. That is what this exchange does
(milestone 19: "after a failed sync the core is never used again").

---

## 12. Measuring inside containers

- **CPU.** A container without `--cpus` or `--cpuset-cpus` sees and can use every host CPU. With
  limits, Rust's `std::thread::available_parallelism`, which Tokio uses to size its workers, takes
  cgroup quotas into account. Part 1 records the worker count the exchange actually started.
- **Disk.** A Docker named volume is a directory on the host's filesystem
  (`/var/lib/docker/volumes/...`), so `fsync` inside the container reaches the host SSD directly.
  The container's own writable layer goes through the overlay filesystem instead. All journals in
  this project's scripts live on named volumes.
- **Network.** Containers on one Docker network talk through a virtual bridge. Containers that
  share a network namespace (`--network container:X`) talk over loopback, which is how a load
  generator reaches the exchange's loopback-only ports.
- **Neighbours.** Everything else running on the host shares the CPU, memory bandwidth and the
  SSD's queue. On the office machine that is about 29 other containers, not counting the ones a
  measurement starts. Their load varies, so
  repeated runs and reporting the spread between them matter more than any single figure.
