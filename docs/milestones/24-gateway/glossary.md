# Milestone 24 — Glossary

Terms used in this milestone's documents, in alphabetical order. Where a term has a full section in
`01-theory.md`, the section is given.

**Authentication.** Establishing *who* a caller is: here, a login with email and password, then a
JWT on each request. Theory §2.

**Authorization.** Deciding whether an authenticated caller *may* do what it asks: here, which
actions belong to customers and which to the operator. Theory §2.

**Back-pressure.** A full bounded queue makes senders wait instead of growing without limit. Safe
for memory, but shared: one heavy sender delays everyone. Theory §6.2.

**bcrypt.** A deliberately slow password hash built on Blowfish's expensive key schedule; its
*cost* is log2 of the number of key-schedule rounds. Theory §7.2; measured in
`performance/02-bcrypt-cost.md`.

**Blocking pool.** Tokio's second set of threads, for work that would block a worker thread; reached
with `spawn_blocking`. Theory §8.4.

**Burst (B).** The number of requests a rate limiter admits at once after an idle period: the token
bucket's capacity. Theory §5.4.

**Closed loop / open loop.** A closed-loop load generator waits for each answer before sending the
next; an open-loop one sends on a schedule regardless. Only an open loop measures latency at a given
load. Theory §10.1.

**Coordinated omission.** The error a closed-loop tester makes by not sending, and so not timing,
requests during a stall. Avoided by timing from each request's *intended* send time. Theory §10.2.

**Cost (bcrypt).** The parameter c such that bcrypt runs 2^c rounds of its key setup; each step
doubles the time. The gateway uses 12. Theory §7.2.

**Defence in depth.** Each layer protects the next without assuming the layer before it worked.
Theory §1.4.

**Deterministic state machine.** The exchange core: its next state and outputs are a function of its
current state and the input only, so replay reproduces them exactly. Theory §3.1.

**Emission interval (T).** In GCRA, the time between requests at exactly the allowed rate: 1 / r.
Theory §5.6.

**Epoch.** The number of a primary's term, journaled at each promotion (milestone 23 part 5).

**fsync / `sync_all`.** The call that returns only when a file's data and metadata are on stable
storage. Theory §11; measured in `performance/04-raw-sync.md`.

**Four-eyes principle.** A sensitive action needs a second person to approve it. Theory §2.3.

**GCRA (generic cell rate algorithm).** A rate limiter that stores one timestamp per key (the
theoretical arrival time) and admits exactly what a token bucket admits. Theory §5.6.

**Gateway.** The part of the exchange that faces customers: HTTP parsing, JWT checks, routing and
(from Part 5) rate limits. It may read the clock and the database, and its refusals are never
journaled. Theory §1.2.

**HdrHistogram.** A histogram whose buckets keep a bounded *relative* error (3 significant digits
here), so it records any range of latencies in bounded memory. Theory §10.4.

**Hyper-Threading.** Intel's two hardware threads per core: two instruction streams share one
core's execution units. The office i3-7100 has 2 cores and 4 hardware threads.

**ISIN.** International Securities Identification Number, a 12-character global instrument
identifier. Theory §4.1.

**Journaled decision.** A decision made by the core, recorded as an output event and reproduced
by replay. Theory §3.

**JWT (JSON Web Token).** `header.claims.signature`; this exchange signs with HS256 (HMAC-SHA256)
and the key `JWT_SECRET`. Theory §2.2.

**Leaky bucket.** Either a meter equivalent to a token bucket, or a queue that releases requests at
a fixed rate. Theory §5.5.

**Listing.** Making a product tradable. Here a journaled operator command, `ProductListRequested`.
Theory §4.

**Little's law.** L = λ × W: the average number in a system equals the arrival rate times the average
time spent there. Theory §10.5.

**Lot size.** The quantity unit an instrument trades in. Not modeled (decision 1). Theory §4.1.

**Operator port.** `127.0.0.1:4004`, loopback only, for the operator's commands: sessions,
replication control, and from this milestone products, deposits and risk limits. Theory §2.3.

**Order-to-trade ratio.** Messages sent per trade done; venues limit it to discourage load without
liquidity. Theory §6.1.

**Percentile (pN).** The value below which N% of samples fall; p50 is the median. Theory §10.3.

**Power-loss protection (PLP).** Capacitors in a server SSD that let it finish writing its volatile
cache after power fails, so it can acknowledge a flush from cache. Theory §11.3.

**Reference data.** What the exchange must know about an instrument before trading it: identifier,
tick size, lot size, status. Theory §4.1.

**Retry-After.** An HTTP header telling a refused client how many seconds to wait. Theory §6.3.

**Salt.** Random bytes stored with each password hash and mixed into it, so equal passwords hash
differently. Theory §7.1.

**Semaphore.** A counter of permits. A task takes one before entering a section and returns it
after, so at most N tasks are inside at once. Theory §8.4.

**Send lag.** In `--http-bench`, how late the generator actually sent an order compared with its
intended time. Large values mean the generator, not the exchange, is falling behind.

**Side channel.** Information leaked by *how* a system answers (time, size, power) rather than
*what* it answers. Theory §9.

**Tick size.** The smallest allowed price increment. Not modeled (decision 1). Theory §4.1.

**Token bucket.** A rate limiter that admits requests while it has tokens; tokens refill at rate r
up to capacity B. Theory §5.4.

**Tolerance (τ).** In GCRA, how far ahead of schedule a sender may get: (B − 1) × T. Theory §5.6.

**Trust boundary.** A line where data from an untrusted party reaches code that must stay correct.
Theory §1.

**User enumeration.** Learning which accounts exist from a system's answers. Theory §9; measured in
`performance/05-login-enumeration.md`.

**Worker thread (Tokio).** One of the threads, one per CPU by default, that poll every async task:
here, all HTTP work. Theory §8.2.
