# Milestone 26 — Decisions

Every owner decision for this milestone, with its date and the options shown. A decision changed
later keeps its old entry, and a new dated entry follows it.

## 2026-10-19 — Selection of the milestone

**Chosen:** pushed updates, after milestone 25 closed. The owner asked "what should be our next
task/milestone"; the recommendation was pushed updates (WebSocket), and the owner answered "yes plan
milestone 26".

**Options shown:**
- pushed updates (recommended): an unmet functional requirement, with new subjects to learn;
- set-based reporter writes or `COPY`: the reporter is off the trading path and catches up, so this
  matters only under sustained volume.

## 2026-10-19 — Decisions 1–6 accepted

The owner answered "accept all decisions, push and start part 1": each decision below is accepted
as recommended.

## 2026-10-19 — Decisions as proposed

Proposed in `PROJECT_DIRECTION.md`, "26. Pushed Updates", "Decisions for the owner". The theory
behind each is in `01-theory.md`.

1. **Transport.** WebSocket, through axum's `ws` feature, which adds `tokio-tungstenite`
   (recommended). The alternative is Server-Sent Events (§2).
2. **Where each feed runs.** The public feed in the MDP, the private feed in a new process
   (recommended). The alternative is the private feed in the primary.
3. **Slow consumers.** A bounded queue per connection and a deadline on each socket write, disconnecting when the queue fills or a write misses its deadline (recommended).
   The alternatives are dropping messages, or conflating to a snapshot (§5).
4. **Private-feed authentication.** The JWT in the first message, with a deadline, closing at
   expiry (recommended). The alternatives are the URL query, `Sec-WebSocket-Protocol`, or a cookie
   with an `Origin` check (§7).
5. **Resume after a disconnect.** No replay: REST fills the gap (recommended). Its limits are
   listed with the decision: REST holds one trading day, its replies carry no stream sequence, and
   clients deduplicate fills on `(symbol, execution_id)`. The alternative is a per-user replay
   buffer.
6. **Book depth.** Full-depth snapshot and deltas (recommended). The alternative is top-N snapshots
   on change.

## 2026-10-20 — Part 3's completion criterion: the explanation accepted

**Chosen:** close Part 3 on the explanation. The owner answered "yes close part 3 i think the CPU
contention is issue".

**The question.** The criterion is "the mean must differ by less than 10%, or the difference must
be explained". With the feeds and 100 clients of each on the same two-core machine, the HTTP mean
at 5,000 orders/s alone rose 65.5% and the maximum rate fell 61% (alone) and 52% (replicated)
(`performance/01-feeds-measured.md`, section C). Pinning the feeds to the other physical core
recovered about 44% of the maximum-rate loss (section E); the rest is explained only by hypothesis
(unpinned other containers on the exchange's core, shared cache and memory, the stream file's
lock).

**Options shown:**
1. accept the explanation, CPU contention on the shared two-core machine, and close the part
   (chosen);
2. first test the remaining hypothesis: the feeds running with no clients, and the stream lock;
3. optimize first, then measure again: a shorter idle poll or a wake-up from the stream for the
   push latency (an inference: about 5 ms of the 5.3–7.4 ms gap), and batched writes for the
   private feed's fan-out.

**What stays open, recorded for later:** options 2 and 3 are not done. On this machine the feeds
cost the exchange CPU; a deployment that runs them on another machine would not share it (an
inference, not measured here).
