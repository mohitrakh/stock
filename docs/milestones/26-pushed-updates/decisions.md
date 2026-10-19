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
