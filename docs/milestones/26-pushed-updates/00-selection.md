# Milestone 26 — Why pushed updates, and why in this order

## Why this milestone now

After milestone 25 closed on 2026-10-19, two candidates remained in `PROJECT_DIRECTION.md`:

| Candidate | What it fixes | Why now or not |
|---|---|---|
| **Pushed updates (WebSocket)** | A functional requirement of the design that is not met: clients cannot receive fills or book changes without polling. | Chosen: a missing feature, not a missing speed-up, and new subjects to learn. |
| Set-based reporter writes or `COPY` | The reporter applies about 1,600 commands/s (`docs/performance/06`), against an exchange of 35,000+ orders/s. | Off the trading path; it catches up after a burst. It matters only under sustained volume, which this project does not run. |

The owner asked for the plan on 2026-10-19 ("yes plan milestone 26").

### What the design asks for

From `stock-exchange-system-design.md`:
- functional requirements: a client can "receive matched trades in real-time", and "view the
  real-time order book";
- the market data flow: the publisher "rebuilds the order book and candlestick charts, then
  publishes to subscribers", and the market data and reporting flows are not on the critical path;
- retail clients see 5 levels of L2 data by default, 10 when they pay (not in this milestone);
- fairness: "all receivers of market data get that data at the same time", and a random order of
  subscribers stops clients fighting to be first (recorded as a later step).

The design names multicast over reliable UDP as a way to deliver market data to every receiver at
once, fairly, and FIX over SBE as the protocol for the external domain. This milestone does
neither. It serves the retail case, over WebSocket, on one host.

## What is known at the start

| What | Value | Source |
|---|---|---|
| The MDP applies batches on catch-up at | about 56,000 commands/s | `docs/performance/05-market-data-keeps-up.md` |
| The MDP's live lag behind the exchange | within a second, measured up to about 34,500 orders/s | the same |
| Whether the MDP keeps up at today's maximum rate, with deltas to compute | not measured; Part 3 measures it | — |
| The MDP saves its state | at most once a second | `src/exchange/market_data.rs`, `SAVE_INTERVAL` |
| The exchange's maximum rate on the office machine | about 50,000–65,000 orders/s | milestone 25, `performance/03` and `04` |
| User ids in the MDP's state | none, by rule and by test | milestone 14; `tests/market_data.rs` |
| User ids in a fill (`ExecutionCreated`) | none; buy and sell order ids only | `src/types/types.rs`, `Execution` |
| WebSocket code in the crate | none; axum's `ws` feature is off | `Cargo.toml` |
| How a request is authenticated | the `Authorization` header only (`AuthUser`) | `src/middleware/auth_middleware.rs` |

Two facts shape the design:
1. **The public feed has a natural home.** The MDP already follows the committed stream, applies
   each batch to its books under one lock, and holds no user data. A delta is the difference one
   batch makes to a book, which the MDP computes anyway.
2. **The private feed needs data the MDP must never hold.** To tell the resting side of a fill
   that it traded, a feed must know who owns each open order. The batch that fills it names the
   incoming order's user, not the resting order's. So the private feed keeps its own map of open
   orders and their owners, as the core and the reporter do. Keeping that map out of the MDP keeps
   the public process free of user ids.

## Why the parts come in this order

1. **The public feed first.** It reuses the MDP's books and needs no authentication. It is where
   the hard protocol questions are: the snapshot, the deltas, sequence numbers, gaps, and slow
   consumers. Once those are settled, the private feed reuses the same fan-out.
2. **The private feed second.** It adds what the public feed lacks: the owner map, authentication
   on a long-lived connection, and isolation between users.
3. **Measurement third,** once both feeds exist. It needs a WebSocket client generator, and it must
   show that the exchange is unaffected, which needs both feeds running.
4. **The close last:** the reviews and documents.

## What is deliberately not in this milestone

- **Multicast, reliable UDP, FIX, SBE.** Each is a large subject of its own, for clients this
  project does not have.
- **Serving beyond loopback, TLS, rate limits.** Every listener but the replication port binds
  loopback. Protection against abusive clients is not a goal of this project (owner, 2026-10-14).
- **Replay of history.** The REST endpoints already serve it.
- **Several feed servers, fairness ordering.** Recorded as possible later steps.
