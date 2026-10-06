# Task 18 - One Order Cannot Stop the Exchange (Milestone 23, Part 1)

## Goal

Close the holes that a second machine cannot fix, before milestone 23 copies the journal to one:
- **One order could stop the exchange.** An order that trades against enough resting orders makes
  a journal record too large to write, and the worker halts.
- **One deposit could stop it too.** A balance at `u64::MAX` overflows when a fill credits it, and
  that halts the worker. The independent review of this part found it.
- **A missing setting accepted forged logins.** Without `JWT_SECRET`, the token check fell back to
  the key `"secret"`.

The milestone's specification is the "23. Two Machines" section of `PROJECT_DIRECTION.md`. This
file covers Part 1 only; the rest of the milestone is written up in `docs/tasks/19-two-machines.md`.

## Why this came first

Milestone 23 makes the exchange survive the loss of a machine. None of these holes is about
machines:
- A command that halts the worker is a poison command. Every promoted primary would halt on it in
  turn, because the client can simply send it again. The design names this risk under fault
  tolerance: "bugs could bring down both primary and backup instances". Replication cannot help
  with it.
- The fallback key lets anyone who guesses it act as any user. Two machines would only double the
  number of servers that accept such a token.

The fixes are small, so they went first.

## Part 1a - The order that stopped the exchange

### The problem

Each command is one journal record: its input and every output it produced. A record may not exceed
64 MiB (`MAX_RECORD_LEN`). A new order's record holds two `ExecutionCreated` events per resting
order it trades against. With the longest ids, one trade took about 1,240 bytes in the live run
below, so an order trading against roughly 54,000 resting orders makes a record of more than
64 MiB. With shorter ids it takes more, up to about 140,000.

Nothing refused such an order:
1. preparation accepted it, because nothing in matching looks at record size;
2. the record is encoded before the command is committed, and `encode_record` refused it;
3. that is reported as a storage failure, so the worker halted, and every later order got 503.

The order was never journaled, so a restart recovered cleanly, with the book exactly as before. The
client could then send the same order again. Any user could do it:
- deposits are self-service;
- two accounts get around self-trade prevention;
- the books hold up to 200,000 resting orders (milestone 22), plenty for a large enough sweep.

`DEFERRED_ITEMS.md` had recorded it as deferred since milestone 22.

### What changed

- **A cap on fills per order.** `MAX_FILLS_PER_ORDER = 10_000` (`src/types/matching_engine.rs`):
  one new order may trade against at most 10,000 resting orders. An order that would trade against
  more is refused while it is prepared, as `OrderRejected { reason: "TooManyFills" }`. The customer
  API answers `409`, like `BookFull`, because the refusal depends on the book's state, not on the
  request alone.
- **The decision comes from the plan.** `OrderBook::plan_order` works out an order's fills against
  the live book before anything changes (milestone 19). It now takes the cap and works in two steps:
  1. it walks the resting orders the order would trade with, keeping only references to them;
  2. only if they are within the cap does it build the fills and executions.

  Beyond the cap, the plan is marked `too_many_fills` with nothing built, and
  `ExchangeCore::prepare_add_order_within` refuses it before the resting-order check. A refused
  sweep therefore costs one walk of at most 10,001 resting orders. Building first would have cost
  20,000 executions, each with copied ids, for every refused attempt, a cheap way to keep the
  single worker busy.
- **Replay agrees.** The check is in preparation, which live trading, replay and the warm replica
  share, and it depends only on the book and the order. A refusal is an ordinary rejection,
  journaled like any other, so replay rebuilds it exactly.

### Why 10,000

The worst case for one trade, at the widest values, is 1,376 bytes in the record:
- the gateway's longest ids and symbol: 64 bytes each, every byte a `"` or `\`, which JSON escapes
  to two;
- the largest price, quantity, execution id and sequence numbers, and the longest timestamp JSON
  prints.

10,000 trades take 13.8 MB, a fifth of the limit. The test
`the_largest_order_the_fill_cap_allows_fits_in_one_record` builds exactly that record and checks
it. Real orders come nowhere near the cap: an order taking 10,000 resting orders is extreme on any
exchange.

### Options considered and rejected

- **Refuse by measuring the record.** The close does this (`fits_one_record`), serializing its
  record with every sequence at its widest. Doing it for every order would serialize each order
  twice, on the trading thread, for a case that never occurs in normal trading. A fixed cap costs
  one comparison, and it is a rule a client can understand.
- **Fill up to the cap, then cancel the rest.** That needs a new engine-generated cancellation
  event, with handling in the decoder, the MDP and the reporter. Refusing reuses the existing
  rejection path. The client can split the order.
- **Fill up to the cap and rest the remainder.** The remainder would rest at a price that crosses
  other users' orders, which matching never allows. (It only allows a user's order to rest across
  that user's own, through self-trade skipping.)
- **Make the record limit larger.** That only moves the threshold. Records this large also hold up
  every other command in their group commit.

## Part 1b - The deposit that stopped the exchange

### The problem

Found by the independent review of this part. Deposits refused only an amount that would overflow
the depositor's own balance, so a client could deposit exactly `u64::MAX`. Then:
1. account A deposits `u64::MAX` and one share, and offers the share at 1;
2. account B deposits 1 and buys it;
3. settlement credits A's cash by 1 and overflows. Preparation treats that as an internal fault,
   because a fill is never supposed to fail, so the worker halted.

As with the sweep, nothing was journaled, a restart recovered, and B could buy again. Share holdings
broke the same way: a buyer holding `u64::MAX` shares overflowed on the next share it bought.

### What changed

- **The exchange's totals are capped at deposit time.** A cash deposit is refused if it would take
  the exchange's total cash, across every user, past `u64::MAX`. A share deposit is refused if it
  would take the exchange's total of that symbol past it. The refusal is the existing
  `FundsDepositRejected` / `SharesDepositRejected` with the reason `Overflow`.
- **Why that is enough.** Fills only move cash and shares between users; they never create either.
  So the totals change only with deposits, no balance or holding can exceed its total, and no fill
  can push one past `u64::MAX`. The settlement's own overflow checks stay, now as checks for a bug.
- **Running totals.** The wallet keeps the total cash, and the positions ledger each symbol's
  total. A deposit adds to them; fills leave them alone, because a fill conserves both; a snapshot
  rebuilds them from its balances and holdings, and refuses one whose totals exceed `u64::MAX`. The
  check therefore costs the same however many users or symbols exist. The first version summed the
  balances, or scanned every holding of every symbol, on each deposit. The re-check of the review
  pointed out that a client can create holdings in unlimited symbols, which made each share
  deposit slower for everyone.

### What it does not fix

Nothing withdraws cash or shares, so the totals never go down. One client can deposit whatever is
left below `u64::MAX` in one request, and every later deposit by anyone is then refused, across
restarts and replay. A share deposit can do the same to one symbol. Before this part, an outsized
deposit affected only its depositor, though it could halt the worker later.

This comes from deposits being self-service placeholders, which already let anyone credit
themselves any amount. Real custody would never come near `u64::MAX`. The recorded gateway
milestone makes deposits operator actions, which removes it. Until then it is a known prototype
limitation.

The three existing tests of internal settlement failures (a seller's cash, a buyer's shares, a later
fill in a sweep) could no longer build their overflowing ledgers through deposits. They now force
that state directly, as `cancellation_failure_does_not_remove_the_book_order` already did, and check
that the internal fault still leaves the whole command uncommitted. The runtime test of a fault in
the middle of a group does the same through `ExchangeCore::force_balance_for_test`.

### Options considered and rejected

- **Refuse the order whose fill would overflow.** A resting order whose owner cannot receive more
  would then block every order that reaches it, a way to freeze a price level. It also blames the
  wrong client.
- **Wider balances (`u128`).** It removes the overflow but changes every ledger type, the snapshot
  format and the API, for amounts no exchange holds.
- **A cap per balance, such as `u64::MAX / 2`.** Two balances can still sum past `u64::MAX` and
  meet in one fill. Only the total bounds every credit. As an addition to the total it would make
  using up the total take many accounts rather than one request. But registration is open, so it
  only raises the cost, and it adds a product rule. Operator-only deposits are the real fix.

## Part 1c - The fallback signing key

### The problem

The login handler signed tokens with `JWT_SECRET` and panicked without it. The request check
(`AuthUser`) instead fell back to the key `"secret"` when the variable was unset. On a server
started without the setting, nobody could log in, but anyone could sign a token for any user id with
`"secret"`, and the exchange accepted it: balances, orders, cancellations, deposits.

### What changed

- `auth_middleware::jwt_secret()` is the one place that reads the key. It treats a missing or
  empty `JWT_SECRET` as unset, because an empty key is as guessable as a fixed one.
- `main` refuses to start without it:
  - the primary, before it opens its journal;
  - the warm replica, before it follows anything. It checks tokens once promoted, and failing after
    it has fenced the old primary would leave no exchange at all.
- The request check answers `500` if the key is somehow missing, and never falls back.
- Login uses the same function.

The market-data publisher, the reporter, the probe and the benchmark do not check tokens, so they
do not need it.

### Options considered and rejected

- **A minimum key length.** Good practice, but it could refuse an existing deployment's
  configuration, and this part is about removing a guessable key rather than grading strong ones.
  The documentation asks for a long random value.
- **Read the key once into the application state.** Cleaner in principle, but it changes every
  handler's state for no behaviour gain, since startup already guarantees the key exists.

## Compatibility

Like milestones 9, 13 and 22, this part changes which commands are accepted, so some old journals no
longer replay. Replay refuses the command, finds the recorded acceptance, and reports an output
mismatch:
- an accepted order with more than 10,000 fills. Its record had to fit in 64 MiB, so only one with
  short ids and between 10,000 and about 140,000 fills could have been accepted;
- deposits that took the exchange's total cash, or a symbol's total shares, past `u64::MAX`.

No journal in this project has either. The benchmark's orders take at most 10 fills, and it deposits
10^15 cash per user, so its total stays far below the limit up to 18,000 users.

For the same reason, a primary and a warm replica must run the same version. A replica of the other
version stops at the first command the two decide differently.

## Verification

Tests:
- Order book, `an_order_beyond_the_fill_cap_is_marked_before_anything_is_built`: a plan at the cap
  is complete; one beyond it is marked at the first extra resting order, with no fill or execution
  built.
- Core:
  - `an_order_that_would_trade_against_too_many_resting_orders_is_refused_and_changes_nothing`
    (cap of two): the refused order leaves the core snapshot unchanged, and an order within the cap
    trades;
  - `deposits_that_could_make_a_fill_overflow_are_refused`: with the totals at `u64::MAX`, one more
    unit of cash or of the symbol is refused, whoever deposits it; another symbol is unaffected; a
    trade still settles at the limit; afterwards the totals are unchanged, both live and in a core
    restored from its snapshot.
- Wallet and positions: a deposit is refused once the total would pass `u64::MAX`, and a snapshot
  beyond it is refused.
- Runtime, the real cap:
  - `an_order_beyond_the_fill_cap_is_an_ordinary_rejection_and_changes_nothing`: with 10,001
    resting orders, the sweep's only output through `prepare_input_event` is
    `OrderRejected { reason: "TooManyFills" }`, and nothing changes. An order taking exactly 10,000
    is accepted with 20,000 executions.
  - `the_largest_order_the_fill_cap_allows_fits_in_one_record`: the widest possible record for
    10,000 trades is 13,760,982 bytes, within the 64 MiB limit. The record is measured directly, so
    the check would see a record over the limit; `encode_record` would only refuse it.
- Process, `tests/startup.rs`: the real binary, as primary and as warm replica, exits with status 1
  and names `JWT_SECRET` when the variable is missing or empty, and the primary has not created its
  journal. The warm-replica process test now sets the variable.

`cargo fmt -- --check` is clean and `cargo test --locked` passes: 164 unit tests and the executable
integration tests.

### Live, on the office Ubuntu machine

Two scripts in `~/stock-scripts` each start a real exchange with a disposable database and journal,
on the milestone 22 binary and on this part's.

`live_sweep.py`:
1. it places 55,000 resting sells whose ids and symbol are the gateway's longest, every byte
   escaped in JSON (41 MB of journal);
2. one buy order sweeps all of them.

| | Milestone 22 binary | This part |
|---|---|---|
| The sweep through 55,000 | `503 exchange unavailable: ... record exceeds size limit` after 0.91 s; the worker halted, `/health` 503 | `409 TooManyFills` in 0.02 s; a 768-byte rejection journaled; `/health` 200 |
| After it | restart in 2.0 s, recovered through sequence 110,006 with the book intact; the same sweep halted the worker again | an order taking exactly 10,000 filled in 0.48 s, a 12.4 MB record; a small order after it filled at once |

`live_overflow.py` runs the review's case: A deposits `u64::MAX` and sells one share at 1, then B
deposits 1 and buys it.

| | Milestone 22 binary | This part |
|---|---|---|
| B deposits 1 | accepted | `400 WalletRejected("Overflow")` |
| B buys the share | `503 exchange internal fault: wallet balance invalid`; the worker halted | `400 WalletRejected("InsufficientFunds")`; `/health` 200 |
| Shares | not run | a deposit of 1 share on top of another user's `u64::MAX` is refused (`PositionRejected("Overflow")`) |

On the old binary one client could stop the exchange every time it was restarted, in two ways. On
the new one both are ordinary rejections.
