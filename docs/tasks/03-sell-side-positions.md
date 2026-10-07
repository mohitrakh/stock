# Task 03 — Sell-Side Positions

**Date:** 2026-09-16
**Status:** Complete. `cargo fmt -- --check` clean, `cargo test` 57 passed, verified live including a
restart and a hand-crafted legacy log.
**Milestone before this:** Durable Event Log and Startup Recovery (`docs/tasks/02-durable-event-log.md`).
**Milestone after this:** Not selected — see section 8.

---

## 1. The bug

The exchange created money out of nothing.

`Wallet::check_and_lock` took a `Side` and returned `Ok(())` immediately for a sell:

```rust
if matches!(side, Side::Sell) {
    return Ok(());
}
```

There was no share ledger anywhere in the system, so there was nothing a sell *could* have been
checked against. And on a fill, `apply_execution` credited the seller unconditionally:

```rust
self.wallet.deposit(seller_user_id, cash_amount);
```

Put together: **any user could sell any quantity of any symbol without owning a single share, and be
paid real cash for it.** Total cash in the system went up after every such trade. The books did not
balance, and nothing in the code noticed.

This was listed as a known limitation from early on ("sell-side inventory/positions are not
modeled"), which undersold it. It is not a missing feature; it is a ledger that does not conserve
value.

Worth noting what it made of earlier work: milestone 5 made prices exact integers, with checked
multiplication and no `f64` anywhere near money, on a ledger where cash could be conjured. Precise
arithmetic on an unbalanced ledger.

## 2. Why now, and the honest caveat

**The design doc does not ask for this.** Searching all 742 lines of
`stock-exchange-system-design.md` for positions, inventory, or holdings returns nothing. The wallet
requirement is only ever about cash:

> "We need to make sure users have sufficient funds when they place orders. If an order is waiting in
> the order book to be filled, the funds required for the order need to be withheld to prevent
> overspending."

So this milestone is an addition to the specification, chosen deliberately over two things the doc
*does* require and that are still unmet: risk limits on the live path (`RiskManager` exists and
rejects nothing) and `GET /execution`.

The argument for taking it first: the wallet exists "to prevent overspending," and over-selling is
the same requirement pointed at the other side of the trade. A daily volume cap on a system that
mints money is a speed limit on a car with no brakes. The doc's silence looks like an oversight in
the doc rather than a considered exclusion.

The argument against, recorded fairly: this is domain modelling, not architecture, and it delays
explicitly-required work. That trade was made with eyes open.

---

## 3. The design

A share ledger that mirrors the cash ledger exactly. Cash has holdings, reservations and
availability; shares now have the same three numbers per `(user, symbol)`.

| | Buy | Sell |
|---|---|---|
| reserve at placement | lock cash = price × quantity | lock shares = quantity |
| settle on fill | pay cash, release reservation | deliver shares, release reservation |
| release on cancel | unlock cash for the unfilled remainder | unlock shares for the unfilled remainder |

That symmetry is the whole design. Once both sides post collateral, a fill becomes a four-legged
transfer where nothing is created:

```
buyer cash  ──→  seller cash
seller shares ──→  buyer shares
```

### `src/types/positions.rs` — new

`Positions` holds `holdings` and `locked`, both keyed by `(user_id, symbol)`, with
`credit` / `check_and_lock` / `commit_sell_fill` / `unlock` / `holding` / `locked` / `available`.
Deliberately a sibling of `Wallet` rather than an extension of it: `Wallet`'s API is cash-shaped
(`deposit`, `balance`, prices and notionals) and folding a second unit into it would have muddied
both.

One deliberate asymmetry: `Positions::credit` returns `Result` and uses `checked_add`, while
`Wallet::deposit` still credits silently and can wrap. The wallet's missing overflow check is a
pre-existing known gap; there was no reason to copy it into new code just for symmetry's sake. It is
still open on the wallet side.

### Shares have to get in somehow

Cash enters through `FundsDepositRequested`. Shares needed the same door, so
`SharesDepositRequested` / `SharesDeposited` were added to the event types, with
`SharesDepositRejected` for the overflow case, plus an `ExchangeCommand::DepositShares` and
`POST /exchange/shares/deposit`.

This is a placeholder for what a real exchange does through settlement and custody. It is the same
shape as the existing cash deposit, which is equally a placeholder.

### Reading positions back

`GET /exchange/positions` returns every holding for the authenticated user as
`[{symbol, quantity, locked, available}]`, sorted by symbol so the response is stable. Without it
there would be no way to observe the fix from outside the process — the lesson from task 01.

---

## 4. Decisions, and what they beat

**Shares move before cash in `apply_execution`.** If a leg is ever going to fail, it should fail
before any money has moved. The seller's shares were reserved at placement so the delivery cannot
realistically fail, but ordering it first means a hypothetical failure leaves less mess. There is
still no rollback — a failure mid-settlement is the same unsolved problem noted in task 02.

**`Wallet::check_and_lock` and `unlock_funds` lost their `Side` parameter.** Now that the caller
branches on side explicitly, the parameter was dead — and it was worse than dead, because
`if sell { return Ok(()) }` *was the bug*, sitting inside a function whose name promised a check.
`commit_fill`, already unreachable, was deleted rather than left carrying the same pattern.

**A test fixture rather than 27 edited call sites.** Twenty existing tests failed the moment the
position check landed, all for the same reason: they sold shares nobody owned. `funded_core()` seeds
the usual test sellers with AAPL, so tests about matching, wallets and lifecycle stay about those
things. Rejection of an unbacked sell has its own dedicated tests, so the fixture hides nothing that
matters.

Those twenty failures are worth dwelling on. **The test suite had encoded the bug as expected
behavior.** Every test that exercised a sell was, without meaning to, asserting that unbacked selling
works. A green suite proved nothing about this, because nothing had ever asked the question.

**Share deposits go in the event log, not around it.** Where a test seeds shares for a history that
will be replayed, the deposit has to be a real input event — otherwise replay rebuilds a seller with
no inventory and rejects the very sell it is meant to reproduce. This is why several tests' event
counts moved from 8 to 10.

---

## 5. Old logs stop replaying, and that is correct

Adding a variant to `ExchangeInputEvent` does not break deserializing old records — but the position
check changes *outcomes*, and that does break replay. A log written before this task containing an
accepted unbacked sell will now refuse to load, because replay regenerates `OrderRejected` where the
file records `OrderAccepted`.

This was tested rather than assumed. A log was hand-crafted in exactly the shape the old code would
have written, with valid framing and valid checksums:

```
refusing to start: stored history did not replay deterministically:
OutputMismatch { seq_num: 4,
                 expected: OrderRejected { order_id: "legacy-sell",
                                           reason: "PositionRejected(\"InsufficientShares\")" },
                 actual:   OrderAccepted { order_id: "legacy-sell", seq_num: 1 } }
exit code: 1
```

Every checksum in that file passes. Framing alone cannot detect this class of damage — only
deterministic replay can, which is the clearest demonstration so far of why that check exists.

Practically: any development log containing an unbacked sell must be deleted. This is the
"intentionally incompatible change may require starting with a fresh event file" rule from
`PROJECT_DIRECTION.md`, meeting its first real case. The file magic was deliberately **not** bumped
to `EXCHLOG2` — the framing is unchanged, and a log with no sells in it still replays perfectly.
Bumping would have rejected files that are actually fine, in exchange for a tidier error on the ones
that are not.

---

## 6. Verification

### Tests: 46 → 57

Six unit tests on `Positions` (reserve without shares, reserve/available split, fill takes shares and
reservation together, cancel returns the reservation, per-symbol isolation, overflow on credit) and
five at the core level:

| Test | What it pins |
|---|---|
| `a_sell_without_shares_is_rejected` | the bug itself |
| `a_sell_larger_than_the_holding_is_rejected` | and the boundary either side of it |
| **`a_fill_creates_no_cash_and_no_shares`** | **the invariant: totals before == totals after** |
| `cancelling_a_sell_returns_the_shares_to_available` | reservations are released, and are reusable |
| `shares_bought_can_then_be_sold` | a fill genuinely delivers, it does not just decrement |

The conservation test is the centrepiece and the one that would have caught this originally. It sums
cash and shares across both parties before and after a partial fill and asserts neither total moved.
On the old code, `cash_after` was 60 higher than `cash_before` on exactly that sequence.

The restart test from task 02 now also asserts that positions and their reservations survive a
restart.

### Live, over HTTP

```
bob sells 10 AAPL he does not own
  -> HTTP 400  PositionRejected("InsufficientShares")
  -> bob's balance stays 0, positions stay []          (nothing minted)

deposit 10 shares -> [{"symbol":"AAPL","quantity":10,"locked":0,"available":10}]
bob sells 10      -> HTTP 201, [{"quantity":10,"locked":10,"available":0}]
bob sells 1 more  -> HTTP 400                          (the same shares cannot be sold twice)
```

Conservation across a real fill — alice buys 6 @ 100 from bob:

```
              cash            shares
before   alice 10000          alice  0
         bob       0          bob   10
after    alice  9400          alice  6
         bob     600          bob    4
         -----------          --------
         total 10000  ✓       total 10  ✓
```

Hard kill and restart: 14 events recovered, positions and reservations intact
(`bob: quantity 4, locked 4, available 0`).

---

## 7. What this task did *not* do

- **Short selling.** There is no borrow mechanism, so a sell must be fully backed. Real exchanges
  permit shorting against located inventory; this is the stricter and simpler rule.
- **Settlement timing.** Delivery is instant at match. Real equity markets settle on T+1 or T+2, and
  modelling that would mean pending positions and a settlement calendar.
- **Share custody or issuance.** `POST /exchange/shares/deposit` lets anyone credit themselves any
  quantity, exactly as the cash deposit does. Both are placeholders.
- **`Wallet::deposit` overflow.** Still silent, still a known gap. `Positions::credit` does check.
- **Symbol validation.** A share deposit accepts any non-empty symbol string; there is still no
  product/instrument registry, which the design doc does describe.
- **Risk limits and `GET /execution`** — both still unmet design-doc requirements, deliberately
  deferred past this task.

Dead-code warnings: 8 → 7.

---

## 8. What comes next

Back to the specification. Both remaining candidates are explicit design-doc requirements:

1. **Risk limits on the live path.** `RiskManager` exists and rejects nothing; `set_limit` is never
   called. The doc requires it twice and frames it as regulatory ("the exchange is a regulated
   facility"). The interesting part is the day boundary: resetting daily volume from the wall clock
   would break deterministic replay, because the same log would produce different results on a
   different day. `Order` already carries a timestamp and it is already in the log, so the day can be
   derived from the event rather than the clock. Two open questions first: does the limit count
   submitted or traded volume (the doc contradicts itself — shares on line 30, notional on line 182),
   and does cancelling return your allowance?
2. **`GET /execution`**, the last read the design doc specifies. Would be the first component built
   as a projection over the event store, which is the doc's reporter pattern in miniature.

Discuss before starting, per the repository's own rule.
