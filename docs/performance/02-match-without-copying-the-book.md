# 02 - Matching Without Copying the Order Book

Part of milestone 19. Code: `OrderBook::plan_order` / `apply_plan` in `src/types/order_book.rs`,
`PriceLevel::iter` / `get_mut` in `src/types/price_level.rs`, and `MatchingEngine::prepare_order`
/ `commit_order` / `prepare_cancel` / `commit_cancel` in `src/types/matching_engine.rs`.

## The problem

Milestone 11 made every command all-or-nothing. A command is first *prepared*: every fill, cash
movement, share movement and risk change is worked out and validated without touching live state.
Before group commit (`01`), it was *committed* only after the journal write; now it is committed
in memory first, and nothing is visible until its group's sync. The reason is that settlement is checked after
matching. For example, crediting a seller whose balance would overflow is caught after the book has
already decided who trades with whom. If matching had already changed the live book, a late failure
would leave a half-changed exchange.

To prepare matching without changing the live book, `MatchingEngine::prepare_order` did this:

```rust
let mut book = self.order_books.get(&symbol).cloned()   // copy the WHOLE book for this symbol
    .unwrap_or_else(|| OrderBook::new(symbol.clone()));
let executions = book.place_order(order);                // match on the copy
// ...commit later swapped the copy in for the original
```

Cancellation did the same. `.cloned()` copies every resting order in the symbol, with each order's
heap-allocated id, user and symbol strings, plus every price level's node list and index map. The
cost grows with the size of the book, not with what the order actually does. The design document
asks for an order book with O(1) add, cancel and match. This made every order O(book size).

The benchmark showed it plainly (tmpfs, so syncing is free, one symbol):

| Resting orders in the book | Orders/s before |
|---|---|
| 0 | 3,510 |
| 1,000 | 869 |
| 10,000 | **90** |

A real stock has thousands of resting orders. At 10,000 the exchange spent about 11 ms per order
copying a book that it then swapped in for the original, only so it could discard the copy if a late
check failed.

## The idea: split matching into a plan and an apply

Matching has two halves:

1. **Deciding** who trades with whom, at what price, for how much, and what is left over. This only
   needs to *read* the book.
2. **Carrying it out**: reduce or remove the resting orders that traded, drop emptied price levels,
   and rest the leftover. This is mechanical once the decision is made.

The copy existed only so that step 1 could be done by running the mutating matcher on a throwaway
book. If step 1 is written to read the live book directly, nothing needs copying:

```text
prepare:  plan = book.plan_order(&order)      // read-only walk of the levels the order crosses
          (settlement is validated against plan.executions, exactly as before; record encoded)
commit:   book.apply_plan(order, &plan)       // in memory: apply exactly those fills, rest the remainder
          (the next command in the group is prepared against this updated book)
after the whole group: one journal write + sync, then publish and reply   (group commit, see 01)
```

### `plan_order`: the read-only half

It walks the opposite side, best price first, but only the levels the order's limit price crosses
(`sell_levels.range(..=price)` for a buy). Within a level it walks resting orders oldest first, using
the new `PriceLevel::iter`, which follows the level's linked list without copying anything. For each
resting order:

- skip it if it belongs to the same user (self-trade prevention, as before);
- otherwise trade `min(remaining, resting.leaves_qty)`, at the resting order's price, and record a
  `Fill { order_id, price, quantity, fills_resting_order }`;
- emit the two executions (one per side) with the same `exec_N` ids, prices and timestamps the old
  matcher produced;
- stop as soon as nothing remains.

The result is a `MatchPlan { fills, executions, remaining }`. Its cost is the number of resting
orders the new order actually reaches, plus a `BTreeMap` range lookup. It does not grow with the
rest of the book.

### `apply_plan`: the mechanical half

For each fill, look the resting order up by id: O(1) through the level's index map and O(log levels)
for the level itself. If the fill takes its whole remaining quantity, unlink it from the level's
list and the book's order map; otherwise reduce its `leaves_qty`. If a level becomes empty, remove
it. Then advance the execution counter and, if anything remains, rest the new order at the back of
its price level.

`apply_plan` uses `expect` for "the planned order is still there". That is safe because nothing can
change the book between plan and apply. There is exactly one exchange worker thread, and each command
is prepared and then committed in memory on it before the next command is prepared; the group's
journal write and sync come after all of them. Group commit (`01`) commits each command before the
next one in the group is prepared, so every plan is made against the book it
will be applied to.

Cancellation became: check read-only that the order is resting (`is_resting`), and at commit call
`cancel_order` on the live book. That is also O(1).

### Why the all-or-nothing guarantee still holds

Preparation still changes nothing. `plan_order` takes `&self`, and the test
`planning_leaves_the_book_untouched` snapshots a book, plans an order that sweeps two levels, and
checks that the book's snapshot (every level, FIFO order, remaining quantities, execution counter) is
unchanged. The existing core tests that inject late failures (for
example `late_seller_credit_overflow_leaves_the_command_uncommitted`) still pass: if settlement
rejects the plan, the plan is simply discarded, and the live book was never touched.

## Proving it matches exactly like before

A faster matcher that trades slightly differently would be worse than a slow one. So the old matcher
was kept, unchanged, as a test-only method (`reference_place_order`), and a differential test runs
both side by side:

`planned_matching_is_identical_to_the_old_in_place_matcher` feeds the same 20,000 random steps to two
books. Each step is a buy or a sell from one of 5 users (so self-trade prevention triggers often), at
one of 10 prices, for 1 to 20 shares, or a cancel of a random earlier order. After **every** step it
asserts that the executions are identical (ids, prices, quantities, timestamps), that the complete
book snapshot (every level, FIFO order, remaining quantities, execution counter) is identical, and
that the order index is identical. It passes.

## Results

tmpfs (sync free), snapshots off. "Before" is group commit alone (`01`); "after" adds this change:

| Resting orders in one symbol's book | Before | After | Change |
|---|---|---|---|
| 0 | 3,879 orders/s | **52,651 orders/s** | 14× |
| 1,000 | 779 orders/s | **55,174 orders/s** | 71× |
| 10,000 | 107 orders/s | **52,465 orders/s** | **490×** |

Speed no longer depends on book depth. That is what O(1) per order means in practice.

Throughput runs with 100 symbols (the books there grow naturally as orders rest):

| Run | Before | After | Change |
|---|---|---|---|
| tmpfs, max rate, 200,000 orders | 3,309 orders/s | **43,438 orders/s** | 13× |
| disk, max rate, 20,000 orders | 18,713 orders/s | **39,153 orders/s** | 2.1× |
| disk, fixed 43,000/s for 200,000 orders (achieved) | 3,194 orders/s | **25,663 orders/s** | 8× |

On tmpfs the exchange now meets the design's *average* target of 43,000 orders/s (a max-rate run).
On the benchmark disk a fixed 43,000/s offered load is served at about 25,000/s. Part of that gap
is the worker idling during each group's sync, about 6 µs per order in the profile; the rest is not
explained by the profile yet (see the milestone write-up).

Why the 20,000-order disk run was already fast before this change: in the first 20,000 orders only
200 orders reach each symbol, and many of them trade, so the books are still shallow. Over 200,000 orders they grow, the copy gets
more expensive, and throughput sank. The planned matcher does not care.

## Options considered and rejected

- **Mutate the live book and keep an undo log** to roll back on a late failure. It is also O(fills),
  but putting an order back exactly where it was in a FIFO linked list is fiddly, and a rollback bug
  silently corrupts priority. A read-only plan cannot corrupt anything.
- **Persistent (copy-on-write) data structures** such as the `im` crate: a cheap O(1) clone with
  O(log n) updates. It needs a new dependency, changes the book's data structures, and makes every
  operation slower by a constant factor. Not needed once planning is read-only.
- **Move settlement validation before matching** so nothing can fail after it. Settlement depends on
  the fills, so it has to come after matching.

## Left alone

`PriceLevel` never reuses slots of removed orders (its `nodes` vector only grows), and
`total_quantity()` walks a whole level. Neither is on the per-order path: the first is a memory cost
and the second is used only for L2 views. Both are recorded in the milestone write-up.
