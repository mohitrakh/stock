# 03 - Stop Keeping the Whole History in RAM

Part of milestone 19. Code: the `event_log` field of `ExchangeRuntime` and the `batch` field of
`Staged` in `src/exchange/runtime.rs`, both now `#[cfg(test)]`.

## The problem

`ExchangeRuntime` had a field `event_log: Vec<EventEnvelope>`. Every command appended its input
event and every output event to it, and nothing ever removed anything. At startup it was filled with
the entire recovered journal (or the suffix after a snapshot). So the exchange's memory grew by
600 to 700 bytes for every order it ever processed, for the life of the process.

Production never read it. The only reader was `ExchangeRuntime::event_log()`, which was already
`#[cfg(test)]`. The complete history is on disk in the journal, which is the source of truth; the
subscribers and the warm replica read the journal and the mmap stream, never this vector. It was a
leftover from milestone 3, when the log lived only in memory, and stayed after the journal took over.

At the design's volume of 1 billion orders a day, it would have grown by 600 to 700 GB a day.

## The change

The field, and the copy of each batch kept for it while a group waits for its sync, now exist only
in test builds:

```rust
#[cfg(test)]
event_log: Vec<EventEnvelope>,
```

Tests still see the durable history exactly as before. In group commit the batch is appended to
`event_log` only after the group's sync, so the vector keeps meaning "what is durable". In production
the batch is dropped as soon as it has been encoded to journal bytes, and the vector filled at
startup is dropped as soon as replay has finished with it.

There is nothing clever here. The code was deleted from the production build, because deleting
unused work is the cheapest optimization there is.

## Results

tmpfs (sync free), snapshots off. "Before" includes optimizations `01` and `02`:

| Run | Before | After |
|---|---|---|
| 200,000 orders: peak process memory (VmHWM) | 386 MB | **252 MB** (−134 MB, about 670 bytes per order) |
| 200,000 orders: throughput | 43,438 orders/s | 42,667 orders/s (same, within noise) |
| 1,000,000 orders: peak process memory | 1.92 GB | **1.32 GB** (−600 MB) |
| 1,000,000 orders: throughput | **4,477 orders/s** | **30,865 orders/s** (6.9×) |

The memory figures are for the whole benchmark process. The remaining 1.32 GB at a million orders
also includes the harness's own million pending reply tasks, and the exchange state that
legitimately grows (see "What still grows" below).

### Why a million orders collapsed

At 200,000 orders, dropping the history saved memory but no time. At a million orders it was the
difference between 4,500 and 31,000 orders a second. The run was repeated while sampling the whole
VM with `vmstat` every 5 seconds (4,281 orders/s the second time):

- free memory in the 8 GB VM fell from 2.1 GB to **61 MB**;
- in 32 of the 47 samples the CPUs spent over half their time in the **kernel** (62% system versus
  14% user on average), reclaiming memory, with about 150 MB swapped out (swap in use grew from 524 MB to 678 MB);
- the benchmark's journal lives in RAM on tmpfs (about 760 MB), on top of the process's 1.9 GB.

So the exchange did not get slower on its own. The machine ran short of memory, and the kernel's
page reclaim took the CPU away from it. The 600 MB saved here kept the run below that point. On a
machine with more RAM the collapse comes later, but unbounded growth always gets there eventually:
memory that grows with every order is a time bomb, whatever the size of the machine.

## What still grows with history

This change removes the one piece of history that served no purpose. Other state still grows with
every order, because features use it:

- `OrderManager` keeps every order ever placed. Duplicate `client_order_id` detection depends on
  it, and order ids are unique forever.
- The per-user execution index behind `GET /exchange/executions` grows with every fill.
- `PriceLevel` never reuses the slots of removed orders, so a busy price level's node vector only
  grows.
- MDP's candle history is never trimmed.

The worst consequence is not memory but the core snapshot. Every 10,000 commands, on the worker
thread, it serializes every order ever placed and the whole execution index, so its cost grows with
history. (The `PriceLevel` slots and MDP's candles grow memory but are not part of the core
snapshot.) With snapshots on, the fully
optimized exchange drops from about 25,000–43,000 orders/s to **8,255** (see the milestone
write-up). Bounding that state needs product decisions (how long order ids stay unique, and how far
back the executions query reaches), so it is left to its own milestone.

## Tests

No behavior changed. All existing tests that inspect `event_log` still compile and pass, because the
field still exists in test builds. The production build is the one that no longer carries it. Both
builds were checked: `cargo build --release` produced no new warnings (13 before and after), and
`cargo test` passed.
