# Task 02 — Durable Event Log and Startup Recovery

**Date:** 2026-09-16
**Status:** Complete. `cargo fmt -- --check` clean, `cargo test` 46 passed, verified live across two
hard kills, a torn log, and a corrupted log.
**Milestone before this:** Observable Exchange (`docs/tasks/01-observable-exchange.md`).
**Milestone after this:** Not selected — see section 8.

---

## 1. Why this task, and why now

Before this task, stopping the process lost everything: every deposit, order, execution, balance,
and both sequence counters. `ExchangeRuntime` kept its history in a `Vec` and `main` always started
from `ExchangeRuntime::new`. The replay machinery from milestone 6 — `replay_event_log`,
`from_event_log` — was complete, tested, and never called by the running binary.

Three reasons this milestone was the right one to take next, out of several candidates:

**The design doc's dependency graph.** `stock-exchange-system-design.md` does not treat the event
store as one component among many. The market data publisher, the reporter, and the warm standby
matching engine all *subscribe to* or *recover from* the event store:

> "Other components (market data processor, reporter) subscribe to the event store and process
> events accordingly." / "When a warm instance restarts, it recovers all states from the event store."

Building the market data publisher first would have meant wiring it to an in-memory `Vec` and
rewriting it once the real store existed. Wrong order.

**An explicit non-functional requirement.** The design doc states "RPO is near zero. Data loss is
not acceptable." Before this task, the recovery point objective was *everything*.

**Closing the loop on unreachable code.** Task 01 was motivated by the observation that milestones
were being completed as test-only machinery. `replay_event_log` was the last big piece in that
state. This task is what makes it reachable from `main`.

Sell-side inventory (the exchange still credits sellers cash for shares they do not own) is the
larger *correctness* gap and was considered. It is a domain-model gap that blocks nothing
architecturally, and durability neither worsens nor depends on it. It stays next in line.

---

## 2. Research that shaped the design

Four findings changed the plan as originally written in `PROJECT_DIRECTION.md`.

**Frame every record; do not write bare JSON lines.** The serialization checkpoint had chosen JSON as
the storage representation. Every production write-ahead log (LevelDB, Kafka, PostgreSQL's WAL)
frames records with a length prefix and a checksum, and the reason maps directly onto this
milestone's own hardest requirement: *"a crash must not leave an input that replay treats as valid
while its required outputs are absent."* Bare JSON lines cannot meet that. A half-written line is a
parse error, and a parse error is indistinguishable from real corruption in the middle of the file.
Framing makes a crash's damage *identifiable*: it can only ever be an incomplete final record.

**One file, not segments.** Multi-file logs need an `fsync` on the *directory* after each new segment
is created, or the file itself can be durable while its directory entry is not. Directory `fsync` is
a no-op on Windows, which this project develops on. A single append-only file has no directory
entry to protect after creation, and matches the "simple append-only file" the milestone already
called for.

**Snapshots stay out — with a number behind it.** LMAX snapshots once a day, and a full restart that
loads the snapshot *and replays an entire trading day* finishes in under a minute; Chronicle Queue
measures roughly 500,000 events per second on replay. This exchange will not approach that volume.
The milestone's existing exclusion of snapshots is justified by data, not only by scope.

**One `fsync` per command is right for now.** The milestone requires that no success reply is sent
until the record is synchronized. Production systems amortize that cost with *group commit* — several
records behind one sync — but there is nothing here to group: one worker thread processes one command
at a time. Start with one sync per record; leave the upgrade path marked.

---

## 3. The on-disk format

```
file header, written once:   "EXCHLOG1"                      8 bytes
each record:                 [len: u32 LE][crc32: u32 LE][payload: len bytes]
payload:                     JSON array of EventEnvelope — one input event and every
                             output event that command produced
```

- The **magic header** rejects a file that is not an event log (`EventStoreError::BadMagic`), and
  the trailing digit pins the format so a future incompatible change fails with a clear message
  instead of somewhere inside a JSON parse.
- The **length** lets the reader know how many bytes to expect *before* reading them, which is what
  makes a torn record detectable: fewer bytes than promised means the write was interrupted.
- The **CRC-32** (IEEE 802.3, reflected — the same polynomial as `zlib.crc32`) catches bytes that
  are all present but wrong.
- The **payload stays JSON**. It was already implemented and tested in the serialization
  checkpoint, it is human-readable when debugging a log by hand, and its cost is irrelevant at this
  scale. Binary encoding is a later optimization, not a correctness matter.
- **One record per command**, never one per event. This is the whole point: input and outputs go to
  disk in one `write_all`, so a crash gives you both or neither.

A length above 64 MiB is rejected as corruption before it is used to size an allocation.

---

## 4. What was built

### 4.1 `src/exchange/event_store.rs` — new, 364 lines

`EventStore::open(path)` opens or creates the file, validates the magic, decodes every record,
**truncates any torn tail**, and returns the store together with the recovered envelopes. They come
back as a pair on purpose: truncation has to happen before anything is appended, and a store that
could be handed out un-recovered would let a caller append after damaged bytes.

`EventStore::append(&[EventEnvelope])` serializes the batch, frames it, writes it with a single
`write_all`, and calls `sync_all` (which is `FlushFileBuffers` on Windows, `fsync` elsewhere). It
returns only when the bytes are durable.

`decode_records` distinguishes two failure shapes, and the distinction is the heart of recovery:

| What the reader finds | Meaning | Action |
|---|---|---|
| Header or payload runs past end of file | Torn tail — the process died mid-write | Stop, truncate, **accept** what came before |
| All bytes present, checksum wrong | Real damage, not a crash | **Refuse** to start |
| Checksum passes, JSON does not parse | Real damage | **Refuse** to start |

A torn write always produces a *short* record; it cannot produce a full-length record with a bad
checksum. So a checksum failure is never something recovery should guess its way past.

The CRC is hand-rolled — ten lines — rather than a dependency. `crc32_matches_known_vector` pins it
to the canonical check value (`crc32(b"123456789") == 0xCBF43926`). That test earned its keep
immediately: the constant was first typed from memory as `0xCBF43F26`, the test failed, and
`python -c "import zlib; ..."` confirmed the *implementation* was right and the *constant* was wrong.

### 4.2 `ExchangeRuntime` — durable before visible

`record_and_process_input_event` used to append the input to the in-memory log, process it, then
append the outputs. It now:

1. processes the input, producing the outputs;
2. numbers the input and all outputs as one batch;
3. appends that batch to the store and waits for the sync;
4. **only then** advances `next_event_seq` and extends the in-memory log;
5. returns the result, which the caller sends to the client.

Step order matters. The core must be mutated before the write because the outputs are not known
until the command has run. That creates exactly the window `PROJECT_DIRECTION.md` warned about —
memory ahead of disk with no rollback — and the runtime handles it the way the milestone specified:
**fail closed.** `handle_command` now returns `Result`; on a store error it sends
`"exchange halted: ..."` to the waiting client and propagates. `run()` breaks out of its loop, the
receiver is dropped, and every subsequent HTTP request gets "exchange worker is unavailable"
instantly rather than a false success.

The three read commands are unaffected; they never write, so they cannot fail this way.

`ExchangeRuntime::from_store(rx, store, events)` replays the recovered history through the
existing `replay_event_log` and attaches the store. `from_event_log` and `new` remain for tests and
now share the same `rebuild` helper.

### 4.3 Startup — `recover_runtime`, called from `main`

```
recover_runtime(rx, path)
  -> EventStore::open        (magic, decode, torn-tail truncation)
  -> replay_event_log        (contiguous sequence, deterministic outputs)
  -> ExchangeRuntime::from_store
```

Recovery runs **on the main thread, before the listener binds**. A `StartupError` prints a clear
reason and exits with status 1. This placement was deliberate: if recovery ran inside the spawned
worker thread and panicked, the thread would die while the HTTP server kept serving — every request
would then time out against a worker that no longer exists. Failing before `bind` means a broken log
produces a process that refuses to start, which is what an operator needs to see.

An absent or empty file starts a fresh exchange. Anything else that cannot be trusted refuses to
start; the process never silently begins with an empty exchange on top of history it could not read.

The path comes from `EVENT_LOG_PATH`, defaulting to `exchange-events.log`. Both `.env.example` and
`.gitignore` were updated — the log is runtime data and must never be committed.

### 4.4 Client order ids — the retry hole durability opens

`PROJECT_DIRECTION.md` had flagged this and deferred it:

> "The server may durably accept an order and then lose the HTTP response. Because the controller
> currently creates a new order ID for each request, a client retry could submit a second order."

It was folded into this milestone, not the next, because durability is what makes the failure mode
real. Before, "accepted then lost the reply" cost nothing worse than a lost in-memory order. Now it
is a durable order the client does not know about, and a retry is a *second* durable order.

The fix is FIX's `ClOrdID` idea, and it needed almost nothing new: `OrderManager::prepare_order`
already rejected duplicate order ids (`OrderManagerError::AlreadyExists`) — the only reason that
check never protected a retry was `Uuid::new_v4()` minting a fresh id per request. `OrderRequest`
gained an optional `client_order_id` (trimmed, 1–64 characters — it is a map key held for the life of
the order, so it is bounded at the edge). When present it becomes the order id; a second submit
with the same id returns **409 Conflict** via a new `AppError::Conflict`, and *no second order
exists*. When absent, the server mints a uuid as before.

---

## 5. Decisions, and the alternatives they beat

**Write the whole command as one record, not the input first and outputs after.** Writing the input
before processing would let a crash between the two writes leave an input on disk with no outputs
— the exact state the spec forbids and that `replay_event_log` would reject at next startup. One
record, written after processing, is the only shape that satisfies "both or neither."

**`Option<EventStore>` on the runtime, not a storage trait.** A trait with a real implementation and a
null implementation would be the textbook shape. It would also be an interface with one production
implementation, built to let fifteen existing tests avoid touching the filesystem. `None` is an
honest mode — "in memory only" is precisely what `ExchangeRuntime::new` always meant — and every
existing test kept working unchanged.

**Truncate the torn tail; refuse on checksum failure.** An earlier idea was to skip any bad record and
carry on. That would silently drop a command from the middle of history and produce a replay that
still passes — the most dangerous possible outcome. The asymmetry in section 4.1 is the safe one:
the only damage recovery ever repairs on its own is the one kind a crash can actually cause.

**Read the whole file at startup.** Marked `ponytail:` in the source. Streaming and snapshots are the
upgrade path; at current scale this is a few kilobytes.

**409, not 400, for a duplicate client order id.** A reused id is a *retry*, not a malformed request.
The distinction lets a client that timed out treat 409 as "it went through the first time."

**Hand-rolled CRC-32 over a crate.** Ten lines, one test vector, no supply chain. The test vector is
non-negotiable — see section 4.1 for why.

---

## 6. How it was verified

### Tests: 38 → 46

| Test | What it would catch |
|---|---|
| `crc32_matches_known_vector` | a wrong CRC that silently accepts corruption |
| `appended_records_survive_reopen` | any break in write → read symmetry |
| `a_torn_tail_is_discarded_and_the_log_keeps_working` | a crash mid-write poisoning the log for good |
| `a_flipped_byte_is_refused_rather_than_skipped` | recovery "helpfully" skipping real damage |
| `a_foreign_file_is_refused` | starting the exchange on some random file |
| `a_failed_append_reports_an_error_instead_of_claiming_durability` | a write error being swallowed (uses a real read-only handle, not a mock) |
| `a_durable_exchange_survives_a_restart_and_continues_both_sequences` | the milestone's acceptance example, at the runtime level: three runs of one file — balances, locks, order state, book, event sequence 9/10 and matching sequence 3 |
| `startup_refuses_history_that_does_not_replay` | a record whose framing and checksum are perfect but whose recorded outcome disagrees with what the core regenerates — the one kind of damage only deterministic replay can catch |

### Live, against a running server

Run against a disposable `postgres:16` container, removed afterwards.

**Run 1** — fresh log. Alice deposits 100,000. Bob rests `SELL 10@100` with client id `bob-sell-1`.
Alice sends `BUY 4@100` with client id `alice-buy-1` and fills. State: Alice 99,600 balance, book
`asks:[{100, 6}]`, log 1,531 bytes.

**Hard kill** — `Stop-Process -Force`, no shutdown hook, no flush. Health check confirms the server
is gone.

**Run 2** — `Event log exchange-events.log recovered with 8 events`. Every value identical: Alice
99,600, Bob 400 (proceeds of the fill), `asks:[{100, 6}]`, `bob-sell-1` `partially_filled` with
`remaining_quantity: 6`, `alice-buy-1` `filled`. Bob cancels his resting 6 → `200`, status
`canceled`. Retry safety: `client_order_id: "retry-me"` sent twice → `201` then
`409 AlreadyExists("retry-me")`, and locked funds show **one** order, not two. Empty
`client_order_id` → `400`.

**Hard kill, Run 3** — `recovered with 14 events`; the cancellation survived.

**Torn tail** — 30 bytes chopped off the end of the 2,577-byte file with the server down.
Restart: `recovered with 12 events`. The last command's input and outputs vanished *together*; the
server started normally.

**Corruption** — one byte flipped inside the first record. Restart:

```
refusing to start: event log is corrupt: checksum mismatch in the record at byte 8
```

Exit code 1. It did not start empty.

---

## 7. What this task did *not* do

- **Sell-side inventory** — still no share ledger; still the largest correctness hole.
- **Risk limits** — `set_limit` is still never called on the live path.
- **Snapshots** — the log grows without bound and replay is always from the beginning. Correct at
  this scale; the upgrade path is marked.
- **Group commit** — one `fsync` per command. Also marked.
- **Reading the log incrementally** — whole file into memory at startup. Also marked.
- **Rollback inside `ExchangeCore`** — on a store failure the worker halts; it does not undo the
  in-memory change. The spec explicitly treats transactional rollback as a separate problem.
- **`client_order_id` reuse after an order is terminal** — FIX allows a `ClOrdID` to be reused once
  the order it named is done. Here every id is unique forever. Simpler, and the stricter direction.
- **Replay of a log written before task 01** — the `OrderAccepted` output event was deliberately
  left byte-identical in task 01 so this would hold, but no pre-task-01 log existed to test it with.

Dead-code warnings: 10 → 8. `ReplayError` and `replay_event_log` are now live. What remains is
pre-existing (`best_bid`/`best_ask`/`get_state` superseded by the view methods, `set_limit` awaiting
the risk milestone, `peek_front`/`pop_front`, `commit_fill`) plus `new`/`from_event_log`, which
only tests call.

---

## 8. What comes next

Not selected. The candidates, in the order I would take them:

1. **Sell-side positions.** A share ledger so a sell reserves inventory the way a buy reserves cash,
   and a fill moves both. This is the last gap between "a matching engine with a wallet" and "an
   exchange." It touches `Wallet` (or a sibling `Positions`), `OrderManager::prepare_order`,
   settlement, and cancellation unlocks — and it adds a new input event (a share deposit), which
   is the first real test of the "incompatible format change means a fresh log" rule.
2. **Risk limits on the live path**, with a day boundary.
3. **`GET /execution`**, the last read the design doc specifies.

Discuss before starting, per the repository's own rule.
