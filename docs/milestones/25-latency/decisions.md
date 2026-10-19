# Milestone 25 — Decisions

Every owner decision for this milestone, with its date and the options shown. A decision changed
later keeps its old entry, and a new dated entry follows it.

## 2026-10-14 — Selection of the milestone

**Chosen:** latency, after milestone 24 closed. The owner asked for latency from the start, and
milestone 24 part 1 built its measurement tools.

**Options shown:** latency (recommended); pushed updates (WebSocket); keep milestone 24 part 5 for
the rate-limiting lesson, then latency.

## 2026-10-14 — The plan and decisions 1 to 4

The owner reviewed the plan in `PROJECT_DIRECTION.md` ("25. Latency") and answered "go ahead with
milestone 25", which accepts each decision as recommended:

1. **How the stage timing is built in: a cargo feature, `stage-timing`, off by default.** The normal
   build carries none of it, and the timing's own cost is measured by comparing the two builds.
   The alternative shown was always-on timing, which adds a measurable cost to every order.
2. **The pipeline depth: 2.** One group syncs while the next is staged. A deeper pipeline was the
   alternative; it holds more state ahead of the disk, complicates the failure rule, and gains
   little when one sync is already shared by many commands.
3. **The binary record format: decided after Part 1**, and only if Part 1 shows that JSON encoding
   matters.
4. **Runs per configuration: 3.** Milestone 24 used 2, which could not settle the tails.

## 2026-10-14 — The documentation rule

The owner restated the rule for every milestone: every test run and its result, and every
optimization with its before-and-after numbers, are written into the milestone's files in depth.
The rule is now in the repository's `CLAUDE.md`.

## 2026-10-14 — Design choices inside decision 1 (made while building Part 1)

These are implementation choices, recorded so the owner can overrule them:

- **How a command carries its timestamp.** `ExchangeCommand::PlaceOrder` gains a field `queued_at`
  that exists only in the feature build. Every place that builds the command (19 of them: the
  gateway, both benchmarks and tests) now calls one constructor, `ExchangeCommand::place_order`, so
  the conditional code lives in one spot.

  Rejected:
  - *wrapping every queue message in a timestamped envelope*: it changes the channel's type at about
    40 sites in both builds;
  - *a global table of timestamps keyed by order id*: it takes a lock per stage per order, which
    would distort what it measures.
- **Where the worker records.** It records into a thread-local set of histograms, with no lock per
  order, and merges them into the global report once per group. The gateway records into the global
  report under a mutex, which is fine for one lock per HTTP stage.
- **How the report is read.** `--bench` prints it at the end. With HTTP, the generator resets it on
  the operator port (`POST /stages/reset`) just before its measured orders, and prints it
  (`GET /stages`) at the end. Both routes exist only in the feature build.
- **Write and sync timed separately in every build.** The un-replicated flush used to call
  `append_record`, which is exactly `write_records` followed by `sync`. It now calls the two halves
  itself, so the feature build can time each. The behaviour of both builds is unchanged, and
  `append_record` remains for tests only.

## 2026-10-14 — Part 2 confirmed; Part 3 still open

After Part 1, two questions were put to the owner:
1. Go ahead with Part 2, a pipelined sync, knowing that Part 1 projects it mainly raises the maximum
   rate (about +37–66% alone, +79% replicated) and gains at most about 8% of latency at 5,000
   orders/s?
2. Drop Part 3, the binary format (recommended), or keep it as a learning exercise?

The owner answered "go ahead with next steps of our current milestone". That is taken as a yes to
question 1. Question 2 was not answered. Part 3 is not started, and is put to the owner again with
Part 2's results.

## 2026-10-19 — Part 3 dropped

After Part 2's results the owner answered "drop part 3, push and start part 4". The binary record
format is dropped. The reasons, from the measurements:
- latency: `Encode` is 0.05–0.33% of an order's latency at 1,000 and 5,000 orders/s
  (`performance/02-where-the-time-goes.md`), so a free encoder changes nothing measurable;
- throughput: with the pipeline, the worker's own work limits the maximum rate alone, and `Encode`
  is 3.0 µs of the 15.9 µs per command there (16.12 ms / 1,011.8), about 19%; a free encoder could
  raise the maximum rate by at most about 23% (1 / (1 − 0.19)), an estimate
  (`performance/03-pipelined-sync.md`);
- replicated, the shared SSD limits the maximum rate, not the worker's CPU;
- the cost: a new journal format (`EXCHLOG3`), a new journal, and changes to every reader
  (recovery, the replica, the mmap stream, the MDP, the reporter, the probe).

## 2026-10-14 — Design choices inside decision 2 (made while building Part 2)

Implementation choices, recorded so the owner can overrule them. The reasoning, with a worked
example, is in `part-2-pipelined-sync.md`, "The design, and why".

- **One sync covers everything written, not one sync per group.** The journaler syncs the journal
  up to where the worker has written. Groups written during a sync share the next one.

  Rejected: *one sync per group, in order*. At moderate load a group of one order would use a whole
  sync, and every order behind it would wait a full sync longer than with no pipeline at all.
- **The depth counted in commands: at most 2,048 (two full groups) waiting for the disk.** This is
  decision 2's "one syncing, one staged" in the unit that matters for memory and for the failure
  rule.

  Rejected: *two worker groups*. Small groups would use up the slots, as above.
- **The worker waits on the queue and the journaler at once**, with a current-thread Tokio runtime
  used only to wait, and a `tokio::sync::Notify`.

  Rejected:
  - *an `ExchangeCommand` variant the journaler sends into the queue*: an internal signal in the
    gateway's type;
  - *polling*: it adds latency or burns CPU.
- **The journaler stops at its first failed sync and never syncs again.** After a failed `fsync`
  the kernel may have dropped the dirty pages and marked them clean, so a later `fsync` could
  succeed without the data. Everything in flight is answered unavailable.
- **On a staging fault or a write failure, the worker drains before it halts.** The groups before
  the fault are valid and may still become durable; they are released if they do. On a failed sync
  or a failed publication, nothing more is released.
- **Two new stages in the timed build:**
  - `WaitForSync`, per command, from its group's write to its release;
  - `PipelineFull`, per wait, when the worker stops at 2,048 commands.

  `Sync` and `Confirm` become per sync and are recorded on the journaler thread.
- **Records go to the replica once per sync, from the journaler, not once per group from the
  worker** (changed after the first measurement, which found the per-group version broken at
  moderate load, replicated: the replica syncs every frame and fell seconds behind;
  `part-2-pipelined-sync.md`, "A bug the measurement found").

  Rejected for now: *coalescing frames on the replica*. It would also work, but it changes the
  replica, which this milestone does not otherwise touch.
