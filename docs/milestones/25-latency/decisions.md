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
