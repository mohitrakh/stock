# Task 13 - Same-host Warm Replica v1

## Goal

This task adds a deliberately manual warm standby. A second process rebuilds the deterministic
exchange state while the primary trades, but it owns no customer routes and cannot write exchange
history. When an operator has stopped the old primary, the warm can take the journal writer lock
and become the normal primary process.

This is not high availability yet. It makes the ownership and promotion boundary real and tested
before adding failure detection, another machine, or a latency promise.

## The central rule

The durable `EXCHLOG1` journal is the source of truth. The mmap file is a bounded same-host
delivery cache. It would be unsafe to promote the warm process merely because it replayed mmap:
a structurally valid cache can disagree with the journal, and a primary must never make cache state
authoritative.

The implementation therefore has two separate paths:

```text
following: journal + mmap StreamReader -> ReplicaCore -> follower health/checkpoint

promotion: exclusive journal writer lock -> full journal recovery -> full deterministic replay
           -> StreamWriter + snapshot schedule -> normal primary startup
```

The first path is a live consistency check. The second path establishes authority. Promotion pays
the cost of a full writer-locked journal replay even when the warm was caught up; that is slower
than reusing its in-memory core, but correct for v1.

## How the follower works

`ReplicaCore` owns only an `ExchangeCore` and its next journal event sequence. It applies one
complete input/output batch using the same prepare, compare, and commit logic as ordinary replay.
It has no `ExchangeCommand` receiver, journal writer, mmap writer, callback publication,
PostgreSQL connection, or customer exchange HTTP routes.

The warm opens a `StreamReader`. A valid primary snapshot is an optional starting checkpoint; it
restores the core and reads only later batches. If the snapshot is absent or unusable, the warm
starts at sequence 1 and catches up from the journal. It catches up before binding its management
listener, then follows new mmap publication and journal fallback like every other reader.

The reader's physical cursor moves when it reads a batch. The warm stores a separate *applied*
checkpoint only after `ReplicaCore` has successfully compared and committed that batch. A missing,
extra, out-of-sequence, or incorrect output leaves the core and applied checkpoint at the earlier
good batch. A semantic replay failure can therefore never become a promotion boundary.

## Manual fencing and hand-off

Run the local control process with:

```sh
cargo run -- --warm-replica exchange-events.log exchange-events.log.mmap exchange-events.log.snapshot

curl http://127.0.0.1:4003/status
curl -X POST http://127.0.0.1:4003/promote
```

The optional fourth argument changes the management address, but it must be loopback. The process
offers `GET /health`, `GET /status`, and `POST /promote`. It has no authentication because this is
a trusted local-development control plane, not a production operator API.

`POST /promote` calls `EventStore::open_existing_matching` with the journal device/inode recorded
in the warm's applied checkpoint. That takes the exclusive journal writer lock. It cannot create a
missing journal and it refuses an empty one. If the old primary still holds the lock, the endpoint
returns `409 Conflict`; the warm remains healthy and continues following. Nothing becomes primary
and no stream or journal bytes change.

With the lock held, the opener compares the file's device/inode with the followed journal *before*
it reads or repairs anything. Only a match proceeds to recovery, which validates every record and
truncates a torn final frame. The hand-off then drops the reader/core and passes the fully
recovered journal to `ExchangeRuntime::from_store`. That factory replays every durable batch from
sequence 1 and checks the regenerated outputs. Only then does it make a new `StreamWriter`,
republish the journal watermark, and attach the ordinary snapshot schedule.

`202 Accepted` means only that the old writer was fenced and the hand-off was accepted. It does
**not** mean the customer-facing primary is ready. After the control server stops, `main` still
builds the runtime, connects the existing database dependency, starts the worker, and binds the
customer listener. Any of those later stages can fail. Readiness needs its own explicit operator
protocol later.

## Why this comes before automation

The important rules are now exercised in code: a primary cannot write without the journal lock, a
candidate primary cannot prefer mmap state over the journal, and a failed lock attempt does not
consume the follower. Those are the hard safety properties to retrofit after automatic failover.

A future faster promotion might use a separately validated durable checkpoint, but it would need
proof that the checkpoint describes exactly the journal being promoted. Reusing an mmap-derived
core is not that proof.

## Finishing v1: identity before repair

This task was completed in two sessions. The first built the follower, fencing, promotion, and
their tests, but ended before the last piece of hardening was connected.

In that first version, the hand-off opened the journal with an opener that recovered the file and
*then* compared its identity with the warm checkpoint. Recovery is not read-only: it truncates a
torn final frame. If the journal path had come to name a different file — an operator moved the
followed journal aside and another appeared in its place — promotion would repair that other file
first, and only afterwards discover it was the wrong one and refuse. The refusal was correct; it
simply arrived after the damage.

The fix had already been written. `open_existing_matching` compared identity under the lock and
before the first read, and it had its own passing test. But promotion never called it, it had
never been through the formatter, and nothing documented it. It was the only code in the repository
that failed `cargo fmt -- --check`. That was the unfinished part of this task, and it is also the
pattern this project's own rules warn about: a passing test on unreachable code.

Completing it took three changes:

- `ReaderCheckpoint::journal_identity()` now exposes the followed journal's device/inode. Those
  fields are private, and the checkpoint previously offered only `matches_journal_identity`, which
  could answer whether a pair matched but could not hand the expected pair to the opener. That gap
  is the likely reason the switch stalled.
- `WarmReplica::try_promote` calls `open_existing_matching`, so identity is compared while holding
  the lock and before any read or repair.
- `open_existing`, `EventStore::journal_identity`, and `matches_journal_identity` were deleted.
  Promotion was their only production caller. Keeping the unchecked opener would have left the
  unsafe order one mistaken call away, so the checked opener is now the only way promotion can open
  a journal.

The regression test `promotion_refuses_a_swapped_journal_without_repairing_the_replacement` was
written first and run against the unfixed code. It failed, and more severely than expected: the
replacement journal's only record was torn, so the old path truncated it back to nothing but its
8-byte `EXCHLOG1` header — the whole record of a journal the warm had never read — before it
refused. With the fix, the same test passes and the replacement file is byte-for-byte unchanged.

## Verification

Focused tests prove journal catch-up, live mmap following without writes, snapshot use, output
mismatch rejection without checkpoint advance, durable-but-unpublished recovery, and the fact that
a valid-but-different mmap cache cannot override the journal. A loopback test exercises a successful
control-plane hand-off into the primary factory. A process-level test starts the real executable
while another process owns the journal and proves `409 Conflict`, continued health, and unchanged
journal/mmap bytes. The opener has direct tests that it never creates or initializes a missing or
empty journal, and that it refuses a foreign journal without repairing its torn tail; the swapped
journal regression test proves the same property through the warm's own promotion path.

Verified on 2026-09-29 on Linux with Rust 1.98.1. The crate uses Unix-only APIs, so it does not
build on Windows; the run used a `rust` container with the repository mounted. `cargo fmt --
--check` is clean and `cargo test --locked` passes 131 unit tests and 6 executable integration
tests. The one ignored test is the opt-in Reporter acceptance test, which needs
`REPORTER_TEST_DATABASE_URL`. Clippy reports nothing in the files this task changed; the
repository's existing warnings remain.

The unit and process tests stop at the hand-off. What `main` does after `202` — rebuild the
runtime, connect PostgreSQL, bind the customer listener — is the part this write-up had flagged as
able to fail, so it was exercised live: the real primary and warm executables in one Linux container
on loopback, with a disposable PostgreSQL beside them.

| Step | Result |
|---|---|
| Primary trades: deposits, a resting sell of 10, a partial fill of 4 | warm catches up to `next_event_sequence` 11 |
| `POST /promote` while the primary runs | `409`; warm `/health` stays `200` |
| Primary accepts one more command | warm follows to 13, writing nothing |
| Primary killed with `SIGKILL` | port 4000 stops answering |
| `POST /promote` | `202`; the promoted process binds port 4000 and `/health` returns `200` |
| Balance, positions, and order state from the promoted primary | identical to the values read before the kill |
| New buy of 6 on the promoted primary | fills the resting remainder; retrying the same `client_order_id` is still `409` |
| Journal read back by an independent `--event-probe` | 7 committed batches, event sequences 1–18, contiguous across the hand-off |

This is a single-host functional run. It is not a latency, throughput, or power-loss test.

## Deliberate limits

This is one trusted host using advisory local file locks. It has no heartbeat, automatic failover,
leader election, authenticated management API, cross-host journal replication, reliable UDP, Raft,
network partition handling, RPO/RTO target, performance measurement, or protection from host,
disk, or power loss. Promotion is manual and its full replay time grows with the journal. It is a
correct ownership experiment, not a claim of exchange-grade HA.
