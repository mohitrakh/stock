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

`POST /promote` calls `EventStore::open_existing`. That takes the exclusive journal writer lock
and fully validates the existing file. It cannot create a missing journal and it refuses an empty
one. If the old primary still holds the lock, the endpoint returns `409 Conflict`; the warm remains
healthy and continues following. Nothing becomes primary and no stream or journal bytes change.

After a successful lock, the hand-off checks that the locked journal has the same device/inode as
the warm checkpoint, drops the reader/core, and passes the fully recovered journal to
`ExchangeRuntime::from_store`. That factory replays every durable batch from sequence 1 and checks
the regenerated outputs. Only then does it make a new `StreamWriter`, republish the journal
watermark, and attach the ordinary snapshot schedule.

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

## Verification

Focused tests prove journal catch-up, live mmap following without writes, snapshot use, output
mismatch rejection without checkpoint advance, durable-but-unpublished recovery, and the fact that
a valid-but-different mmap cache cannot override the journal. A loopback test exercises a successful
control-plane hand-off into the primary factory. A process-level test starts the real executable
while another process owns the journal and proves `409 Conflict`, continued health, and unchanged
journal/mmap bytes. `open_existing` also has a direct test that it never creates or initializes a
missing or empty journal.

The final full-suite command and result are recorded in `PROJECT_DIRECTION.md`,
`EXCHANGE_PIPELINE_TODO.md`, and `SYSTEM_DOCUMENTATION.md` after the completed verification run.

## Deliberate limits

This is one trusted host using advisory local file locks. It has no heartbeat, automatic failover,
leader election, authenticated management API, cross-host journal replication, reliable UDP, Raft,
network partition handling, RPO/RTO target, performance measurement, or protection from host,
disk, or power loss. Promotion is manual and its full replay time grows with the journal. It is a
correct ownership experiment, not a claim of exchange-grade HA.
