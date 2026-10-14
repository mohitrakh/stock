# Milestone 25 — Glossary

Terms used in this milestone, alphabetically. Milestone 24's `glossary.md` defines the measurement
terms it builds on (coordinated omission, HdrHistogram, open loop, percentile, Little's law, fsync,
power-loss protection).

**Amdahl's law.** If a fraction f of a time is sped up by a factor k, the new time is
(1 − f) + f / k of the old one. It bounds what any one optimization can gain. Theory §6.

**Batch-service queue.** A queue whose server takes every waiting customer at once, as the
exchange worker takes every queued command into one group. Theory §3.

**Critical path.** The chain of stages an order must wait for, one after another. Its latency is
their sum. Theory §1.

**Drain.** In Part 2, the worker waiting until every group in flight is durable and released, before
it halts (after a staging fault or a write failure) or returns (when the queue closes).

**Durable before visible.** Nothing about a command (reply, read, publication, callback) may be
seen before the disk holds it, and before both disks hold it when replicated. Theory §7.

**fsync failure ("fsyncgate").** After a failed `fsync`, Linux may have dropped the dirty pages it
could not write and marked them clean, so a second `fsync` can succeed although the data never
reached the disk. This was found by PostgreSQL's developers in 2018. It is why the journaler never
syncs again after a failure (Part 2).

**Group / group commit.** The commands the worker takes off the queue together, written with one
`write_all` and made durable with one `sync_all`. Theory §3.

**Group cycle.** The time from one group being taken to the next: staging, write, sync, publish,
release.

**Inspection paradox.** An observer arriving at a random moment is more likely to land in a long
interval than a short one, so the wait it sees is longer than half the mean interval. Its mean
residual is E[C²] / (2 E[C]). Theory §3.

**Instrumentation overhead / observer effect.** The time that measuring takes, which changes what
is measured. Measured here by comparing the build with timing against the build without it.
Theory §5.

**In flight.** In Part 2, a group written to the journal but not yet released: it waits for the disk.

**Journaler.** The thread that syncs the journal in Part 2 (`Journaler` in `runtime.rs`). Each sync
covers everything the worker has written before it began, and, when replicated, waits for the
replica's confirmation. Database logs call the same role a log flusher.

**Linearity of expectation.** The mean of a sum is the sum of the means, always. That is why stage
means can be checked against an end-to-end mean, and stage medians cannot. Theory §2.

**Monotonic clock.** A clock that never goes backwards (Rust's `Instant`), unlike wall time.
Theory §5.

**Per-command stage / per-group stage.** A stage each order pays on its own (Prepare, Encode,
Commit), against one paid once for a whole group and waited for by every order in it (Write, Sync,
Confirm, Publish, Release). Theory §1.

**Pipelining.** Overlapping consecutive steps of a sequence, here staging group N+1 while group N
syncs. Throughput is then limited by the slowest step rather than by their sum. Theory §6.

**Notify permit.** `tokio::sync::Notify::notify_one` with nobody waiting stores one permit, and the
next wait returns at once. So a signal sent between "check the state" and "wait" is never lost.

**Pipeline depth.** How many groups can be in flight at once: 2 here, one syncing and one staging.
Part 2 counts it in commands, at most 2,048 (`MAX_UNDURABLE`), because one sync can cover several
small groups.

**Stage.** One span of an order's round trip between two timestamps, named in
`src/exchange/stage_timing.rs`. Theory §1.

**`stage-timing`.** The cargo feature that compiles the stage timing in. Off by default.

**Thread-local.** A value each thread has its own copy of, so using it takes no lock. The worker's
stage histograms are one.

**vDSO.** Kernel code mapped into every process, so that calls such as reading the clock need no
system call. Theory §5.
