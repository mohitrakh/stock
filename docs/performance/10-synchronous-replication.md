# 10 - Synchronous Replication

Milestone 23, Part 4. The reasons are in `docs/tasks/19-two-machines.md`; this file is the
measurement. The main code:
- the primary's side of the link and the wire format: `Replication` in
  `src/exchange/replication.rs`;
- the worker's group commit with replication: `ExchangeRuntime::flush` in
  `src/exchange/runtime.rs`;
- the second machine's copy: `Replica` in `src/exchange/replica.rs`.

## The question

This part is not an optimization: it adds a cost on purpose. Every group of commands now waits
until a second machine's disk holds it too, so that losing a machine loses no acknowledged
command. The question is how much throughput and latency that costs, and where the cost comes
from.

## How it was measured

- **Machine:** the office Ubuntu machine, an i3-7100 with 2 cores and 4 threads and a SATA SSD;
  release builds in the `stock-rust` container.
- **Two containers** on one Docker network, each with its own volume: the benchmark (`--bench`,
  which runs the production worker, journal and stream) and the replica (`--replica`). They share
  the 2 cores and the SSD. Two real machines would each have their own disk, and a real network
  between them instead of a bridge.
- **Three configurations:**
  - alone: no `REPLICATION_LISTEN_ADDR`, as before this part;
  - replicated;
  - replicated with the replica's journal and stream in memory (tmpfs), so that its syncs cost
    nothing. This separates the shared disk's cost from the cost of the link and of the replica's
    checks.
- **Workloads:** 200,000 orders at the maximum rate (`--rate 0`), and 100,000 orders at 5,000
  orders/s.
- **Runs:** each configuration twice, in turn, with the build after the independent review's fixes.
  The re-check's two later changes, to the replica's hello and to how a warm replica starts, are
  off the measured path. After every replicated run the two journals were compared byte for byte:
  identical every time.
- **Script:** `~/stock-scripts/m23p4-bench.sh 2`, with the binary `~/stock-scripts/stock-m23p4`.
- **Noise:** on this shared machine, maximum-rate throughput varies by up to a fifth between runs of
  the same configuration, and the highest latency percentiles by much more.

## Results

**Maximum rate**, orders/s:

| | Run 1 | Run 2 |
|---|---|---|
| Alone | 44,610 | 43,968 |
| Replicated | 32,372 | 34,511 |
| Replicated, the replica's files in memory | 45,274 | 40,081 |

At the maximum rate every group is full, 1,024 commands, in every configuration: 196 or 197 syncs
for 200,000 orders. Throughput is therefore 1,024 commands per group time. Alone a group took about
23 ms; replicated, 29 to 31 ms. Latency at the maximum rate measures the benchmark's own queue, not
the exchange, so it is not shown.

**5,000 orders/s**, latency in milliseconds:

| | p50 | p90 | p99 | p99.9 | max | Orders per sync |
|---|---|---|---|---|---|---|
| Alone, run 1 | 7.4 | 12.1 | 18.3 | 26.5 | 35.1 | 17.9 |
| Alone, run 2 | 7.5 | 15.1 | 25.6 | 60.5 | 72.9 | 13.3 |
| Replicated, run 1 | 14.7 | 29.2 | 53.3 | 77.6 | 97.3 | 28.2 |
| Replicated, run 2 | 12.3 | 21.2 | 31.9 | 49.2 | 55.8 | 21.1 |
| Replica's files in memory, run 1 | 7.5 | 12.3 | 25.2 | 67.6 | 84.1 | 15.3 |
| Replica's files in memory, run 2 | 7.0 | 12.8 | 27.0 | 51.7 | 66.0 | 11.4 |

Replicated, the median rose by 5 to 7 ms, and p99 by 6 to 35 ms. The highest percentiles are
dominated by outliers in every configuration.

Two earlier runs of each configuration, with the build before the independent review's fixes, gave
the same picture: 32,728 and 37,142 orders/s replicated against 48,392 and 45,692 alone, and at
5,000 orders/s a p50 of 11.4 and 10.9 ms against 6.8 and 6.2 ms. The fixes add a frame each way at
least every second, and a sync of the replica's stream header at most once a second. That sync
shares the SSD with the primary's, and may account for some of the one high p99 above: the five
other replicated runs at this rate, over this build and two earlier ones, had a p99 of 24.9 to
36.8 ms.

## Where the cost comes from

- **The replica's sync, on the same disk.** With the replica's files in memory, neither workload
  shows a difference beyond the noise. So on this machine the whole cost is the second sync, which
  waits for the same SSD as the primary's own. The link and the replica's checks, which decode
  every record, cost nothing measurable.
- **Bigger groups.** Replicated, each group takes longer, so more commands queue meanwhile and share
  the next sync: 21 to 28 orders per sync at 5,000 orders/s instead of 13 to 18. Each sync costs
  more, but there are fewer of them on each machine.
- **On two machines** the two syncs would each have their own disk, and they would overlap: the
  primary ships a group while it syncs it. A group would then wait for the slower of the two syncs,
  plus one network round trip and the replica's checks. Part 6 measures throughput and p99 with and
  without replication again, as the milestone's completion criteria ask.

## What it costs

- **Throughput at the maximum rate:** 21 to 27% less in these runs, and 19 to 32% across all the
  runs made, on this machine where both syncs share one disk.
- **Latency at 5,000 orders/s:** the median from about 7.5 ms to 12 to 15 ms.
- **Availability:** without a replica, nothing is acknowledged until the operator either brings it
  back or lets the primary run alone. That is the owner's decision for this milestone: a recovery
  point of zero over availability.
- **The replica's readers read the journal,** not a cache: the replica keeps no records in memory.
  Its only reader, the warm replica, reads the journal either way.

## Options considered and rejected

- **Pipelining: preparing the next group while the replica confirms this one.** It would hide the
  wait, but answers must still be held until both disks hold their group, and the worker's
  in-memory state would run ahead of what both disks hold. It is the recorded "pipelined journal
  sync" of the latency milestone, and stays out of this one.
- **Checking only each record's checksum on the replica,** not its sequence and shape. The
  measurement shows the full check costs nothing measurable here, and it is what stops the replica
  from appending a record that recovery would refuse.
