# 11 - Failover Under Load

Milestone 23, Part 6. The reasons are in `docs/tasks/19-two-machines.md`; this file is the
measurement. Raw output: `results/results-m23.txt`. The code it exercises:
- replication and the pause: `Replication` in `src/exchange/replication.rs`, the replica in
  `src/exchange/replica.rs`;
- promotion and terms: `src/exchange/warm_replica.rs`, `promote_replica` and `begin_term` in
  `src/exchange/runtime.rs`, `src/exchange/terms.rs`.

## The questions

Parts 4 and 5 each ran live, but with commands sent one at a time from the operator port. Three
things the milestone promises had not been shown with customers trading:
- killing the primary at any moment under load loses no acknowledged command, over repeated runs,
  and the replica's journal is a byte-identical prefix of the primary's;
- how long a failover takes, from the promote request to the first accepted order;
- with the replica killed or its network cut, the primary acknowledges nothing until the operator
  acts or the replica is back.

And Part 4's throughput and latency, again on the current code.

## How it was measured

- **Machine:** the office Ubuntu machine, an i3-7100 with 2 cores and 4 threads and a SATA SSD;
  release builds in the `stock-rust` container (`~/stock-scripts/stock-m23p6`). Other services'
  containers were running on it throughout.
- **Two containers** on one Docker network, each with its own volume, as in Parts 4 and 5: machine
  A and machine B. They share the 2 cores and the SSD. Two real machines would each have their own
  disk and a real network between them.
- **The customers:** `~/stock-scripts/m23p6-load.py`, 16 users each sending orders back to back
  over HTTP with its own `client_order_id`, from a third container that shares A's network
  namespace (the customer port binds loopback only). It survives A, and it writes every 201 to an
  acknowledgement file the moment it arrives, with the wall time. The orders cross, so they trade,
  rest and fill. Afterwards it asks the surviving exchange for every acknowledged order, as its
  user.
- **The failover**, timed by a client in B's network namespace: `POST /promote` until 202, `POST
  /replication/run-alone` until 200, then an order until 201, each retried every 5 ms. Part 5's
  figure (375 to 697 ms) was taken from the host and included `docker exec` polling every 0.2 s.
- **Scripts:**
  - `m23p6-failover.sh 10`: ten runs; each kills A after a random 2 to 8 s of load (2.2 to 6.7 s
    in these runs). In even runs
    A's network is cut 0.3 s before the kill, as when a machine is lost, so that A holds records B
    never received; in odd runs only the process is killed, and the kernel still delivers what A
    had sent.
  - `m23p6-faults.sh`: the replica killed, then its network cut, under load.
  - `m23p6-gap.sh 40`: B's replica killed 40 times under load, with the Part 5 and the Part 6
    binary, then A lost and the warm replica promoted.
  - `m23p6-bench.sh 2`: Part 4's `--bench` comparison (alone, replicated, the replica's files in
    memory), twice, with the journals compared byte for byte after each replicated run.
- **Noise:** on this shared machine, maximum-rate throughput varies by up to a fifth between runs of
  the same configuration, and the highest latency percentiles by much more.

## Results

### The primary killed under load

| Run | A lost | Load before | Acknowledged | Found on B | A's unconfirmed tail | Promote accepted | Running alone | First order accepted |
|---|---|---|---|---|---|---|---|---|
| 1 | killed | 6.5 s | 9,991 | 9,991 | 0 | 8.7 ms | 58.1 ms | 74.2 ms |
| 2 | network cut, killed | 5.4 s | 8,603 | 8,603 | 7,144 bytes | 2.7 ms | 24.3 ms | 26.1 ms |
| 3 | killed | 2.9 s | 6,228 | 6,228 | 0 | 4.1 ms | 25.7 ms | 28.3 ms |
| 4 | network cut, killed | 4.8 s | 8,680 | 8,680 | 5,717 bytes | 7.0 ms | 28.7 ms | 30.5 ms |
| 5 | killed | 2.2 s | 3,982 | 3,982 | 0 | 6.5 ms | 38.7 ms | 50.3 ms |
| 6 | network cut, killed | 5.8 s | 10,895 | 10,895 | 0 | 11.8 ms | 38.5 ms | 41.9 ms |
| 7 | killed | 3.2 s | 5,994 | 5,994 | 0 | 5.2 ms | 37.6 ms | 56.3 ms |
| 8 | network cut, killed | 3.5 s | 7,494 | 7,494 | 0 | 4.2 ms | 36.2 ms | 47.1 ms |
| 9 | killed | 6.7 s | 12,100 | 12,100 | 0 | 2.6 ms | 34.5 ms | 40.1 ms |
| 10 | network cut, killed | 5.9 s | 9,219 | 9,219 | 7,137 bytes | 8.4 ms | 35.3 ms | 42.0 ms |

The times are from the promote request. In every run:
- B's journal was a byte-identical prefix of A's;
- every one of the 83,186 acknowledged orders was on B;
- a reporter started on B resumed at the sequence A's reporter had saved;
- A, restarted as B's replica, cut what B did not hold (the tail above, where there was one), and
  after one more command on B the two journals were byte-identical.

From the promote request to the first accepted order took 26 to 74 ms, median 42 ms. Most of it is
the promoted primary starting: the promotion is answered within 12 ms, its operator port accepts
"run alone" 24 to 58 ms after the request, and the first order follows 2 to 19 ms later. The operator's own steps come on top: on the
replica's machine, stopping the replica process before the promotion, and asking to run alone.

In three of the five network-cut runs A held 5.7 to 7.1 KB that B never received: the records of
the commands it had written after the cut, which it never acknowledged. In the other two the cut
came while A waited for B's confirmation of a group B already held.

### The replica killed, and its network cut, under load

- **Killed.** The primary paused at once, `/health` 503. From 0.5 s after the kill until the
  operator ran it alone 3.4 s later, nothing was acknowledged; the longest silence between two
  acknowledgements was 3,445 ms. Running alone, it acknowledged 11,024 orders in 3 s. The replica
  restarted, caught up, and the primary was synchronous again 1.0 s after it started.
- **Network cut** (the replica's container disconnected without a word). Nothing was acknowledged
  from 0.5 s after the cut until the network came back 12.6 s later; the primary dropped the
  silent link after 10.6 s and answered `/health` 503. The replica dialled again and the primary
  was synchronous 1.0 s after the network came back.
- 27,308 orders were acknowledged over the 30 s; all were on the primary, and the two journals
  were byte-identical.

### The replica killed while it publishes

40 kills of B's replica under load, with each binary: the warm replica never stopped, and never
met an interrupted publication (with the Part 6 binary, `/status` never reported
`stream_interrupted` when sampled after each kill; the Part 5 binary has no such field, and would
have stopped). A publication is a copy of a few kilobytes into the mapped file, a small share of
each group, so a kill there is rare; the unit and process tests create the state directly instead. After the kills, A was lost and B's warm
replica promoted: 49,555 (Part 5 binary) and 65,342 (Part 6 binary) acknowledged orders, all on B;
the first order accepted 37 and 36 ms after the promote request.

### Throughput and latency, with and without replication

**Maximum rate**, 200,000 orders, orders/s:

| | Run 1 | Run 2 |
|---|---|---|
| Alone | 56,107 | 45,193 |
| Replicated | 35,782 | 34,254 |
| Replicated, the replica's files in memory | 44,954 | 46,501 |

**5,000 orders/s**, 100,000 orders, latency in milliseconds:

| | p50 | p90 | p99 | p99.9 | max | Orders per sync |
|---|---|---|---|---|---|---|
| Alone, run 1 | 7.9 | 13.6 | 30.4 | 65.0 | 75.1 | 12.1 |
| Alone, run 2 | 7.2 | 11.9 | 16.7 | 27.6 | 34.1 | 12.7 |
| Replicated, run 1 | 11.4 | 20.6 | 28.8 | 40.9 | 49.1 | 19.1 |
| Replicated, run 2 | 12.4 | 24.5 | 49.2 | 88.9 | 108.7 | 25.5 |
| Replica's files in memory, run 1 | 6.7 | 12.2 | 18.7 | 32.9 | 42.8 | 10.3 |
| Replica's files in memory, run 2 | 7.8 | 13.6 | 23.0 | 47.3 | 58.3 | 14.5 |

After every replicated run the two journals were byte-identical. These agree with Part 4's
(`10-synchronous-replication.md`): replication costs a quarter to over a third of
maximum-rate throughput (24% and 36%, run against run) and 3.5 to 5 ms of median latency here, and with the replica's files in memory the cost disappears
within the noise. On this machine the cost is the second sync waiting for the same SSD; two real
machines would not share it, but would add a network round trip.

The HTTP customers above, 16 of them sending one order at a time, were acknowledged at about 1,500
to 2,100 orders/s replicated and about 3,700 running alone: each waits for its own acknowledgement,
so their rate follows the latency, not the exchange's capacity.

## What it shows, and what it does not

- No acknowledged command was lost in 12 primary losses under load (10 failover runs and the two
  gap runs), 198,083 acknowledged orders in all.
- A failover's own work takes tens of milliseconds. The operator's steps, which are manual by
  design, take longer than that.
- Not shown: a power loss. A killed container keeps its page cache, so a record the primary wrote
  but did not sync is still read back. Part 4's design covers that case (a record is acknowledged
  only once both disks have synced it), but only a machine that loses power can test it.
- Not shown: two machines with their own disks and a real network. Every number here is two
  containers sharing 2 cores and one SSD.

## Options considered and rejected

- **Driving the failover with `--bench`.** It runs in-process: killing the primary kills the
  client, and it records no acknowledged ids. A separate HTTP client in the primary's network
  namespace survives the primary and records exactly what was acknowledged.
- **Checking the acknowledged orders in the reporter's tables.** The reporter is itself a
  subscriber that can lag; asking the promoted exchange for each order checks the state customers
  see.
- **Timing the failover from the host with `docker exec`.** Each check costs about 100 ms, more
  than the failover itself; the client inside B's namespace times it within 5 ms.
