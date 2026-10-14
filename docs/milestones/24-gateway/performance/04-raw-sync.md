# 04 — The office SSD's raw sync latency

**Part 1 baseline. Measured 2026-10-12.** Raw output: `results/m24p1-probes.txt` (the "sync"
sections). Script: `scripts/m24p1-probes.sh`. Tool: `stock --sync-probe` (`src/exchange/probes.rs`).

## The question

Every command this exchange acknowledges waits for at least one `sync_all` on the journal, and two
when replicated (`01-theory.md`, section 11). So the disk's flush time is the floor under every
order's latency: no software change can make an acknowledged order faster than one flush.

Before this milestone the only raw figure was from the Docker Desktop VM's virtual disk on the
Windows development PC: about 1.8 ms at p50 and 22 ms at p99 (`docs/performance/00-benchmark-harness.md`,
"Where it ran"). Every milestone 22 and 23 measurement, though, ran on the office machine, whose
own disk had never been measured on its own. This file measures it.

## Method

- **Machine:** the office Ubuntu machine (`decisions.md`): Intel Core i3-7100 at 3.9 GHz, 2 cores
  and 4 hardware threads, 15 GiB RAM, Linux 6.8.0-138, Docker with the overlayfs storage driver.
- **Disk:** ADATA SU650, 223.6 GB, a consumer SATA SSD; the root filesystem `/dev/sda2`, 85% full.
- **Neighbours:** 30 other containers running. Load average 1.45 to 2.63 during the runs (each
  run's own value is in the raw file).
- **Build:** release build of the Part 1 tree, `/scripts/stock-m24p1`, in the `stock-rust` image.
- **Storage:** a fresh Docker *named volume* per run. A named volume is a plain directory under
  `/var/lib/docker/volumes/` on the host's filesystem, so `sync_all` reaches the SSD directly, the
  same way the journal does in every script. For contrast, one run on `tmpfs` (RAM).
- **What one sample is:** `write_all` of a 600-byte record, appended to a growing file opened with
  `append` and `create_new`, then `sync_all`. 600 bytes is about the size of one order's record
  (`--bench` reports about 753 bytes per order). The tool times the two calls separately and
  together, in microseconds, into HdrHistograms with 3 significant digits.
- **Runs:** 10,000 samples each, three runs on the named volume, one on tmpfs. Command:
  `stock --sync-probe /data/p --count 10000 --size 600`.

## Results

Named volume (the SSD). All times in microseconds (µs); 1,000 µs = 1 ms.

| Run | call | mean | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| 1 | `write_all` | 21.3 | 15 | 43 | 79 | 148 | 240 |
| 1 | `sync_all` | 1,296.7 | 780 | 3,171 | 5,219 | 7,923 | 15,527 |
| 1 | total | 1,318.5 | 801 | 3,205 | 5,267 | 7,959 | 15,551 |
| 2 | `write_all` | 23.9 | 20 | 44 | 79 | 184 | 303 |
| 2 | `sync_all` | 1,360.6 | 880 | 3,003 | 5,463 | 8,703 | 25,903 |
| 2 | total | 1,385.0 | 904 | 3,037 | 5,503 | 8,711 | 25,935 |
| 3 | `write_all` | 22.1 | 19 | 41 | 72 | 160 | 587 |
| 3 | `sync_all` | 1,283.2 | 877 | 2,711 | 5,327 | 6,699 | 24,095 |
| 3 | total | 1,305.8 | 904 | 2,737 | 5,359 | 6,803 | 24,143 |

Rate, as the tool printed it: 758, 721 and 765 durable appends per second.

Check of the rate against the mean: one append takes the mean total time, so the rate should be its
inverse. Run 1: 1 / 1,318.5 µs = 1 / 0.0013185 s = 758 per second, as printed. Run 2:
1 / 0.001385 s = 722 per second (printed 721; the printed rate uses wall time, which also includes
the loop's own tiny overhead).

tmpfs, one run: `write_all` mean 0.9 µs, `sync_all` mean 0.1 µs, total mean 1.6 µs, p99 4 µs,
max 72 µs; 437,274 appends per second.

## What the numbers say

**1. A flush on this SSD takes about 0.8–0.9 ms in the typical case, and about 5.3–5.5 ms at
p99.** The three runs agree closely: p50 between 780 and 880 µs, p99 between 5,219 and 5,463 µs.
The tail is long: p99.9 between 6.7 and 8.7 ms, and single flushes up to 26 ms.

**2. The cost is the flush, not the write.** `write_all` copies 600 bytes into the kernel's page
cache and returns: about 22 µs on average. `sync_all` is 1,283–1,361 µs on average, so the flush
is about 98% of each durable append (1,296.7 / 1,318.5 = 98.3% in run 1).

**3. Without a disk, the same code costs almost nothing.** On tmpfs the whole append is 1.6 µs on
average. The difference between the two, about 1,300 µs per append, is the SSD. This is the floor
that `00-selection.md` B.7 described: the milliseconds are in the disk.

**4. Better than the development PC's virtual disk, but the same order of magnitude.** The Docker
Desktop VM measured about 1,800 µs at p50 and 22,000 µs at p99. The office SSD is about twice as
fast at p50 (800–900 µs) and about four times as fast at p99 (about 5,300 µs). Both are
milliseconds. A drive with power-loss protection would typically be tens of microseconds
(`01-theory.md`, section 11.3), which is not measured here.

**5. What this says about milestone 23's 5,000 orders/s runs.** `00-selection.md` B.5 worked out
that the exchange's group cycle at 5,000 orders/s averaged 2.42 ms (run 1) and 2.53 ms (run 2). The
mean raw flush measured here is 1.28–1.36 ms. So, *if* the disk behaved the same on 2026-10-09 as
today, the flush is roughly half of each cycle:

```text
cycle 2.42 ms − mean flush 1.30 ms ≈ 1.1 ms per cycle not spent in the flush
CPU per cycle ≈ 12.1 orders × 25.5 µs ≈ 0.31 ms (Task 14's CPU per order, measured in the VM)
remaining ≈ 1.1 − 0.3 = 0.8 ms per cycle not accounted for by flush or CPU
```

Treat this as an indication, not a result:
- the two measurements were taken three days apart, with different background load;
- a group's record (about 12 × 753 bytes ≈ 9 KB) is larger than one 600-byte append;
- the CPU figure comes from a different machine.

**The same-day comparison tells a different story.** Part 1 also ran the in-process bench at
5,000 orders/s, alone, about two minutes after these probes (`results/m24p1-http.txt`):

| Run | syncs | over | syncs / s | mean cycle | mean raw flush (this file) | flush share of the cycle |
|---|---|---|---|---|---|---|
| 1 | 13,731 | 20.004 s | 686.4 | 1 / 686.4 = 1.46 ms | 1.28–1.36 ms | 1.28 / 1.46 to 1.36 / 1.46 = 88–93% |
| 2 | 12,133 | 20.002 s | 606.6 | 1 / 606.6 = 1.65 ms | 1.28–1.36 ms | 78–82% |

On the same afternoon, then, the flush was about 80–90% of each group cycle, and the median order
latency was 3.6–3.8 ms, half of milestone 23's. With the trading path unchanged, something in the
machine's state must have moved the exchange's speed by a factor of two between 2026-10-09 and
2026-10-12. That is an inference by elimination; which part of the state is not measured. The lesson is to compare only measurements
taken on the same day, interleaved (`00-selection.md`, "Correction after Part 1").

## What this does not show

- **Power-loss durability.** Whether this drive tells the truth about its flushes cannot be
  measured without cutting its power.
- **A group's sync.** The exchange syncs one record per *group*, often several kilobytes. A larger
  write before the flush may take longer than a 600-byte one.
- **Two writers at once.** With replication, the primary's and the replica's syncs share this one
  SSD (milestone 23 part 4). Each of them would see longer flushes than a single writer does here.
- **Stability over time.** These are three runs of 13 seconds on one afternoon. A drive's flush
  time also depends on its internal state: how full it is (85% here), and garbage collection.
