#!/usr/bin/env bash
# Milestone 25 part 2: the pipelined sync, before and after. The normal builds of part 1
# (stock-m25p1, sync on the worker) and part 2 (stock-m25p2, sync on the journaler thread) run the
# same benchmarks, interleaved, RUNS times each:
#   in-process (`--bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s, maximum rate
#     over 200,000 orders};
#   HTTP (`--http-bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s}. The generator
#     is always stock-m25p1's, so only the exchange differs.
# The order of the two builds alternates from run to run (part 1 found the second of a back-to-back
# pair about 9.5% faster at the maximum rate; performance/01-instrumentation-overhead.md).
# Usage: m25p2-compare.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
GEN=/scripts/stock-m25p1
order() { # RUN: the two builds, before first on odd runs, after first on even ones
  if [ $(($1 % 2)) -eq 1 ]; then echo "stock-m25p1 stock-m25p2"; else echo "stock-m25p2 stock-m25p1"; fi
}
echo "### m25p2-compare: $RUNS run(s) of each, generator $GEN, $(date -Is)"
for replicated in no yes; do
  for args in "--orders 20000 --rate 1000" "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
    for run in $(seq 1 "$RUNS"); do
      for binary in $(order "$run"); do
        BIN=/scripts/$binary
        echo "### $binary, in-process, replicated $replicated, run $run: --bench $args"
        host_note
        inproc_bench $replicated $args | grep -E "^bench:|throughput|latency|syncs|rejected"
      done
    done
  done
  for rate in 1000 5000; do
    orders=$((rate * 20))
    for run in $(seq 1 "$RUNS"); do
      for binary in $(order "$run"); do
        BIN=/scripts/$binary
        echo "### $binary, http, replicated $replicated, run $run: --http-bench --orders $orders --rate $rate"
        start_exchange $replicated
        host_note
        http_bench --orders $orders --rate $rate --deadline 120
      done
    done
  done
done
clean
echo "### done"
