#!/usr/bin/env bash
# Milestone 25 part 2: where an order's time goes, before and after the pipelined sync. The
# stage-timing builds of part 1 (stock-m25p1-timed) and part 2 (stock-m25p2-timed), interleaved,
# in m25p1-stages.sh's configurations, RUNS times each:
#   in-process (`--bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s, maximum rate
#     over 200,000 orders};
#   HTTP (`--http-bench`, generator stock-m25p1): {alone, replicated} x {1,000, 5,000} orders/s
#     for 20 s.
# Each run prints its end-to-end latency and its stage report. The order of the two builds
# alternates from run to run, as in m25p2-compare.sh.
# Usage: m25p2-stages.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
GEN=/scripts/stock-m25p1
order() {
  if [ $(($1 % 2)) -eq 1 ]; then echo "stock-m25p1-timed stock-m25p2-timed"; else echo "stock-m25p2-timed stock-m25p1-timed"; fi
}
echo "### m25p2-stages: $RUNS run(s) of each, generator $GEN, $(date -Is)"
for replicated in no yes; do
  for args in "--orders 20000 --rate 1000" "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
    for run in $(seq 1 "$RUNS"); do
      for binary in $(order "$run"); do
        BIN=/scripts/$binary
        echo "### $binary, in-process, replicated $replicated, run $run: --bench $args"
        host_note
        inproc_bench $replicated $args | grep -vE "^  day "
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
