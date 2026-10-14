#!/usr/bin/env bash
# Milestone 25 part 1: where an order's time goes. The stage-timing build (stock-m25p1-timed), in
# every configuration RUNS times:
#   in-process (`--bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s, maximum rate
#     over 200,000 orders};
#   HTTP (`--http-bench`, generator from the normal build): {alone, replicated} x {1,000, 5,000}
#     orders/s for 20 s.
# Each run prints its end-to-end latency and then the stage report (the bench prints it itself; the
# HTTP generator fetches it from the operator port, which it reset just before the measured orders).
# Usage: m25p1-stages.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
BIN=/scripts/stock-m25p1-timed
GEN=/scripts/stock-m25p1
echo "### m25p1-stages: $RUNS run(s) of each, exchange $BIN, generator $GEN, $(date -Is)"
for replicated in no yes; do
  for args in "--orders 20000 --rate 1000" "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
    for run in $(seq 1 "$RUNS"); do
      echo "### in-process, replicated $replicated, run $run: --bench $args"
      host_note
      inproc_bench $replicated $args | grep -vE "^  day "
    done
  done
  for rate in 1000 5000; do
    orders=$((rate * 20))
    for run in $(seq 1 "$RUNS"); do
      echo "### http, replicated $replicated, run $run: --http-bench --orders $orders --rate $rate"
      start_exchange $replicated
      host_note
      http_bench --orders $orders --rate $rate --deadline 120
    done
  done
done
clean
echo "### done"
