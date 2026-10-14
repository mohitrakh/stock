#!/usr/bin/env bash
# Milestone 24 part 1: order latency through HTTP against the same orders sent in-process.
# For each of {alone, replicated} x {1,000, 5,000 orders/s}, RUNS times:
#   - `--bench` (in-process, behind the command queue, no HTTP), as milestone 23 part 6 ran it;
#   - `--http-bench` against a real exchange process, through Axum, JSON and JWT, with the
#     generator in the exchange's network namespace.
# Both send the same orders (same seed, users, symbols, prices, quantities, ids) for 20 seconds.
# The difference between the two is the gateway's share of the round trip.
# Usage: m24p1-http.sh [RUNS]   (default 2)
set -u
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-2}
echo "### m24p1-http: $RUNS run(s) of each, $BIN, $(date -Is)"
for replicated in no yes; do
  for rate in 1000 5000; do
    orders=$((rate * 20))
    for run in $(seq 1 "$RUNS"); do
      echo "### in-process, replicated $replicated, rate $rate, run $run: --bench --orders $orders --rate $rate"
      host_note
      inproc_bench $replicated --orders $orders --rate $rate | grep -E "^bench:|throughput|latency|syncs|rejected"
      echo "### http, replicated $replicated, rate $rate, run $run: --http-bench --orders $orders --rate $rate"
      start_exchange $replicated
      host_note
      http_bench --orders $orders --rate $rate
    done
  done
done
clean
echo "### done"
