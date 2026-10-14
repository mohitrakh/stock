#!/usr/bin/env bash
# Milestone 25 part 1: what the stage timing itself costs. The normal build (stock-m25p1) and the
# stage-timing build (stock-m25p1-timed) of the same tree run the same in-process benchmark,
# interleaved: alone at 5,000 orders/s for 20 s, and alone at the maximum rate (200,000 orders).
# RUNS of each.
# Usage: m25p1-overhead.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
echo "### m25p1-overhead: $RUNS run(s) of each, $(date -Is)"
for args in "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
  for run in $(seq 1 "$RUNS"); do
    for binary in stock-m25p1 stock-m25p1-timed; do
      BIN=/scripts/$binary
      echo "### $binary, run $run: --bench $args"
      host_note
      inproc_bench no $args | grep -E "^bench:|throughput|latency|syncs|rejected"
    done
  done
done
clean
echo "### done"
