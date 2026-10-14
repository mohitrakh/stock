#!/usr/bin/env bash
# Milestone 25 part 1: m25p1-overhead.sh again with the order of the two builds swapped in every
# pair (the timed build first). In the first script the normal build always ran first, and at the
# maximum rate the timed build was faster in all three pairs; this checks whether the position in
# the pair, not the build, explains that.
# Usage: m25p1-overhead-reversed.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
echo "### m25p1-overhead-reversed: $RUNS run(s) of each, $(date -Is)"
for args in "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
  for run in $(seq 1 "$RUNS"); do
    for binary in stock-m25p1-timed stock-m25p1; do
      BIN=/scripts/$binary
      echo "### $binary, run $run: --bench $args"
      host_note
      inproc_bench no $args | grep -E "^bench:|throughput|latency|syncs|rejected"
    done
  done
done
clean
echo "### done"
