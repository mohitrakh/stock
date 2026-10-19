#!/usr/bin/env bash
# Milestone 25 part 4, second follow-up: in-process replicated at 5,000 orders/s, the one
# configuration whose median got worse in m25p4-final.sh, with an even number of runs so that each
# build runs first in half the pairs. In m25p4-final.sh and m25p4-stages.sh (3 runs) the pipeline
# ran second in two of three pairs, and its median was high exactly then.
# Usage: m25p4-order.sh [RUNS]   (default 6; keep it even)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-6}
order() {
  if [ $(($1 % 2)) -eq 1 ]; then echo "stock-m24p2 stock-m25final"; else echo "stock-m25final stock-m24p2"; fi
}
echo "### m25p4-order: $RUNS run(s) of each, $(date -Is)"
for run in $(seq 1 "$RUNS"); do
  position=1
  for binary in $(order "$run"); do
    BIN=/scripts/$binary
    echo "### $binary, in-process, replicated yes, run $run, position $position: --bench --orders 100000 --rate 5000"
    host_note
    inproc_bench yes --orders 100000 --rate 5000 | grep -E "^bench:|throughput|latency|syncs|rejected"
    position=$((position + 1))
  done
done
clean
echo "### done"
