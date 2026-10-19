#!/usr/bin/env bash
# Milestone 25 part 4, follow-up: stage reports for the two configurations whose final measurement
# (m25p4-final.sh) disagreed with Part 2's: in-process alone at the maximum rate, and in-process
# replicated at 5,000 orders/s. The stage-timing builds of Part 1 (stock-m25p1-timed, the sync on
# the worker, which behaves as stock-m24p2) and Part 2 (stock-m25p2-timed, the pipeline),
# interleaved, RUNS times each, alternating which build runs first.
# Usage: m25p4-stages.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
order() {
  if [ $(($1 % 2)) -eq 1 ]; then echo "stock-m25p1-timed stock-m25p2-timed"; else echo "stock-m25p2-timed stock-m25p1-timed"; fi
}
echo "### m25p4-stages: $RUNS run(s) of each, $(date -Is)"
for config in "no --orders 200000 --rate 0" "yes --orders 100000 --rate 5000"; do
  set -- $config
  replicated=$1
  shift
  for run in $(seq 1 "$RUNS"); do
    for binary in $(order "$run"); do
      BIN=/scripts/$binary
      echo "### $binary, in-process, replicated $replicated, run $run: --bench $*"
      host_note
      inproc_bench $replicated "$@" | grep -vE "^  day "
    done
  done
done
clean
echo "### done"
