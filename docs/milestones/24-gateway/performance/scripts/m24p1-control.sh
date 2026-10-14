#!/usr/bin/env bash
# Milestone 24 part 1: the control for m24p1-http.sh. The same generator, the same open-loop
# schedule, but every request is GET /health, which the gateway answers alone (no JWT, no command
# queue, no journal, no exchange worker). Its latency is the floor of generator + HTTP + Axum on
# this machine. The exchange runs alone (a replica would change nothing: /health never waits for
# it), at 1,000 and 5,000 requests/s.
# Usage: m24p1-control.sh [RUNS]   (default 2)
set -u
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-2}
echo "### m24p1-control: $RUNS run(s) of each, $BIN, $(date -Is)"
for rate in 1000 5000; do
  orders=$((rate * 20))
  for run in $(seq 1 "$RUNS"); do
    echo "### health control, rate $rate, run $run: --http-bench --target health --orders $orders --rate $rate"
    start_exchange no
    host_note
    http_bench --target health --orders $orders --rate $rate
  done
done
clean
echo "### done"
