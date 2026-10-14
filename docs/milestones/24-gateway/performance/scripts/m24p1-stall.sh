#!/usr/bin/env bash
# Milestone 24 part 1: how simultaneous logins delay orders, on the milestone 23 gateway, where
# bcrypt runs inside the request's `async fn` on a Tokio worker thread.
# The exchange runs alone (no replica) and receives 1,000 orders/s over HTTP for 20 seconds: a
# light load, so any extra latency comes from the logins. Alongside, C tasks log in back to back
# (each waits for its answer, then logs in again), for C = 0, 1, 2, 4, 8 and 16.
# Every run has a 120 s deadline: an order still unanswered 120 s after the first was due is
# abandoned and counted. (A first version without it never ended at C = 8: a few orders were never
# answered, so the login loops never stopped.)
# For C >= 8 the script also probes the exchange: GET /health on the customer port and GET /session
# on the operator port (new connections, each with a 10 s limit), and the journal's size, three times,
# each after 30 s of sleep (about 30, 60 and 90 s after the generator starts when the probes answer at
# once; later when they time out).
# Usage: m24p1-stall.sh [RUNS]   (default 2)
set -u
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-2}
probe() {
  local health operator size
  health=$(docker exec $X curl -s -m 10 -o /dev/null -w '%{http_code} in %{time_total}s' 127.0.0.1:4000/health)
  operator=$(docker exec $X curl -s -m 10 -o /dev/null -w '%{http_code} in %{time_total}s' 127.0.0.1:4004/session)
  size=$(docker exec $X stat -c %s /data/events.log)
  echo "  probe $(date +%T): /health $health; operator /session $operator; journal $size bytes"
}
echo "### m24p1-stall: $RUNS run(s) of each, $BIN, $(date -Is)"
for logins in 0 1 2 4 8 16; do
  for run in $(seq 1 "$RUNS"); do
    echo "### logins $logins, run $run: --http-bench --orders 20000 --rate 1000 --logins $logins --deadline 120"
    start_exchange no
    host_note
    if [ "$logins" -ge 8 ]; then
      http_bench --orders 20000 --rate 1000 --logins $logins --deadline 120 > /tmp/m24-stall-run.txt &
      bench=$!
      for at in 30 60 90; do
        sleep 30
        probe
      done
      wait $bench
      cat /tmp/m24-stall-run.txt
    else
      http_bench --orders 20000 --rate 1000 --logins $logins --deadline 120
    fi
  done
done
clean
echo "### done"
