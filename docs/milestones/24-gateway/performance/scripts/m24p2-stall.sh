#!/usr/bin/env bash
# Milestone 24 part 2: the login stall again, the Part 1 exchange (bcrypt on the Tokio workers) and
# the Part 2 exchange (bcrypt on the blocking pool, at most 2 at a time) interleaved, on the same day.
# Same load as m24p1-stall.sh: 1,000 orders/s over HTTP for 20 s, alone, with C clients logging in
# back to back, two runs of each binary, a 120 s deadline.
# Both exchanges are driven by the same generator, $GEN (default the Part 2 build), whose login loops
# back off 10 ms after a failed request. (A first run of this script used each build's own generator;
# the Part 1 build's loops retried failed logins without pausing and spun at 366% CPU.)
# For C >= 16 the script also probes the exchange at about 30, 60 and 90 s (later if a probe times
# out): GET /health and GET /session on new connections (10 s limit each), the journal's size, and
# the CPU use of the exchange and generator containers (`docker stats`).
# Usage: LOGINS="0 4 16" m24p2-stall.sh [RUNS]   (defaults: those three counts, two runs)
set -u
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-2}
GEN=${GEN:-/scripts/stock-m24p2}
probe() {
  local health operator size cpu
  health=$(docker exec $X curl -s -m 10 -o /dev/null -w '%{http_code} in %{time_total}s' 127.0.0.1:4000/health)
  operator=$(docker exec $X curl -s -m 10 -o /dev/null -w '%{http_code} in %{time_total}s' 127.0.0.1:4004/session)
  size=$(docker exec $X stat -c %s /data/events.log)
  cpu=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' $X $G 2>/dev/null | tr '\n' ' ')
  echo "  probe $(date +%T): /health $health; operator /session $operator; journal $size bytes; cpu $cpu"
}
echo "### m24p2-stall: $RUNS run(s) of each, generator $GEN, $(date -Is)"
for logins in ${LOGINS:-0 4 16}; do
  for run in $(seq 1 "$RUNS"); do
    for binary in stock-m24p1 stock-m24p2; do
      BIN=/scripts/$binary
      echo "### $binary, logins $logins, run $run: --http-bench --orders 20000 --rate 1000 --logins $logins --deadline 120"
      start_exchange no
      host_note
      if [ "$logins" -ge 16 ]; then
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
done
clean
echo "### done"
