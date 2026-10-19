#!/usr/bin/env bash
# Milestone 25 part 4: the final measurement. The milestone 24 part 2 binary (stock-m24p2, the
# exchange before milestone 25) against the final binary (stock-m25final, built from the milestone
# 25 commit), interleaved, RUNS times each, alternating which build runs first:
#   on the SSD:
#     in-process (`--bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s, maximum
#       rate over 200,000 orders};
#     HTTP (`--http-bench`): {alone, replicated} x {1,000 and 5,000 orders/s for 20 s}, generator
#       always stock-m25final, with each container's CPU over the measured orders (cpu line);
#   on tmpfs (both journals in RAM, so no disk at all):
#     in-process: {alone, replicated} x {5,000 orders/s for 20 s, maximum rate}.
# Usage: m25p4-final.sh [RUNS]   (default 3)
set -u
MIGRATIONS=$HOME/stock-m25/migrations
source ~/stock-scripts/m24-lib.sh
RUNS=${1:-3}
BEFORE=stock-m24p2
AFTER=stock-m25final
GEN=/scripts/$AFTER
order() { # RUN: before first on odd runs, after first on even ones
  if [ $(($1 % 2)) -eq 1 ]; then echo "$BEFORE $AFTER"; else echo "$AFTER $BEFORE"; fi
}

# Every second, until killed: the time, each container's CPU time (cgroup v2 cpu.stat), and the
# host's busy and idle jiffies. Reads files only: no docker call, so it adds almost no load.
cpu_sampler() { # FILE
  local f id k v u n s idle io irq sirq
  while :; do
    printf 'T %s\n' "$(date +%s%N)"
    for f in /sys/fs/cgroup/system.slice/docker-*.scope/cpu.stat; do
      id=${f#*/docker-}
      id=${id%%.scope/*}
      while read -r k v; do
        [ "$k" = usage_usec ] && { printf 'C %s %s\n' "$id" "$v"; break; }
      done 2> /dev/null < "$f" # stderr first: a scope can vanish between the glob and the read
    done
    read -r _ u n s idle io irq sirq _ < /proc/stat
    printf 'H %s %s\n' $((u + n + s + irq + sirq)) $((idle + io))
    sleep 1
  done >> "$1"
}

# http_bench, plus one line with each container's CPU over the measured orders: from the generator's
# "http-bench:" line (registration done, the orders start) to its "throughput" line (all answered).
http_bench_cpu() { # ARGS...
  local samples marks before xid rid sampler
  samples=$(mktemp)
  marks=$(mktemp)
  xid=$(docker inspect -f '{{.Id}}' $X)
  rid=$(docker inspect -f '{{.Id}}' $R 2> /dev/null || true)
  before=$(ls /sys/fs/cgroup/system.slice | sed -n 's/^docker-\(.*\)\.scope$/\1/p' | tr '\n' ' ')
  cpu_sampler "$samples" &
  sampler=$!
  http_bench "$@" | while IFS= read -r line; do
    case "$line" in
      "http-bench:"*) echo "M start $(date +%s%N)" >> "$marks" ;;
      "  throughput"*) echo "M end $(date +%s%N)" >> "$marks" ;;
    esac
    printf '%s\n' "$line"
  done
  kill $sampler
  wait $sampler 2> /dev/null
  awk -v x="$xid" -v r="$rid" -v before="$before" -v n="$(nproc)" \
    -f ~/stock-scripts/m25p4-cpu.awk "$marks" "$samples"
  rm -f "$samples" "$marks"
}

# inproc_bench with both journals in RAM (tmpfs) rather than on the SSD.
inproc_bench_tmpfs() { # no|yes ARGS...
  local replicated=$1
  shift
  clean
  local replication=()
  if [ "$replicated" = yes ]; then
    replication=(-e REPLICATION_LISTEN_ADDR=0.0.0.0:4005)
    docker run -d --name $R --network $NET --tmpfs /data:rw,size=2g -v ~/stock-scripts:/scripts \
      stock-rust $BIN --replica $X:4005 /data/replica.log /data/replica.mmap > /dev/null
  fi
  docker run --rm --name $X --network $NET --tmpfs /data:rw,size=2g -v ~/stock-scripts:/scripts \
    "${replication[@]}" stock-rust $BIN --bench /data/b "$@" 2>&1 | grep -vE "^bench: replicating"
  docker rm -f $R > /dev/null 2>&1
}

echo "### m25p4-final: $RUNS run(s) of each, $BEFORE against $AFTER, generator $GEN, $(date -Is)"
for replicated in no yes; do
  for args in "--orders 20000 --rate 1000" "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
    for run in $(seq 1 "$RUNS"); do
      for binary in $(order "$run"); do
        BIN=/scripts/$binary
        echo "### $binary, in-process, replicated $replicated, run $run: --bench $args"
        host_note
        inproc_bench $replicated $args | grep -E "^bench:|throughput|latency|syncs|rejected"
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
        http_bench_cpu --orders $orders --rate $rate --deadline 120
      done
    done
  done
done
for replicated in no yes; do
  for args in "--orders 100000 --rate 5000" "--orders 200000 --rate 0"; do
    for run in $(seq 1 "$RUNS"); do
      for binary in $(order "$run"); do
        BIN=/scripts/$binary
        echo "### $binary, tmpfs in-process, replicated $replicated, run $run: --bench $args"
        host_note
        inproc_bench_tmpfs $replicated $args | grep -E "^bench:|throughput|latency|syncs|rejected"
      done
    done
  done
done
clean
echo "### done"
