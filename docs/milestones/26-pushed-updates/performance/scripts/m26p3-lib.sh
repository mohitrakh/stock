#!/usr/bin/env bash
# Milestone 26 part 3: shared helpers for the feed measurements. Runs on the office machine's host,
# from ~/stock-scripts, on top of milestone 24's m24-lib.sh (the exchange $X, the replica $R, the
# HTTP generator $G, PostgreSQL).
#
# Added to the layout, each in the exchange's network namespace (its ports bind loopback only), each
# reading the exchange's journal volume read-only:
#   $M  the market-data process (MDP), with the book feed on 127.0.0.1:4001
#   $U  the private feed on 127.0.0.1:4006
#   $F  the feed client generator, `--feed-bench`
# Every container shares the host's 2 cores (4 threads) and its one SATA SSD.

MIGRATIONS=${MIGRATIONS:-$HOME/stock-m25/migrations}
source ~/stock-scripts/m24-lib.sh
M=m26-m
U=m26-u
F=m26-f
# The image's default soft limit is 1,024 open files (hard 524,288); a thousand clients of each
# feed need more on both ends.
FILES=--ulimit=nofile=65536:65536
# measured_inproc only: optional CPU pinning, for example XPIN=--cpuset-cpus=0,2 (the exchange
# and replica on one physical core: on the office machine threads 0 and 2 are core 0, 1 and 3
# core 1, from `lscpu -e`) and FPIN=--cpuset-cpus=1,3 (the feeds and their clients on the other).
XPIN=${XPIN:-}
FPIN=${FPIN:-}

clean_feeds() {
  docker rm -f $M $U $F > /dev/null 2>&1
}

# m24-lib.sh's start_exchange and http_bench, with the raised open-file limit: under load the
# generator holds more than 1,024 connections, and the exchange accepts them. With the default,
# the exchange's accepts fail (EMFILE), the server pauses, and orders stall (seen in a pilot).
start_exchange() { # no|yes
  clean
  fresh_db
  docker volume create $X > /dev/null
  SECRET="m26-$(date +%s%N)"
  local replication=()
  [ "$1" = yes ] && replication=(-e REPLICATION_LISTEN_ADDR=0.0.0.0:4005)
  docker run -d --name $X $FILES --network $NET -v $X:/data -v ~/stock-scripts:/scripts \
    -e DATABASE_URL=$DBURL -e JWT_SECRET=$SECRET -e EVENT_LOG_PATH=/data/events.log \
    "${replication[@]}" stock-rust $BIN > /dev/null
  until docker logs $X 2>&1 | grep -q "Server is listening"; do sleep 0.2; done
  if [ "$1" = yes ]; then
    docker volume create $R > /dev/null
    docker run -d --name $R $FILES --network $NET -v $R:/data -v ~/stock-scripts:/scripts \
      stock-rust $BIN --replica $X:4005 /data/events.log /data/events.log.mmap > /dev/null
    until op GET /replication | grep -q '"mode":"synchronous"'; do sleep 0.2; done
  fi
  echo "# exchange: $(docker logs $X 2>&1 | grep 'Tokio worker threads'), replicated: $1"
}

http_bench() {
  docker run --rm --name $G $FILES --network container:$X -v ~/stock-scripts:/scripts \
    -e JWT_SECRET=$SECRET stock-rust ${GEN:-$BIN} --http-bench "$@" 2>&1
}

# start_feeds: the MDP and the private feed, after the exchange; each catches up, then listens.
start_feeds() {
  clean_feeds
  docker run -d --name $M $FILES --network container:$X -v $X:/data:ro -v ~/stock-scripts:/scripts \
    stock-rust $BIN --market-data /data/events.log /data/events.log.mmap /tmp/market-data.json \
    127.0.0.1:4001 > /dev/null
  docker run -d --name $U $FILES --network container:$X -v $X:/data:ro -v ~/stock-scripts:/scripts \
    -e JWT_SECRET=$SECRET stock-rust $BIN --user-feed /data/events.log /data/events.log.mmap \
    /tmp/user-feed.json 127.0.0.1:4006 > /dev/null
  until docker logs $M 2>&1 | grep -q "Market data is listening"; do sleep 0.2; done
  until docker logs $U 2>&1 | grep -q "User feed is listening"; do sleep 0.2; done
}

# feed_bench ARGS...: the feed clients, in the background; their report goes to $FEED_OUT.
feed_bench() {
  docker run --rm --name $F $FILES --network container:$X -v $X:/data:ro -v ~/stock-scripts:/scripts \
    -e JWT_SECRET=$SECRET stock-rust $BIN --feed-bench --journal /data/events.log \
    --stream /data/events.log.mmap "$@" > "$FEED_OUT" 2>&1
}

# Every second, until killed: the time, each container's CPU time (cgroup v2 cpu.stat), and the
# host's busy and idle jiffies (milestone 25 part 4's sampler).
cpu_sampler() { # FILE
  local f id k v u n s idle io irq sirq
  while :; do
    printf 'T %s\n' "$(date +%s%N)"
    for f in /sys/fs/cgroup/system.slice/docker-*.scope/cpu.stat; do
      id=${f#*/docker-}
      id=${id%%.scope/*}
      while read -r k v; do
        [ "$k" = usage_usec ] && { printf 'C %s %s\n' "$id" "$v"; break; }
      done 2> /dev/null < "$f"
    done
    read -r _ u n s idle io irq sirq _ < /proc/stat
    printf 'H %s %s\n' $((u + n + s + irq + sirq)) $((idle + io))
    sleep 1
  done >> "$1"
}

id_of() { docker inspect -f '{{.Id}}' "$1" 2> /dev/null || true; }

# measured_inproc REPLICATED FEEDS ORDERS [FEED-BENCH ARGS...]: one maximum-rate run of the
# in-process `--bench` (milestone 25's way to find the maximum: `--http-bench --rate 0` sends every
# order at once, and the generator, not the exchange, is then the limit). The bench runs in $X and
# writes its journal to /data/b; with FEEDS yes, the MDP, the private feed and `--feed-bench`
# follow that journal. The bench waits for --start-at after its setup; the CPU window runs from its
# start line to its last reply line (or to the feeds' last message, if later).
measured_inproc() {
  local replicated=$1 feeds=$2 orders=$3
  shift 3
  local samples marks before sampler start_at feed_pid replication=()
  clean_feeds
  clean
  docker volume create $X > /dev/null
  SECRET="m26-$(date +%s%N)"
  if [ "$replicated" = yes ]; then
    replication=(-e REPLICATION_LISTEN_ADDR=0.0.0.0:4005)
    docker volume create $R > /dev/null
    # It retries every second until the benchmark listens.
    docker run -d --name $R $FILES $XPIN --network $NET -v $R:/data -v ~/stock-scripts:/scripts \
      stock-rust $BIN --replica $X:4005 /data/replica.log /data/replica.mmap > /dev/null
  fi
  start_at=$(($(date +%s%3N) + 25000))
  # The container outlives the bench, because the feeds share its network namespace: they must
  # not lose it while they still deliver what the bench committed. It is removed by `clean`.
  docker run -d --name $X $FILES $XPIN --network $NET -v $X:/data -v ~/stock-scripts:/scripts \
    "${replication[@]}" stock-rust sh -c "$BIN --bench /data/b --orders $orders --rate 0 \
    --start-at $start_at; echo bench-exited \$?; sleep 3600" > /dev/null
  echo "# exchange: in-process --bench, replicated: $replicated"
  if [ "$feeds" = yes ]; then
    until docker exec $X test -e /data/b/bench-events.log.mmap 2> /dev/null; do sleep 0.2; done
    docker run -d --name $M $FILES $FPIN --network container:$X -v $X:/data:ro -v ~/stock-scripts:/scripts \
      stock-rust $BIN --market-data /data/b/bench-events.log /data/b/bench-events.log.mmap \
      /tmp/market-data.json 127.0.0.1:4001 > /dev/null
    docker run -d --name $U $FILES $FPIN --network container:$X -v $X:/data:ro -v ~/stock-scripts:/scripts \
      -e JWT_SECRET=$SECRET stock-rust $BIN --user-feed /data/b/bench-events.log \
      /data/b/bench-events.log.mmap /tmp/user-feed.json 127.0.0.1:4006 > /dev/null
    until docker logs $M 2>&1 | grep -q "Market data is listening"; do sleep 0.2; done
    until docker logs $U 2>&1 | grep -q "User feed is listening"; do sleep 0.2; done
  fi
  host_note
  samples=$(mktemp)
  marks=$(mktemp)
  FEED_OUT=$(mktemp)
  before=$(ls /sys/fs/cgroup/system.slice | sed -n 's/^docker-\(.*\)\.scope$/\1/p' | tr '\n' ' ')
  if [ "$feeds" = yes ]; then
    docker run --rm --name $F $FILES $FPIN --network container:$X -v $X:/data:ro \
      -v ~/stock-scripts:/scripts -e JWT_SECRET=$SECRET stock-rust $BIN --feed-bench \
      --journal /data/b/bench-events.log --stream /data/b/bench-events.log.mmap \
      --start-at $start_at --rate 0 --orders $orders --idle 15 "$@" > "$FEED_OUT" 2>&1 &
    feed_pid=$!
  fi
  cpu_sampler "$samples" &
  sampler=$!
  until docker logs $X 2>&1 | grep -q "^bench-exited"; do sleep 1; done
  docker logs $X 2>&1 | grep -vE "^bench: replicating"
  # The window's marks are the bench's own wall-clock start and last reply (microseconds).
  docker logs $X 2>&1 | sed -n 's/^  start      : unix us \([0-9]*\)$/M start \1000/p' >> "$marks"
  docker logs $X 2>&1 | sed -n 's/^  last reply : unix us \([0-9]*\)$/M end \1000/p' >> "$marks"
  if [ "$feeds" = yes ]; then
    wait $feed_pid
    local feed_end
    feed_end=$(sed -n 's/^  last msg   :.* any \([0-9][0-9]*\)$/\1/p' "$FEED_OUT")
    [ -n "$feed_end" ] && [ "$feed_end" -gt 0 ] && echo "M feedend ${feed_end}000" >> "$marks"
    sleep 1
  fi
  kill $sampler
  wait $sampler 2> /dev/null
  [ "$feeds" = yes ] && cat "$FEED_OUT"
  awk -v x="$(id_of $X)" -v r="$(id_of $R)" -v m="$(id_of $M)" -v u="$(id_of $U)" \
    -v before="$before" -v n="$(nproc)" -f ~/stock-scripts/m26p3-cpu.awk "$marks" "$samples"
  if [ "$feeds" = yes ]; then
    docker logs $M 2>&1 | grep -vE "listening|saved through" | sed 's/^/  mdp says   : /'
    docker logs $U 2>&1 | grep -vE "listening" | sed 's/^/  feed says  : /'
  fi
  clean_feeds
  clean
  rm -f "$samples" "$marks" "$FEED_OUT"
}

# measured_run REPLICATED FEEDS ORDERS RATE [FEED-BENCH ARGS...]: one run on a new exchange.
#   FEEDS no: the exchange (and replica) and the HTTP generator only.
#   FEEDS yes: also the MDP, the private feed and `--feed-bench` with the given arguments.
# Prints the generator's report, the feed bench's report, and one CPU line over the measured
# window: from the generator's start line to its throughput line, or to the feeds' last message
# if that came later.
measured_run() {
  local replicated=$1 feeds=$2 orders=$3 rate=$4
  shift 4
  local samples marks before sampler start_at feed_pid
  clean_feeds
  start_exchange "$replicated"
  [ "$feeds" = yes ] && start_feeds
  host_note
  samples=$(mktemp)
  marks=$(mktemp)
  FEED_OUT=$(mktemp)
  before=$(ls /sys/fs/cgroup/system.slice | sed -n 's/^docker-\(.*\)\.scope$/\1/p' | tr '\n' ' ')
  # Long enough for the generator's setup (deposits for 100 users in 100 symbols) and for a
  # thousand clients to connect.
  start_at=$(($(date +%s%3N) + 20000))
  if [ "$feeds" = yes ]; then
    feed_bench --start-at $start_at --rate $rate --orders $orders --idle 15 "$@" &
    feed_pid=$!
  fi
  cpu_sampler "$samples" &
  sampler=$!
  http_bench --orders $orders --rate $rate --deadline 120 --start-at $start_at |
    while IFS= read -r line; do
      case "$line" in
        "  start      :"*) echo "M start $(date +%s%N)" >> "$marks" ;;
        "  throughput"*) echo "M end $(date +%s%N)" >> "$marks" ;;
      esac
      printf '%s\n' "$line"
    done
  if [ "$feeds" = yes ]; then
    wait $feed_pid
    # The CPU window runs to whichever ends later, the generator's last reply or the feeds' last
    # message, so a feed still catching up after the exchange finished is counted.
    local feed_end
    feed_end=$(sed -n 's/^  last msg   :.* any \([0-9][0-9]*\)$/\1/p' "$FEED_OUT")
    [ -n "$feed_end" ] && [ "$feed_end" -gt 0 ] && echo "M feedend ${feed_end}000" >> "$marks"
    sleep 1 # one more sample after it
  fi
  kill $sampler
  wait $sampler 2> /dev/null
  [ "$feeds" = yes ] && cat "$FEED_OUT"
  awk -v x="$(id_of $X)" -v r="$(id_of $R)" -v m="$(id_of $M)" -v u="$(id_of $U)" \
    -v before="$before" -v n="$(nproc)" -f ~/stock-scripts/m26p3-cpu.awk "$marks" "$samples"
  if [ "$feeds" = yes ]; then
    # Anything the feeds printed beyond their startup lines: a halt, an error.
    docker logs $M 2>&1 | grep -vE "listening|saved through" | sed 's/^/  mdp says   : /'
    docker logs $U 2>&1 | grep -vE "listening" | sed 's/^/  feed says  : /'
  fi
  clean_feeds
  rm -f "$samples" "$marks" "$FEED_OUT"
}
