#!/usr/bin/env bash
# Shared helpers for milestone 24's measurement scripts. Runs on the office machine's host, from
# ~/stock-scripts (the scripts are copied there; the repository keeps the originals under
# docs/milestones/24-gateway/performance/scripts/).
#
# Layout of one measured exchange, all on the Docker network stock-test-net:
#   $X  the exchange (primary), journal on its own named volume
#   $R  the replica on "the second machine", its own volume (replicated runs only)
#   $G  the HTTP load generator, sharing $X's network namespace (the customer and operator ports
#       bind loopback only)
#   stock-test-pg  PostgreSQL 16 for the users table, one fresh database per exchange start
# Every container shares the host's 2 cores (4 threads) and its one SATA SSD.

BIN=/scripts/${BIN:-stock-m24p1}
NET=stock-test-net
X=m24-x
R=m24-r
G=m24-g
DB=exchange_m24
DBURL=postgres://postgres:bench@stock-test-pg:5432/$DB
MIGRATIONS=${MIGRATIONS:-$HOME/stock-m24/migrations}
SECRET=""

# What else the machine was doing: printed before every measured run.
host_note() {
  echo "# host $(date -Is): loadavg $(cut -d' ' -f1-3 /proc/loadavg), containers running $(docker ps -q | wc -l)"
}

clean() {
  docker rm -f $X $R $G > /dev/null 2>&1
  docker volume rm $X $R > /dev/null 2>&1
}

fresh_db() {
  docker start stock-test-pg > /dev/null
  until docker exec stock-test-pg pg_isready -U postgres -q; do sleep 0.2; done
  docker exec stock-test-pg psql -U postgres -q -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
  docker exec stock-test-pg psql -U postgres -q -c "CREATE DATABASE $DB"
  for migration in $(ls "$MIGRATIONS"/*.sql | sort); do
    docker exec -i stock-test-pg psql -U postgres -q -d $DB < "$migration"
  done
}

op() { # METHOD PATH [BODY]: the operator port, from inside the exchange's network namespace
  docker exec $X curl -s -m 5 -X "$1" "127.0.0.1:4004$2" -H 'Content-Type: application/json' ${3:+-d "$3"}
}

# start_exchange no|yes: a new journal, a new database, and (yes) a synchronous replica.
start_exchange() {
  clean
  fresh_db
  docker volume create $X > /dev/null
  SECRET="m24-$(date +%s%N)"
  local replication=()
  [ "$1" = yes ] && replication=(-e REPLICATION_LISTEN_ADDR=0.0.0.0:4005)
  docker run -d --name $X --network $NET -v $X:/data -v ~/stock-scripts:/scripts \
    -e DATABASE_URL=$DBURL -e JWT_SECRET=$SECRET -e EVENT_LOG_PATH=/data/events.log \
    "${replication[@]}" stock-rust $BIN > /dev/null
  until docker logs $X 2>&1 | grep -q "Server is listening"; do sleep 0.2; done
  if [ "$1" = yes ]; then
    docker volume create $R > /dev/null
    docker run -d --name $R --network $NET -v $R:/data -v ~/stock-scripts:/scripts \
      stock-rust $BIN --replica $X:4005 /data/events.log /data/events.log.mmap > /dev/null
    until op GET /replication | grep -q '"mode":"synchronous"'; do sleep 0.2; done
  fi
  echo "# exchange: $(docker logs $X 2>&1 | grep 'Tokio worker threads'), replicated: $1"
}

# http_bench ARGS...: the generator in the exchange's network namespace. GEN, when set, is the
# binary the generator runs from, so two exchange builds can be driven by the same generator.
http_bench() {
  docker run --rm --name $G --network container:$X -v ~/stock-scripts:/scripts \
    -e JWT_SECRET=$SECRET stock-rust ${GEN:-$BIN} --http-bench "$@" 2>&1
}

# inproc_bench no|yes ARGS...: `--bench` in its own container, as milestone 23 part 6 ran it.
inproc_bench() {
  local replicated=$1
  shift
  clean
  docker volume create $X > /dev/null
  local replication=()
  if [ "$replicated" = yes ]; then
    replication=(-e REPLICATION_LISTEN_ADDR=0.0.0.0:4005)
    docker volume create $R > /dev/null
    # It retries every second until the benchmark listens.
    docker run -d --name $R --network $NET -v $R:/data -v ~/stock-scripts:/scripts stock-rust \
      $BIN --replica $X:4005 /data/replica.log /data/replica.mmap > /dev/null
  fi
  docker run --rm --name $X --network $NET -v $X:/data -v ~/stock-scripts:/scripts \
    "${replication[@]}" stock-rust $BIN --bench /data/b "$@" 2>&1 | grep -vE "^bench: replicating"
  docker rm -f $R > /dev/null 2>&1
}
