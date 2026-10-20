#!/usr/bin/env bash
# Milestone 26 part 3: the measurement. One binary (stock-m26p3) runs every process: the exchange,
# the replica, the MDP, the private feed, `--http-bench` and `--feed-bench`. Each run starts a new
# exchange (new journal, new database), so runs do not share state. Every configuration runs RUNS
# times, interleaved with the configurations it is compared with, the order balanced (the
# completion criteria: "on the same day, interleaved, six runs each with the order balanced").
#   A. push latency against the number of clients: {alone, replicated} x {1,000 and 5,000 orders/s
#      for 20 s} x {1, 100, 1,000} clients of each feed. The three client counts rotate, so over 6
#      runs each comes first, second and third twice;
#   B. whether the feeds keep up at the maximum rate: {alone, replicated} x 200,000 orders from the
#      in-process `--bench` at its maximum rate (as milestone 25 measured the maximum; over HTTP,
#      `--rate 0` sends every order at once and the generator itself is the limit), 100 clients of
#      each feed;
#   C. the exchange with the feeds running against without: {alone, replicated} x {HTTP at 5,000
#      orders/s for 20 s (mean and p99), in-process at the maximum rate, 200,000 orders
#      (throughput)}, interleaved with-without, without-with, ... (ABBA). "With" is the MDP, the
#      private feed and 100 clients of each;
#   D. slow consumers under load: alone, 5,000 orders/s for 20 s, 100 clients of each feed, with 0
#      and with 10 stalled clients (each subscribed to every symbol from the start, never
#      reading), interleaved ABBA;
#   E. C's maximum-rate comparison with the exchange pinned to one physical core (threads 0 and 2)
#      and the feeds and their clients to the other (1 and 3), alone, interleaved ABBA: whether the
#      slowdown with feeds is the CPU they take from the exchange on this two-core machine.
# Usage: m26p3-measure.sh [SECTIONS] [RUNS]   (defaults: ABCDE 6)
set -u
SECTIONS=${1:-ABCDE}
RUNS=${2:-6}
BIN=stock-m26p3
source ~/stock-scripts/m26p3-lib.sh
BIN=/scripts/stock-m26p3

clients() { echo --book-clients $1 --user-clients $1; }
balanced() { # RUN A B: the two configurations in that run's order, ABBA...
  case $(($1 % 4)) in 1 | 0) echo "$2 $3" ;; *) echo "$3 $2" ;; esac
}
rotated() { # RUN X Y Z: run 1 X Y Z, run 2 Y Z X, run 3 Z X Y, run 4 X Y Z, ...
  local items=("$2" "$3" "$4") k=$((($1 - 1) % 3))
  echo "${items[k]} ${items[(k + 1) % 3]} ${items[(k + 2) % 3]}"
}

echo "### m26p3-measure: sections $SECTIONS, $RUNS runs, binary $BIN ($(sha256sum ~/stock-scripts/stock-m26p3 | cut -c1-16)), $(date -Is)"
if [[ $SECTIONS == *A* ]]; then
  for replicated in no yes; do
    for rate in 1000 5000; do
      for run in $(seq 1 "$RUNS"); do
        for n in $(rotated $run 1 100 1000); do
          echo "### A: replicated $replicated, rate $rate, $n clients of each feed, run $run"
          measured_run $replicated yes $((rate * 20)) $rate $(clients $n)
        done
      done
    done
  done
fi
if [[ $SECTIONS == *B* ]]; then
  for run in $(seq 1 "$RUNS"); do
    for replicated in $(balanced $run no yes); do
      echo "### B: replicated $replicated, maximum rate (in-process), 200000 orders, 100 clients of each feed, run $run"
      measured_inproc $replicated yes 200000 $(clients 100)
    done
  done
fi
if [[ $SECTIONS == *C* ]]; then
  for replicated in no yes; do
    for run in $(seq 1 "$RUNS"); do
      for feeds in $(balanced $run yes no); do
        echo "### C: replicated $replicated, http, orders 100000, rate 5000, feeds $feeds, run $run"
        if [ $feeds = yes ]; then
          measured_run $replicated yes 100000 5000 $(clients 100)
        else
          measured_run $replicated no 100000 5000
        fi
      done
    done
    for run in $(seq 1 "$RUNS"); do
      for feeds in $(balanced $run yes no); do
        echo "### C: replicated $replicated, in-process, orders 200000, maximum rate, feeds $feeds, run $run"
        if [ $feeds = yes ]; then
          measured_inproc $replicated yes 200000 $(clients 100)
        else
          measured_inproc $replicated no 200000
        fi
      done
    done
  done
fi
if [[ $SECTIONS == *E* ]]; then
  for run in $(seq 1 "$RUNS"); do
    for feeds in $(balanced $run yes no); do
      echo "### E: replicated no, in-process, orders 200000, maximum rate, pinned, feeds $feeds, run $run"
      if [ $feeds = yes ]; then
        XPIN=--cpuset-cpus=0,2 FPIN=--cpuset-cpus=1,3 measured_inproc no yes 200000 $(clients 100)
      else
        XPIN=--cpuset-cpus=0,2 FPIN=--cpuset-cpus=1,3 measured_inproc no no 200000
      fi
    done
  done
fi
if [[ $SECTIONS == *D* ]]; then
  for run in $(seq 1 "$RUNS"); do
    for stalled in $(balanced $run 0 10); do
      echo "### D: alone, rate 5000, 100 clients of each feed, $stalled stalled, run $run"
      measured_run no yes 100000 5000 $(clients 100) --stalled $stalled
    done
  done
fi
clean
echo "### done $(date -Is)"
