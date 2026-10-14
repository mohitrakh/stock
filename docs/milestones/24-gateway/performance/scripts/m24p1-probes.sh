#!/usr/bin/env bash
# Milestone 24 part 1: the two floors, measured on their own.
#   1. The disk: 600-byte appends, each made durable with sync_all, on a Docker named volume (the
#      same kind of storage every journal in these scripts uses), three runs; then once on tmpfs,
#      where sync_all touches no disk, to show the software's share.
#   2. bcrypt: one hash and one verify at the gateway's cost (12) on one thread; then 2, 4 and 8
#      threads at once, as simultaneous logins would run; then costs 10 and 11 to check that the
#      time doubles per cost step.
# Usage: m24p1-probes.sh   (writes nothing outside a throwaway volume)
set -u
source ~/stock-scripts/m24-lib.sh
V=m24-sync
echo "### m24p1-probes: $BIN, $(date -Is)"
host_note
for run in 1 2 3; do
  docker volume rm $V > /dev/null 2>&1
  docker volume create $V > /dev/null
  echo "### sync, named volume, run $run"
  host_note
  docker run --rm -v $V:/data -v ~/stock-scripts:/scripts stock-rust $BIN --sync-probe /data/p --count 10000 --size 600
done
docker volume rm $V > /dev/null 2>&1
echo "### sync, tmpfs"
host_note
docker run --rm --tmpfs /data:size=256m -v ~/stock-scripts:/scripts stock-rust $BIN --sync-probe /data/p --count 10000 --size 600

echo "### bcrypt, cost 12, 1 thread"
host_note
docker run --rm -v ~/stock-scripts:/scripts stock-rust $BIN --bcrypt-probe --cost 12 --threads 1 --count 20
for threads in 2 4 8; do
  echo "### bcrypt, cost 12, $threads threads at once"
  host_note
  docker run --rm -v ~/stock-scripts:/scripts stock-rust $BIN --bcrypt-probe --cost 12 --threads $threads --count 5
done
for cost in 10 11; do
  echo "### bcrypt, cost $cost, 1 thread"
  host_note
  docker run --rm -v ~/stock-scripts:/scripts stock-rust $BIN --bcrypt-probe --cost $cost --threads 1 --count 10
done
echo "### done"
