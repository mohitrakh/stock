# Milestone 25 part 4: each container's CPU over the measured window of one HTTP run, from
# m25p4-final.sh's cpu_sampler. Prints one line, in cores (1.00 = one hardware thread busy for the
# whole window): the exchange, the replica, the generator (the container that was not running
# before the run), every other container together, and the whole host.
# Usage: awk -v x=EXCHANGE_ID -v r=REPLICA_ID -v before="IDS RUNNING BEFORE" -v n=NPROC \
#          -f m25p4-cpu.awk MARKS SAMPLES
FNR == NR {
  if ($2 == "start") start = $3
  if ($2 == "end") end = $3
  next
}
$1 == "T" {
  inwin = (start != "" && end != "" && $2 >= start && $2 <= end)
  if (inwin) { if (t0 == "") t0 = $2; t1 = $2 }
  next
}
!inwin { next }
$1 == "C" { if (!($2 in first)) first[$2] = $3; last[$2] = $3; next }
$1 == "H" {
  if (b0 == "") { b0 = $2; i0 = $3 }
  b1 = $2; i1 = $3
}
END {
  secs = (t1 - t0) / 1e9
  if (t0 == "" || secs < 1) { print "  cpu        : not measured (window shorter than one sample)"; exit }
  split(before, ids, " ")
  for (k in ids) existed[ids[k]] = 1
  for (id in last) {
    cores = (last[id] - first[id]) / 1e6 / secs
    if (id == x) exchange += cores
    else if (r != "" && id == r) replica += cores
    else if (!(id in existed)) generator += cores
    else { others += cores; count++ }
  }
  busy = (b1 - b0) / ((b1 - b0) + (i1 - i0)) * n
  printf "  cpu        : window %.1f s; exchange %.2f, replica %.2f, generator %.2f, other containers %.2f (%d), host busy %.2f of %d hardware threads\n", secs, exchange, replica, generator, others, count, busy, n
}
