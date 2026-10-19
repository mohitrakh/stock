# Milestone 25 part 2: averages the runs of each build and configuration in m25p2-compare.txt or
# m25p2-stages.txt. A configuration is the "### <binary>, ..." header without its run number.
# For each it prints, as the mean over runs and [lowest, highest run]: throughput (orders/s), the
# end-to-end p50, p99 and mean (us), orders per sync, and unanswered orders (HTTP). For stage
# reports it then prints each stage's sample count, mean, p50 and p99, averaged over the runs that
# reported the stage (a stage with no samples is left out of a report), with how many those were.
# Usage: awk -f m25p2-summarize.awk results/m25p2-compare.txt
function acc(key, v) {
  sum[key] += v
  seen_runs[key]++
  if (!(key in lo) || v < lo[key]) lo[key] = v
  if (!(key in hi) || v > hi[key]) hi[key] = v
}
function show(c, what, digits,   key) {
  key = c SUBSEP what
  if (!(key in sum)) return
  # Divided by the runs that reported this value: a run can lack one (a "not measured" cpu line).
  printf "  %-11s %12." digits "f   [%." digits "f, %." digits "f]\n", what, sum[key] / seen_runs[key], lo[key], hi[key]
}
/^### stock-/ {
  config = $0
  sub(/^### /, "", config)
  sub(/, run [0-9]+/, "", config)
  if (!(config in seen)) { seen[config] = 1; order[++n] = config }
  runs[config]++
  next
}
/throughput :/ { acc(config SUBSEP "throughput", $3); next }
/unanswered :/ { acc(config SUBSEP "unanswered", $3); next }
/latency us :/ {
  for (i = 1; i <= NF; i++) {
    if ($i == "p50") acc(config SUBSEP "p50", $(i + 1))
    if ($i == "p99") acc(config SUBSEP "p99", $(i + 1))
    if ($i == "mean") acc(config SUBSEP "mean", $(i + 1))
  }
  next
}
# Milestone 25 part 4's per-container CPU line, in cores ("+ 0" drops the trailing comma).
/^  cpu        : window/ {
  for (i = 1; i <= NF; i++) {
    if ($i == "exchange") acc(config SUBSEP "cpu exchange", $(i + 1) + 0)
    if ($i == "replica") acc(config SUBSEP "cpu replica", $(i + 1) + 0)
    if ($i == "generator") acc(config SUBSEP "cpu generator", $(i + 1) + 0)
    if ($i == "containers") acc(config SUBSEP "cpu others", $(i + 1) + 0)
    if ($i == "busy") acc(config SUBSEP "cpu host", $(i + 1) + 0)
  }
  next
}
# "+ 0": after gsub, v is a string, and min and max would compare as text.
/syncs      :/ { v = $4; gsub(/\(/, "", v); acc(config SUBSEP "per sync", v + 0); next }
/^(HttpBeforeQueue|HttpAsk|HttpTotal|Queue|WaitInGroup|Prepare|Encode|Commit|WaitForGroup|Write|Sync|Confirm|WaitForSync|PipelineFull|Publish|Release|Worker|GroupSize) / {
  stage = $1
  if (!((config, stage) in stageseen)) { stageseen[config, stage] = 1; stages[config] = stages[config] " " stage }
  # A stage with no samples is left out of a report, so divide by the runs that reported it.
  stageruns[config, stage]++
  count[config, stage] += $2; mean[config, stage] += $3; p50[config, stage] += $4; p99[config, stage] += $6
}
END {
  for (k = 1; k <= n; k++) {
    c = order[k]; r = runs[c]
    printf "\n## %s (%d runs)\n", c, r
    show(c, "throughput", 0)
    show(c, "p50", 0)
    show(c, "p99", 0)
    show(c, "mean", 0)
    show(c, "per sync", 1)
    show(c, "unanswered", 0)
    show(c, "cpu exchange", 2)
    show(c, "cpu replica", 2)
    show(c, "cpu generator", 2)
    show(c, "cpu others", 2)
    show(c, "cpu host", 2)
    if (stages[c] == "") continue
    split(substr(stages[c], 2), list, " ")
    for (s = 1; s in list; s++) {
      st = list[s]; sr = stageruns[c, st]
      printf "  %-15s n %9.0f  mean %9.1f  p50 %8.0f  p99 %8.0f", st, count[c, st] / sr, mean[c, st] / sr, p50[c, st] / sr, p99[c, st] / sr
      if (sr < r) printf "  (in %d of %d runs)", sr, r
      printf "\n"
    }
  }
}
