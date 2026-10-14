# Milestone 25 part 1: averages the runs of each configuration in m25p1-stages.txt.
# For every configuration (the "### ..." header without its run number) it prints the mean over
# runs of: the end-to-end p50, p99 and mean, and each stage's mean, p50 and p99 (us).
# Usage: awk -f m25p1-summarize.awk results/m25p1-stages.txt
/^### (in-process|http)/ {
  config = $0
  sub(/^### /, "", config)
  sub(/, run [0-9]+/, "", config)
  if (!(config in seen)) { seen[config] = 1; order[++n] = config }
  runs[config]++
  next
}
/latency us :/ {
  for (i = 1; i <= NF; i++) {
    if ($i == "p50") e2e[config, "p50"] += $(i + 1)
    if ($i == "p99") e2e[config, "p99"] += $(i + 1)
    if ($i == "mean") { e2e[config, "mean"] += $(i + 1); hasmean[config] = 1 }
  }
  next
}
/^(HttpBeforeQueue|HttpAsk|HttpTotal|Queue|WaitInGroup|Prepare|Encode|Commit|WaitForGroup|Write|Sync|Confirm|Publish|Release|Worker|GroupSize) / {
  stage = $1
  if (!((config, stage) in stageseen)) { stageseen[config, stage] = 1; stages[config] = stages[config] " " stage }
  mean[config, stage] += $3; p50[config, stage] += $4; p99[config, stage] += $6
}
END {
  for (k = 1; k <= n; k++) {
    c = order[k]; r = runs[c]
    printf "\n## %s (%d runs)\n", c, r
    printf "end-to-end: p50 %.0f  p99 %.0f", e2e[c, "p50"] / r, e2e[c, "p99"] / r
    if (hasmean[c]) printf "  mean %.0f", e2e[c, "mean"] / r
    printf "\n"
    split(substr(stages[c], 2), list, " ")
    for (s = 1; s in list; s++) {
      st = list[s]
      printf "  %-15s mean %9.1f  p50 %8.0f  p99 %8.0f\n", st, mean[c, st] / r, p50[c, st] / r, p99[c, st] / r
    }
  }
}
