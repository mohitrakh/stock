# Milestone 25 part 1: checks that the stage means add up, per configuration, from the output of
# m25p1-summarize.awk. Means add along a path (01-theory.md §2), so:
#   in-process: end-to-end mean = Queue + Worker + (generator lateness + reply wake-up, not timed)
#   HTTP:       end-to-end mean = HttpTotal + (generator lateness, loopback, response write: not timed)
#               HttpTotal       = HttpBeforeQueue + HttpAsk + (response serialization)
#               HttpAsk         = Queue + Worker + (reply wake-up)
#   worker:     Worker ~ WaitInGroup + Prepare + Encode + Commit + WaitForGroup
#                        + Write + Sync + Confirm + Publish + Release   (group stages counted whole)
# Usage: awk -f m25p1-decompose.awk results/m25p1-stages-summary.txt
function show(c) {
  if (c == "") return
  printf "\n## %s\n", c
  printf "  end-to-end mean %.0f\n", e2e
  qw = m["Queue"] + m["Worker"]
  if (http) {
    printf "  HttpTotal %.0f -> outside the server %.0f (%.1f%%)\n", m["HttpTotal"], e2e - m["HttpTotal"], 100 * (e2e - m["HttpTotal"]) / e2e
    printf "  HttpBeforeQueue %.0f, HttpAsk %.0f, after the reply %.0f\n", m["HttpBeforeQueue"], m["HttpAsk"], m["HttpTotal"] - m["HttpBeforeQueue"] - m["HttpAsk"]
    printf "  Queue + Worker %.0f -> reply wake-up %.0f\n", qw, m["HttpAsk"] - qw
  } else {
    printf "  Queue + Worker %.0f -> generator lateness + reply wake-up %.0f (%.1f%%)\n", qw, e2e - qw, 100 * (e2e - qw) / e2e
  }
  parts = m["WaitInGroup"] + m["Prepare"] + m["Encode"] + m["Commit"] + m["WaitForGroup"] + m["Write"] + m["Sync"] + m["Confirm"] + m["Publish"] + m["Release"]
  cpu = m["Prepare"] + m["Encode"] + m["Commit"]
  printf "  Worker %.0f, its stages %.0f, not covered %.0f (%.1f%% of Worker)\n", m["Worker"], parts, m["Worker"] - parts, 100 * (m["Worker"] - parts) / m["Worker"]
  printf "  shares of end-to-end mean: Queue %.1f%%  Sync %.1f%%  Confirm %.1f%%  waits in group %.1f%%  own CPU %.1f%% (%.1f us)\n", 100 * m["Queue"] / e2e, 100 * m["Sync"] / e2e, 100 * m["Confirm"] / e2e, 100 * (m["WaitInGroup"] + m["WaitForGroup"]) / e2e, 100 * cpu / e2e, cpu
}
/^## / { show(config); config = substr($0, 4); delete m; http = (config ~ /^http/); next }
/^end-to-end:/ { for (i = 1; i <= NF; i++) if ($i == "mean") e2e = $(i + 1); next }
/^  [A-Za-z]+ +mean/ { m[$1] = $3 }
END { show(config) }
