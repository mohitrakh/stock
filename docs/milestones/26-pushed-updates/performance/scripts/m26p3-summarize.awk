# Milestone 26 part 3: summarizes m26p3-measure.sh's output.
# 1. One row per run: the generator's throughput and latency; each feed's latency, how many of the
#    orders its messages covered, and how far behind the generator's last reply its last message
#    came (lag, ms); clients closed early; any warning; the CPU line.
# 2. For every configuration (a row's label without its run number): the mean and [min, max] over
#    its runs of the figures that the part's comparisons use.
# Usage: awk -f m26p3-summarize.awk m26p3-measure.txt
function field(name,    i) { for (i = 1; i < NF; i++) if ($i == name) return $(i + 1); return "-" }
function covering(    i) { for (i = 1; i < NF; i++) if ($i == "covering") return $(i + 1); return "-" }
function add(key, name, value) {
  if (value == "-" || value == "") return
  value += 0
  n_[key, name]++
  sum_[key, name] += value
  if (!((key, name) in min_) || value < min_[key, name]) min_[key, name] = value
  if (!((key, name) in max_) || value > max_[key, name]) max_[key, name] = value
}
function stat(key, name) {
  if (!((key, name) in n_)) return "-"
  return sprintf("%.0f [%.0f, %.0f]", sum_[key, name] / n_[key, name], min_[key, name], max_[key, name])
}
function lag(last) { return (last > 0 && reply > 0) ? sprintf("%.0f", (last - reply) / 1000) : "-" }
function flush(    key, booklag, userlag) {
  if (label == "") return
  booklag = lag(booklast); userlag = lag(userlast)
  rows[++n] = sprintf("%-60s | %6s %7s %7s %7s | %7s %7s %6s %6s | %7s %7s %6s %6s | %-12s | %s%s", label, tput, hp50, hp99, hmean, bp50, bp99, bcov, booklag, up50, up99, ucov, userlag, closed, cpu, warn)
  key = label; sub(/, run [0-9]+$/, "", key)
  if (!(key in seen)) { order[++keys] = key; seen[key] = 1 }
  add(key, "tput", tput); add(key, "hp50", hp50); add(key, "hp99", hp99); add(key, "hmean", hmean)
  add(key, "bp50", bp50); add(key, "bp99", bp99); add(key, "bmean", bmean)
  add(key, "up50", up50); add(key, "up99", up99); add(key, "umean", umean)
  add(key, "booklag", booklag); add(key, "userlag", userlag)
  label = ""
}
/^### [A-E]:/ {
  flush()
  label = $0; sub(/^### /, "", label)
  tput = hp50 = hp99 = hmean = bp50 = bp99 = bmean = up50 = up99 = umean = bcov = ucov = "-"
  closed = "-"; cpu = "-"; warn = ""; reply = booklast = userlast = 0
  next
}
/^  throughput :/ { tput = ($3 == "not") ? "-" : $3 }
/^  latency us :/ { hp50 = field("p50"); hp99 = field("p99"); hmean = field("mean") }
# "  last reply : unix us N"
/^  last reply :/ { reply = $6 + 0 }
/^  book       :/ { bp50 = field("p50"); bp99 = field("p99"); bmean = field("mean"); sub(/;$/, "", bmean); bcov = covering() }
/^  user       :/ { up50 = field("p50"); up99 = field("p99"); umean = field("mean"); sub(/;$/, "", umean); ucov = covering() }
# "  last msg   : book unix us A, user unix us B, any C" ("A," + 0 is A)
/^  last msg   :/ { booklast = $7 + 0; userlast = $11 + 0 }
# Clients the server closed early, as "b<book> u<user>", plus " s<ended>/<stalled>" if any stalled.
/^  closed     :/ {
  match($0, /book [0-9]+ of/); b = substr($0, RSTART + 5, RLENGTH - 8)
  match($0, /user [0-9]+ of/); u = substr($0, RSTART + 5, RLENGTH - 8)
  closed = "b" b " u" u
}
/^  stalled    :/ { closed = closed " s" $3 "/" $5 }
/^  (warning|failed)/ { warn = warn " !" $1 }
/^  mdp says|^  feed says/ { warn = warn " !log" }
/^  cpu        :/ { cpu = $0; sub(/^  cpu        : window [0-9.]+ s; /, "", cpu) }
END {
  flush()
  printf "%-60s | %6s %7s %7s %7s | %7s %7s %6s %6s | %7s %7s %6s %6s | %-12s | %s\n", "run", "ord/s", "http50", "http99", "httpavg", "book50", "book99", "bcover", "blagms", "user50", "user99", "ucover", "ulagms", "closed", "cpu (hardware threads)"
  for (i = 1; i <= n; i++) print rows[i]
  print ""
  print "Per configuration over its runs: mean [min, max]. Latency in us, lag in ms."
  for (k = 1; k <= keys; k++) {
    key = order[k]
    runs = ((key, "hp50") in n_) ? n_[key, "hp50"] : n_[key, "booklag"]
    printf "%s (%d runs)\n", key, runs
    printf "  orders/s %s; http p50 %s, p99 %s, mean %s\n", stat(key, "tput"), stat(key, "hp50"), stat(key, "hp99"), stat(key, "hmean")
    if ((key, "bp50") in n_ || (key, "booklag") in n_)
      printf "  book p50 %s, p99 %s, mean %s, lag %s\n", stat(key, "bp50"), stat(key, "bp99"), stat(key, "bmean"), stat(key, "booklag")
    if ((key, "up50") in n_ || (key, "userlag") in n_)
      printf "  user p50 %s, p99 %s, mean %s, lag %s\n", stat(key, "up50"), stat(key, "up99"), stat(key, "umean"), stat(key, "userlag")
  }
}
