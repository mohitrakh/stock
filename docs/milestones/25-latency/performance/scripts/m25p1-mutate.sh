#!/usr/bin/env bash
# Milestone 25 part 1: each removal of a stage's recording must fail the test that covers it, run
# ALONE (the global stage report is shared, so in the full suite other tests could fill a stage).
# Mutations build in their own cargo target volume: the mutated copy and the real tree are both
# mounted at /src, and in a shared target dir the real tree (older file times) can silently reuse
# a mutated build. A first, hand-run version of this check fell into exactly that.
# Usage: m25p1-mutate.sh   (on the office machine's host; the tree is ~/stock-m25)
set -u
run() { # LABEL TEST PYTHON-REPLACEMENT-SCRIPT
  local label=$1 test=$2
  # -L: copy the tree a symlink points to, never the link itself (a copied link would make every
  # mutation edit the real tree).
  rm -rf ~/stock-m25-mut && cp -rL ~/stock-m25 ~/stock-m25-mut
  python3 -c "$3" || { echo "  $label: mutation did not apply"; return; }
  local result
  result=$(docker run --rm -v ~/stock-m25-mut:/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    timeout 600 cargo test --locked --features stage-timing --bin stock "$test" 2>&1 | grep -E "^test result|^error" | head -1)
  echo "  $label -> ${result:-no result} ($test)"
}
R=$HOME/stock-m25-mut/src/exchange/runtime.rs
echo "### m25p1-mutate: $(date -Is)"
echo "## control: the real tree, the test alone (own target volume, fresh build)"
docker run --rm -v ~/stock-m25:/src -v stock-cargo-registry:/usr/local/cargo/registry \
  -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
  bash -c "touch src/exchange/runtime.rs; timeout 600 cargo test --locked --features stage-timing --bin stock a_group_records_every_worker_stage 2>&1 | grep -E '^test result'"
run "Publish never recorded" a_group_records_every_worker_stage "
p='$R'; s=open(p).read()
old='timing::record(|stages| stages.record(Stage::Publish, publishing.elapsed()));'
assert old in s
open(p,'w').write(s.replace(old, 'let _ = publishing;', 1))
"
run "Queue never recorded" a_group_records_every_worker_stage "
p='$R'; s=open(p).read()
old='stages.record(Stage::Queue, dequeued.saturating_duration_since(*queued_at));'
assert old in s
open(p,'w').write(s.replace(old, 'let _ = queued_at;', 1))
"
run "the worker never merges into the report" a_group_records_every_worker_stage "
p='$R'; s=open(p).read()
old='stages.merge_into_global();'
assert old in s
open(p,'w').write(s.replace(old, '', 1))
"
rm -rf ~/stock-m25-mut
echo "### done"
