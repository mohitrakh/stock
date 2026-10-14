#!/usr/bin/env bash
# Milestone 24 part 2: each removal of the fix must fail the test that covers it. For each
# mutation: copy the tree, apply the change, run only the covering test, print its result line.
# A mutation that still passes means the test does not guard what it claims.
# Usage: m24p2-mutate.sh   (on the office machine's host; the tree is ~/stock-m24)
# Mutations build in their own target volume (stock-cargo-target-mut), added after milestone 25 found
# that a shared target dir lets the real tree (older file times, same /src path) reuse a mutated
# build. The 2026-10-12 results were not affected: each mutated copy was newer, so it was rebuilt.
set -u
run() { # LABEL TEST PYTHON-REPLACEMENT-SCRIPT
  local label=$1 test=$2
  rm -rf ~/stock-m24-mut && cp -r ~/stock-m24 ~/stock-m24-mut
  python3 -c "$3" || { echo "  $label: mutation did not apply"; return; }
  local result
  result=$(docker run --rm -v ~/stock-m24-mut:/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    timeout 300 cargo test --locked --bin stock "$test" 2>&1 | grep -E "^test result|^error" | head -1)
  echo "  $label -> ${result:-no result} ($test)"
}
F=/home/ubuntu/stock-m24-mut/src/controllers/user_controller.rs
echo "### m24p2-mutate: $(date -Is)"
run "bcrypt inline on the worker (no spawn_blocking)" bcrypt_work_runs_off_the_worker_threads "
import re
p='$F'; s=open(p).read()
old='''    tokio::task::spawn_blocking(move || {
        let result = work();
        drop(permit);
        result
    })
    .await
    .map_err(|_| AppError::Unavailable(\"password check failed to run\".to_string()))'''
assert old in s
open(p,'w').write(s.replace(old, '''    let result = work();
    drop(permit);
    Ok(result)''', 1))
"
run "no cap (permit not taken)" bcrypt_work_runs_off_the_worker_threads "
p='$F'; s=open(p).read()
old='''    let permit = permits
        .acquire_owned()
        .await
        .map_err(|_| AppError::Unavailable(\"password checks are unavailable\".to_string()))?;'''
assert old in s
open(p,'w').write(s.replace(old, '''    let permit = ();
    let _ = permits;''', 1))
"
run "dummy hash at a lower cost" the_dummy_hash_is_a_real_hash "
p='$F'; s=open(p).read()
old='hash(\"no account has this password\", DEFAULT_COST)'
assert old in s
open(p,'w').write(s.replace(old, 'hash(\"no account has this password\", 4)', 1))
"
rm -rf ~/stock-m24-mut
echo "### done"
