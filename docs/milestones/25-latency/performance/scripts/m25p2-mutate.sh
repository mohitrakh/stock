#!/usr/bin/env bash
# Milestone 25 part 2: each removal of a pipeline guarantee must fail the test that covers it. For
# each mutation: copy the tree, apply the change, run only the covering test, print its result line.
# A mutation that still passes means the test does not guard what it claims. A control run of the
# real tree comes first.
# Mutations build in their own cargo target volume (stock-cargo-target-mut): see m25p1-mutate.sh.
# Usage: [TREE=DIR] m25p2-mutate.sh   (on the host; the tree defaults to ~/stock-m25, the checkout
# of SETUP.md. Part 2's first run used TREE=~/stock-m25p2, an uncommitted copy.)
set -u
TREE=${TREE:-$HOME/stock-m25}
MUT=$HOME/stock-m25p2-mut
run() { # LABEL TEST PYTHON-REPLACEMENT-SCRIPT
  local label=$1 test=$2
  # -L: copy the tree a symlink points to, never the link itself.
  rm -rf "$MUT" && cp -rL "$TREE" "$MUT"
  python3 -c "$3" || { echo "  $label: mutation did not apply"; return; }
  local result
  result=$(docker run --rm -v "$MUT":/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    bash -c "touch src/exchange/runtime.rs; timeout 600 cargo test --locked --bin stock $test 2>&1" \
    | grep -E "^test result|^error" | head -1)
  echo "  $label -> ${result:-no result} ($test)"
}
R=$MUT/src/exchange/runtime.rs
echo "### m25p2-mutate: $(date -Is), tree $TREE"
echo "## control: the real tree, each test alone (own target volume, fresh build)"
for test in the_next_group_is_written_during_a_sync a_failed_sync_with_a_group_in_flight \
  a_fault_behind_a_group_in_flight at_most_two_groups_wait_for_the_disk \
  the_replica_gets_records_once_per_sync; do
  docker run --rm -v "$TREE":/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    bash -c "touch src/exchange/runtime.rs; timeout 600 cargo test --locked --bin stock $test 2>&1" \
    | grep -E "^test result" | sed "s/^/  $test: /"
done
run "a group is released before its sync" the_next_group_is_written_during_a_sync "
p='$R'; s=open(p).read()
old='.take_while(|group| group.end <= durable)'
assert old in s
open(p,'w').write(s.replace(old, '.take_while(|_| true)', 1))
"
run "the worker waits for each sync (no pipeline)" the_next_group_is_written_during_a_sync "
p='$R'; s=open(p).read()
old='''            in_flight.groups.push_back(group);
'''
assert old in s
open(p,'w').write(s.replace(old, old + '''            self.drain(&mut in_flight, journaler.as_ref(), &waiter)?;
''', 1))
"
run "a failed sync counts as durable" a_failed_sync_with_a_group_in_flight "
p='$R'; s=open(p).read()
old='Err(failure) => state.failed = Some(failure),'
assert old in s
open(p,'w').write(s.replace(old, 'Err(_) => state.durable = end,', 1))
"
run "a fault halts without draining the group in flight" a_fault_behind_a_group_in_flight "
p='$R'; s=open(p).read()
old='''                self.drain(&mut in_flight, journaler.as_ref(), &waiter)?;
                return Err(fault);'''
assert old in s
open(p,'w').write(s.replace(old, '''                return Err(fault);''', 1))
"
run "each group shipped to the replica as it is written" the_replica_gets_records_once_per_sync "
p='$R'; s=open(p).read()
old1='''    if let Some(replication) = replication {
        replication.written(end, next_sequence);
    }
'''
old2='''        group.end = store.end().map_err(RuntimeFailure::Store)?;
        Ok(true)'''
assert old1 in s and old2 in s
s=s.replace(old1, '''    let _ = next_sequence;
''', 1).replace(old2, '''        group.end = store.end().map_err(RuntimeFailure::Store)?;
        if let Some(replication) = &self.replication {
            replication.written(group.end, self.next_event_seq);
        }
        Ok(true)''', 1)
open(p,'w').write(s)
"
run "no limit on commands waiting for the disk" at_most_two_groups_wait_for_the_disk "
p='$R'; s=open(p).read()
old1='if in_flight.commands >= MAX_UNDURABLE {'
old2='let room = MAX_GROUP.min(MAX_UNDURABLE - in_flight.commands);'
assert old1 in s and old2 in s
s=s.replace(old1, 'if false {', 1).replace(old2, 'let room = MAX_GROUP;', 1)
open(p,'w').write(s)
"
rm -rf "$MUT"
echo "### done"
