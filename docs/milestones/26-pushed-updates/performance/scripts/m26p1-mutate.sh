#!/usr/bin/env bash
# Milestone 26 part 1: each removal of a feed guarantee must fail the test that covers it. For each
# mutation: copy the tree, apply the change, run only the covering test, print its result line. A
# control run of the real tree comes first. Mutations build in their own cargo target volume
# (stock-cargo-target-mut), as milestone 25's scripts do.
# Usage: [TREE=DIR] m26p1-mutate.sh   (on the host; the tree defaults to ~/stock-m25, the checkout
# of SETUP.md. Part 1's runs used TREE=~/stock-m26, an uncommitted copy.)
set -u
TREE=${TREE:-$HOME/stock-m25}
MUT=$HOME/stock-m26p1-mut
TESTS="feed_snapshots_and_deltas_rebuild_every_book_at_every_sequence a_client_that_stops_reading a_client_whose_queue_overflows a_halted_follower_closes_every_connection a_client_that_never_answers_pings"
cargo_test() { # DIR TEST
  docker run --rm -v "$1":/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    bash -c "touch src/exchange/market_data.rs src/exchange/book_feed.rs; timeout 600 cargo test --locked --bin stock $2 2>&1" \
    | grep -E "^test result|^error" | head -1 | grep . || echo "no result (killed after 600 s)"
}
run() { # LABEL TEST PYTHON-REPLACEMENT-SCRIPT
  local label=$1 test=$2
  # -L: copy the tree a symlink points to, never the link itself.
  rm -rf "$MUT" && cp -rL "$TREE" "$MUT"
  python3 -c "$3" || { echo "  $label: mutation did not apply"; return; }
  echo "  $label -> $(cargo_test "$MUT" "$test") ($test)"
}
F=$MUT/src/exchange/book_feed.rs
M=$MUT/src/exchange/market_data.rs
echo "### m26p1-mutate: $(date -Is), tree $TREE"
echo "## control: the real tree, each test alone (own target volume, fresh build)"
for test in $TESTS; do
  echo "  $test: $(cargo_test "$TREE" "$test")"
done
run "a delta's prev is not chained (last never moves)" feed_snapshots_and_deltas_rebuild_every_book_at_every_sequence "
p='$F'; s=open(p).read()
old='let prev = std::mem::replace(&mut feed.last, seq);'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'let prev = feed.last;'))
"
run "a snapshot's last is always 0" feed_snapshots_and_deltas_rebuild_every_book_at_every_sequence "
p='$F'; s=open(p).read()
old='Some((feed.last, sender.subscribe()))'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'Some((0, sender.subscribe()))'))
"
run "a level's decrease is never reported (subtract_level not recorded)" feed_snapshots_and_deltas_rebuild_every_book_at_every_sequence "
p='$M'; s=open(p).read()
old='''        if empty {
            self.books.remove(symbol);
        }
        self.touched.push((symbol.to_string(), side.clone(), price));'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''        if empty {
            self.books.remove(symbol);
        }'''))
"
run "no write deadline" a_client_that_stops_reading "
p='$F'; s=open(p).read()
old='match timeout(deadline, socket.send(message)).await {'
assert s.count(old) == 1
s=s.replace(old, 'let _ = deadline;\n    match Ok::<_, tokio::time::error::Elapsed>(socket.send(message).await) {')
open(p,'w').write(s)
"
run "a full queue is not reported" a_client_whose_queue_overflows "
p='$F'; s=open(p).read()
old='''                Err(TrySendError::Full(_)) => {
                    let _ = closing.send(Close::SlowConsumer);
                    return;'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''                Err(TrySendError::Full(_)) => {
                    return;'''))
"
run "a halt does not reach idle connections" a_halted_follower_closes_every_connection "
p='$F'; s=open(p).read()
old='self.halted.send_replace(true);'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, ''))
"
run "silence is never noticed" a_client_that_never_answers_pings "
p='$F'; s=open(p).read()
old='if last_heard.elapsed() > limits.ping * SILENT_PINGS {'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'if last_heard.elapsed() > limits.ping * SILENT_PINGS && false {'))
"
rm -rf "$MUT"
echo "### done"
