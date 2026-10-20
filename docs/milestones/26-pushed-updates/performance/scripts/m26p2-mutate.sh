#!/usr/bin/env bash
# Milestone 26 part 2: each removal of a private-feed guarantee must fail the test that covers it.
# For each mutation: copy the tree, apply the change, run only the covering test, print its result
# line. A control run of the real tree comes first. Mutations build in their own cargo target
# volume (stock-cargo-target-mut), as milestone 25's scripts do.
# Usage: [TREE=DIR] m26p2-mutate.sh   (on the host; the tree defaults to ~/stock-m25, the checkout
# of SETUP.md. Part 2's runs used TREE=~/stock-m26, an uncommitted copy.)
set -u
TREE=${TREE:-$HOME/stock-m25}
MUT=$HOME/stock-m26p2-mut
# Full paths: the book feed has tests with some of the same names.
T=user_feed::tests
TESTS="$T::every_event_reaches_only_its_owner_and_matches_the_core
$T::refusals_go_to_whoever_asked_and_never_touch_the_owner_map
$T::a_batch_the_owner_map_cannot_explain_is_an_error
$T::a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation
$T::a_connection_closes_when_its_token_expires
$T::a_halted_follower_closes_every_connection
$T::a_users_ring_is_freed_when_their_last_connection_leaves
$T::each_user_receives_their_own_events_and_nothing_else
$T::a_restart_from_the_saved_map_still_tells_the_owners_of_older_orders
$T::a_client_that_never_answers_pings_is_closed_as_gone
$T::a_join_racing_the_follower_gets_exactly_the_events_after_its_sequence
$T::a_joiner_holds_the_view_until_it_has_joined
$T::a_client_message_over_the_limit_ends_the_connection_unread"
cargo_test() { # DIR TEST
  docker run --rm -v "$1":/src -v stock-cargo-registry:/usr/local/cargo/registry \
    -v stock-cargo-target-mut:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    bash -c "touch src/exchange/user_feed.rs src/middleware/auth_middleware.rs; timeout 600 cargo test --locked --bin stock $2 2>&1" \
    | grep -E "^test result|^error" | head -1 | grep . || echo "no result (killed after 600 s)"
}
run() { # LABEL TEST PYTHON-REPLACEMENT-SCRIPT
  local label=$1 test=$2
  # -L: copy the tree a symlink points to, never the link itself.
  rm -rf "$MUT" && cp -rL "$TREE" "$MUT"
  python3 -c "$3" || { echo "  $label: mutation did not apply"; return; }
  echo "  $label -> $(cargo_test "$MUT" "$test") (${test#$T::})"
}
U=$MUT/src/exchange/user_feed.rs
A=$MUT/src/middleware/auth_middleware.rs
echo "### m26p2-mutate: $(date -Is), tree $TREE"
echo "## control: the real tree, each test alone (own target volume, fresh build)"
for test in $TESTS; do
  echo "  ${test#$T::}: $(cargo_test "$TREE" "$test")"
done
run "a resting order's fill goes to the incoming order's user" $T::every_event_reaches_only_its_owner_and_matches_the_core "
p='$U'; s=open(p).read()
old='''                    told.push((
                        resting.user_id.clone(),
                        Event::Fill {'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''                    told.push((
                        incoming.user_id.clone(),
                        Event::Fill {'''))
"
run "a rejection goes to the open order's owner of that id" $T::refusals_go_to_whoever_asked_and_never_touch_the_owner_map "
p='$U'; s=open(p).read()
old='''            } => Ok(vec![(
                order.user_id.clone(),
                Event::Order {'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''            } => Ok(vec![(
                self.orders.get(&order.order_id).map_or(order.user_id.clone(), |open| open.user_id.clone()),
                Event::Order {'''))
"
run "a refused cancellation goes to the order's owner" $T::every_event_reaches_only_its_owner_and_matches_the_core "
p='$U'; s=open(p).read()
old='''            } => Ok(vec![(
                user_id,
                Event::CancelRejected {'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''            } => Ok(vec![(
                self.orders.get(&order_id).map_or(user_id, |open| open.user_id.clone()),
                Event::CancelRejected {'''))
"
run "a cancellation by someone else is accepted" $T::a_batch_the_owner_map_cannot_explain_is_an_error "
p='$U'; s=open(p).read()
old='if order.user_id != user_id {'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'if false && order.user_id != user_id {'))
"
run "no auth deadline" $T::a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation "
p='$U'; s=open(p).read()
old='timeout(AUTH_DEADLINE, first_text(&mut socket))'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'timeout(AUTH_DEADLINE * 100, first_text(&mut socket))'))
"
run "a token's signature is not checked" $T::a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation "
p='$A'; s=open(p).read()
old='decode::<Claims>(token, key, &Validation::default())'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '{ let mut validation = Validation::default(); validation.insecure_disable_signature_validation(); decode::<Claims>(token, key, &validation) }'))
"
run "a token's expiry is ignored once connected" $T::a_connection_closes_when_its_token_expires "
p='$U'; s=open(p).read()
old='() = &mut expiry => break Some(Close::Policy(\"token expired\")),'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, ''))
"
run "a halt leaves the rings open" $T::a_halted_follower_closes_every_connection "
p='$U'; s=open(p).read()
old='        state.halted = true;\n        state.users.clear();'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '        state.halted = true;'))
"
run "a ring is never freed" $T::a_users_ring_is_freed_when_their_last_connection_leaves "
p='$U'; s=open(p).read()
old='            state.users.remove(user);'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, ''))
"
run "every event goes to every connected user" $T::each_user_receives_their_own_events_and_nothing_else "
p='$U'; s=open(p).read()
old='if let Some(sender) = state.users.get(&user).filter(|s| s.receiver_count() > 0) {'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'let _ = &user;\n            for sender in state.users.values().filter(|s| s.receiver_count() > 0) {'))
"
run "ready names the next sequence, not the last applied" $T::each_user_receives_their_own_events_and_nothing_else "
p='$U'; s=open(p).read()
old='Some((view.applied.next_sequence - 1, receiver))'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'Some((view.applied.next_sequence, receiver))'))
"
run "the saved state holds no orders" $T::a_restart_from_the_saved_map_still_tells_the_owners_of_older_orders "
p='$U'; s=open(p).read()
old='orders: view.owners.persisted(),'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'orders: Vec::new(),'))
"
run "silence is never noticed" $T::a_client_that_never_answers_pings_is_closed_as_gone "
p='$U'; s=open(p).read()
old='if last_heard.elapsed() > limits.ping * SILENT_PINGS {'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, 'if last_heard.elapsed() > limits.ping * SILENT_PINGS && false {'))
"
run "a resting order's own order message is dropped" $T::every_event_reaches_only_its_owner_and_matches_the_core "
p='$U'; s=open(p).read()
old='                    told.push((resting.user_id.clone(), resting.event(seq)));\n'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, ''))
"
# The racing test missed this mutation in the second run (the race is timing); the deterministic
# test covers it, and the racing test runs on it too, for the record.
for test in $T::a_joiner_holds_the_view_until_it_has_joined $T::a_join_racing_the_follower_gets_exactly_the_events_after_its_sequence; do
run "ready's sequence and the join under separate locks" $test "
p='$U'; s=open(p).read()
old='''    let receiver = hub.join(user)?;
    Some((view.applied.next_sequence - 1, receiver))'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '''    let seq = view.applied.next_sequence - 1;
    drop(served);
    std::thread::yield_now();
    let receiver = hub.join(user)?;
    Some((seq, receiver))'''))
"
done
run "a token within the leeway past exp is accepted" $T::a_connection_that_does_not_authenticate_is_closed_as_a_policy_violation "
p='$U'; s=open(p).read()
old='        .filter(|expires| *expires > SystemTime::now())\n'
assert s.count(old) == 1
open(p,'w').write(s.replace(old, ''))
"
run "no limit on a client's message size" $T::a_client_message_over_the_limit_ends_the_connection_unread "
p='$U'; s=open(p).read()
old='''    let upgrade = upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .max_frame_size(MAX_CLIENT_MESSAGE);'''
assert s.count(old) == 1
open(p,'w').write(s.replace(old, '    let _ = MAX_CLIENT_MESSAGE;'))
"
rm -rf "$MUT"
echo "### done"
