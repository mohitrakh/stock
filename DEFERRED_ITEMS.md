# Deferred Items

This file is the deliberate parking lot for concerns that matter but are not the selected
milestone. An entry here is not forgotten and is not permission to change it incidentally.

## Strict self-trade prevention

The current matcher skips a resting order owned by the incoming order's user so it does not create
a self-execution. A remaining incoming quantity can still potentially rest at a price that crosses
that user's opposite-side resting order after valid third-party fills.

This needs a separate matching-policy decision: for example, cancel the incoming remainder or
cancel the resting order. The chosen behavior must be explicit and replayable, then reflected in
the core, event schema, committed-batch decoder, MDP, Reporter, snapshots, and recovery tests.

It remains deferred. No claim is made here that the concern has been fixed.

## Reporter halts permanently on a reused client order id

Every new-order batch, accepted or rejected, becomes a plain `INSERT` into `reported_orders`,
whose primary key is `order_id` (`reporter.rs`, `insert_order`, called for both outcomes). A client
that retries an order with the same `client_order_id` — exactly the retry that id exists for —
receives `409` from the exchange, and the journal records `OrderRejected` carrying that same
`order_id`. The reporter then inserts a second row with an existing key; the transaction rolls
back and the follower halts. Every restart replays the same batch and fails the same way, so the
reporter stays down until someone intervenes. Trading and MDP are unaffected.

The trigger is ordinary retry behavior, not an attack or a corrupt journal. The live warm-replica
run on 2026-09-29 performed exactly such a retry after promotion; no reporter was running.

Confirmed by reading the code path end to end; not yet reproduced against PostgreSQL. A fix needs
a decision on how a rejected duplicate submission is recorded — for example a separate
rejected-submission table keyed by journal sequence rather than by `order_id` — and a regression
case in the reporter acceptance test.

## Rejected cancellation overwrites the owner's order row

A rejected cancellation runs `UPDATE reported_orders SET cancellation_outcome = 'rejected' ...
WHERE order_id = $2`, with no ownership, status, or affected-row check. The committed-batch decoder
also discards the requester's `user_id` (`CancelOrderRequested { order_id, .. }`), so the reporter
cannot tell whose attempt it was. Two consequences:

- A user who tries to cancel someone else's order is refused by the exchange as `Unauthorized`,
  yet the reporter writes that rejection onto the owner's row.
- A rejected attempt to cancel an already-canceled order leaves `status = 'canceled'` beside
  `cancellation_outcome = 'rejected'`.

The schema has no constraint tying the cancellation columns to `status`. A fix likely records
rejected cancellation attempts separately from the order's own lifecycle, keyed by journal
sequence and carrying the requester.
