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

## Commands too large for one journal record

A journal record holds one command and all its outputs, and may not exceed 64 MiB. Two commands can
outgrow it:

- **The close.** It expires every resting order in one record. Milestone 22 part 2 caps the books at
  200,000 resting orders, so a close of gateway-checked orders always fits; see
  `docs/tasks/17-trading-day.md`. Lifting the cap needs a close spread across several records, with
  a "closing" state that refuses new orders, a new event, and handling in every subscriber.
- **An order that sweeps the book.** One order filling against more than roughly 60,000 to 140,000
  resting orders, depending on id lengths, produces a record over the limit. The worker then halts
  instead of refusing the order. The command was never journaled, so a restart recovers, but a
  client can repeat it. A fix must decide deterministically, during preparation, either to refuse
  such an order or to cap how many fills one order may take.

Both remain deferred.
