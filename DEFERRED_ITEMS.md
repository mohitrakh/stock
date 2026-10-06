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

## A close spread across several journal records

A journal record holds one command and all its outputs, and may not exceed 64 MiB. The close
expires every resting order in one record. Milestone 22 part 2 caps the books at 200,000 resting
orders, so a close of gateway-checked orders always fits; see `docs/tasks/17-trading-day.md`.
Lifting the cap needs a close spread across several records, with a "closing" state that refuses
new orders, a new event, and handling in every subscriber.

The other command that could outgrow a record, one order sweeping the book, has been refused since
milestone 23 part 1 (`TooManyFills`; see `docs/tasks/18-one-order-cannot-stop-the-exchange.md`).

The multi-record close remains deferred.
