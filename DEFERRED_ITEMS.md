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

It is deferred while the next milestone is Candlestick Publisher v1. No claim is made here that
the concern has been fixed.
