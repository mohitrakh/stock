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

## Report identities that assume ids are never reused

Milestone 21 fixed the reporter's two recorded defects (a reused client order id halting it, and a
refused cancellation overwriting the owner's row) and a third found by its benchmark (execution ids
repeating across symbols). See `docs/tasks/16-subscribers-keep-up.md`. The fixes leave two keys
that are correct only under today's engine rules:

- `reported_orders` is keyed by `order_id`. That holds because the engine never accepts an id
  twice: its duplicate check covers every order it has ever accepted, including finished ones.
- `reported_trades` keeps `(symbol, execution id)` unique. That holds because each symbol's book
  numbers its executions from `exec_0` once and never restarts the count.

A trading-day boundary would likely break both. FIX only requires a client order id to be unique
within one trading day, and books emptied at the close could restart their execution numbering. Such
a milestone must first change these keys, for example to include the trading day or to use the
journal sequence that already identifies each trade and each acceptance.
