# Overnight Risk Accounting

Implemented 2026-09-26. This milestone fixes a daily-limit bypass involving orders that remain open across a day boundary.

## Problem

Risk usage was stored as one daily counter. When the first order from a later day arrived, `RiskManager` cleared that counter. Resting orders from the previous day then stopped counting even though they could still execute. If an old order was later cancelled, its remaining quantity was subtracted from the new day's counter as well.

With a limit of 10, the old behavior could leave 10 shares resting from yesterday, accept 10 more today, then cancel yesterday's order and reduce today's recorded usage to zero. Deterministic replay reproduced this behavior, but that only made the bug repeatable.

## Design

Risk now keeps two values per user and symbol:

- current-day usage: shares traded today plus shares still open;
- open exposure: the remaining quantity of every resting order, regardless of its submission day.

An accepted order increases both values. A fill decreases open exposure but leaves current-day usage unchanged because the quantity moves from "could trade today" to "did trade today." A cancellation decreases both by only the cancelled remainder. At a day rollover, executed quantity expires and current-day usage is rebuilt from open exposure.

The trading day still comes from recorded order timestamps and only moves forward, preserving deterministic replay. Fill reductions are aggregated and validated during command preparation. Cancellation also validates its risk release before commit. The commit phase then applies only checked, prevalidated changes alongside the prepared order, book, cash, and position transition.

No event schema changed. Replay derives both risk values from the existing accepted orders, executions, and cancellations. Historical logs that relied on the old bypass can now fail deterministic replay when a formerly accepted over-limit order is regenerated as a rejection; such a failure must be handled explicitly rather than by deleting the journal.

## Verification

Tests cover an overnight resting order remaining counted, a day-two partial fill, cancellation of the overnight remainder, rejection above the exact remaining allowance, acceptance at the exact allowance, and two subsequent durable recoveries with the same usage.

`cargo fmt -- --check` passes. `cargo test --locked --offline` passes 95 unit tests and 2 executable integration tests.

## Remaining work

The daily boundary is still a UTC 86,400-second bucket rather than an exchange calendar. Limits remain per user and symbol, count shares rather than notional, and can currently be changed by the trader through the placeholder API. These are separate policy milestones.
