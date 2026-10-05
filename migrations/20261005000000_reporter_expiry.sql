-- Milestone 22, part 2: every order still resting at the close of a trading day expires, and the
-- report records it as 'expired' with the matching sequence its expiry consumed.
--
-- A journal from before part 2 does not replay any more, so the exchange starts a new one; the
-- report is derived from the journal, so this migration clears it and the next reporter start
-- rebuilds it from journal sequence 1. Stop the reporter before applying it. One transaction: a
-- failure part-way leaves the old schema and its data untouched.
BEGIN;

TRUNCATE reported_trades, reported_orders, rejected_orders, rejected_cancellations,
    reporter_checkpoint;

-- Only a reporter that records expiries may save a position into this report.
ALTER TABLE reporter_checkpoint DROP CONSTRAINT reporter_checkpoint_report_version_check;
ALTER TABLE reporter_checkpoint ADD CONSTRAINT reporter_checkpoint_report_version_check
    CHECK (report_version = 3);

ALTER TABLE reported_orders ADD COLUMN expiry_sequence NUMERIC(20, 0);
ALTER TABLE reported_orders DROP CONSTRAINT reported_orders_status_check;
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_status_check
    CHECK (status IN ('new', 'partially_filled', 'filled', 'canceled', 'expired'));
-- The original table's unnamed check that ties each status to its quantities. Like a canceled
-- order, an expired one still had quantity left. 'rejected' is gone: since milestone 21 a
-- rejected submission is a row in rejected_orders.
ALTER TABLE reported_orders DROP CONSTRAINT reported_orders_check1;
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_status_quantities CHECK (
    (status = 'new' AND filled_quantity = 0)
    OR (status = 'partially_filled' AND filled_quantity > 0 AND remaining_quantity > 0)
    OR (status = 'filled' AND remaining_quantity = 0)
    OR (status IN ('canceled', 'expired') AND remaining_quantity > 0));
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_expired_has_sequence
    CHECK ((status = 'expired') = (expiry_sequence IS NOT NULL));

COMMIT;
