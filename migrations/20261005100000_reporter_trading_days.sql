-- Milestone 22, part 3: a client order id is unique within one trading day only, so orders are
-- keyed by (trading_day, order_id), and each trade carries the day of the two orders it fills. The
-- reporter learns the day from the opens in the journal and keeps it in its checkpoint.
--
-- A journal from before part 3 may not replay any more (an id reused on a later day was refused
-- then and is accepted now), so the exchange starts a new one; the report is derived from the
-- journal, so this migration clears it and the next reporter start rebuilds it from journal
-- sequence 1. Stop the reporter before applying it. One transaction: a failure part-way leaves the
-- old schema and its data untouched.
BEGIN;

TRUNCATE reported_trades, reported_orders, rejected_orders, rejected_cancellations,
    reporter_checkpoint;

-- Only a reporter that keys orders by day may save a position into this report.
ALTER TABLE reporter_checkpoint DROP CONSTRAINT reporter_checkpoint_report_version_check;
ALTER TABLE reporter_checkpoint ADD CONSTRAINT reporter_checkpoint_report_version_check
    CHECK (report_version = 4);
-- The trading day the journal is in at the checkpoint; NULL before the first open.
ALTER TABLE reporter_checkpoint ADD COLUMN trading_day DATE;

ALTER TABLE reported_trades DROP CONSTRAINT reported_trades_buy_order_id_fkey;
ALTER TABLE reported_trades DROP CONSTRAINT reported_trades_sell_order_id_fkey;
ALTER TABLE reported_orders DROP CONSTRAINT reported_orders_pkey;
ALTER TABLE reported_orders ADD COLUMN trading_day DATE NOT NULL;
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_pkey
    PRIMARY KEY (trading_day, order_id);

-- A trade happens on one day, between two orders of that day. Execution ids stay unique per
-- symbol across days, because each symbol's book keeps its execution counter.
ALTER TABLE reported_trades ADD COLUMN trading_day DATE NOT NULL;
ALTER TABLE reported_trades ADD CONSTRAINT reported_trades_buy_order_fkey
    FOREIGN KEY (trading_day, buy_order_id) REFERENCES reported_orders (trading_day, order_id);
ALTER TABLE reported_trades ADD CONSTRAINT reported_trades_sell_order_fkey
    FOREIGN KEY (trading_day, sell_order_id) REFERENCES reported_orders (trading_day, order_id);

-- A refusal names an order id, which repeats across days, so it records the trading day the
-- journal was in: the open day, or the day just closed. NULL before the first open.
ALTER TABLE rejected_orders ADD COLUMN trading_day DATE;
ALTER TABLE rejected_cancellations ADD COLUMN trading_day DATE;

COMMIT;
