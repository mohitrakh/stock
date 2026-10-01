-- Milestone 21: rejected submissions and rejected cancellations are recorded per journal command
-- instead of on order rows, and execution ids are unique only within one symbol's book.
--
-- The report is derived from the authoritative journal, so this migration clears it and the next
-- reporter start rebuilds it from journal sequence 1. Stop the reporter before applying it. One
-- transaction: a failure part-way leaves the old schema and its data untouched.
BEGIN;

TRUNCATE reported_trades, reported_orders, reporter_checkpoint;

-- The checkpoint records which report format it belongs to. The column has no default, so a
-- reporter from before this migration, left running by mistake, can no longer save a position into
-- the emptied table: it stops instead of making the new reporter skip the history before it.
ALTER TABLE reporter_checkpoint ADD COLUMN report_version SMALLINT NOT NULL
    CHECK (report_version = 2);

-- A rejected new order, keyed by the journal sequence of its input. Its order id is not unique:
-- a client may retry an id, reuse the id of a rejected order, or collide with another user's id.
CREATE TABLE rejected_orders (
    input_sequence NUMERIC(20, 0) PRIMARY KEY,
    order_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    limit_price NUMERIC(20, 0) NOT NULL,
    quantity BIGINT NOT NULL,
    creation_time DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX rejected_orders_order_id_idx ON rejected_orders (order_id);
CREATE INDEX rejected_orders_user_id_idx ON rejected_orders (user_id);

-- A refused cancellation attempt and who made it. It never touches the order's own row.
CREATE TABLE rejected_cancellations (
    input_sequence NUMERIC(20, 0) PRIMARY KEY,
    order_id TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX rejected_cancellations_order_id_idx ON rejected_cancellations (order_id);

-- reported_orders now holds accepted orders only, and only a real cancellation marks one canceled.
-- The rejection and cancellation-outcome columns had no remaining meaning: an order row is never
-- rejected, and a refused cancellation is a row in rejected_cancellations.
ALTER TABLE reported_orders DROP COLUMN rejection_reason;
ALTER TABLE reported_orders DROP COLUMN cancellation_outcome;
ALTER TABLE reported_orders DROP COLUMN cancellation_reason;
ALTER TABLE reported_orders ALTER COLUMN acceptance_sequence SET NOT NULL;
ALTER TABLE reported_orders DROP CONSTRAINT reported_orders_status_check;
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_status_check
    CHECK (status IN ('new', 'partially_filled', 'filled', 'canceled'));
ALTER TABLE reported_orders ADD CONSTRAINT reported_orders_canceled_has_sequence
    CHECK ((status = 'canceled') = (cancellation_sequence IS NOT NULL));

-- Every symbol's book numbers its executions from exec_0, so an id repeats across symbols.
ALTER TABLE reported_trades DROP CONSTRAINT reported_trades_first_execution_id_key;
ALTER TABLE reported_trades DROP CONSTRAINT reported_trades_second_execution_id_key;
ALTER TABLE reported_trades ADD CONSTRAINT reported_trades_first_execution_per_symbol
    UNIQUE (symbol, first_execution_id);
ALTER TABLE reported_trades ADD CONSTRAINT reported_trades_second_execution_per_symbol
    UNIQUE (symbol, second_execution_id);

COMMIT;
