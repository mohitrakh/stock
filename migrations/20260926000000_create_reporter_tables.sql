CREATE TABLE reporter_checkpoint (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    journal_device NUMERIC(20, 0) NOT NULL,
    journal_inode NUMERIC(20, 0) NOT NULL,
    next_sequence NUMERIC(20, 0) NOT NULL CHECK (next_sequence > 0),
    byte_offset NUMERIC(20, 0) NOT NULL CHECK (byte_offset >= 8)
);

CREATE TABLE reported_orders (
    order_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    limit_price NUMERIC(20, 0) NOT NULL CHECK (limit_price > 0),
    original_quantity BIGINT NOT NULL CHECK (original_quantity > 0),
    filled_quantity BIGINT NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
    remaining_quantity BIGINT NOT NULL CHECK (remaining_quantity >= 0),
    status TEXT NOT NULL CHECK (status IN ('new', 'partially_filled', 'filled', 'canceled', 'rejected')),
    creation_time DOUBLE PRECISION NOT NULL,
    acceptance_sequence NUMERIC(20, 0),
    rejection_reason TEXT,
    cancellation_sequence NUMERIC(20, 0),
    cancellation_outcome TEXT CHECK (cancellation_outcome IN ('canceled', 'rejected')),
    cancellation_reason TEXT,
    CHECK (filled_quantity + remaining_quantity = original_quantity),
    CHECK ((status = 'new' AND filled_quantity = 0)
        OR (status = 'partially_filled' AND filled_quantity > 0 AND remaining_quantity > 0)
        OR (status = 'filled' AND remaining_quantity = 0)
        OR (status = 'canceled' AND remaining_quantity > 0)
        OR (status = 'rejected' AND filled_quantity = 0)),
    CHECK ((status = 'rejected') = (rejection_reason IS NOT NULL))
);

CREATE TABLE reported_trades (
    trade_sequence NUMERIC(20, 0) PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC(20, 0) NOT NULL CHECK (price > 0),
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    buy_order_id TEXT NOT NULL REFERENCES reported_orders(order_id),
    sell_order_id TEXT NOT NULL REFERENCES reported_orders(order_id),
    first_execution_id TEXT NOT NULL UNIQUE,
    second_execution_id TEXT NOT NULL UNIQUE,
    trade_time DOUBLE PRECISION NOT NULL,
    CHECK (buy_order_id <> sell_order_id),
    CHECK (first_execution_id <> second_execution_id)
);

CREATE INDEX reported_orders_user_id_order_id_idx ON reported_orders (user_id, order_id);
CREATE INDEX reported_orders_symbol_status_idx ON reported_orders (symbol, status);
