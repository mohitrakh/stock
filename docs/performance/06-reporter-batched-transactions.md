# 06 - Reporter: Many Batches per Transaction, One Round Trip per Trade

Milestone 21, part 2. Code: `apply_available` and `apply_trade` in `src/exchange/reporter.rs`.

## The problem

The reporter is the separate process that writes order history and trades into PostgreSQL. It
followed the journal one command at a time, and every command was its own database transaction:

```text
BEGIN
  INSERT the order                        1 round trip
  for each trade:  UPDATE buyer's order   1 round trip
                   UPDATE seller's order  1 round trip
                   INSERT the trade       1 round trip
  UPSERT the reporter checkpoint          1 round trip
COMMIT                                    1 round trip + PostgreSQL waits for its WAL fsync
```

The COMMIT is the expensive part: PostgreSQL makes the transaction durable by syncing its
write-ahead log before it answers. Paying that sync for every command is the same mistake milestone
19 fixed in the exchange's own journal.

Measured on a 200,000-order journal (one symbol, 200,020 commands): about **365 commands/s**.

## Step 1: group commit for the reporter

The fix is the same idea as `01`. Put many committed batches into one transaction, and commit them
together with the checkpoint of the last one:

```text
BEGIN
  batch 1: order + trades
  batch 2: order + trades
  ...                                  up to 1,000 batches, or until caught up
  UPSERT checkpoint = position after the LAST batch in the group
COMMIT                                 one WAL sync for the whole group
```

`apply_available` is used both for catch-up and for live following, so there is one code path. The
rules that keep it correct:

- **Rows and checkpoint commit together.** A crash before COMMIT loses the whole group, rows and
  checkpoint alike, and the restart re-applies it. A crash after COMMIT has both. So no batch is
  ever missing or applied twice.
- **The checkpoint is taken right after the last *applied* batch.** Whether the group is full is
  decided after applying a batch, and the position is read before fetching the next one. Otherwise
  the checkpoint could claim a batch that was never written.
- **A group ends when the reporter is caught up**, not only at 1,000. On a quiet exchange each
  batch still commits immediately, so the report is never held back waiting for more work.
- **Any error rolls back the whole group and stops the reporter**, as before. After a rollback the
  reader's cursor is ahead of the database. That is harmless only because the process stops, and a
  restart resumes from the database's checkpoint.
- **Catch-up's last group commits before `/health` binds**, because catch-up runs until the reader
  reports "caught up", and that ends the group.

Result: **1,050 commands/s** (2.9×). Same rows: 200,000 orders, 144,670 trades, and the same final
checkpoint.

## Step 2: one round trip per trade

With one commit per 1,000 batches, the time went into round trips: every trade still cost three
statements. They became one, using a PostgreSQL *data-modifying CTE*:

```sql
WITH filled AS (
    UPDATE reported_orders
       SET filled_quantity = filled_quantity + $4, remaining_quantity = remaining_quantity - $4,
           status = CASE WHEN remaining_quantity - $4 = 0 THEN 'filled' ELSE 'partially_filled' END
     WHERE order_id IN ($5, $6)                        -- the buy and the sell order
       AND status IN ('new', 'partially_filled') AND remaining_quantity >= $4
 RETURNING order_id)
INSERT INTO reported_trades (...)
SELECT $1, ... WHERE (SELECT count(*) FROM filled) = 2  -- only if BOTH orders were filled
```

The UPDATE fills both orders, and the INSERT writes the trade only if exactly two rows were filled.
The reporter then checks that one trade row was inserted. If not, one of the orders was missing or
not resting, and it stops, exactly as the two separate checks did before. The UPDATE's effects are
then rolled back with the rest of the group.

Result: **1,578 commands/s** (1.5× on top of step 1; **4.3×** overall). Same rows and checkpoint
again.

## Results

| Reporter | Catch-up, 200,020 commands | Speed |
|---|---|---|
| Before (one transaction per command) | about 9 minutes (60-second sample) | ~365 commands/s |
| + group commit (up to 1,000 per transaction) | 190 s | 1,050 commands/s |
| + one round trip per trade | **127 s** | **1,578 commands/s** |

**Live**, with the exchange running at a fixed rate and the reporter following from empty tables:

| Exchange load | Reporter |
|---|---|
| 1,000 orders/s for 30 s | **kept up**: its checkpoint was at the end of the journal when the exchange stopped |
| 5,000 orders/s for 10 s | fell behind: 30% through when the exchange stopped, caught up about 21 s later (~1,600 commands/s) |

## Why it stops here, and what the next lever is

A bare round trip to this PostgreSQL (`SELECT 1`, measured with `pgbench`) takes 0.11 ms over the
Docker network. Each of the reporter's statements takes about 0.37 ms. So most of the remaining time
is PostgreSQL's own per-row work, not the network:

- index maintenance on the order's primary key and two secondary indexes;
- foreign-key checks from each trade to both of its orders;
- a new row version (MVCC) for every filled or canceled order.

Grouping more batches per transaction would not help: the commit is already paid once per 1,000.

The next lever is **set-based writes**: work out each group's final effect in memory and send it as
a handful of multi-row statements, or PostgreSQL `COPY`. That would mean
- one multi-row INSERT for the group's new orders, with their fills already applied;
- one UPDATE of older orders from an array of per-order fill amounts;
- one INSERT of all the trades.

It is a real design change, with an in-memory view of every order a group touches and its own
correctness tests, so it is recorded as the next step rather than squeezed into this one.

The reporter is not on the trading path. When it lags, trading and market data are unaffected, and
it catches up after the burst: the 10-second burst at 5,000/s took about 21 s to drain.

## Tests

The PostgreSQL acceptance test (`tests/reporter.rs`, run against a disposable PostgreSQL) passes
unchanged with both steps. Its six-batch fixture is now one transaction. Its injected checkpoint
failure therefore rolls back the entire group, and the test's "nothing was written" check holds.
Part 3 of this milestone extends the test with a group that crosses the 1,000-batch boundary.
