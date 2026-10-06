-- Milestone 23, part 2: a journal names itself with a random id in its header, so a byte-identical
-- copy on another machine is the same journal. The reporter's checkpoint records that id instead
-- of the journal file's device and inode, which a copy does not share.
--
-- A journal from before part 2 has no id and does not open any more, so the exchange starts a new
-- one; the report is derived from the journal, so this migration clears it and the next reporter
-- start rebuilds it from journal sequence 1. Stop the reporter before applying it. One
-- transaction: a failure part-way leaves the old schema and its data untouched.
BEGIN;

TRUNCATE reported_trades, reported_orders, rejected_orders, rejected_cancellations,
    reporter_checkpoint;

-- Only a reporter that names the journal by its id may save a position into this report.
ALTER TABLE reporter_checkpoint DROP CONSTRAINT reporter_checkpoint_report_version_check;
ALTER TABLE reporter_checkpoint ADD CONSTRAINT reporter_checkpoint_report_version_check
    CHECK (report_version = 5);

ALTER TABLE reporter_checkpoint DROP COLUMN journal_device;
ALTER TABLE reporter_checkpoint DROP COLUMN journal_inode;
ALTER TABLE reporter_checkpoint ADD COLUMN journal_id UUID NOT NULL;

-- The journal's header is now 24 bytes (magic and id), so its first record starts there.
ALTER TABLE reporter_checkpoint DROP CONSTRAINT reporter_checkpoint_byte_offset_check;
ALTER TABLE reporter_checkpoint ADD CONSTRAINT reporter_checkpoint_byte_offset_check
    CHECK (byte_offset >= 24);

COMMIT;
