-- Visibility watermark for the projection cursor (ADR 0016, issue #69).
--
-- `seq` is assigned at insert, not at commit, so with several writers a lower
-- `seq` can become visible after a higher one, and a `seq`-only cursor skipped
-- it forever. Each row now records its inserting transaction. The projector
-- folds only rows below its snapshot's xmin (every such transaction has
-- finished, so no new rows can appear there) in `(xid, seq)` order, and the
-- cursor is that pair.
--
-- Existing rows get the constant `0` (no table rewrite): they sort first, in
-- their `seq` order, so the cursor carries over as `(0, last_seq)`. Only then
-- does the default switch to the inserting transaction.
ALTER TABLE events ADD COLUMN xid xid8 NOT NULL DEFAULT '0';
ALTER TABLE events ALTER COLUMN xid SET DEFAULT pg_current_xact_id();
CREATE INDEX events_xid_seq_idx ON events (xid, seq);

ALTER TABLE projection_state ADD COLUMN last_xid xid8 NOT NULL DEFAULT '0';
