-- Latest-wins guard timestamps for the Phase-3 facet-metadata folds.
--
-- `jobs` already has `meta_at` (Phase 0, for description/tags) which the job
-- metadata fold reuses. `runs` and `datasets` need their own guard so a later
-- event's run/dataset facet values supersede an earlier one's, and an older
-- (out-of-order replay) event never clobbers newer values.
alter table runs     add column meta_at timestamptz;
alter table datasets add column meta_at timestamptz;
