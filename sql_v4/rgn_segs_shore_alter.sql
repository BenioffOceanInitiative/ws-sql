--# add column touches_coast
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS touches_shore BOOL;
