--# add columns
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS final_speed_knots FLOAT64,
  ADD COLUMN IF NOT EXISTS speedbin_str STRING,
  ADD COLUMN IF NOT EXISTS speedbin_num INT64,
  ADD COLUMN IF NOT EXISTS speedbin_implied_str STRING,
  ADD COLUMN IF NOT EXISTS speedbin_implied_num INT64,
  ADD COLUMN IF NOT EXISTS speedbin_calculated_str STRING,
  ADD COLUMN IF NOT EXISTS speedbin_calculated_num INT64,
  ADD COLUMN IF NOT EXISTS speedbin_final_str STRING,
  ADD COLUMN IF NOT EXISTS speedbin_final_num INT64;

