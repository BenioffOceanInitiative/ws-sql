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

UPDATE `{tbl_rgn_segs}` SET
  final_speed_knots = ROUND((
    CASE
      WHEN speed_knots         BETWEEN 0.001 AND 50 THEN speed_knots
      WHEN calculated_knots    BETWEEN 0.001 AND 50 THEN calculated_knots
      WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
      ELSE NULL
    END), 3),
  speedbin_str            = {sql_speedbins_str},
  speedbin_num            = {sql_speedbins_num},
  speedbin_implied_str    = {sql_speedbins_implied_str},
  speedbin_implied_num    = {sql_speedbins_implied_num},
  speedbin_calculated_str = {sql_speedbins_calculated_str},
  speedbin_calculated_num = {sql_speedbins_calculated_num},
  speedbin_final_str      = {sql_speedbins_final_str},
  speedbin_final_num      = {sql_speedbins_final_num}
WHERE 
  DATE(timestamp) > DATE('1900-01-01') AND
  rgn = '{rgn}';
  -- AND
  -- ("final_speed_knots" IS NULL OR 
  -- "speedbin" IS NULL OR
  -- "speedbin_implied" IS NULL OR
  -- "speedbin_calculated" IS NULL OR
  -- "speedbin_final"  IS NULL);
  -- TODO fix error: Cannot query over table 'benioff-ocean-initiative.whalesafe_v4.rgn_segs' without a filter over column(s) 'timestamp' that can be used for partition elimination at [9:1]
