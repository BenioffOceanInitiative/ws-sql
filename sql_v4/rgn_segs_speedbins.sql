--# add columns
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS final_speed_knots NUMERIC,
  ADD COLUMN IF NOT EXISTS speedbin STRING,
  ADD COLUMN IF NOT EXISTS speedbin_implied STRING,
  ADD COLUMN IF NOT EXISTS speedbin_calculated STRING,
  ADD COLUMN IF NOT EXISTS speedbin_final STRING;

UPDATE `{tbl_rgn_segs}` SET
final_speed_knots = ROUND((
  CASE
    WHEN speed_knots         BETWEEN 0.001 AND 50 THEN speed_knots
    WHEN calculated_knots    BETWEEN 0.001 AND 50 THEN calculated_knots
    WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
    ELSE NULL
  END), 3),
speedbin            = {sql_speedbin},
speedbin_implied    = {sql_speedbin_implied},
speedbin_calculated = {sql_speedbin_calculated},
speedbin_final      = {sql_speedbin_final}
WHERE DATE(timestamp) > DATE('1900-01-01');
  -- AND
  -- ("final_speed_knots" IS NULL OR 
  -- "speedbin" IS NULL OR
  -- "speedbin_implied" IS NULL OR
  -- "speedbin_calculated" IS NULL OR
  -- "speedbin_final"  IS NULL);
  -- TODO fix error: Cannot query over table 'benioff-ocean-initiative.whalesafe_v4.rgn_segs' without a filter over column(s) 'timestamp' that can be used for partition elimination at [9:1]
