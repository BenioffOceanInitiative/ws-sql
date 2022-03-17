--# add columns
ALTER TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` 
  ADD COLUMN IF NOT EXISTS final_speed_knots NUMERIC,
  ADD COLUMN IF NOT EXISTS speedbin STRING,
  ADD COLUMN IF NOT EXISTS speedbin_implied STRING,
  ADD COLUMN IF NOT EXISTS speedbin_calculated STRING,
  ADD COLUMN IF NOT EXISTS speedbin_final STRING;

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` SET
final_speed_knots = ROUND((
  CASE
    WHEN speed_knots         BETWEEN 0.001 AND 50 THEN speed_knots
    WHEN calculated_knots    BETWEEN 0.001 AND 50 THEN calculated_knots
    WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
    ELSE NULL
  END), 3),
speedbin            = 
  CASE
    WHEN speed_knots > 0 AND speed_knots <= 0 THEN '[0]'
    WHEN speed_knots > 0 AND speed_knots <= 5 THEN '(0,5]'
    WHEN speed_knots > 5 AND speed_knots <= 10 THEN '(5,10]'
    WHEN speed_knots > 10 AND speed_knots <= 12 THEN '(10,12]'
    WHEN speed_knots > 12 AND speed_knots <= 15 THEN '(12,15]'
    WHEN speed_knots > 15 AND speed_knots <= 20 THEN '(15,20]'
    WHEN speed_knots > 20 AND speed_knots <= 25 THEN '(20,25]'
    WHEN speed_knots > 25 AND speed_knots <= 30 THEN '(25,30]'
    WHEN speed_knots > 30 AND speed_knots <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
  END
,
speedbin_implied    = 
  CASE
    WHEN implied_speed_knots > 0 AND implied_speed_knots <= 0 THEN '[0]'
    WHEN implied_speed_knots > 0 AND implied_speed_knots <= 5 THEN '(0,5]'
    WHEN implied_speed_knots > 5 AND implied_speed_knots <= 10 THEN '(5,10]'
    WHEN implied_speed_knots > 10 AND implied_speed_knots <= 12 THEN '(10,12]'
    WHEN implied_speed_knots > 12 AND implied_speed_knots <= 15 THEN '(12,15]'
    WHEN implied_speed_knots > 15 AND implied_speed_knots <= 20 THEN '(15,20]'
    WHEN implied_speed_knots > 20 AND implied_speed_knots <= 25 THEN '(20,25]'
    WHEN implied_speed_knots > 25 AND implied_speed_knots <= 30 THEN '(25,30]'
    WHEN implied_speed_knots > 30 AND implied_speed_knots <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
  END
,
speedbin_calculated = 
  CASE
    WHEN calculated_knots > 0 AND calculated_knots <= 0 THEN '[0]'
    WHEN calculated_knots > 0 AND calculated_knots <= 5 THEN '(0,5]'
    WHEN calculated_knots > 5 AND calculated_knots <= 10 THEN '(5,10]'
    WHEN calculated_knots > 10 AND calculated_knots <= 12 THEN '(10,12]'
    WHEN calculated_knots > 12 AND calculated_knots <= 15 THEN '(12,15]'
    WHEN calculated_knots > 15 AND calculated_knots <= 20 THEN '(15,20]'
    WHEN calculated_knots > 20 AND calculated_knots <= 25 THEN '(20,25]'
    WHEN calculated_knots > 25 AND calculated_knots <= 30 THEN '(25,30]'
    WHEN calculated_knots > 30 AND calculated_knots <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
  END
,
speedbin_final      = 
  CASE
    WHEN final_speed_knots > 0 AND final_speed_knots <= 0 THEN '[0]'
    WHEN final_speed_knots > 0 AND final_speed_knots <= 5 THEN '(0,5]'
    WHEN final_speed_knots > 5 AND final_speed_knots <= 10 THEN '(5,10]'
    WHEN final_speed_knots > 10 AND final_speed_knots <= 12 THEN '(10,12]'
    WHEN final_speed_knots > 12 AND final_speed_knots <= 15 THEN '(12,15]'
    WHEN final_speed_knots > 15 AND final_speed_knots <= 20 THEN '(15,20]'
    WHEN final_speed_knots > 20 AND final_speed_knots <= 25 THEN '(20,25]'
    WHEN final_speed_knots > 25 AND final_speed_knots <= 30 THEN '(25,30]'
    WHEN final_speed_knots > 30 AND final_speed_knots <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
  END

WHERE DATE(timestamp) > DATE('1900-01-01') 
  -- AND
  -- ("final_speed_knots" IS NULL OR 
  -- "speedbin" IS NULL OR
  -- "speedbin_implied" IS NULL OR
  -- "speedbin_calculated" IS NULL OR
  -- "speedbin_final"  IS NULL);
  -- TODO fix error: Cannot query over table 'benioff-ocean-initiative.whalesafe_v4.rgn_segs' without a filter over column(s) 'timestamp' that can be used for partition elimination at [9:1]
