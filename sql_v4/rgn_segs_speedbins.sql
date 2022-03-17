
SELECT *,
  ROUND((
    CASE
      WHEN speed_knots         BETWEEN 0.001 AND 50 THEN speed_knots
      WHEN calculated_knots    BETWEEN 0.001 AND 50 THEN calculated_knots
      WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
      ELSE NULL
    END), 3) AS final_speed_knots



--# add columns
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS final_speed_knots NUMERIC,
  ADD COLUMN IF NOT EXISTS speedbin STRING,
  ADD COLUMN IF NOT EXISTS implied_speedbin STRING,
  ADD COLUMN IF NOT EXISTS calculated_speedbin STRING,
  ADD COLUMN IF NOT EXISTS final_speedbin STRING;


INSERT INTO `{tbl_rgn_segs}`
  SELECT *,
  CASE 
    WHEN speed_knots =  0 THEN 0
    WHEN speed_knots >  0 AND speed_knots <= 10 THEN 1
    WHEN speed_knots > 10 AND speed_knots <= 12 THEN 2
    WHEN speed_knots > 12 AND speed_knots <= 15 THEN 3
    WHEN speed_knots > 15 AND speed_knots <= 50 THEN 4
    ELSE 5
  END AS speed_bin_num,
  -- Assign speed bin number for 'speed_knots'
  CASE 
    WHEN implied_speed_knots =  0 THEN 0
    WHEN implied_speed_knots >  0 AND implied_speed_knots <= 10 THEN 1
    WHEN implied_speed_knots > 10 AND implied_speed_knots <= 12 THEN 2
    WHEN implied_speed_knots > 12 AND implied_speed_knots <= 15 THEN 3
    WHEN implied_speed_knots > 15 AND implied_speed_knots <= 50 THEN 4
    ELSE 5
  END AS implied_speed_bin_num,
  -- Assign speed bin number for 'implied_speed'
  CASE
    WHEN calculated_knots =  0 THEN 0
    WHEN calculated_knots >  0 AND calculated_knots <= 10 THEN 1
    WHEN calculated_knots > 10 AND calculated_knots <= 12 THEN 2
    WHEN calculated_knots > 12 AND calculated_knots <= 15 THEN 3
    WHEN calculated_knots > 15 AND calculated_knots <= 50 THEN 4
    ELSE 5
  END AS calculated_speed_bin_num,
  -- Assign speed bin number for 'calculated_knots'
  CASE 
    WHEN final_speed_knots =  0 THEN 0
    WHEN final_speed_knots >  0 AND final_speed_knots <= 10 THEN 1
    WHEN final_speed_knots > 10 AND final_speed_knots <= 12 THEN 2
    WHEN final_speed_knots > 12 AND final_speed_knots <= 15 THEN 3
    WHEN final_speed_knots > 15 AND final_speed_knots <= 50 THEN 4
    ELSE 5
  END AS final_speed_bin_num
-- # Assign speed bin number for 'final_speed_knots'.
-- # This is the field used for aggregation in the segments_agg script
FROM (