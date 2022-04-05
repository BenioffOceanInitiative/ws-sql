UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` SET
  final_speed_knots = ROUND((
    CASE
      WHEN speed_knots         BETWEEN 0.001 AND 50 THEN speed_knots
      WHEN calculated_knots    BETWEEN 0.001 AND 50 THEN calculated_knots
      WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
      ELSE NULL
    END), 3),
  speedbin_str            = 
  CASE

            WHEN  (speed_knots * 1) > 0
              AND (speed_knots * 1) <= 0 THEN '[0]'
        

            WHEN  (speed_knots * 1) > 0
              AND (speed_knots * 1) <= 5 THEN '(0,5]'
        

            WHEN  (speed_knots * 1) > 5
              AND (speed_knots * 1) <= 10 THEN '(5,10]'
        

            WHEN  (speed_knots * 1) > 10
              AND (speed_knots * 1) <= 12 THEN '(10,12]'
        

            WHEN  (speed_knots * 1) > 12
              AND (speed_knots * 1) <= 15 THEN '(12,15]'
        

            WHEN  (speed_knots * 1) > 15
              AND (speed_knots * 1) <= 20 THEN '(15,20]'
        

            WHEN  (speed_knots * 1) > 20
              AND (speed_knots * 1) <= 25 THEN '(20,25]'
        

            WHEN  (speed_knots * 1) > 25
              AND (speed_knots * 1) <= 30 THEN '(25,30]'
        

            WHEN  (speed_knots * 1) > 30
              AND (speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
        
  END
,
  speedbin_num            = 
  CASE

            WHEN  (speed_knots * 1) > 0
              AND (speed_knots * 1) <= 0 THEN 0
        

            WHEN  (speed_knots * 1) > 0
              AND (speed_knots * 1) <= 5 THEN 5
        

            WHEN  (speed_knots * 1) > 5
              AND (speed_knots * 1) <= 10 THEN 10
        

            WHEN  (speed_knots * 1) > 10
              AND (speed_knots * 1) <= 12 THEN 12
        

            WHEN  (speed_knots * 1) > 12
              AND (speed_knots * 1) <= 15 THEN 15
        

            WHEN  (speed_knots * 1) > 15
              AND (speed_knots * 1) <= 20 THEN 20
        

            WHEN  (speed_knots * 1) > 20
              AND (speed_knots * 1) <= 25 THEN 25
        

            WHEN  (speed_knots * 1) > 25
              AND (speed_knots * 1) <= 30 THEN 30
        

            WHEN  (speed_knots * 1) > 30
              AND (speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN 300
        
  END
,
  speedbin_implied_str    = 
  CASE

            WHEN  (implied_speed_knots * 1) > 0
              AND (implied_speed_knots * 1) <= 0 THEN '[0]'
        

            WHEN  (implied_speed_knots * 1) > 0
              AND (implied_speed_knots * 1) <= 5 THEN '(0,5]'
        

            WHEN  (implied_speed_knots * 1) > 5
              AND (implied_speed_knots * 1) <= 10 THEN '(5,10]'
        

            WHEN  (implied_speed_knots * 1) > 10
              AND (implied_speed_knots * 1) <= 12 THEN '(10,12]'
        

            WHEN  (implied_speed_knots * 1) > 12
              AND (implied_speed_knots * 1) <= 15 THEN '(12,15]'
        

            WHEN  (implied_speed_knots * 1) > 15
              AND (implied_speed_knots * 1) <= 20 THEN '(15,20]'
        

            WHEN  (implied_speed_knots * 1) > 20
              AND (implied_speed_knots * 1) <= 25 THEN '(20,25]'
        

            WHEN  (implied_speed_knots * 1) > 25
              AND (implied_speed_knots * 1) <= 30 THEN '(25,30]'
        

            WHEN  (implied_speed_knots * 1) > 30
              AND (implied_speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
        
  END
,
  speedbin_implied_num    = 
  CASE

            WHEN  (implied_speed_knots * 1) > 0
              AND (implied_speed_knots * 1) <= 0 THEN 0
        

            WHEN  (implied_speed_knots * 1) > 0
              AND (implied_speed_knots * 1) <= 5 THEN 5
        

            WHEN  (implied_speed_knots * 1) > 5
              AND (implied_speed_knots * 1) <= 10 THEN 10
        

            WHEN  (implied_speed_knots * 1) > 10
              AND (implied_speed_knots * 1) <= 12 THEN 12
        

            WHEN  (implied_speed_knots * 1) > 12
              AND (implied_speed_knots * 1) <= 15 THEN 15
        

            WHEN  (implied_speed_knots * 1) > 15
              AND (implied_speed_knots * 1) <= 20 THEN 20
        

            WHEN  (implied_speed_knots * 1) > 20
              AND (implied_speed_knots * 1) <= 25 THEN 25
        

            WHEN  (implied_speed_knots * 1) > 25
              AND (implied_speed_knots * 1) <= 30 THEN 30
        

            WHEN  (implied_speed_knots * 1) > 30
              AND (implied_speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN 300
        
  END
,
  speedbin_calculated_str = 
  CASE

            WHEN  (calculated_knots * 1) > 0
              AND (calculated_knots * 1) <= 0 THEN '[0]'
        

            WHEN  (calculated_knots * 1) > 0
              AND (calculated_knots * 1) <= 5 THEN '(0,5]'
        

            WHEN  (calculated_knots * 1) > 5
              AND (calculated_knots * 1) <= 10 THEN '(5,10]'
        

            WHEN  (calculated_knots * 1) > 10
              AND (calculated_knots * 1) <= 12 THEN '(10,12]'
        

            WHEN  (calculated_knots * 1) > 12
              AND (calculated_knots * 1) <= 15 THEN '(12,15]'
        

            WHEN  (calculated_knots * 1) > 15
              AND (calculated_knots * 1) <= 20 THEN '(15,20]'
        

            WHEN  (calculated_knots * 1) > 20
              AND (calculated_knots * 1) <= 25 THEN '(20,25]'
        

            WHEN  (calculated_knots * 1) > 25
              AND (calculated_knots * 1) <= 30 THEN '(25,30]'
        

            WHEN  (calculated_knots * 1) > 30
              AND (calculated_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
        
  END
,
  speedbin_calculated_num = 
  CASE

            WHEN  (calculated_knots * 1) > 0
              AND (calculated_knots * 1) <= 0 THEN 0
        

            WHEN  (calculated_knots * 1) > 0
              AND (calculated_knots * 1) <= 5 THEN 5
        

            WHEN  (calculated_knots * 1) > 5
              AND (calculated_knots * 1) <= 10 THEN 10
        

            WHEN  (calculated_knots * 1) > 10
              AND (calculated_knots * 1) <= 12 THEN 12
        

            WHEN  (calculated_knots * 1) > 12
              AND (calculated_knots * 1) <= 15 THEN 15
        

            WHEN  (calculated_knots * 1) > 15
              AND (calculated_knots * 1) <= 20 THEN 20
        

            WHEN  (calculated_knots * 1) > 20
              AND (calculated_knots * 1) <= 25 THEN 25
        

            WHEN  (calculated_knots * 1) > 25
              AND (calculated_knots * 1) <= 30 THEN 30
        

            WHEN  (calculated_knots * 1) > 30
              AND (calculated_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN 300
        
  END
,
  speedbin_final_str      = 
  CASE

            WHEN  (final_speed_knots * 1) > 0
              AND (final_speed_knots * 1) <= 0 THEN '[0]'
        

            WHEN  (final_speed_knots * 1) > 0
              AND (final_speed_knots * 1) <= 5 THEN '(0,5]'
        

            WHEN  (final_speed_knots * 1) > 5
              AND (final_speed_knots * 1) <= 10 THEN '(5,10]'
        

            WHEN  (final_speed_knots * 1) > 10
              AND (final_speed_knots * 1) <= 12 THEN '(10,12]'
        

            WHEN  (final_speed_knots * 1) > 12
              AND (final_speed_knots * 1) <= 15 THEN '(12,15]'
        

            WHEN  (final_speed_knots * 1) > 15
              AND (final_speed_knots * 1) <= 20 THEN '(15,20]'
        

            WHEN  (final_speed_knots * 1) > 20
              AND (final_speed_knots * 1) <= 25 THEN '(20,25]'
        

            WHEN  (final_speed_knots * 1) > 25
              AND (final_speed_knots * 1) <= 30 THEN '(25,30]'
        

            WHEN  (final_speed_knots * 1) > 30
              AND (final_speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN '(30,Inf]'
        
  END
,
  speedbin_final_num      = 
  CASE

            WHEN  (final_speed_knots * 1) > 0
              AND (final_speed_knots * 1) <= 0 THEN 0
        

            WHEN  (final_speed_knots * 1) > 0
              AND (final_speed_knots * 1) <= 5 THEN 5
        

            WHEN  (final_speed_knots * 1) > 5
              AND (final_speed_knots * 1) <= 10 THEN 10
        

            WHEN  (final_speed_knots * 1) > 10
              AND (final_speed_knots * 1) <= 12 THEN 12
        

            WHEN  (final_speed_knots * 1) > 12
              AND (final_speed_knots * 1) <= 15 THEN 15
        

            WHEN  (final_speed_knots * 1) > 15
              AND (final_speed_knots * 1) <= 20 THEN 20
        

            WHEN  (final_speed_knots * 1) > 20
              AND (final_speed_knots * 1) <= 25 THEN 25
        

            WHEN  (final_speed_knots * 1) > 25
              AND (final_speed_knots * 1) <= 30 THEN 30
        

            WHEN  (final_speed_knots * 1) > 30
              AND (final_speed_knots * 1) <= CAST('Infinity' AS FLOAT64) THEN 300
        
  END

WHERE 
  DATE(timestamp) > DATE('1900-01-01') AND
  rgn = 'USA-East';
  -- AND
  -- ("final_speed_knots" IS NULL OR 
  -- "speedbin" IS NULL OR
  -- "speedbin_implied" IS NULL OR
  -- "speedbin_calculated" IS NULL OR
  -- "speedbin_final"  IS NULL);
  -- TODO fix error: Cannot query over table 'benioff-ocean-initiative.whalesafe_v4.rgn_segs' without a filter over column(s) 'timestamp' that can be used for partition elimination at [9:1]
