--# add ihs column(s)
ALTER TABLE `whalesafe_v4.rgn_segs` 
  ADD COLUMN IF NOT EXISTS vessel_pull_date date,
  ADD COLUMN IF NOT EXISTS vessel_pull_source date,
  ADD COLUMN IF NOT EXISTS length_m NUMERIC,
  ADD COLUMN IF NOT EXISTS weight_gt NUMERIC,
  ADD COLUMN IF NOT EXISTS operator STRING; --# gross tonnage for union with ihs data
 
-- after upload via web of csv, fix FLOAT to INT of mmsi
CREATE OR REPLACE TABLE 
`benioff-ocean-initiative.whalesafe_v4.ihs_vessels_20211123` 
AS
SELECT *
  REPLACE(CAST(mmsi AS Integer) AS mmsi)
FROM 
`benioff-ocean-initiative.whalesafe_v4.ihs_vessels_20211123`

SELECT s.mmsi, s.date, COUNT(*) AS cnt
FROM `whalesafe_v4.rgn_segs` AS s
INNER JOIN 
  -- `whale safe_v4.ihs_vessels_20220513` AS i
  `whalesafe_v4.ihs_vessels_20211123` AS i
   ON s.mmsi = i.mmsi
WHERE 
  DATE(timestamp) >= '2022-01-01' AND
  DATE(timestamp) <  '2022-02-01'
GROUP BY s.mmsi, s.date
ORDER BY cnt DESC;


-- mmsi = 366867690: most frequent rgn_segs for 2022-01-01
UPDATE `whalesafe_v4.rgn_segs` s
SET 
  length_m  = i.length,
  weight_gt = i.gt,
  operator  = i.operator
  vessel_pull_date   = i.vessel_pull_date,
  vessel_pull_source = vessel_pull_source
FROM (
  SELECT
    mmsi, 
    s.date AS date,
    i.length AS length_m, 
    i.gt AS weight_gt, 
    i.operator AS operator, 
    i.ihs_pull_date AS pull_date,
    'IHS' AS vessel_pull_source,
    s.date AS vessel_pull_date,
    ABS(DATE_DIFF(s.date, i.ihs_pull_date, DAY)) as days_diff
  FROM `whalesafe_v4.rgn_segs` AS s
  INNER JOIN (
    SELECT 
      mmsi, length, gt, operator,
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS ihs_pull_date
    FROM `whalesafe_v4.ihs_vessels_*`) AS i 
    USING (mmsi)
  WHERE 
    timestamp > '2022-01-01' AND
    date = '2022-01-01' AND
    mmsi = 366867690
  ORDER BY date  
   ) AS i
WHERE
  (s.length_m  IS NULL OR
   s.weight_gt IS NULL OR
   s.operator  IS NULL) AND
  s.mmsi = i.mmsi

-- NEW...

-- vessel_attributes_create.sql
-- mmsi | date | attribute | value | source | source_date
-- 366867690 | '2022-01-02' | 'length_m' | 'IHS' | 
CREATE OR REPLACE TABLE `benioff-ocean-initiative.whalesafe_v4.ship_attrs` (
  mmsi INT64,
  date DATE,
  attr STRING,
  val_flt FLOAT64,
  vl_str STRING,
  src STRING,
  src_date DATE,
  days_diff INT64)
CLUSTER BY mmsi, date;

-- DELETE `whalesafe_v4.ship_attrs` WHERE TRUE;

MERGE `benioff-ocean-initiative.whalesafe_v4.ship_attrs` a
USING (
  WITH 
    s AS (
      -- get distinct (mmsi, date) from rgn_segs
      SELECT DISTINCT
        mmsi, 
        date,
      FROM `whalesafe_v4.rgn_segs`
      WHERE 
        timestamp > '2022-01-01'), --AND 
        --timestamp > '2022-01-01' AND timestamp < '2022-01-10' AND
        --mmsi = 366867690),
    i AS (
      -- get non-null IHS attribute with source date
      SELECT 
        mmsi, 
        length AS val, 
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date
      FROM `whalesafe_v4.ihs_vessels_*`
      WHERE
        length IS NOT NULL AND length != 0), -- AND
        -- mmsi = 368066040 ),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'length_m' AS attr,
      FIRST_VALUE(val) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val,
      'IHS' AS src,
      FIRST_VALUE(src_date) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS src_date,
      FIRST_VALUE(days_diff) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS days_diff
    FROM v
    ORDER BY mmsi, date) AS z
ON 
  a.mmsi = z.mmsi AND
  a.date = z.date AND
  a.attr = z.attr
WHEN MATCHED AND z.days_diff < a.days_diff THEN
  UPDATE SET 
    mmsi      = z.mmsi,
    date      = z.date,
    attr      = z.attr,
    val       = z.val,
    src       = z.src,
    src_date  = z.src_date,
    days_diff = z.days_diff
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val, src, src_date, days_diff)

-- look at result:
-- SELECT * FROM `whalesafe_v4.vessel_attributes` ORDER BY mmsi, date;

-- mess with a few values to see if update works:
-- UPDATE `whalesafe_v4.vessel_attributes`
-- SET days_diff = 100
-- WHERE days_diff < 42;

-- next steps:
-- I. get all possible length/weight/operator associated with segs
  -- 1. run with UPDATE on rgn_segs
  -- 2. do the same with GFW data WHERE length/weight/operator IS NULL
-- II. finish report card table outputs
-- III. finish spatial summary 
  -- 3. update rgn_segs_hex
  -- 4. update the docker container to have latest postgis for dynamic geometry
  -- 5. update the hexagons with buffer
  -- 6. seperate analyze hexagons with spatial detail for running spatial intersection given area
  -- 7. results hexagon that allows LGND to overlay their land over whole hexagons
  -- 8. dynamic geom


