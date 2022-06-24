--# add ihs column(s)
ALTER TABLE `whalesafe_v4.rgn_segs` 
  ADD COLUMN IF NOT EXISTS pull_date date,
  ADD COLUMN IF NOT EXISTS pull_source date,
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
UPDATE `whalesafe_v4.rgn_segs`
SET 
  length_m  = i.length,
  weight_gt = i.gt,
  operator  = i.operator
  ihs_pull_date = i.ihs_pull_date

SELECT
  mmsi, 
  i.length AS length_m, 
  i.gt AS weight_gt, 
  i.operator AS operator, 
  i.ihs_pull_date AS pull_date,
  'IHS' AS pull_source,
  s.date AS date,
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
  mmsi = 366867690;

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


