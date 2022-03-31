DECLARE rgn_str    STRING DEFAULT '{rgn}';
DECLARE period_str STRING DEFAULT '{period}';
-- DECLARE rgn_str    STRING DEFAULT 'CAN-GoStLawrence';
-- DECLARE period_str STRING DEFAULT 'last30days';

--# depends on: rgns_h3_segsum_create.sql

--# get min, max dates based on period_str
DECLARE max_date, min_date DATE;
SET (max_date) = (
  SELECT AS STRUCT MAX(DATE(timestamp)) 
  FROM `{tbl_rgn_segs}`
  WHERE rgn = rgn_str AND timestamp > '1900-01-01' );
SET (min_date) = (
  SELECT AS STRUCT CASE period_str
    WHEN 'last30days'  THEN DATE_SUB(max_date, INTERVAL 30 DAY)
    WHEN 'last5days'   THEN DATE_SUB(max_date, INTERVAL 5 DAY)
    WHEN 'last24hours' THEN DATE_SUB(max_date, INTERVAL 1 DAY)
  END AS min_date );
-- SELECT FORMAT('For %s: min_date = %t; max_date = %t', period_str, min_date, max_date) AS result;

DELETE 
FROM `{tbl_rgns_h3_segsum}`
WHERE
  rgn = rgn_str AND
  period = period_str;

INSERT `{tbl_rgns_h3_segsum}` (
  rgn, h3res, h3id, 
  length_m_gt10knots, length_m_all, pct_length_gt10knots, 
  period, date_min, date_max)
WITH
  --# s: intersect [s]egments with hexagons (of all h3res values)
  s AS (
    SELECT rgn, h3res, h3id, timestamp, final_speed_knots, ST_LENGTH(geog) AS length_m, geog
    FROM (
      SELECT h.rgn, h.h3res, h.h3id, s.timestamp, s.final_speed_knots, 
        ST_UNION(ST_DUMP(ST_INTERSECTION(h.geog, s.linestring), 1)) AS geog
      FROM (
          SELECT *
          FROM `{tbl_rgns_h3}`
          WHERE rgn = rgn_str )
          AS h
        INNER JOIN (
          SELECT *
          FROM `{tbl_rgn_segs}`
          WHERE 
            rgn = rgn_str AND
            DATE(timestamp) >= min_date AND
            DATE(timestamp) <  max_date )
          AS s 
        ON ST_Intersects(h.geog, s.linestring) )),
  --# g: summarize segments per hexagon, [g]reater than 10 knots
  g AS (
    SELECT 
      rgn, h3res, h3id, SUM(length_m) AS length_m_gt10knots
    FROM s
    WHERE 
      final_speed_knots > 10
    GROUP BY rgn, h3res, h3id ),
  --# a: summarize segments per hexagon, [a]ll
  a AS (
    SELECT 
      rgn, h3res, h3id, SUM(length_m) AS length_m_all
    FROM s
    GROUP BY rgn, h3res, h3id )
SELECT 
  rgn, h3res, h3id, 
  length_m_gt10knots, length_m_all,
  length_m_gt10knots / length_m_all AS pct_length_gt10knots, 
  period_str AS period, min_date AS date_min, max_date AS date_max
  FROM a
  LEFT JOIN g USING (rgn, h3res, h3id);

UPDATE `{tbl_rgns_h3_segsum}` SET
  hexbin_num = {sql_hexbins_num},
  hexbin_str = {sql_hexbins_str}
WHERE pct_length_gt10knots IS NOT NULL;

--# TEST: https://bigquerygeoviz.appspot.com
--# Query: 
-- SELECT 
--   h.rgn, h.h3res, h.h3id, 
--   s.length_m_gt10knots, s.length_m_all, s.pct_length_gt10knots, 
--   bin_pct_length_gt10knots,
--   s.period, s.date_min, s.date_max,
--   h.geog
-- FROM 
--   `benioff-ocean-initiative.whalesafe_v4.rgns_h3` 
--   AS h
-- LEFT JOIN 
--   (SELECT *
--     FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3_segsum`
--     WHERE 
--     period = 'last_30days') 
--   AS s
--   USING (h3id)
-- WHERE
--   h.rgn = 'CAN-GoStLawrence' AND
--   h.h3res = 5; -- 4, 5, 6 or 7
--# Style: 
--# - Data-driven; linear; pct_length_gt10knots; 
--# - Domain: 0, 0.5, 1
--# - Range: #4CAF50 (green), #FFC107 (yellow), #F44336 (red)