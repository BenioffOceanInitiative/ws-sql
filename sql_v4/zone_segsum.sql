DECLARE rgn_str    STRING DEFAULT '{rgn}';
DECLARE zone_str   STRING DEFAULT '{zone}';
DECLARE period_str STRING DEFAULT '{period}';
-- DECLARE rgn_str    STRING DEFAULT 'USA-West';
-- DECLARE zone_str   STRING DEFAULT 'SoCal-VSR';
-- DECLARE period_str STRING DEFAULT 'last30days';

--# depends on: rgns_h3_segsum_create.sql

--# get min, max dates based on period_str
DECLARE max_date, min_date DATE;
SET (max_date) = (
  SELECT AS STRUCT MAX(DATE(timestamp)) 
  FROM `whalesafe_v4.zone_segsum`
  WHERE 
    rgn = rgn_str AND 
    zone = zone_str AND 
    DATE(timestamp) >= DATE('{date_beg:%Y-%m-%d}') AND
		DATE(timestamp) <= DATE('{date_end:%Y-%m-%d}') );
SET (min_date) = (
  SELECT AS STRUCT CASE period_str
    WHEN 'last30days'  THEN DATE_SUB(max_date, INTERVAL 30 DAY)
    WHEN 'last5days'   THEN DATE_SUB(max_date, INTERVAL 5 DAY)
    WHEN 'last24hours' THEN DATE_SUB(max_date, INTERVAL 1 DAY)
  END AS min_date );
-- SELECT FORMAT('For %s: min_date = %t; max_date = %t', period_str, min_date, max_date) AS result;

DELETE 
FROM `whalesafe_v4.zone_segsum`
WHERE
  rgn = rgn_str AND
  zone = zone_str AND 
  period = period_str;

INSERT `whalesafe_v4.zone_segsum` (
  rgn, h3res, h3id, 
  length_m_gt10knots, length_m_all, pct_length_gt10knots, 
  period, date_min, date_max
)
SELECT 
  s.*,
  period_str AS period
FROM `whalesafe_v4.zone_segs` s
WHERE
  rgn = rgn_str AND
  zone = zone_str AND 
  DATE(timestamp) >= min_date AND
  DATE(timestamp) <  max_date
  )
SELECT 
  rgn, h3res, h3id, 
  COALESCE(length_m_gt10knots, 0) AS length_m_gt10knots, 
  COALESCE(length_m_all, 0) AS length_m_all,
  COALESCE(length_m_gt10knots, 0) / COALESCE(length_m_all, 0) AS pct_length_gt10knots, 
  period_str AS period, min_date AS date_min, max_date AS date_max
  FROM a
  LEFT JOIN g USING (rgn, h3res, h3id);

UPDATE `{tbl_rgns_h3_segsum}` SET
  hexbin_num = {sql_hexbins_num},
  hexbin_str = {sql_hexbins_str}
WHERE pct_length_gt10knots IS NOT NULL;

--# Example MapBox GL JS green-red hex viz: https://jsfiddle.net/bdbest/hyLt0782/34/
--# TEST: https://bigquerygeoviz.appspot.com
--# Query: 
-- SELECT 
--   h.rgn, h.h3res, h.h3id, 
--   s.length_m_gt10knots, s.length_m_all, s.pct_length_gt10knots, 
--   s.hexbin_num, s.hexbin_str,
--   s.period, s.date_min, s.date_max,
--   h.geog
-- FROM 
--   `{tbl_rgns_h3}` 
--   AS h
-- LEFT JOIN 
--   (SELECT *
--     FROM `{tbl_rgns_h3_segsum}`
--     WHERE 
--     period = '{period}') 
--   AS s
--   USING (h3id)
-- WHERE
--   h.rgn = '{rgn}' AND
--   h.h3res = 4; -- 4, 5, 6 or 7
--# Style: 
--# - Data-driven; linear; pct_length_gt10knots; 
--# - Domain: 0, 0.5, 1
--# - Range: #4CAF50 (green), #FFC107 (yellow), #F44336 (red)