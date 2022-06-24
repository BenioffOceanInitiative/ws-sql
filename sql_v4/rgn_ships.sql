CREATE OR REPLACE TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_ships` AS
WITH
  r AS (
    SELECT mmsi, FORMAT_DATE("%Y-%m", date) AS yr_mo
    FROM `benioff-ocean-initiative.whalesafe_v4.rgn_segs`
    WHERE DATE(timestamp) >= "2022-01-01"
    GROUP BY mmsi, yr_mo)
SELECT mmsi, feature.length_m, feature.tonnage_gt, registry.length_m, registry.tonnage_gt 
FROM r
LEFT JOIN
  `world-fishing-827.vessel_database.all_vessels_v20220401` AS g
  ON r.mmsi = CAST(g.identity.ssvid AS INT64)

-- REDO
CREATE TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_ships2` AS
WITH
  s AS (
    SELECT mmsi, FORMAT_DATE("%Y%m", date) AS yrmo
    FROM `benioff-ocean-initiative.whalesafe_v4.rgn_segs`
    WHERE DATE(timestamp) >= "2022-01-01"
    GROUP BY mmsi, yrmo
    ORDER BY mmsi, yrm
  ),
  g AS (
    SELECT 
      CAST(f.identity.ssvid AS INT64) AS mmsi, 
      _TABLE_SUFFIX AS date_yrmo01,
      f.identity.n_shipname AS shipname_gfw_i,
      f.identity.n_callsign AS callsign_gfw_i,
      r.mmsi_registry AS mmsi_gfw_r,
      r.shipname AS shipname_gfw_r,
      r.callsign AS callsign_gfw_r,
      r.geartype AS geartype_gfw_r,
      r.geartype_original AS geartype_gfw_ro,
      r.length_m AS length_m_gfw_r, 
      r.tonnage_gt AS tonnage_gt_gfw_r,
      r.owner AS owner_gfw_r,
      r.owner_address AS owner_address_gfw_r
      r.operator AS operator_gfw_r
    FROM `world-fishing-827.vessel_database.all_vessels_v*` AS f
    CROSS JOIN UNNEST(registry) as r 
    WHERE 
      matched IS TRUE AND
      -- f.identity.ssvid IS NOT NULL AND 
      --CAST(f.identity.ssvid AS INT64) != 0 AND
      LENGTH(f.identity.ssvid) = 9 AND
      (r.length_m IS NOT NULL OR
      r.tonnage_gt IS NOT NULL)
    ORDER BY mmsi, date_yrmo01
    LIMIT 1000
       -- AND
      -- _TABLE_SUFFIX IN (
        -- SELECT date_yrmo01 FROM s GROUP BY date_yrmo01)
  )
SELECT 
  s.mmsi, s.date_yrmo01, 
  shipname_gfw_i, callsign_gfw_i, 
  shipname_gfw_r, callsign_gfw_r,
  geartype_gfw_r, geartype_gfw_ro,
  length_m_gfw_r, tonnage_gt_gfw_r,
  owner_gfw_r, owner_address_gfw_r
FROM s
LEFT JOIN g
  ON s.mmsi = g.mmsi
ORDER BY mmsi, date_yrmo01;

-- single ship info per mmsi

SELECT 
  CAST(f.identity.ssvid AS INT64) AS mmsi, 
  _TABLE_SUFFIX AS date_yrmo01,
  f.identity.n_shipname AS shipname_gfw_i,
  f.identity.n_callsign AS callsign_gfw_i,
  LAST(r.mmsi_registry) AS mmsi_gfw_r,
  LAST(r.shipname) AS shipname_gfw_r,
  LAST(r.callsign) AS callsign_gfw_r,
  LAST(r.geartype) AS geartype_gfw_r,
  LAST(r.geartype_original) AS geartype_gfw_ro,
  LAST(r.length_m) AS length_m_gfw_r, 
  LAST(r.tonnage_gt) AS tonnage_gt_gfw_r,
  LAST(r.owner) AS owner_gfw_r,
  LAST(r.owner_address) AS owner_address_gfw_r
  LAST(r.operator) AS operator_gfw_r
FROM `world-fishing-827.vessel_database.all_vessels_v*` AS f
CROSS JOIN UNNEST(registry) as r 
WHERE 
  -- matched IS TRUE AND
  -- f.identity.ssvid IS NOT NULL AND 
  --CAST(f.identity.ssvid AS INT64) != 0 AND
  LENGTH(f.identity.ssvid) = 9 AND
  (r.length_m IS NOT NULL OR
    r.tonnage_gt IS NOT NULL) AND
  _TABLE_SUFFIX >= "20220101"
ORDER BY 
  matched, loose_match
  mmsi, date_yrmo01
GROUP BY
LIMIT 1000

-- single ship info per mmsi WITH SUBQUERY
WITH
  v_r_l AS (
    SELECT 
      CAST(v.identity.ssvid AS INT64) AS mmsi,
      "20220101" AS date_yrmo01,
      r.length_m
      -- r.tonnage_gt AS tonnage_gt
    FROM `world-fishing-827.vessel_database.all_vessels_v*` AS v
    CROSS JOIN UNNEST(registry) as r 
    WHERE 
      LENGTH(v.identity.ssvid) = 9 AND
      r.length_m IS NOT NULL AND
      --  r.tonnage_gt IS NOT NULL AND
      _TABLE_SUFFIX >= "20220101"
    ORDER BY 
      matched, loose_match DESC )
SELECT
  mmsi, 
  LAST(length_m) AS length_m
FROM v_r_l
GROUP BY mmsi



  CREATE TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_ships` AS
WITH

SELECT * from x where mmsi = 251847770

SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20220101` AS v
-- WHERE v.identity.ssvid = '271072320' -- tonnage_gt: NULL
WHERE v.identity.ssvid = '251847770' -- tonnage_gt: NULL
ORDER BY matched DESC, loose_match

WHERE v.identity.ssvid = '412440369' -- length_m: NULL
SELECT
  mmsi, 
  LAST(length_m) AS length_m
FROM v_r_l
GROUP BY mmsi



SELECT 
  mmsi, date_yrmo01,
  LAST_VALUE(geartype_original IGNORE NULLS) 
    OVER(PARTITION BY mmsi, date_yrmo01) 
    ORDER BY CASE WHEN val1 is NULL then 0 else 1 END DESC, t desc) AS val1,
        
    OVER 
    AS geartype_gfw_ro
  -- COALESCE(length_m) AS length_m_gfw_r, 
  -- COALESCE(tonnage_gt) AS tonnage_gt_gfw_r,
  -- COALESCE(owner) AS owner_gfw_r,
  -- COALESCE(owner_address) AS owner_address_gfw_r,
  -- COALESCE(operator) AS operator_gfw_r
FROM (
  
  )
GROUP BY
  mmsi, date_yrmo01
ORDER BY 
  mmsi, date_yrmo01
LIMIT 1000

-- INSPECT

SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20200401`
WHERE identity.ssvid = "367025860";

SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20200501`
CROSS JOIN UNNEST(registry)
WHERE identity.ssvid = "357";

SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20200101`
CROSS JOIN UNNEST(registry)
WHERE identity.ssvid = "1023535";
-- mmsi_registry: 366948840

SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20200101`
CROSS JOIN UNNEST(registry)
WHERE identity.ssvid = "100000000";
-- mmsi_registry: 

-- SUMMARY
SELECT mmsi, date_yrmo01 FROM `benioff-ocean-initiative.whalesafe_v4.rgn_ships2`
WHERE 
  length_m_gfw_r IS NOT NULL OR
  tonnage_gt_gfw_r IS NOT NULL;

-- OLD...



SELECT 
  CAST(f.identity.ssvid AS INT64) AS mmsi, 
  -- f.feature.length_m AS feature_length_m, 
  -- f.feature.tonnage_gt AS feature_tonnage_gt, 
  f.identity.shipname AS shipname_gfw_i,
  f.identity.callsign AS callsign_gfw_i,
  r.shipname AS shipname_gfw_r,
  r.callsign AS callsign_gfw_r,
  r.geartype AS geartype_gfw_r,
  r.geartyp_original AS geartype_gfw_ro,
  r.length_m AS length_m_gfw_r, 
  r.tonnage_gt AS tonnage_gt_gfw_r,
  r.owner AS owner_gfw_r,
  r.owner_address AS owner_address_gfw_r
FROM `world-fishing-827.vessel_database.all_vessels_v20220401` AS f
CROSS JOIN UNNEST(registry) as r 
WHERE 
  f.identity.ssvid IS NOT NULL AND 
  CAST(f.identity.ssvid AS INT64) != 0 AND
  (r.length_m IS NOT NULL OR
   r.tonnage_gt IS NOT NULL)


ALTER TABLE rgn_ships
  ADD COLUMN dim_length_m FLOAT,
  ADD COLUMN dim_tonnage_gt FLOAT,
  ADD COLUMN dim_src STRING,
  ADD COLUMN dim_date DATE


-- WITH 
--   a AS (SELECT identity.ssvid, registry FROM `world-fishing-827.vessel_database.all_vessels_v20220401`),
--    v CROSS JOIN UNNEST(a.registry) n LIMIT 100;

SELECT 
  CAST(g.identity.ssvid AS INT64) AS mmsi, 
  feature.length_m AS feature_length_m, 
  feature.tonnage_gt AS feature_tonnage_gt, 
  r.length_m AS registry_length_m, 
  r.tonnage_gt AS registry_tonnage_gt 
FROM `world-fishing-827.vessel_database.all_vessels_v20220401` AS f
CROSS JOIN UNNEST(registry) as r 
LIMIT 100

SELECT * FROM `spaceships` 
CROSS JOIN UNNEST(crew) as crew_member 
WHERE crew_member = "Zoe"


LIMIT 10000


SELECT *
FROM `world-fishing-827.vessel_database.all_vessels_v20220401` AS f
WHERE identity.ssvid IS NULL
LIMIT 10

-- use ssvid
SELECT ssvid, best.best_vessel_class, best.best_length_m, best.best_tonnage_gt
FROM `world-fishing-827.gfw_research.vi_ssvid_v20220401` 
WHERE 
  -- best.best_vessel_class IS NOT NULL OR
  best.best_length_m IS NOT NULL OR
  best.best_tonnage_gt IS NOT NULL
LIMIT 1000

SELECT ssvid, operator, best.best_vessel_class, best.best_length_m, best.best_tonnage_gt
FROM `world-fishing-827.gfw_research.vi_ssvid_v20220401` 
WHERE 
  -- best.best_vessel_class IS NOT NULL OR
  best.best_length_m IS NOT NULL OR
  best.best_tonnage_gt IS NOT NULL
LIMIT 1000


CAST(ssvid AS INT64) AS mmsi