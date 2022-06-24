-- GFW [mmsi, length, tonnage] after 2022-01
CREATE OR REPLACE TABLE `benioff-ocean-initiative.whalesafe_v4.gfw_vessels` AS
  SELECT 
    PARSE_DATE('%Y%m%d',  _TABLE_SUFFIX) AS date_src,
    CAST(v.identity.ssvid AS INT64) AS mmsi,
    ARRAY_AGG(
      r.shipname ORDER BY r.shipname DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS shipname,
    ARRAY_AGG(
      r.length_m ORDER BY r.length_m DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS length_m,
    ARRAY_AGG(
      r.tonnage_gt ORDER BY r.tonnage_gt DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS tonnage_gt,
    ARRAY_AGG(
      r.owner ORDER BY r.owner DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS owner,
    ARRAY_AGG(
      r.owner_address ORDER BY r.owner_address DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS owner_address,
    ARRAY_AGG(
      r.operator ORDER BY r.operator DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] 
      AS operator
  FROM `world-fishing-827.vessel_database.all_vessels_v*` AS v
  CROSS JOIN UNNEST(registry) as r 
  WHERE 
    LENGTH(v.identity.ssvid) = 9 AND
    _TABLE_SUFFIX >= "20220101"
  GROUP BY 
    mmsi, date_src
  ORDER BY
    date_src, mmsi


CREATE OR REPLACE TABLE whalesafe_v4.gfw_vessels_20220101 AS
SELECT * EXCEPT(date_src) FROM 
`benioff-ocean-initiative.whalesafe_v4.gfw_vessels` 
WHERE date_src = '2022-01-01'

CREATE OR REPLACE TABLE whalesafe_v4.gfw_vessels_20220101 AS
SELECT * EXCEPT(date_src) FROM 
`benioff-ocean-initiative.whalesafe_v4.gfw_vessels` 
WHERE date_src = '2022-01-01'



CREATE OR REPLACE TABLE whalesafe_v4.ihs_vessels_20220513 AS
SELECT * EXCEPT(date_pulled) FROM 
`benioff-ocean-initiative.whalesafe_v4.ihs` 
WHERE date_pulled = '2022-05-13' -- 2021-11-23 -- 2022-05-13

CREATE OR REPLACE TABLE whalesafe_v4.ihs_vessels_20211123 AS
SELECT * EXCEPT(date_pulled) FROM 
`benioff-ocean-initiative.whalesafe_v4.ihs` 
WHERE date_pulled = '2021-11-23'; -- 2021-11-23 -- 2022-05-13