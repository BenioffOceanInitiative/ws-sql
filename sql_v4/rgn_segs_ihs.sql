-- ship_attrs_create.sql
-- mmsi | date | attribute | value | source | source_date
-- 366867690 | '2022-01-02' | 'length_m' | 'IHS' | 
CREATE OR REPLACE TABLE `benioff-ocean-initiative.whalesafe_v4.ship_attrs` (
  mmsi INT64,      -- mmsi
  date DATE,       -- date ship observed
  attr STRING,     -- length_m, weight_gt, operator, ...
  val_flt FLOAT64, -- value stored as a number, eg attr='length_m'
  val_str STRING,  -- value stored as a string, eg attr='operator'
  src STRING,      -- IHS or GFW
  src_date DATE,   -- YYYY-MM-DD auto monthly for GFW; manual monthly for IHS
  days_diff INT64  -- difference in days between observed and source 
  )
CLUSTER BY mmsi, date;

-- get ship attributes (length_m, weight_gt, operator) per mmsi & date
-- IHS (as priority source):
-- * length_m
-- * weight_gt
-- * operator
-- GFW (if not already defined by IHS):
-- * length_m
-- * weight_gt
-- * operator

-- This data is to be used at these spatial scales:
-- REGION
--   filter by big ships, ie minimum length_m or weight_gt
-- ZONE
--   filter by IHS operator (not GFW yet)

-- DELETE `whalesafe_v4.ship_attrs` WHERE TRUE;

-- IHS length_m: insert/update the latest in ship_attrs from IHS
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
        length AS val_flt, 
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date
      FROM `whalesafe_v4.ihs_vessels_*`
      WHERE
        length IS NOT NULL AND length != 0), -- AND --), 
        -- mmsi = 368066040 ),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_flt,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'length_m' AS attr,
      FIRST_VALUE(val_flt) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_flt,
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
    val_flt   = z.val_flt,
    src       = z.src,
    src_date  = z.src_date,
    days_diff = z.days_diff
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_flt, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_flt, src, src_date, days_diff)

-- IHS weight_gt: insert/update the latest in ship_attrs from IHS
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
        gt AS val_flt, 
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date
      FROM `whalesafe_v4.ihs_vessels_*`
      WHERE
        gt IS NOT NULL AND gt != 0), -- AND --), 
        -- mmsi = 368066040 ),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_flt,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'weight_gt' AS attr,
      FIRST_VALUE(val_flt) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_flt,
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
    val_flt   = z.val_flt,
    src       = z.src,
    src_date  = z.src_date,
    days_diff = z.days_diff
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_flt, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_flt, src, src_date, days_diff)

-- IHS operator: insert/update the latest in ship_attrs from IHS
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
        operator AS val_str, 
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date
      FROM `whalesafe_v4.ihs_vessels_*`
      WHERE
        length IS NOT NULL AND length != 0), -- AND --), 
        -- mmsi = 368066040 ),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_str,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'operator' AS attr,
      FIRST_VALUE(val_str) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_str,
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
    val_str   = z.val_str,
    src       = z.src,
    src_date  = z.src_date,
    days_diff = z.days_diff
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_str, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_str, src, src_date, days_diff)


-- GFW length_m: insert missing into ship_attrs from GFW
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
        timestamp > '2022-01-01'), 
    i AS (
      -- get non-null GFW attribute with source date
      SELECT 
        CAST(v.identity.ssvid AS INT64) AS mmsi,
        -- matched, loose_match,
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date,
        r.length_m AS val_flt
      FROM `world-fishing-827.vessel_database.all_vessels_v*` AS v
      CROSS JOIN UNNEST(registry) as r 
      WHERE 
        LENGTH(v.identity.ssvid) = 9 AND
        --  r.tonnage_gt IS NOT NULL
        r.length_m IS NOT NULL),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_flt,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'length_m' AS attr,
      FIRST_VALUE(val_flt) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_flt,
      'GFW' AS src,
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
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_flt, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_flt, src, src_date, days_diff)

-- GFW weigh_gt: insert missing into ship_attrs from GFW
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
        timestamp > '2022-01-01'),
    i AS (
      -- get non-null GFW attribute with source date
      SELECT 
        CAST(v.identity.ssvid AS INT64) AS mmsi,
        -- matched, loose_match,
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date,
        r.tonnage_gt AS val_flt
      FROM `world-fishing-827.vessel_database.all_vessels_v*` AS v
      CROSS JOIN UNNEST(registry) as r 
      WHERE 
        LENGTH(v.identity.ssvid) = 9 AND
        r.tonnage_gt IS NOT NULL),
    v AS (
      -- merge rgn_segs with IHS and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_flt,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'weight_gt' AS attr,
      FIRST_VALUE(val_flt) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_flt,
      'GFW' AS src,
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
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_flt, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_flt, src, src_date, days_diff)

-- GFW operator: insert missing into ship_attrs from GFW
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
      -- get non-null GFW attribute with source date
      SELECT 
        CAST(v.identity.ssvid AS INT64) AS mmsi,
        -- matched, loose_match,
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS src_date,
        r.operator AS val_str
      FROM `world-fishing-827.vessel_database.all_vessels_v*` AS v
      CROSS JOIN UNNEST(registry) as r 
      WHERE 
        LENGTH(v.identity.ssvid) = 9 AND
        r.operator IS NOT NULL),
    v AS (
      -- merge rgn_segs with GFW and calculate days difference
      --   between observed segment and ship information 
      SELECT
        s.mmsi,
        s.date,
        i.val_str,
        i.src_date,
        ABS(DATE_DIFF(s.date, i.src_date, DAY)) as days_diff
        FROM s 
        INNER JOIN i USING (mmsi))
    -- reduce to single attribute value per (mmsi, date) based on IHS data closest in time
    SELECT DISTINCT
      mmsi,
      date,
      'operator' AS attr,
      FIRST_VALUE(val_str) OVER (PARTITION BY mmsi, date ORDER BY days_diff ASC)
        AS val_str,
      'GFW' AS src,
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
WHEN NOT MATCHED THEN
  INSERT(mmsi, date, attr, val_str, src, src_date, days_diff)
  VALUES(mmsi, date, attr, val_str, src, src_date, days_diff)

-- check that values got inserted
SELECT attr, count(*) AS cnt FROM `benioff-ocean-initiative.whalesafe_v4.ship_attrs`
WHERE
  src = 'GFW'
GROUP BY attr

-- next steps:
-- II. finish report card table outputs
-- III. finish spatial summary 
  -- 3. update rgn_segs_hex
  -- 4. update the docker container to have latest postgis for dynamic geometry
  -- 5. update the hexagons with buffer
  -- 6. seperate analyze hexagons with spatial detail for running spatial intersection given area
  -- 7. results hexagon that allows LGND to overlay their land over whole hexagons
  -- 8. dynamic geom
