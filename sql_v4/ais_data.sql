-- -- # Benioff Ocean Initiative: 2021-01-28
-- -- #                    __       __
-- -- #                     '.'--.--'.-'
-- -- #       .,_------.___,   \' r'
-- -- #       ', '-._a      '-' .'
-- -- #        '.    '-'Y \._  /
-- -- #          '--;____'--.'-,
-- -- #Sean Goral/..'Ben Best''\ Callie Steffen ''' Morgan Visalli # --

-- -- # -- # Step 0: DECLARE newest timestamp.
-- -- # IF STARTING FROM SCRATCH, USE FIRST DECLARE STATEMENT AND COMMENT SECOND DECLARE STATEMENT BELOW:
-- -- DECLARE
-- -- new_ais_ts DEFAULT
-- -- (SELECT SAFE_CAST('2016-12-31 12:59:59 UTC' AS TIMESTAMP));

-- -- # -- # IF UPDATING: DECLARE newest AIS timestamp from `whalesafe_v4.ais_data` table as 'new_ais_ts'
DECLARE
-- new_ais_ts DEFAULT(
--           SELECT
--           (MAX(timestamp))
--           FROM `whalesafe_v4.ais_data`
--           WHERE
--           DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) -- # query last 2 MONTHS for max timestamp
--           LIMIT 1);
new_ais_ts DEFAULT(TIMESTAMP('2021-10-13')); -- DEBUG: manually set to day before max(timestamp) FROM gfw_daily_spireonly

-- # -- # Step 1: Make temporary table `temp_ais_data` for incoming GFW AIS data
CREATE TEMPORARY TABLE `temp_ais_data` (
mmsi INT64,
timestamp TIMESTAMP,
lon FLOAT64,
lat FLOAT64,
speed_knots NUMERIC,
implied_speed_knots NUMERIC,
source STRING,
seg_id STRING,
good_seg BOOL,
overlapping_and_short BOOL,
region STRING
);

-- -- # -- # Step 2: Insert GFW AIS data (with a timestamp > than the max tiestamp in the existing `ais_data` table) into the `temp_ais_data` table
INSERT INTO `temp_ais_data`
SELECT
SAFE_CAST (ais.ssvid AS INT64) AS mmsi, -- # CAST ssvid to NUMERIC and rename AS mmsi
ais.timestamp,
ais.lon,
ais.lat,
SAFE_CAST (ais.speed_knots AS NUMERIC) AS speed_knots, -- # CAST speed_knots to NUMERIC
SAFE_CAST (ais.implied_speed_knots AS NUMERIC) AS implied_speed_knots, --# CAST implied_speed_knots to NUMERIC
ais.source,
ais.seg_id,
segs.good_seg,
segs.overlapping_and_short,
CASE WHEN
lat >= (33.290)     -- # 33.2998838
AND lat <= (34.5739)    -- # 34.5736988
AND lon >= (- 125.013)   -- # -121.0392169
AND lon <= (- 117.460)  -- # -117.4701519
THEN 'sc' -- Southern CA Region.
WHEN
lat > (34.5739)     -- # 33.2998838
AND lat <= (35.557)    -- # 34.5736988
AND lon >= (- 125.013)   -- # -121.0392169
AND lon <= (- 117.460)  -- # -117.4701519
THEN 'cc' -- Central Coast CA Region.
WHEN
lat > (35.557)     -- # 33.2998838
AND lat <= (39.032)    -- # 34.5736988
AND lon >= (- 125.013)   -- # -121.0392169
AND lon <= (- 117.460)  -- # -117.4701519
THEN 'sf' -- San Francisco Region
ELSE 'other'
END AS region
FROM
-- # Querying GFW AIS pipeline. Requires permissions.
-- # New GWF pipeline (world-fishing-827.gfw_research.pipe_v20201001) uses '_PARTITIONDATE' as partitioning column.
-- # Important for keeping query costs as cheap as possible.
-- # Old pipeline was world-fishing-827.gfw_research.pipe_v20190502. Switched over on 2020-01-08.
-- `world-fishing-827.gfw_research.pipe_v20201001` AS ais
`{bq_tbl_gfw}` AS ais
LEFT JOIN
`world-fishing-827.gfw_research.pipe_v20201001_segs` AS segs
ON
segs.seg_id = ais.seg_id
WHERE
--(_PARTITIONDATE) > DATE(new_ais_ts)
DATE(timestamp) > DATE(new_ais_ts)
--     AND NOT overlapping_and_short
-- # Bounding box for waters off CA coast.
AND lat >= (33.285)     -- # 33.2998838
AND lat <= (39.032)    -- # 34.5736988
AND lon <= (- 117.455)  -- # -117.4701519
AND lon >= (- 125.013)   -- # -121.0392169
;

-- # -- # Step 4: Insert everything from `temp_ais_data` table into partitioned, clustered table,
-- # -- # `whalesafe_v4.ais_data` table
INSERT INTO
`benioff-ocean-initiative.whalesafe_v4.ais_data`
SELECT
*
FROM
`temp_ais_data`;

-- # -- Step 6: Insert 'new_ais_ts', the new timestamp from `ais_data` BEFORE querying GFW
INSERT INTO `whalesafe_v4.whalesafe_timestamp_log`
SELECT
new_ais_ts AS newest_timestamp,
CURRENT_TIMESTAMP() AS date_accessed,
'ais_data' AS table_name,
'query_start' AS query_exec;

-- # -- Step 7: Insert 'new_ais_ts', the new timestamp from `ais_data` AFTER querying GFW
INSERT INTO `whalesafe_v4.whalesafe_timestamp_log`
SELECT
(
SELECT
MAX(timestamp)
FROM
`whalesafe_v4.ais_data`
WHERE
DATE(timestamp) > DATE_SUB(DATE(new_ais_ts), INTERVAL 3 MONTH)
LIMIT 1) AS newest_timestamp,
CURRENT_TIMESTAMP() AS date_accessed,
'ais_data' AS table_name,
'query_end' AS query_exec;
