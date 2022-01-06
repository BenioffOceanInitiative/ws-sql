-- -- # Benioff Ocean Initiative: 2021-01-28
-- -- #                    __       __
-- -- #                     '.'--.--'.-'
-- -- #       .,_------.___,   \' r'
-- -- #       ', '-._a      '-' .'
-- -- #        '.    '-'Y \._  /
-- -- #          '--;____'--.'-,
-- -- #Sean Goral/..'Ben Best''\ Callie Steffen ''' Morgan Visalli # --

-- # Step 1: Make temporary table `temp_ais_data` for incoming GFW AIS data
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
  -- region STRING
  zone STRING);

-- # Step 2: Insert GFW AIS data (with a timestamp > than the max tiestamp in the existing `ais_data` table) into the `temp_ais_data` table
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
  '{zone}' AS zone
FROM (
-- # Querying GFW AIS pipeline. Requires permissions.
-- # New GWF pipeline (world-fishing-827.gfw_research.pipe_v20201001) uses '_PARTITIONDATE' as partitioning column.
-- # Important for keeping query costs as cheap as possible.
-- # Old pipeline was world-fishing-827.gfw_research.pipe_v20190502. Switched over on 2020-01-08.
-- `world-fishing-827.gfw_research.pipe_v20201001` AS ais
  SELECT *
  FROM `{tbl_gfw_pts}`
  WHERE
    DATE(timestamp) > '{date_beg}' AND
    ST_COVERS(
      (SELECT geog
        FROM `{tbl_zones}`
        WHERE zone = '{zone}'),
      geog)) AS ais
LEFT JOIN `{tbl_gfw_segs}` AS segs
  ON
  segs.seg_id = ais.seg_id
WHERE
  DATE(timestamp) > '{date_beg}';

-- # -- Step 3: Create empty partitioned and clustered table, for GFW AIS DATA if not already existing.
-- # -- `whalesafe_v4.ais_data` table
CREATE TABLE IF NOT EXISTS `{tbl_ais_data}` (
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
  --region STRING
  zone STRING)
PARTITION BY DATE(timestamp)
CLUSTER BY mmsi, zone
OPTIONS (
  description              = "partitioned by day, clustered by (mmsi, zone)",
  require_partition_filter = TRUE);

-- # -- Step 5: Make whalesafe_v4 timestamp log table if not already existing.
CREATE TABLE IF NOT EXISTS
`{tbl_log}` (
  newest_timestamp TIMESTAMP,
  date_accessed TIMESTAMP,
  table_name STRING,
  query_exec STRING);

-- # Step 4: Insert everything from `temp_ais_data` table into partitioned, clustered table,
-- # `whalesafe_v4.ais_data` table
INSERT INTO `{tbl_ais_data}`
  SELECT
  *
  FROM
  `temp_ais_data`;

-- # Step 6: Insert 'date_beg', the new timestamp from `ais_data` BEFORE querying GFW
INSERT INTO `{tbl_log}`
SELECT
  '{date_beg}' AS newest_timestamp,
  CURRENT_TIMESTAMP() AS date_accessed,
  'ais_data' AS table_name,
  'query_start' AS query_exec;

-- # Step 7: Insert 'date_beg', the new timestamp from `ais_data` AFTER querying GFW
INSERT INTO `{tbl_log}`
SELECT(
  SELECT MAX(timestamp)
  FROM `{tbl_ais_data}`
  WHERE DATE(timestamp) > DATE_SUB('{date_beg}', INTERVAL 3 MONTH) -- ?! INTERVAL 3 MONTH
  LIMIT 1) AS newest_timestamp,
CURRENT_TIMESTAMP() AS date_accessed,
'ais_data' AS table_name,
'query_end' AS query_exec;
