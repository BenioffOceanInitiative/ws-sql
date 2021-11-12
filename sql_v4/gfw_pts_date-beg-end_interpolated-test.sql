-- Get AIS data points from Global Fishing Watch (GFW) using only Spire data for which Benioff is licensed.
-- Run by day and rgn.
--
-- Canabalized from original query provided by Tyler Clavelle at GFW:
---------------------------------------------------------------
-- research_daily.sql SPIRE only R glue::glue() version
--   of GFW's Jinja research_daily.sql.j2
--
-- This query takes one day of data from the pipeline
-- `messages_scored` and adds to it:
-- meters_to_prev: meters to previous position in the segment
-- hours: hours to the previous position in the segment
-- implied_speed_knots: implied speed between previous position
-- and the given one in the segment
-- speed_knots: speed field renamed
-- night: boolean -- true if it is night
-- distance_from_sat_km: if available, altitude of sat when
-- when position was recorded
-- sat_altitude_km: if available, distance to the satellite,
-- not including vertical
-- sat_lat: latitude of satellite
-- sat_lon: longitude of satellite
--
-- Also note that satellite recievers that are off by more
-- than 60 seconds on a given day are eliminated.
-- Also, all segments are thined to one position every minute.
---------------------------------------------------------------
-- 
-- Test spatially per rgn and date after execution with:
--   https://bigquerygeoviz.appspot.com/
-- 
-- SELECT geog AS rgn_geog
--   FROM `benioff-ocean-initiative.whalesafe_v4.rgns`
--   WHERE rgn = 'USA-GoMex';
-- 
-- SELECT geog AS pt_geog 
--   FROM `benioff-ocean-initiative.whalesafe_v4.gfw_pts`
--   WHERE 
--   	DATE(timestamp) >= DATE('2017-01-01') AND
--   	DATE(timestamp) <= DATE('2017-02-01') AND
-- 	rgn = 'USA-GoMex';
-- 
-- SELECT geog 
--   FROM `benioff-ocean-initiative.whalesafe_v4.gfw_pts` AS pts
--   WHERE
--   	DATE(timestamp) >= DATE('2017-01-01') AND
--   	DATE(timestamp) <= DATE('2017-02-01') AND
--     NOT ST_COVERS(
--       (SELECT geog 
--         FROM `benioff-ocean-initiative.whalesafe_v4.rgns`
--         WHERE rgn = 'USA-GoMex'), 
--       pts.geog)
-- 
---------------------------------------------------------------

-- ---------------------------------------------------------------
-- -- Create container for Global Fishing Watch daily data for final insert
-- -- see load_regions.Rmd for creation of below with DBI::sqlCreateTable()
-- ---------------------------------------------------------------
-- -- DROP TABLE IF EXISTS `benioff-ocean-initiative.whalesafe_v4.gfw_pts`;
-- CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe_v4.gfw_pts` (
--   msgid STRING,
--   ssvid STRING,
--   seg_id STRING,
--   `timestamp` TIMESTAMP,
--   lat FLOAT64,
--   lon FLOAT64,
--   speed_knots FLOAT64,
--   heading FLOAT64,
--   course FLOAT64,
--   meters_to_prev FLOAT64,
--   implied_speed_knots FLOAT64,
--   hours FLOAT64,
--   night BOOL,
--   nnet_score FLOAT64,
--   logistic_score FLOAT64,
--   type STRING,
--   source STRING,
--   receiver_type STRING,
--   receiver STRING,
--   distance_from_sat_km FLOAT64,
--   sat_altitude_km FLOAT64,
--   sat_lat FLOAT64,
--   sat_lon FLOAT64,
--   elevation_m FLOAT64,
--   distance_from_shore_m FLOAT64,
--   distance_from_port_m FLOAT64,
--   -- regions ARRAY<STRING>,
--   rgn STRING
-- )
-- PARTITION BY DATE(timestamp)
-- CLUSTER BY ssvid, rgn
-- OPTIONS (
--     description              = "partitioned by day, clustered by (ssvid, rgn)",
--     require_partition_filter = TRUE);

-- -- add geography points
-- ALTER TABLE `benioff-ocean-initiative.whalesafe_v4.gfw_pts` ADD COLUMN IF NOT EXISTS geog GEOGRAPHY;

-- -- set description
-- ALTER TABLE `benioff-ocean-initiative.whalesafe_v4.gfw_pts`
--   ALTER COLUMN `msgid` SET OPTIONS (description = "GFW: unique message id. every row in the the table has a unique msg_id"),
--   ALTER COLUMN `ssvid` SET OPTIONS (description = "GFW: source specific vessel id. This is the transponder id, and for AIS this is the MMSI"),
--   ALTER COLUMN `seg_id` SET OPTIONS (description = "GFW: unique segment id. This table has one row per segment id per day"),
--   ALTER COLUMN `timestamp` SET OPTIONS (description = "GFW: message timestamp"),
--   ALTER COLUMN `lat` SET OPTIONS (description = "GFW: latitude"),
--   ALTER COLUMN `lon` SET OPTIONS (description = "GFW: longitude"),
--   ALTER COLUMN `speed_knots` SET OPTIONS (description = "GFW: speed in knots"),
--   ALTER COLUMN `heading` SET OPTIONS (description = "GFW: vessel heading in degrees"),
--   ALTER COLUMN `course` SET OPTIONS (description = "GFW: course over ground in degrees, where north is 0 degrees"),
--   ALTER COLUMN `meters_to_prev` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `implied_speed_knots` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `hours` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `night` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `nnet_score` SET OPTIONS (description = "GFW: The score assigned by the neural network."),
--   ALTER COLUMN `logistic_score` SET OPTIONS (description = "GFW: The score assigned by the logistic regression modeld."),
--   ALTER COLUMN `type` SET OPTIONS (description = "GFW: Message type. For AIS this is the message id (eg. 1, 5, 18, 24 etc)"),
--   ALTER COLUMN `source` SET OPTIONS (description = "GFW: Source of this messages. Generally this is the provider"),
--   ALTER COLUMN `receiver_type` SET OPTIONS (description = "GFW: terrestrial or satellite obtained from the raw ais messages."),
--   ALTER COLUMN `receiver` SET OPTIONS (description = "GFW: The receiver obtained from the source ais messages."),
--   ALTER COLUMN `distance_from_sat_km` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `sat_altitude_km` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `sat_lat` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `sat_lon` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `elevation_m` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `distance_from_shore_m` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `distance_from_port_m` SET OPTIONS (description = "GFW: "),
--   ALTER COLUMN `rgn` SET OPTIONS (description = "WS: WhaleSafe regions. See https://github.com/BenioffOceanInitiative/ws-sql/issues/7"),
--   ALTER COLUMN `geog` SET OPTIONS (description = "WS: geography of POINT(lon, lat)");
-- -- TODO: GFW vessel_id	STRING Unique vessel id. Each vessel_id can be associated with many seg_ids, and only one ssvid

---------------------------------------------------------------
-- User defined JS helper functions
---------------------------------------------------------------
CREATE TEMP FUNCTION begDAY() AS (DATE('2017-01-01'));
CREATE TEMP FUNCTION endDAY() AS (DATE('2017-05-12'));
CREATE TEMP FUNCTION priorDAY() AS (DATE_SUB(begDAY(), INTERVAL 1 DAY));
-- SELECT (DATE_SUB(DATE('2021-01-01'), INTERVAL 1 DAY)) AS priorDAY ;
-- SELECT priorDAY(); -- 2016-12-31

-- Define some utility functions to make thinks more readable
CREATE TEMP FUNCTION YYYYMMDD(d DATE) AS (
  -- Format a date as YYYYMMDD
  -- e.g. DATE('2018-01-01') => '20180101'
  FORMAT_DATE('%Y%m%d', d) );
-- SELECT YYYYMMDD(priorDAY()); -- 20161231

CREATE TEMP FUNCTION distance_m(lat1 FLOAT64,
  lon1 FLOAT64,
  lat2 FLOAT64,
  lon2 FLOAT64) AS (
  -- Return the distance between two lat/lon locations in meters
  -- if any of the parameters are null, returns null
  -- if the distance is less than .0001 degrees, returns 0
  IF ( (ABS(lat2 - lat1) < .0001
      AND ABS(lon2- lon1) < .0001 ), 0.0, ACOS( COS(0.01745329251 * (90 - lat1)) * COS(0.01745329251 * (90 - lat2)) + SIN(0.01745329251 * (90 - lat1)) * SIN(0.01745329251 * (90 - lat2)) * COS(0.01745329251 * (lon2 - lon1)) ) * 6371000 ) );

  CREATE TEMP FUNCTION hours_diff_ABS(timestamp1 TIMESTAMP,
  timestamp2 TIMESTAMP) AS (
  --
  -- Return the absolute value of the diff between the two timestamps in hours with microsecond precision
  -- If either parameter is null, return null
  --
  ABS(TIMESTAMP_DIFF(timestamp1,
      timestamp2,
      microsecond) / 3600000000.0) );

-- ---------------------------------------------------------------
-- -- Query
-- ---------------------------------------------------------------
-- DELETE FROM `benioff-ocean-initiative.whalesafe_v4.gfw_pts`
--   WHERE
--     DATE(timestamp) >= DATE('2017-01-01') AND
--     DATE(timestamp) <= DATE('2017-05-12') AND
--     rgn = 'CAN-GoStLawrence';

-- INSERT INTO `benioff-ocean-initiative.whalesafe_v4.gfw_pts` (msgid, ssvid, seg_id, timestamp, lat, lon, speed_knots,heading, course, meters_to_prev, implied_speed_knots,
--   hours, night,  nnet_score,  logistic_score,type,
--   source, receiver_type,receiver, distance_from_sat_km, sat_altitude_km,  sat_lat,  sat_lon,
--   elevation_m,  distance_from_shore_m,  distance_from_port_m, -- regions,
--   rgn, geog)

WITH

  --
  -- Gets position messages for the target date
  --
  raw_message AS (
    SELECT
      msgid,
      seg_id,
      ssvid,
      timestamp,
      type,
      lat,
      lon,
      nnet_score,
      logistic_score,
      speed AS speed_knots,
      course,
      heading,
      source,
      receiver_type,
      receiver,
      elevation_m,
      distance_from_shore_m,
      distance_from_port_m
      -- regions
    FROM
      `world-fishing-827.pipe_production_v20201001.messages_scored_*`
    WHERE _TABLE_SUFFIX = YYYYMMDD( begDAY() )
    AND source = 'spire'
    AND (receiver is null -- receiver is null is important,
                          -- otherwise null spire positions are ignored
      -- OR receiver in ('rORBCOMM000', 'rORBCOMM999') -- exclude ORBCOM
      OR receiver not in (
        SELECT
          receiver
        FROM
          `world-fishing-827.gfw_research.pipe_v20201001_satellite_timing`
        WHERE 
            DATE(_partitiontime) >= DATE('2017-01-01') AND
            DATE(_partitiontime) <= DATE('2017-05-12') AND
            ABS(dt) > 60
      ))
      -- only valid positions
      AND abs(lat) < 90
      AND abs(lon) < 180
      -- specific to rgn
      AND lon >= -74.86481000000002
      AND lon <= -54.70344999999999
      AND lat >= 44.958499999999965
      AND lat <= 52.22242000000003
  ),

  --
  -- Gets positions from yesterday
  --
  positions_yesterday AS (
    SELECT
      msgid,
      timestamp,
      seg_id,
      lat,
      lon
    FROM
      `world-fishing-827.pipe_production_v20201001.messages_scored_*`
      WHERE 
      _TABLE_SUFFIX = YYYYMMDD( priorDAY() ) AND 
      -- SELECT COUNT(*) AS cnt FROM positions_yesterday
      -- cnt: 12926551
      (receiver is null -- receiver is null is important,
                            -- otherwise null spire positions are ignored
        -- OR receiver in ('rORBCOMM000','rORBCOMM999') -- exclude ORBCOM
      OR receiver not in (
        SELECT
          receiver
        FROM
          `world-fishing-827.gfw_research.pipe_v20201001_satellite_timing`
        WHERE _partitiontime = timestamp(priorDAY())
        AND ABS(dt) > 60))
      AND lat < 90
      AND lat > -90
      AND lon < 180
      -- specific to rgn
      AND lon >= -74.86481000000002
      AND lon <= -54.70344999999999
      AND lat >= 44.958499999999965
      AND lat <= 52.22242000000003)

  SELECT MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp, -- 2016-12-31 00:00:00 UTC 2016-12-31 23:59:59 UTC
    MIN(lon) AS min_lon, MAX(lon) AS max_lon, -- -74.78725 -54.70534
    MIN(lat) AS min_lat, MAX(lat) AS max_lat, --  44.95962  52.22193
    COUNT(*) AS cnt FROM positions_yesterday; -- 25323
  -- cnt: 25323

  --
  -- Loads sunrise lookup table
  --
  sunrise_lookup AS (
    SELECT
      lat,
      day,
      AVG(sunrise) AS sunrise
    FROM
      `world-fishing-827.pipe_static.sunrise`
    GROUP BY
      lat,
      day
  ),

  -- Eliminates duplicate messages with the same msg_id, but only if lat,lon is nearly identical
  -- NB: the window function is ordered by timestamp lat and lon to make the ordering deterministic
  -- so if there are different lat/lon in the same second with the same msg_id, we will always get the
  -- the same record

  dedup_message AS (
    SELECT
      * EXCEPT (row_number)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY seg_id, msgid, timestamp, CAST(lat*1000000 AS INT64),
          CAST(lon*1000000 AS INT64)
          ORDER BY
            timestamp,
            lat,
            lon) AS row_number
      FROM
        raw_message )
    WHERE row_number = 1
  ),
  -- SELECT COUNT(*) AS cnt FROM all_positions;
  -- cnt: 11016
  
  -- Combines all positions and timestamps from yesterday and today
  -- no need to dedup yesterday because we will throw them away later
  -- NB: we drop a bunch of fields that we don't need here so that we don't have to also
  -- have those fields read from yesterday. We will add them back in at the end
  all_positions AS (
    SELECT
      msgid,
      timestamp,
      seg_id,
      lat,
      lon
    FROM
      dedup_message
    UNION ALL
    SELECT
      *
    FROM
      positions_yesterday
  ),
  -- SELECT COUNT(*) AS cnt FROM all_positions;
  -- -- cnt: 36339

  --  
  -- Thin messages to one per minute per seg_id
  --
  thinned_positions AS (
    SELECT
      * EXCEPT (row_number)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY seg_id, minute ORDER BY timestamp, lat, lon) row_number
      FROM (
        SELECT
          *,
          CAST( EXTRACT(dayofyear
            FROM
              timestamp)*100000 + EXTRACT(hour
            FROM
              timestamp)*100 + EXTRACT(minute
            FROM
              timestamp) AS int64 ) AS minute
        FROM
          all_positions ) )
    WHERE row_number = 1
  ),  
  --  SELECT COUNT(*) AS cnt FROM thinned_positions;
  -- cnt: 32207

  --
  -- Gets previous position and timestamp
  -- NB: This is why we need data from yesterday.
  prev_position AS (
    SELECT
      *,
      LAG(timestamp, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_timestamp,
      LAG(lat, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_lat,
      LAG(lon, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_lon
    FROM
      thinned_positions
  ),
  -- SELECT COUNT(*) AS cnt FROM prev_position;
  -- cnt: 32207

  --
  -- Computes distance and time to previous position, and derive implied speed
  -- We no longer need yesterday, so filter those out
  --
  prev_time_dist AS (
    SELECT
      *,
      IFNULL (distance_m (prev_lat,
          prev_lon,
          lat,
          lon), 0) meters_to_prev,
      IFNULL (hours_diff_abs (timestamp,
          prev_timestamp), 0) hours
    FROM
      prev_position
    WHERE DATE(timestamp) = DATE_SUB(DATE(timestamp), INTERVAL 1 DAY)
  ),
  -- SELECT COUNT(*) AS cnt FROM prev_time_dist;
  -- cnt: 0

  hours_and_distance AS (
    SELECT
      *
    FROM
      prev_time_dist
  ),

  --
  -- Computes average distance and implied speed in knots
  --
  implied_speed AS (
    SELECT
      *,
      SAFE_DIVIDE(meters_to_prev,
        hours ) * 0.00053995 implied_speed_knots
    FROM
      hours_and_distance
  )
  -- SELECT COUNT(*) AS cnt FROM implied_speed;
  -- cnt: 0

  --
  -- Computes day of year and local time
  --
  day_and_time AS (
    SELECT
      *,
      EXTRACT(dayofyear
        FROM
          timestamp) day_of_year,
      EXTRACT(hour
        FROM
          timestamp) + EXTRACT(minute
        FROM
          timestamp)/60 + lon/360*24 local_hours,
      FLOOR(lat) lat_bin
    FROM
      implied_speed
  ),
  -- SELECT COUNT(*) AS cnt FROM day_and_time;
  -- cnt: 0

  --
  -- Determines local sunrise and sunset for each position message
  --
  local_sunrise AS (
    SELECT
      message.*,
      sunrise,
      24 - sunrise sunset,
      IF(local_hours < 0, local_hours + 24, IF(local_hours > 24, local_hours - 24, local_hours)) local_time
    FROM
      day_and_time AS message
    LEFT JOIN
      sunrise_lookup
    ON
      message.day_of_year = sunrise_lookup.day
      AND message.lat_bin = sunrise_lookup.lat
  )
  -- SELECT COUNT(*) AS cnt FROM local_sunrise;
  -- cnt: 