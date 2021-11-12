---------------------------------------------------------------
-- User defined JS helper functions
---------------------------------------------------------------
CREATE TEMP FUNCTION begDAY() AS (DATE('2017-01-01'));
CREATE TEMP FUNCTION endDAY() AS (DATE('2017-05-12'));
CREATE TEMP FUNCTION priorDAY() AS (DATE_SUB(begDAY(), INTERVAL 1 DAY));

-- Define some utility functions to make thinks more readable
CREATE TEMP FUNCTION YYYYMMDD(d DATE) AS (
  -- Format a date as YYYYMMDD
  -- e.g. DATE('2018-01-01') => '20180101'
  FORMAT_DATE('%Y%m%d', d) );


--
-- Gets position messages for the target date
--
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
    AND lat <= 52.22242000000003;
-- INTERPOLATED RESULTS: 11,016 rows


--
-- Gets positions from yesterday
--
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
    AND lat <= 52.22242000000003;
-- INTERPOLATED RESULTS: 25,323 rows