-- !preview conn=conn

CREATE TEMP FUNCTION toDAY() AS (DATE('{ date }'));

  CREATE TEMP FUNCTION yesterDAY() AS (DATE_SUB(toDAY(), INTERVAL 1 DAY));

  CREATE TEMP FUNCTION tomorrow() AS (DATE_ADD(toDAY(), INTERVAL 1 DAY));

  # Define some utility functions to make thinks more readable
  CREATE TEMP FUNCTION YYYYMMDD(d DATE) AS (
    # Format a date as YYYYMMDD
    # e.g. DATE('2018-01-01') => '20180101'
    FORMAT_DATE('%Y%m%d',
      d) );

  CREATE TEMP FUNCTION distance_m(lat1 FLOAT64,
    lon1 FLOAT64,
    lat2 FLOAT64,
    lon2 FLOAT64) AS (
    # Return the distance between two lat/lon locations in meters
    # if any of the parameters are null, returns null
    # if the distance is less than .0001 degrees, returns 0
    IF ( (ABS(lat2 - lat1) < .0001
        AND ABS(lon2- lon1) < .0001 ), 0.0, ACOS( COS(0.01745329251 * (90 - lat1)) * COS(0.01745329251 * (90 - lat2)) + SIN(0.01745329251 * (90 - lat1)) * SIN(0.01745329251 * (90 - lat2)) * COS(0.01745329251 * (lon2 - lon1)) ) * 6371000 ) );

    CREATE TEMP FUNCTION hours_diff_ABS(timestamp1 TIMESTAMP,
    timestamp2 TIMESTAMP) AS (
    #
    # Return the absolute value of the diff between the two timestamps in hours with microsecond precision
    # If either parameter is null, return null
    #
    ABS(TIMESTAMP_DIFF(timestamp1,
        timestamp2,
        microsecond) / 3600000000.0) );

---------------------------------------------------------------
-- Query
---------------------------------------------------------------

WITH

  #
  # Gets position messages for the target date
  #
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
      distance_from_port_m,
      regions
    FROM
      `{ messages_scored_table }*`
    WHERE _TABLE_SUFFIX = YYYYMMDD( toDAY() )
    AND (receiver is null # receiver is null is important,
                          # otherwise null spire positions are ignored
      -- OR receiver in ('rORBCOMM000', 'rORBCOMM999')
      OR receiver not in (
        SELECT
          receiver
        FROM
          `{ research_satellite_timing_table }`
        WHERE _partitiontime = timestamp(toDAY())
        AND ABS(dt) > 60
      ))
      # only valid positions
      AND abs(lat) < 90
      AND abs(lon) < 180
  )

  SELECT * FROM raw_message;
