-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

 Source: world-fishing-827:pipe_production_v20201001.messages_scored_
* Satellite Timing: world-fishing-827:gfw_research.pipe_v20201001_satellite_timing
* Sunrise: world-fishing-827:pipe_static.sunrise
* Norad to Receiver: world-fishing-827:pipe_static.norad_to_receiver_v20200127
* Satellite positions one second resolution: world-fishing-827:satellite_positions_v20190208.satellite_positions_one_second_resolution_

  ---------------------------------------------------------------
  -- research_daily.sql.j2
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
​
​
​
  ---------------------------------------------------------------
  -- User defined JS helper functions
  ---------------------------------------------------------------
​
  CREATE TEMP FUNCTION toDAY() AS (DATE('{{ date }}'));
​
  CREATE TEMP FUNCTION yesterDAY() AS (DATE_SUB(toDAY(), INTERVAL 1 DAY));
​
  CREATE TEMP FUNCTION tomorrow() AS (DATE_ADD(toDAY(), INTERVAL 1 DAY));
​
  # Define some utility functions to make thinks more readable
  CREATE TEMP FUNCTION YYYYMMDD(d DATE) AS (
    # Format a date as YYYYMMDD
    # e.g. DATE('2018-01-01') => '20180101'
    FORMAT_DATE('%Y%m%d',
      d) );
​
  CREATE TEMP FUNCTION distance_m(lat1 FLOAT64,
    lon1 FLOAT64,
    lat2 FLOAT64,
    lon2 FLOAT64) AS (
    # Return the distance between two lat/lon locations in meters
    # if any of the parameters are null, returns null
    # if the distance is less than .0001 degrees, returns 0
    IF ( (ABS(lat2 - lat1) < .0001
        AND ABS(lon2- lon1) < .0001 ), 0.0, ACOS( COS(0.01745329251 * (90 - lat1)) * COS(0.01745329251 * (90 - lat2)) + SIN(0.01745329251 * (90 - lat1)) * SIN(0.01745329251 * (90 - lat2)) * COS(0.01745329251 * (lon2 - lon1)) ) * 6371000 ) );
​
    CREATE TEMP FUNCTION hours_diff_ABS(timestamp1 TIMESTAMP,
    timestamp2 TIMESTAMP) AS (
    #
    # Return the absolute value of the diff between the two timestamps in hours with microsecond precision
    # If either parameter is null, return null
    #
    ABS(TIMESTAMP_DIFF(timestamp1,
        timestamp2,
        microsecond) / 3600000000.0) );
​
---------------------------------------------------------------
-- Query
---------------------------------------------------------------
​
WITH
​
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
      `{{ messages_scored_table }}*`
    WHERE _TABLE_SUFFIX = YYYYMMDD( toDAY() )
    AND (receiver is null # receiver is null is important,
                          # otherwise null spire positions are ignored
      OR receiver in ('rORBCOMM000', 'rORBCOMM999')
      OR receiver not in (
        SELECT
          receiver
        FROM
          `{{ research_satellite_timing_table }}`
        WHERE _partitiontime = timestamp(toDAY())
        AND ABS(dt) > 60
      ))
      # only valid positions
      AND abs(lat) < 90
      AND abs(lon) < 180
  ),
​
  #
  # Gets positions from yesterday
  #
  positions_yesterday AS (
    SELECT
      msgid,
      timestamp,
      seg_id,
      lat,
      lon
    FROM
      `{{ messages_scored_table }}*`
      WHERE _TABLE_SUFFIX = YYYYMMDD( yesterDAY() )
      AND (receiver is null # receiver is null is important,
                            # otherwise null spire positions are ignored
        OR receiver in ('rORBCOMM000','rORBCOMM999')
        OR receiver not in (
          SELECT
            receiver
      FROM
        `{{ research_satellite_timing_table }}`
      WHERE _partitiontime = timestamp(yesterDAY())
      AND ABS(dt) > 60
    ))
    AND lat < 90
    AND lat > -90
    AND lon < 180
  ),
​
  #
  # Loads sunrise lookup table
  #
  sunrise_lookup AS (
    SELECT
      lat,
      day,
      AVG(sunrise) AS sunrise
    FROM
      `{{ static_sunrise_dataset_and_table }}`
    GROUP BY
      lat,
      day
  ),
​
  # Eliminates dulicate messages with the same msg_id, but only if lat,lon is nearly identical
  # NB: the window function is ordered by timestamp lat and lon to make the ordering deterministic
  # so if there are different lat/lon in the same second with the same msg_id, we will always get the
  # the same record
​
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
​
​
  # Combines all positions and timestamps from yesterday and today
  # no need to dedup yesterday because we will throw them away later
  # NB: we drop a bunch of fields that we don't need here so that we don't have to also
  # have those fields read from yesterday. We will add them back in at the end
  all_positions AS (
    SELECT
      msgid,
      timestamp,
      seg_id,
      lat,
      lon
    FROM
      dedup_message UNION ALL
    SELECT
      *
    FROM
      positions_yesterday
  ),
​
  #
  # Thin messages to one per minute per seg_id
  #
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
​
  #
  # Gets previous position and timestamp
  # NB: This is why we need data from yesterday.
  prev_position AS (
    SELECT
      *,
      LAG(timestamp, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_timestamp,
      LAG(lat, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_lat,
      LAG(lon, 1) OVER (PARTITION BY seg_id ORDER BY timestamp) prev_lon
    FROM
      thinned_positions
  ),
​
  #
  # Computes distance and time to previous position, and derive implied speed
  # We no longer need yesterday, so filter those out
  #
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
    WHERE DATE(timestamp) = toDAY()
  ),
​
  hours_and_distance AS (
    SELECT
      *
    FROM
      prev_time_dist
  ),
​
  #
  # Computes average distance and implied speed in knots
  #
  implied_speed AS (
    SELECT
      *,
      SAFE_DIVIDE(meters_to_prev,
        hours ) * 0.00053995 implied_speed_knots
    FROM
      hours_and_distance
  ),
​
  #
  # Computes day of year and local time
  #
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
​
  #
  # Determines local sunrise and sunset for each position message
  #
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
  ),
​
  #
  # Adds a boolean to indicate whether is it night for each position
  #
  local_night AS (
    SELECT
      *,
      IF(local_time > sunset
        OR local_time < sunrise, TRUE, FALSE) night
    FROM
      local_sunrise
  ),
​
  #
  # Now we need to add back in the other message fields that we left behind when we combined positions
  # from yesterday, and add in receiver from the raw messages
  #
  combined_message AS (
    SELECT
      dedup_message.*,
      local_night.meters_to_prev,
      local_night.implied_speed_knots,
      local_night.hours,
      local_night.night
    FROM
      local_night
    LEFT JOIN
      dedup_message USING (msgid,
        lat,
        lon)
  ),
​
  #
  # Calculates the distance to the satellite that received the message
  distance_from_satellite as (
    SELECT
      a.timestamp timestamp,
      a.lat lat,
      a.lon lon,
      a.receiver receiver,
      * except(lat,lon, timestamp, norad_id, receiver, altitude),
      distance_m(a.lat, a.lon, c.lat, c.lon)/1000 distance_from_sat_km,
      altitude/1000 as sat_altitude_km,
      c.lat as sat_lat,
      c.lon as sat_lon
    FROM
      combined_message a
    LEFT JOIN (
      SELECT
        norad_id,
        receiver
      FROM
        `{{ static_norad_to_receiver }}`) b
    ON a.receiver = b.receiver
    LEFT JOIN (
      SELECT
        avg(lat) lat,
        avg(lon) lon,
        avg(altitude) altitude,
        timestamp,
        norad_id
      FROM
        `{{ satellite_positions_one_second_resolution }}*`
      WHERE _table_suffix = YYYYMMDD( toDAY() )
      GROUP BY
        norad_id, timestamp) c
    ON a.timestamp = c.timestamp
    AND b.norad_id = c.norad_id
)
​
#
# Actually run the entire pipeline
#
SELECT
  msgid, ssvid, seg_id, timestamp, lat, lon, speed_knots,heading, course, meters_to_prev, implied_speed_knots,
  hours, night,  nnet_score,  logistic_score,type,
  source, receiver_type,receiver, distance_from_sat_km, sat_altitude_km,  sat_lat,  sat_lon,
  elevation_m,  distance_from_shore_m,  distance_from_port_m, regions
FROM
  distance_from_satellite
