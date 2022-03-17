--# OLD: [ais_segments.sql](https://github.com/BenioffOceanInitiative/ws-sql/blob/2bb89c2c96cf199b9e93d63fd54742f020c2c5a0/sql_v4/ais_segments.sql)

# create table if needed
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe_v4.rgn_segs` (
  timestamp TIMESTAMP,
  date DATE,
  mmsi INT64,
  num INT64,
  timestamp_beg TIMESTAMP,
  timestamp_end TIMESTAMP,
  speed_knots NUMERIC,
  implied_speed_knots NUMERIC,
  calculated_knots NUMERIC,
  distance_km NUMERIC,
  distance_nm NUMERIC,
  segment_time_minutes FLOAT64,
  lon FLOAT64,
  lat FLOAT64,
  source STRING,
  rgn STRING,
  seg_id STRING,
  good_seg BOOL,
  overlapping_and_short BOOL,
  point GEOGRAPHY,
  linestring GEOGRAPHY
PARTITION BY DATE(timestamp) 
CLUSTER BY mmsi, region, point, linestring
OPTIONS (
  description              = "partitioned by day, clustered by (mmsi, point, linestring)", 
  require_partition_filter = TRUE);

--# delete for given region and dates
DELETE FROM `benioff-ocean-initiative.whalesafe_v4.rgn_segs`
  WHERE
    DATE(timestamp) >= DATE('2022-02-22') AND
    DATE(timestamp) <= DATE('2022-03-09') AND
    rgn = 'CAN-GoStLawrence';

--# construct vessel segments from benioff-ocean-initiative.whalesafe_v4.rgn_pts
SELECT
  timestamp,
  DATE(timestamp) AS date,
  mmsi,
  num,
  SAFE_CAST (t1 AS TIMESTAMP) AS timestamp_beg,
  SAFE_CAST (t2 AS TIMESTAMP) AS timestamp_end,
  ROUND(speed_knots, 4) AS speed_knots,
  ROUND(implied_speed_knots, 4) AS implied_speed_knots,
  -- # If the time elapsed between points is greater than 0 milliseconds, then calculate the speed in knots, otherwise return 0 knots
  ROUND(SAFE_CAST (
    CASE WHEN (TIMESTAMP_DIFF ((t2),(t1),MILLISECOND)) > 0
      THEN
      ROUND(((SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC) / (TIMESTAMP_DIFF ((t2), (t1), MILLISECOND) / 3600000)) * 0.539957), 4)
      ELSE
      NULL
    END AS numeric),4) 
    AS calculated_knots,
  -- # Get distance in kilometers
  ROUND(SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC), 5) 
    AS distance_km,
  -- # Get distance in nautical miles
  ROUND(SAFE_CAST (st_distance(geom2, geom) * 0.000539957 AS NUMERIC), 5) 
    AS distance_nm,
  -- # Get time elapsed between points in minutes
  ROUND((TIMESTAMP_DIFF ((t2), (t1), SECOND) / 60), 2) 
    AS segment_time_minutes,
  lon,
  lat,
  --# AIS message source
  source,
  rgn,
  seg_id,
  good_seg,
  overlapping_and_short,
  -- # Point Geography
  geom AS point,
  -- geom AS point_beg,
  -- geom2 AS point_end,
  -- # Linestring Geography
  ST_MAKELINE (geom, geom2) AS linestring
FROM (
  SELECT
    * EXCEPT (geom2, geom, t1, t2),
    row_number() OVER w1 
      AS num,                    -- # assign row number for each segment using WINDOW FUNCTION BELOW
    ST_GeogPoint (lon, lat) AS geom,                -- # Create point 1 for ST_MAKELINE function above.
    LEAD(ST_GeogPoint (lon, lat)) OVER w1 
      AS geom2, -- # Create point 2 for ST_MAKELINE function above.
    LEAD( (timestamp), 0) OVER w1 
      AS t1,            -- # t1 for point 1
    LEAD( (timestamp), 1) OVER w1 
      AS t2,
    ROUND((ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) / 1000), 2) 
      AS distance_km,
    ROUND(SAFE_CAST((ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) * 0.000539957) AS NUMERIC), 2) 
      AS distance_nm,
    ROUND((TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w1),(LEAD( (timestamp), 0) OVER w1), SECOND) / 60),2) 
      AS min,
    ROUND( SAFE_CAST( SAFE_DIVIDE(
      (ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) * 0.000539957) ,
      (TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w1),(LEAD( (timestamp), 0) OVER w1), SECOND) / 3600)) AS NUMERIC), 2)
      AS calculated_knots
  FROM (
    SELECT *
    FROM (
      SELECT
        *,
        CASE
          WHEN 
            calc_knots     BETWEEN 0.001 AND 50 AND 
            lag_calc_knots BETWEEN 0.001 AND 50 THEN TRUE
          ELSE FALSE
        END AS good_point,
        CASE 
          WHEN ABS(implied_speed_knots - speed_knots) > 8 AND
                ABS(calc_knots - speed_knots) > 8 AND
                ABS(lag_calc_knots - speed_knots) > 8
          THEN TRUE
          ELSE FALSE
        END AS speed_diff_8,
      FROM (
        SELECT
          (mmsi),
          timestamp,
          DATE(timestamp) AS date,
          SAFE_CAST (LEAD((speed_knots), 1) OVER w AS NUMERIC) 
            AS speed_knots,
          SAFE_CAST (LEAD((implied_speed_knots), 1) OVER w AS NUMERIC) 
            AS implied_speed_knots,
          lon,
          lat,
          source,
          seg_id,
          good_seg,
          overlapping_and_short,
          region,
          ihs_data.gt,
          (ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) 
            AS dist_nm,
          SAFE_CAST(SAFE_DIVIDE(
            (ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) ,
            (TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w),(LEAD( (timestamp), 0) OVER w), SECOND) / 3600)) AS NUMERIC)
            AS calc_knots,
          SAFE_CAST(SAFE_DIVIDE(
            (ST_DISTANCE(LAG(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) ,
            (TIMESTAMP_DIFF((LEAD( (timestamp), 0) OVER w),(LAG( (timestamp), 1) OVER w), SECOND) / 3600)) AS NUMERIC)
            AS lag_calc_knots,
          ST_GeogPoint (lon, lat) AS geom, -- # Create point 1 for ST_MAKELINE function above.
          LEAD(ST_GeogPoint (lon, lat)) OVER w AS geom2, -- # Create point 2 for ST_MAKELINE function above.
          LEAD( (timestamp), 0) OVER w AS t1, -- # t1 for point 1
          LEAD( (timestamp), 1) OVER w AS t2, -- # t2 for point 2
          -- ST_MAKELINE(LEAD(ST_GeogPoint (lon, lat)) OVER w, (ST_GeogPoint (lon, lat)) ) AS line
        FROM
          `benioff-ocean-initiative.whalesafe_v4.rgn_pts`
        WHERE
          DATE(timestamp) >= DATE('2022-02-22') AND
          DATE(timestamp) <= DATE('2022-03-09') AND
          rgn = 'CAN-GoStLawrence' AND
          --AND good_seg IS TRUE
          speed_knots         > 0.001 AND 
          implied_speed_knots > 0.001 AND 
          overlapping_and_short IS FALSE
        WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)
      )
      WHERE 
        dist_nm > 0 AND dist_nm <= 120 AND geom2 IS NOT NULL
    )
    WHERE
      good_point IS TRUE AND 
      speed_diff_8 IS FALSE
  )
  WINDOW w1 AS (PARTITION BY mmsi, date ORDER BY timestamp)
)
WHERE geom2 IS NOT NULL;