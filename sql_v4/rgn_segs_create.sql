--# OLD: [ais_segments.sql](https://github.com/BenioffOceanInitiative/ws-sql/blob/2bb89c2c96cf199b9e93d63fd54742f020c2c5a0/sql_v4/ais_segments.sql)

--# create table if needed
CREATE TABLE IF NOT EXISTS `{tbl_rgn_segs}` (
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
  linestring GEOGRAPHY)
PARTITION BY DATE(timestamp) 
CLUSTER BY mmsi, rgn, point, linestring
OPTIONS (
  description              = "partitioned by day, clustered by (mmsi, point, linestring)", 
  require_partition_filter = TRUE);
