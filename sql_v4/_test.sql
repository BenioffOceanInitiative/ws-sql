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
