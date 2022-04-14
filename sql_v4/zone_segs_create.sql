CREATE TABLE IF NOT EXISTS `whalesafe_v4.zone_segs` (
	rgn STRING,  -- OLD: region
	zone STRING, -- OLD: vsr_region; DROP: vsr_category
	timestamp TIMESTAMP,
	mmsi INT64,
	num INT64, -- ?
	timestamp_beg TIMESTAMP,
	timestamp_end TIMESTAMP,
	final_speed_knots FLOAT64,
	speed_knots FLOAT64,
	implied_speed_knots FLOAT64,
	calculated_knots FLOAT64,
	distance_km FLOAT64,
	distance_nm FLOAT64,
	segment_time_minutes FLOAT64,
	lon FLOAT64,
	lat FLOAT64,
	source STRING,
	seg_id STRING,
	good_seg BOOL,
	overlapping_and_short BOOL,
	-- gt FLOAT64, -- TODO: gross tonnage
	point GEOGRAPHY,
	-- linestring GEOGRAPHY,  -- of non-intersected segment
	touches_shore BOOL,
	speedbin_num INT64,
	speedbin_str STRING,
	speedbin_implied_num INT64,
	speedbin_implied_str STRING,
	speedbin_calculated_num INT64,
	speedbin_calculated_str STRING,
	speedbin_final_num INT64,
	speedbin_final_str STRING,
	length_m FLOAT64,
	geog GEOGRAPHY
)
PARTITION BY DATE(timestamp)
CLUSTER BY
	rgn, zone, mmsi, geog
OPTIONS (
	description              = "partitioned by day, clustered by (rgn, zone, mmsi, geog)", 
	require_partition_filter = TRUE);