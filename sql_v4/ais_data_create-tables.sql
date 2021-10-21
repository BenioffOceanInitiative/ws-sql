-- # -- Step 3: Create empty partitioned ad clustered table, for GFW AIS DATA if not already existing.
-- # -- `whalesafe_v4.ais_data` table
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe_v4.ais_data` (
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
)
PARTITION BY DATE(timestamp) CLUSTER BY
mmsi, region OPTIONS (description = "partitioned by day, clustered by (mmsi, region)", require_partition_filter = TRUE);

-- # -- Step 5: Make whalesafe_v4 timestamp log table if not already existing.
CREATE TABLE IF NOT EXISTS
`whalesafe_v4.whalesafe_timestamp_log` (
newest_timestamp TIMESTAMP,
date_accessed TIMESTAMP,
table_name STRING,
query_exec STRING
);
