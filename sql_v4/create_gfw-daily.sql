---------------------------------------------------------------
-- Create container for Global Fishing Watch daily data for final insert
-- see load_regions.Rmd for creation of below with DBI::sqlCreateTable()
---------------------------------------------------------------
DROP TABLE IF EXISTS `{bq_tbl_gfw}`;
CREATE TABLE IF NOT EXISTS `{bq_tbl_gfw}` (
  msgid STRING,
  ssvid STRING,
  seg_id STRING,
  `timestamp` TIMESTAMP,
  lat FLOAT64,
  lon FLOAT64,
  speed_knots FLOAT64,
  heading FLOAT64,
  course FLOAT64,
  meters_to_prev FLOAT64,
  implied_speed_knots FLOAT64,
  hours FLOAT64,
  night BOOL,
  nnet_score FLOAT64,
  logistic_score FLOAT64,
  type STRING,
  source STRING,
  receiver_type STRING,
  receiver STRING,
  distance_from_sat_km FLOAT64,
  sat_altitude_km FLOAT64,
  sat_lat FLOAT64,
  sat_lon FLOAT64,
  elevation_m FLOAT64,
  distance_from_shore_m FLOAT64,
  distance_from_port_m FLOAT64,
  -- regions ARRAY<STRING>,
  ws_region STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY ssvid, ws_region
OPTIONS (
    description              = "partitioned by day, clustered by (ssvid, region)",
    require_partition_filter = TRUE);

-- add geography points
ALTER TABLE `{bq_tbl_gfw}` ADD COLUMN IF NOT EXISTS geog GEOGRAPHY;

-- set description
ALTER TABLE `{bq_tbl_gfw}`
  ALTER COLUMN `msgid` SET OPTIONS (description = "GFW: unique message id. every row in the the table has a unique msg_id"),
  ALTER COLUMN `ssvid` SET OPTIONS (description = "GFW: source specific vessel id. This is the transponder id, and for AIS this is the MMSI"),
  ALTER COLUMN `seg_id` SET OPTIONS (description = "GFW: unique segment id. This table has one row per segment id per day"),
  ALTER COLUMN `timestamp` SET OPTIONS (description = "GFW: message timestamp"),
  ALTER COLUMN `lat` SET OPTIONS (description = "GFW: latitude"),
  ALTER COLUMN `lon` SET OPTIONS (description = "GFW: longitude"),
  ALTER COLUMN `speed_knots` SET OPTIONS (description = "GFW: speed in knots"),
  ALTER COLUMN `heading` SET OPTIONS (description = "GFW: vessel heading in degrees"),
  ALTER COLUMN `course` SET OPTIONS (description = "GFW: course over ground in degrees, where north is 0 degrees"),
  ALTER COLUMN `meters_to_prev` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `implied_speed_knots` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `hours` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `night` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `nnet_score` SET OPTIONS (description = "GFW: The score assigned by the neural network."),
  ALTER COLUMN `logistic_score` SET OPTIONS (description = "GFW: The score assigned by the logistic regression modeld."),
  ALTER COLUMN `type` SET OPTIONS (description = "GFW: Message type. For AIS this is the message id (eg. 1, 5, 18, 24 etc)"),
  ALTER COLUMN `source` SET OPTIONS (description = "GFW: Source of this messages. Generally this is the provider"),
  ALTER COLUMN `receiver_type` SET OPTIONS (description = "GFW: terrestrial or satellite obtained from the raw ais messages."),
  ALTER COLUMN `receiver` SET OPTIONS (description = "GFW: The receiver obtained from the source ais messages."),
  ALTER COLUMN `distance_from_sat_km` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `sat_altitude_km` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `sat_lat` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `sat_lon` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `elevation_m` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `distance_from_shore_m` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `distance_from_port_m` SET OPTIONS (description = "GFW: "),
  ALTER COLUMN `ws_region` SET OPTIONS (description = "WS: WhaleSafe regions. See https://github.com/BenioffOceanInitiative/ws-sql/issues/7"),
  ALTER COLUMN `geog` SET OPTIONS (description = "WS: geography of POINT(lon, lat)");
-- TODO: GFW vessel_id	STRING Unique vessel id. Each vessel_id can be associated with many seg_ids, and only one ssvid
