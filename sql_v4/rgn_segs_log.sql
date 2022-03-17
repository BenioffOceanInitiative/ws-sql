--# Step 6: Make whalesafe_v3 timestamp log table if not already existing.
CREATE TABLE IF NOT EXISTS
  `whalesafe_v3.whalesafe_timestamp_log` (
  newest_timestamp TIMESTAMP,
  date_accessed TIMESTAMP,
  table_name STRING,
  query_exec STRING);

--# Step 7: Insert 'new_seg_ts', the new timestamp in `rgn_segs` from BEFORE querying rgn_pts
INSERT INTO `whalesafe_v3.whalesafe_timestamp_log`
SELECT
  new_seg_ts AS newest_timestamp,
  CURRENT_TIMESTAMP() AS date_accessed,
  'rgn_segs' AS table_name,
  'query_start' AS query_exec;

--# Step 8: Insert 'new_seg_ts', the new timestamp in `rgn_segs` from AFTER querying rgn_pts
INSERT INTO `whalesafe_v3.whalesafe_timestamp_log`
SELECT (
  SELECT MAX(timestamp_end)
  FROM `whalesafe_v3.rgn_segs`
  WHERE DATE(timestamp) > DATE_SUB(DATE(new_seg_ts), INTERVAL 3 MONTH)
  LIMIT 1) AS newest_timestamp,
  CURRENT_TIMESTAMP() AS date_accessed,
  'rgn_segs' AS table_name,
  'query_end' AS query_exec;
