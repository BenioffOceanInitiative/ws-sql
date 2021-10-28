-- !preview conn=con

SELECT
  msgid,
  timestamp,
  seg_id,
  lat,
  lon
FROM
  -- `world-fishing-827.pipe_production_v20201001.messages_scored_*`
  `world-fishing-827.pipe_production_v20201001.messages_scored_20210518`
  --WHERE _TABLE_SUFFIX = '20210518'
LIMIT 10;
